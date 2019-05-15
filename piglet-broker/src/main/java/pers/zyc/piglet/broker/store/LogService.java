package pers.zyc.piglet.broker.store;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import pers.zyc.piglet.ByteBufPool;
import pers.zyc.piglet.ChecksumException;
import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;
import pers.zyc.piglet.broker.store.file.AppendDir;
import pers.zyc.piglet.broker.store.file.AppendFile;
import pers.zyc.piglet.model.BrokerMessage;
import pers.zyc.tools.utils.BatchFetchQueue;
import pers.zyc.tools.utils.SystemMillis;
import pers.zyc.tools.utils.event.EventBus;
import pers.zyc.tools.utils.lifecycle.Service;
import pers.zyc.tools.utils.lifecycle.ThreadService;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangyancheng
 */
@Slf4j
public class LogService extends Service {

	private final StoreConfig storeConfig;

	private final EventBus<StoreEvent> storeEventBus;

	private final AppendDir logAppendDir;

	private final BatchFetchQueue<MsgAppendContext> appendQueue;
	private final BatchFetchQueue<MsgAppendContext> commitQueue;

	private final MessageLogAppender messageLogAppender = new MessageLogAppender();
	private final LogGroupCommitter logGroupCommitter = new LogGroupCommitter();

	private final IndexService indexService;

	private final ByteBufPool byteBufPool;

	private final FlushCondition flushCondition;

	LogService(StoreConfig storeConfig, EventBus<StoreEvent> storeEventBus, IndexService indexService) {
		this.storeConfig = storeConfig;
		this.storeEventBus = storeEventBus;
		this.indexService = indexService;
		this.logAppendDir = new AppendDir(storeConfig.getLogDir(), storeConfig.getLogFileLength());
		this.appendQueue = new BatchFetchQueue<>(storeConfig.getMsgAppendQueueSize());
		this.commitQueue = new BatchFetchQueue<>(storeConfig.getMsgCommitQueueSize());
		this.byteBufPool = new ByteBufPool();
		this.flushCondition = new FlushPolicy().createCondition();
	}

	@Override
	protected void doStart() throws Exception {
		logGroupCommitter.start();
		messageLogAppender.start();
	}

	@Override
	protected void doStop() throws Exception {
		messageLogAppender.stop();
		logGroupCommitter.stop();
	}

	private static BrokerMessage decode(ByteBuffer buffer) {
		BrokerMessage message = new BrokerMessage();
		message.decode(Unpooled.wrappedBuffer(buffer));
		buffer.position(buffer.position() + message.getSize());
		return message;
	}

	void recover(long recoverOffset) {
		log.info("Log recover from {}", recoverOffset);
		ByteBuffer blockBuffer = ByteBuffer.allocateDirect(StoreConfig.LOG_RECOVER_READ_BLOCK_SIZE);
		AppendFile[] appendFiles = logAppendDir.getAllFiles();
		files:for (int i = 0; i < appendFiles.length; i++) {
			AppendFile file = appendFiles[i];
			long fileEndOffset = file.getMaxOffset();
			if (fileEndOffset <= recoverOffset) {
				continue;
			}
			boolean fileReadEnd;
			do {
				blockBuffer.clear();
				file.read(recoverOffset, blockBuffer);
				fileReadEnd = recoverOffset + blockBuffer.remaining() == fileEndOffset;
				while (blockBuffer.remaining() > 4) {
					blockBuffer.mark();
					int nextMessageSize = blockBuffer.getInt();
					if (nextMessageSize == -1) {
						// 文件末尾，一定不是最后一个文件
						if (i == appendFiles.length - 1) {
							throw new SystemException(SystemCode.STORE_BAD_FILE.getCode(),
									"Log recover failed, expect more file after " + file.getId());
						}
						fileReadEnd = true;
						break;
					} else if (nextMessageSize == 0) {
						// 数据末尾，一定是最后一个文件
						if (i != appendFiles.length - 1) {
							throw new SystemException(SystemCode.STORE_BAD_FILE.getCode(),
									"Log recover failed, unexpected file after " + file.getId());
						}
						break files;
					} else {
						if (blockBuffer.remaining() < nextMessageSize - 4) {
							// 块数据不足
							break;
						}
						blockBuffer.reset();
						try {
							BrokerMessage message = decode(blockBuffer);
							reIndex(message);
							recoverOffset += nextMessageSize;
						} catch (ChecksumException e) {
							log.error("Log data checksum error, file id: " + file.getId());
							if (i != appendFiles.length - 1) {
								throw new SystemException(SystemCode.STORE_BAD_FILE.getCode(),
										"Log recover failed, decode checksum error");
							}
							break files;
						}
					}
				}
				if (fileReadEnd && i < appendFiles.length - 1) {
					//非最后一个文件，定位到下个文件开始继续恢复
					recoverOffset = fileEndOffset;
				}
			} while (!fileReadEnd);
		}
		((DirectBuffer) blockBuffer).cleaner().clean();

		log.info("Log recovered offset {}", recoverOffset);
		indexService.flush();
		logAppendDir.truncate(recoverOffset);
	}

	private void reIndex(BrokerMessage message) {
		IndexContext indexContext = new IndexContext(true);
		indexContext.setLogOffset(message.getLogOffset());
		indexContext.setIndexOffset(message.getIndexOffset());
		indexContext.setMessageSize(message.getSize());
		IndexQueue queue = indexService.getIndexQueue(message.getTopic(), message.getQueueNum());
		indexContext.setQueue(queue);
		indexService.writeIndex(indexContext);
	}

	void writeMessage(BrokerMessage message) throws Exception {
		TopicIndex topicIndex = indexService.getTopicIndex(message.getTopic());
		message.setQueueNum(topicIndex.randomQueue());
		ByteBuf byteBuf = byteBufPool.borrow();
		try {
			message.encode(byteBuf);

			int latches = 1;// 等待写盘

			boolean flush = flushCondition.reachedWhen(message.getSize());
			if (flush) {
				latches++;// 等待刷盘
			}

			CountDownLatch latch = new CountDownLatch(latches);

			MsgAppendContext ctx = new MsgAppendContext(message, byteBuf, flush, latch);

			if (!appendQueue.add(ctx, storeConfig.getAppendEnqueueTimeout(), TimeUnit.MILLISECONDS)) {
				throw new SystemException(SystemCode.STORE_SERVICE_BUSY.getCode(),
						"Append enqueue timeout, exceed " + storeConfig.getAppendEnqueueTimeout() + "ms");
			}

			boolean storeSuccess = latch.await(storeConfig.getStoreMsgTimeout(), TimeUnit.MILLISECONDS);
			if (!storeSuccess) {
				throw new SystemException(SystemCode.STORE_SERVICE_BUSY.getCode(),
						"Store message timeout, exceed " + storeConfig.getStoreMsgTimeout() + "ms");
			}
		} finally {
			byteBufPool.recycle(byteBuf);
		}
	}

	private class MessageLogAppender extends ThreadService {

		@Override
		public String getName() {
			return "Message Log Appender";
		}

		@Override
		protected Runnable getRunnable() {
			return new ServiceRunnable() {

				@Override
				protected void execute() throws InterruptedException {
					List<MsgAppendContext> contexts = appendQueue.fetch();
					for (MsgAppendContext appendContext : contexts) {
						try {
							appendMessage(appendContext);
						} catch (InterruptedException e) {
							throw e;
						} catch (Exception e) {
							storeEventBus.add(StoreEvent.create(e));
						}
					}
				}
			};
		}
	}

	private void appendMessage(MsgAppendContext ctx) throws Exception {
		// 在最后一个日志文件追加写入
		AppendFile logFile = logAppendDir.getLastFile();
		int writeRemain = logFile.remaining();
		if (writeRemain < ctx.message.getSize()) {
			ByteBuffer blankBuffer = ByteBuffer.allocate(writeRemain);
			blankBuffer.limit(writeRemain);
			if (writeRemain >= 4) {
				blankBuffer.putInt(-1);
				blankBuffer.rewind();
			}

			logFile.append(blankBuffer);
			logFile.flush();
			logFile = logAppendDir.createNewFile();
		}
		// 计算并写入日志偏移量
		long logOffset = logFile.getId() + logFile.getWritePosition();
		ctx.message.setLogOffset(logOffset);
		ctx.buf.setLong(BrokerMessage.LOG_OFFSET_WRITE_INDEX, logOffset);

		// 计算并写入索引偏移量
		IndexQueue queue = indexService.getIndexQueue(ctx.message.getTopic(), ctx.message.getQueueNum());
		long indexOffset = queue.getOffset();
		ctx.message.setIndexOffset(indexOffset);
		ctx.buf.setLong(BrokerMessage.INDEX_OFFSET_WRITE_INDEX, indexOffset);

		// 存储前写入时间
		long storeTime = SystemMillis.current();
		ctx.message.setStoreTime(storeTime);
		ctx.buf.setInt(BrokerMessage.STORE_TIME_WRITE_INDEX, (int) (storeTime - ctx.message.getClientSendTime()));

		logFile.append(ctx.buf.nioBuffer());

		ctx.latch.countDown();

		if (ctx.flush) {
			if (!commitQueue.add(ctx, storeConfig.getCommitEnqueueTimeout(), TimeUnit.MILLISECONDS)) {
				throw new SystemException(SystemCode.STORE_SERVICE_BUSY.getCode(),
						"Commit enqueue timeout, exceed " + storeConfig.getCommitEnqueueTimeout() + "ms");
			}
		}

		// 索引消息
		IndexContext indexContext = new IndexContext();
		indexContext.setQueue(queue);
		indexContext.setIndexOffset(indexOffset);
		indexContext.setLogOffset(logOffset);
		indexContext.setMessageSize(ctx.message.getSize());
		indexService.index(indexContext);
	}

	private class LogGroupCommitter extends ThreadService {

		@Override
		public String getName() {
			return "Log Group Committer";
		}

		@Override
		protected Runnable getRunnable() {
			return new ServiceRunnable() {

				@Override
				protected void execute() throws InterruptedException {
					List<MsgAppendContext> contexts = commitQueue.fetch();
					try {
						logAppendDir.getLastFile().flush();
						contexts.forEach(ctx -> ctx.latch.countDown());
					} catch (Exception e) {
						storeEventBus.add(StoreEvent.create(e));
					}
				}
			};
		}
	}
}
