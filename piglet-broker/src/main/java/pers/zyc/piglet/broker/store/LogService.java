package pers.zyc.piglet.broker.store;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import pers.zyc.piglet.ByteBufPool;
import pers.zyc.piglet.ChecksumException;
import pers.zyc.piglet.broker.store.file.AppendDir;
import pers.zyc.piglet.broker.store.file.AppendFile;
import pers.zyc.piglet.model.BrokerMessage;
import pers.zyc.tools.utils.BatchFetchQueue;
import pers.zyc.tools.utils.SystemMillis;
import pers.zyc.tools.utils.event.EventBus;
import pers.zyc.tools.utils.lifecycle.Service;
import pers.zyc.tools.utils.lifecycle.ThreadService;

import java.io.IOException;
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

	public LogService(StoreConfig storeConfig, EventBus<StoreEvent> storeEventBus, IndexService indexService) {
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

	void recover(long recoverOffset) throws IOException {
		log.info("Log recover from {}", recoverOffset);
		AppendFile[] appendFiles = logAppendDir.getAllFiles();
		files:for (AppendFile file : appendFiles) {
			if (file.getMaxOffset() <= recoverOffset) {
				continue;
			}
			long fileEndOffset = file.getId() + file.getFileLength();
			boolean fileReadEnd;
			file:do {
				ByteBuffer blockData = file.read(recoverOffset, 5 * 1024 * 1024);
				fileReadEnd = recoverOffset + blockData.remaining() == fileEndOffset;
				while (blockData.remaining() > 4) {
					blockData.mark();
					int nextMessageSize = blockData.getInt();
					if (nextMessageSize == -1) {
						// 文件末尾，一定不是最后一个文件，设置到下个文件开头继续读取
						recoverOffset = fileEndOffset;
						break file;
					} else if (nextMessageSize == 0) {
						// 数据末尾，一定是最后一个文件
						break file;
					} else {
						if (blockData.remaining() < nextMessageSize) {
							// 块数据不足错误
							break;
						}
						blockData.reset();
						try {
							BrokerMessage message = decode(blockData);
							reIndex(message);
							recoverOffset += nextMessageSize;
						} catch (ChecksumException e) {
							log.error("Log data checksum error, recover finish.");
							break files;
						}
					}
				}
			} while (!fileReadEnd);
		}

		log.info("Log recovered offset {}", recoverOffset);
		logAppendDir.truncate(recoverOffset);
	}

	private void reIndex(BrokerMessage message) {
		IndexContext indexContext = new IndexContext(true);
		indexContext.setLogOffset(message.getLogOffset());
		indexContext.setIndexOffset(message.getIndexOffset());
		indexContext.setMessageSize(message.getSize());
		IndexQueue queue = indexService.getIndexQueue(message.getTopic(), message.getQueueNum());
		indexContext.setQueue(queue);
		indexService.index(indexContext);
	}

	void writeMessage(BrokerMessage message) throws Exception {
		TopicIndex topicIndex = indexService.getTopicIndex(message.getTopic());
		message.setQueueNum(topicIndex.randomQueue());
		message.setReceiveTime(SystemMillis.current());
		ByteBuf byteBuf = byteBufPool.borrow();
		try {
			message.encode(byteBuf);

			int latches = 1;// 等待写盘

			boolean flush = flushCondition.reachedWhen(message.getSize());
			if (flush) {
				latches++;// 刷盘等待
			}

			CountDownLatch latch = new CountDownLatch(latches);

			appendQueue.add(new MsgAppendContext(message, byteBuf, flush, latch));

			boolean success = latch.await(100000000, TimeUnit.MILLISECONDS);
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
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			};
		}
	}

	private void appendMessage(MsgAppendContext ctx) throws IOException {
		IndexQueue queue = indexService.getIndexQueue(ctx.message.getTopic(), ctx.message.getQueueNum());

		long indexOffset = queue.getOffset();
		ctx.setIndexOffset(indexOffset);

		// 在最后一个日志文件追加写入
		AppendFile lastFile = logAppendDir.getLastFile();
		int writeRemain = lastFile.remaining();
		if (writeRemain < ctx.message.getSize()) {
			ByteBuffer blankBuffer = ByteBuffer.allocate(writeRemain);
			blankBuffer.limit(writeRemain);
			if (writeRemain >= 4) {
				blankBuffer.putInt(-1);
				blankBuffer.rewind();
			}

			lastFile.append(blankBuffer);
			lastFile.persistent();
			lastFile = logAppendDir.createNewFile();
		}
		long logOffset = lastFile.getId() + lastFile.getWritePosition();
		ctx.setLogOffset(logOffset);

		lastFile.append(ctx.buf.nioBuffer());
		ctx.latch.countDown();

		if (ctx.flush) {
			commitQueue.add(ctx);
		}

		// 索引消息
		IndexContext indexContext = new IndexContext();
		indexContext.setQueue(queue);
		indexContext.setIndexOffset(indexOffset);
		indexContext.setLogOffset(logOffset);
		indexContext.setMessageSize(ctx.message.getSize());
		indexService.writeIndex(indexContext);
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
						logAppendDir.getLastFile().persistent();
						contexts.forEach(ctx -> ctx.latch.countDown());
					} catch (IOException e) {
						storeEventBus.add(StoreEvent.create(e));
					}
				}
			};
		}
	}
}
