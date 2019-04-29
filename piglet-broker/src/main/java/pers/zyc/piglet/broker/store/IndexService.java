package pers.zyc.piglet.broker.store;

import lombok.extern.slf4j.Slf4j;
import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;
import pers.zyc.tools.utils.BatchFetchQueue;
import pers.zyc.tools.utils.SystemMillis;
import pers.zyc.tools.utils.event.EventBus;
import pers.zyc.tools.utils.lifecycle.Service;
import pers.zyc.tools.utils.lifecycle.ThreadService;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;
import java.util.zip.Adler32;

/**
 * @author zhangyancheng
 */
@Slf4j
public class IndexService extends Service implements Persistently {
	private static final int CP_DATA_LENGTH = 16 + 4;
	private static final int CP_NEXT_POS = 1004;

	/**
	 * 索引目录
	 */
	private final StoreConfig storeConfig;

	private final EventBus<StoreEvent> storeEventBus;

	private final Checkpoint checkpoint;

	private final BatchFetchQueue<IndexContext> indexQueue;

	private final MessageIndexAppender indexAppender = new MessageIndexAppender();

	private final ConcurrentMap<String, TopicIndex> topicIndexMap = new ConcurrentHashMap<>();

	private long logIndexedOffset;

	IndexService(StoreConfig storeConfig, EventBus<StoreEvent> storeEventBus) {
		this.storeConfig = storeConfig;
		this.storeEventBus = storeEventBus;
		this.checkpoint = new Checkpoint();
		this.indexQueue = new BatchFetchQueue<>(storeConfig.getIndexAppendQueueSize());
		Optional.ofNullable(storeConfig.getIndexDir().listFiles(File::isDirectory))
				.filter(topicDirs -> topicDirs.length > 0)
				.ifPresent(topicDirs -> Stream.of(topicDirs).forEach(topicDir -> {
					String topic = topicDir.getName();
					topicIndexMap.put(topic, new TopicIndex(topic, topicDir, storeConfig.getIndexFileItems()));
				}));
	}

	@Override
	protected void doStart() throws Exception {
		indexAppender.start();
		checkpoint.start();
	}

	@Override
	protected void doStop() throws Exception {
		indexAppender.stop();
		checkpoint.stop();
	}

	@Override
	public File getFile() {
		return storeConfig.getIndexDir();
	}

	@Override
	public void persistent() {
		topicIndexMap.values().parallelStream().forEach(topicIndex -> {
			try {
				topicIndex.persistent();
			} catch (IOException e) {
				storeEventBus.offer(StoreEvent.create(e));
			}
		});
	}

	/**
	 * 恢复，服务启动时执行，返回最小的未正确刷盘的索引项对应的日志偏移量
	 *
	 * @return 最小的未正确刷盘的索引项对应的日志偏移量
	 */
	public long recover() {
		long offset = topicIndexMap.values().parallelStream().mapToLong(TopicIndex::recover).min().orElse(-1);
		return offset > 0 && offset < logIndexedOffset ? offset : logIndexedOffset;
	}

	public TopicIndex getTopicIndex(String topic) {
		return Optional.ofNullable(topicIndexMap.get(topic)).orElseThrow(() ->
				new SystemException(SystemCode.TOPIC_NOT_EXISTS));
	}

	public IndexQueue getIndexQueue(String topic, short queueNum) {
		return getTopicIndex(topic).getIndexQueue(queueNum);
	}

	/**
	 * 新增主题后添加索引队列
	 *
	 * @param topic 主题
	 * @param queues 队列数
	 */
	public void addTopic(String topic, short queues) {
		TopicIndex topicIndex = topicIndexMap.computeIfAbsent(topic, t -> {
			File topicDir = new File(storeConfig.getIndexDir(), t + "/");
			return new TopicIndex(topic, topicDir, storeConfig.getIndexFileItems());
		});
		topicIndex.updateQueueCount(queues);
	}

	public void writeIndex(IndexContext context) {
		indexQueue.add(context);
		context.getQueue().updateOffset();
	}

	public void index(IndexContext indexContext) {
		IndexQueue queue = indexContext.getQueue();
		if (indexContext.isRecover()) {
			if (queue.getOffset() > indexContext.getIndexOffset()) {
				return;
			}
			assert queue.getOffset() == indexContext.getIndexOffset();
			queue.updateOffset();
		}
		queue.append(indexContext);
		logIndexedOffset = indexContext.getLogOffset() + indexContext.getMessageSize();// 记录已索引的日志位置
	}

	public long getLogIndexedOffset() {
		return logIndexedOffset;
	}

	private class MessageIndexAppender extends ThreadService {

		@Override
		public String getName() {
			return "Message Index Appender";
		}

		@Override
		protected Runnable getRunnable() {
			return new ServiceRunnable() {

				@Override
				protected void execute() throws InterruptedException {
					List<IndexContext> contexts = indexQueue.fetch();
					try {
						contexts.forEach(IndexService.this::index);
					} catch (Exception e) {
						storeEventBus.add(StoreEvent.create(e));
					}
				}
			};
		}
	}

	private class Checkpoint extends ThreadService {

		private ByteBuffer dataBuffer;

		private FileChannel fileChannel;

		private long timestamp;

		@Override
		protected void doStart() throws Exception {
			fileChannel = new RandomAccessFile(storeConfig.getCheckpointFile(), "rw").getChannel();
			dataBuffer = ByteBuffer.allocateDirect(CP_DATA_LENGTH);

			long size = fileChannel.size();
			if (size >= CP_DATA_LENGTH) {
				if (!validFrom(0)) {
					if (size >= CP_NEXT_POS + CP_DATA_LENGTH) {
						if (!validFrom(CP_NEXT_POS)) {
							throw new SystemException(SystemCode.CHECKSUM_WRONG);
						}
					} else {
						throw new SystemException(SystemCode.CHECKSUM_WRONG);
					}
				}
			}
		}

		@Override
		protected void doStop() throws Exception {
			super.doStop();
			((DirectBuffer) dataBuffer).cleaner().clean();
			fileChannel.close();
		}

		@Override
		protected Runnable getRunnable() {
			return new ServiceRunnable() {

				@Override
				protected long getInterval() {
					// 定时持久化队列文件，且记录写盘位置
					return 1000 * 30;
				}

				@Override
				protected void execute() throws InterruptedException {
					long offsetBeforePersistent = getLogIndexedOffset();

					persistent();

					log.info("Index data is persistent, log offset: " + offsetBeforePersistent);
					if (offsetBeforePersistent > 0) {
						persistentCheckpoint(offsetBeforePersistent);
					}
				}
			};
		}

		private void persistentCheckpoint(long logIndexedOffset) throws InterruptedException {
			dataBuffer.clear();

			timestamp = SystemMillis.current();
			dataBuffer.putLong(logIndexedOffset);
			dataBuffer.putLong(timestamp);
			dataBuffer.flip();
			int checksum = getChecksum(dataBuffer);
			dataBuffer.limit(CP_DATA_LENGTH);
			dataBuffer.putInt(checksum);
			// 双写，避免写入异常时仍有一份数据不被脏写
			try {
				writeData(0);
				writeData(CP_NEXT_POS);
				fileChannel.force(false);
			} catch (IOException e) {
				log.error("Checkpoint persistent failed: {}", e.getMessage());
				storeEventBus.add(StoreEvent.create(e));
			}
		}

		private boolean validFrom(int position) throws IOException {
			readData(position);

			dataBuffer.rewind();
			dataBuffer.limit(16);// 读前16字节

			int checksum = getChecksum(dataBuffer);

			dataBuffer.limit(CP_DATA_LENGTH);// 读最后4字节
			if (checksum == dataBuffer.getInt()) {
				dataBuffer.rewind();
				logIndexedOffset = dataBuffer.getLong();
				timestamp = dataBuffer.getLong();
				return true;
			}
			return false;
		}

		private void readData(int position) throws IOException {
			dataBuffer.clear();
			fileChannel.position(position);
			while (dataBuffer.hasRemaining()) {
				fileChannel.read(dataBuffer);
			}
		}

		private void writeData(int position) throws IOException {
			dataBuffer.rewind();
			fileChannel.position(position);
			while (dataBuffer.hasRemaining()) {
				fileChannel.write(dataBuffer);
			}
		}
	}

	private static int getChecksum(ByteBuffer buffer) {
		Adler32 checksum = new Adler32();
		checksum.update(buffer);
		return (int) checksum.getValue();
	}
}
