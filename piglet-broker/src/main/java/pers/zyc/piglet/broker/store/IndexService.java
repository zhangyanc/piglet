package pers.zyc.piglet.broker.store;

import lombok.extern.slf4j.Slf4j;
import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;
import pers.zyc.tools.utils.BatchFetchQueue;
import pers.zyc.tools.utils.event.EventBus;
import pers.zyc.tools.utils.lifecycle.ThreadService;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

/**
 * @author zhangyancheng
 */
@Slf4j
public class IndexService extends ThreadService implements Persistently {

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
		this.checkpoint = new Checkpoint(storeConfig.getCheckpointFile());
		this.logIndexedOffset = checkpoint.getCheckpointOffset();
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
	}

	@Override
	protected void doStop() throws Exception {
		indexAppender.stop();
		checkpoint.close();
		super.doStop();
	}

	@Override
	protected Runnable getRunnable() {
		return new ServiceRunnable() {

			@Override
			protected long getInterval() {
				return storeConfig.getIndexFlushInterval();
			}

			@Override
			protected void execute() throws InterruptedException {
				try {
					persistent();
				} catch (Exception e) {
					log.error("Index persistent failed", e);
					storeEventBus.offer(StoreEvent.create(e));
				}
			}
		};
	}

	@Override
	public File getFile() {
		return storeConfig.getIndexDir();
	}

	@Override
	public void persistent() {
		long offsetBeforePersistent = getLogIndexedOffset();

		if (offsetBeforePersistent > checkpoint.getCheckpointOffset()) {
			topicIndexMap.values().forEach(TopicIndex::persistent);
			log.info("Index data is persistent, log offset: {}", offsetBeforePersistent);
			checkpoint.setCheckpointOffset(offsetBeforePersistent);
			checkpoint.persistent();
		}
	}

	/**
	 * 恢复，服务启动时执行，返回最小的未正确刷盘的索引项对应的日志偏移量
	 *
	 * @return 最小的未正确刷盘的索引项对应的日志偏移量
	 */
	public long recover() {
		long offset = topicIndexMap.values()
				.stream()
				.map(TopicIndex::getAllIndexQueue)
				.flatMap(Arrays::stream)
				.parallel()
				.mapToLong(IndexQueue::recover)
				.min()
				.orElse(-1);
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
				// 索引已经持久化
				logIndexedOffset = indexContext.getLogOffset() + indexContext.getMessageSize();// 记录已索引的日志位置
				return;
			}
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
						storeEventBus.offer(StoreEvent.create(e));
					}
				}
			};
		}
	}
}
