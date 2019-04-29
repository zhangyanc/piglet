package pers.zyc.piglet.broker.store;

import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;
import pers.zyc.tools.utils.Locks;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author zhangyancheng
 */
public class TopicIndex implements Persistently {

	private final String topic;

	private final File directory;

	private final int indexFileItems;

	private short queues;

	private final Map<Short, IndexQueue> indexQueueMap = new HashMap<>();

	private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

	public TopicIndex(String topic, File directory, int indexFileItems) {
		this.topic = topic;
		this.directory = directory;
		this.indexFileItems = indexFileItems;
		if (!directory.exists() && !directory.mkdir()) {
			throw new IllegalStateException("Create index directory failed, directory: " + directory);
		}
		Optional.ofNullable(directory.listFiles(File::isDirectory))
				.filter(queueDirs -> queueDirs.length > 0)
				.ifPresent(queueDirs -> {
					for (File queueDir : queueDirs) {
						// 加载每个队列目录
						short queueNum = Integer.valueOf(queueDir.getName()).shortValue();
						indexQueueMap.put(queueNum, new IndexQueue(topic, queueNum, queueDir, indexFileItems));
					}
				});
		this.queues = (short) indexQueueMap.size();
	}

	@Override
	public File getFile() {
		return directory;
	}

	@Override
	public void persistent() throws IOException {
		Locks.execute(rwLock.readLock(), () -> {
			for (IndexQueue queue : indexQueueMap.values()) {
				queue.persistent();
			}
		});
	}

	public long recover() {
		return indexQueueMap.values().parallelStream().mapToLong(IndexQueue::recover).min().orElse(-1);
	}

	private IndexQueue createQueue(short queueNum) {
		File queueDir = new File(directory, queueNum + "/");
		return new IndexQueue(topic, queueNum, queueDir, indexFileItems);
	}

	public IndexQueue[] getAllIndexQueue() {
		return Locks.execute(rwLock.readLock(), () -> {
			IndexQueue[] indexQueues = new IndexQueue[queues];
			indexQueueMap.values().toArray(indexQueues);
			return indexQueues;
		});
	}

	public void updateQueueCount(int queueCount) {
		if (queues == queueCount) {
			return;
		}
		Locks.execute(rwLock.writeLock(), () -> {
			if (queues < queueCount) {
				for (short qn = (short) (queues + 1); qn <= queueCount; qn++) {
					indexQueueMap.put(qn, createQueue(qn));
				}
			} else if (queues > queueCount) {
				for (short qn = (short) (queueCount + 1); qn <= queues; qn++) {
					IndexQueue queue = indexQueueMap.remove(qn);
					queue.delete();
				}
			}
			this.queues = (short) queueCount;
		});
	}

	public short getQueues() {
		return Locks.execute(rwLock.readLock(), () -> this.queues);
	}

	public IndexQueue getIndexQueue(short queueNum) {
		return Optional.ofNullable(Locks.execute(rwLock.readLock(), ()-> indexQueueMap.get(queueNum)))
				.orElseThrow(() -> new SystemException(SystemCode.TOPIC_NOT_EXISTS));
	}

	public short randomQueue() {
		return Locks.execute(rwLock.readLock(), () -> (short) ThreadLocalRandom.current().nextInt(1, queues + 1));
	}
}
