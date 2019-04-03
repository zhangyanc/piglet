package pers.zyc.piglet.broker.store;

import lombok.Getter;
import lombok.Setter;

import java.io.File;

/**
 * @author zhangyancheng
 */
public class StoreConfig {
	
	private static final long LOG_FILE_LENGTH = 1024 * 1024 * 256;
	
	/**
	 * 数据文件目录
	 */
	@Getter
	private final File dataDir;
	
	/**
	 * 消息日志目录
	 */
	@Getter
	private final File logDir;
	
	/**
	 * 消息索引目录
	 */
	@Getter
	private final File indexDir;
	
	/**
	 * 索引检查点文件
	 */
	@Getter
	private final File checkpointFile;
	
	/**
	 * 消费位置
	 */
	@Getter
	private final File consumeOffsetFile;
	
	/**
	 * 主题队列数
	 */
	@Getter
	private final File topicQueuesFile;
	
	/**
	 * 锁文件
	 */
	@Getter
	private final File lockFile;
	
	/**
	 * 日志文件大小（字节数）
	 */
	@Getter
	@Setter
	private long logFileLength = LOG_FILE_LENGTH;
	
	
	public StoreConfig(String file) {
		this(new File(file));
	}
	
	public StoreConfig(File dataDir) {
		if (dataDir.exists()) {
			if (!dataDir.isDirectory()) {
				throw new IllegalArgumentException("File isn't a directory, file:" + dataDir);
			}
		} else {
			if (!dataDir.mkdirs() && !dataDir.exists()) {
				throw new IllegalStateException("Create directory failed, file: " + dataDir);
			}
		}
		if (!dataDir.canWrite()) {
			throw new IllegalStateException("File can't write, file: " + dataDir);
		}
		if (!dataDir.canRead()) {
			throw new IllegalStateException("File can't read, file: " + dataDir);
		}
		
		this.dataDir = dataDir;
		this.logDir = new File(dataDir, "log/");
		this.indexDir = new File(dataDir, "index/");
		this.checkpointFile = new File(dataDir, "checkpoint");
		this.consumeOffsetFile = new File(dataDir, "consume-offset");
		this.topicQueuesFile = new File(dataDir, "topic-queues");
		this.lockFile = new File(dataDir, "lock");
	}
}
