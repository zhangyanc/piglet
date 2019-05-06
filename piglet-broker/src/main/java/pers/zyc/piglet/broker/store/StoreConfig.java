package pers.zyc.piglet.broker.store;

import lombok.Getter;
import lombok.Setter;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * @author zhangyancheng
 */
@Getter
@Setter
public class StoreConfig {
	
	/**
	 * 数据文件目录
	 */
	private File dataDir;
	
	/**
	 * 消息日志目录
	 */
	private File logDir;
	
	/**
	 * 消息索引目录
	 */
	private File indexDir;

	/**
	 * 索引位置检查点文件
	 */
	private File checkpointFile;
	
	/**
	 * 消费位置
	 */
	private File consumeOffsetFile;
	
	/**
	 * 锁文件
	 */
	private File lockFile;
	
	/**
	 * 日志文件大小（字节数）
	 */
	private int logFileLength = 1024 * 1024 * 128;

	private int msgAppendQueueSize = 50000;

	private int msgCommitQueueSize = 50000;

	/**
	 * 索引文件可写入索引条目个数
	 */
	private int indexFileItems = 600000;

	private int indexAppendQueueSize = 50000;

	private int indexFlushInterval = 30000;
	
	
	public StoreConfig(String file) {
		this(new File(file));
	}
	
	public StoreConfig(File dataDir) {
		if (dataDir.exists()) {
			if (!dataDir.isDirectory()) {
				throw new IllegalStateException("File isn't a directory, file:" + dataDir);
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
		if (!logDir.exists() && !logDir.mkdir()) {
			throw new IllegalStateException("Create log directory failed, file: " + logDir);
		}
		this.indexDir = new File(dataDir, "index/");
		if (!indexDir.exists() && !indexDir.mkdir()) {
			throw new IllegalStateException("Create queue directory failed, file: " + indexDir);
		}
		this.checkpointFile = new File(indexDir, "checkpoint");
		this.consumeOffsetFile = new File(dataDir, "consume-offset");
		this.lockFile = new File(dataDir, "lock");
		try {
			if (!checkpointFile.exists() && !checkpointFile.createNewFile()) {
				throw new IllegalStateException("Create checkpoint file failed, file: " + checkpointFile);
			}
			if (!consumeOffsetFile.exists() && !consumeOffsetFile.createNewFile()) {
				throw new IllegalStateException("Create consume offset file failed, file: " + consumeOffsetFile);
			}
			if (!lockFile.exists() && !lockFile.createNewFile() && !lockFile.exists()) {
				throw new IllegalStateException("Create lock file failed, file: " + lockFile);
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
