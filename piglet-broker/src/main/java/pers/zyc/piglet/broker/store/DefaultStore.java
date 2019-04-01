package pers.zyc.piglet.broker.store;

import pers.zyc.piglet.model.BrokerMessage;
import pers.zyc.tools.utils.lifecycle.Service;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;

/**
 * @author zhangyancheng
 */
public class DefaultStore extends Service implements Store {
	
	private final StoreConfig config;
	
	private RandomAccessFile lockRaf;
	private FileLock fileLock;
	
	public DefaultStore(StoreConfig config) {
		this.config = config;
	}
	
	@Override
	protected void beforeStart() throws Exception {
		File logDir = config.getLogDir();
		if (!logDir.exists() && !logDir.mkdir() && !logDir.exists()) {
			throw new IllegalStateException("Create log directory failed, file: " + logDir);
		}
		File queueDir = config.getQueueDir();
		if (!queueDir.exists() && !queueDir.mkdir() && !queueDir.exists()) {
			throw new IllegalStateException("Create queue directory failed, file: " + queueDir);
		}
		File checkpointFile = config.getCheckpointFile();
		if (!checkpointFile.exists() && !checkpointFile.createNewFile() && !checkpointFile.exists()) {
			throw new IllegalStateException("Create checkpoint file failed, file: " + checkpointFile);
		}
		File lockFile = config.getLockFile();
		if (!lockFile.exists() && !lockFile.createNewFile() && !lockFile.exists()) {
			throw new IllegalStateException("Create lock file failed, file: " + lockFile);
		}
	}
	
	@Override
	protected void doStart() {
		
	}
	
	private void lockFile() throws Exception {
		lockRaf = new RandomAccessFile(config.getLockFile(), "rw");
		fileLock = lockRaf.getChannel().tryLock();
		if (fileLock == null) {
			throw new IllegalStateException("File lock failed, it's likely locked by another progress");
		}
	}
	
	@Override
	protected void doStop() throws Exception {
		
	}
	
	@Override
	public StoreConfig getConfig() {
		return config;
	}
	
	@Override
	public void putMessage(BrokerMessage message) {
		
	}
	
	@Override
	public PutResult putMessage(BrokerMessage[] messages) {
		return null;
	}
}
