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
	
	private CheckpointRecovery checkpointRecovery;
	
	public DefaultStore(StoreConfig config) {
		this.config = config;
	}
	
	@Override
	protected void beforeStart() throws Exception {
		File logDir = config.getLogDir();
		if (!logDir.exists() && !logDir.mkdir() && !logDir.exists()) {
			throw new IllegalStateException("Create log directory failed, file: " + logDir);
		}
		File indexDir = config.getIndexDir();
		if (!indexDir.exists() && !indexDir.mkdir() && !indexDir.exists()) {
			throw new IllegalStateException("Create queue directory failed, file: " + indexDir);
		}
		File lockFile = config.getLockFile();
		if (!lockFile.exists() && !lockFile.createNewFile() && !lockFile.exists()) {
			throw new IllegalStateException("Create lock file failed, file: " + lockFile);
		}
		
		checkpointRecovery = new CheckpointRecovery(config.getCheckpointFile());
	}
	
	@Override
	protected void doStart() throws Exception {
		lockFile();
		
		
	}
	
	private void lockFile() throws Exception {
		lockRaf = new RandomAccessFile(config.getLockFile(), "rw");
		fileLock = lockRaf.getChannel().tryLock();
		if (fileLock == null) {
			lockRaf.close();
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
