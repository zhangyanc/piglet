package pers.zyc.piglet.broker.store;

import pers.zyc.piglet.model.BrokerMessage;
import pers.zyc.tools.utils.event.EventBus;
import pers.zyc.tools.utils.event.EventListener;
import pers.zyc.tools.utils.lifecycle.Service;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;

/**
 * @author zhangyancheng
 */
public class DefaultStore extends Service implements Store {
	
	private final StoreConfig config;
	private final EventBus<StoreEvent> storeEventBus;

	private RandomAccessFile lockRaf;
	private FileLock fileLock;

	private IndexService indexService;
	private LogService logService;

	public DefaultStore(StoreConfig config) {
		this.config = config;
		storeEventBus = new EventBus.Builder<StoreEvent>().name("Store Event Bus").build();
	}
	
	@Override
	protected void beforeStart() throws Exception {
		lockRaf = new RandomAccessFile(config.getLockFile(), "rw");
		fileLock = lockRaf.getChannel().tryLock();
		if (fileLock == null) {
			lockRaf.close();
			throw new IOException("File lock failed, it's likely locked by another progress");
		}
	}
	
	@Override
	protected void doStart() throws Exception {
		indexService = new IndexService(config.getIndexDir(), storeEventBus);
		logService = new LogService(config.getLogDir(), storeEventBus, indexService);

		storeEventBus.start();
		indexService.start();
		logService.start();

		recover();
	}

	@Override
	protected void doStop() throws Exception {
		logService.stop();
		indexService.stop();
		storeEventBus.stop();
	}

	@Override
	public void addListener(EventListener<StoreEvent> listener) {
		storeEventBus.addListener(listener);
	}

	@Override
	public void removeListener(EventListener<StoreEvent> listener) {
		storeEventBus.removeListener(listener);
	}

	@Override
	public StoreConfig getConfig() {
		return config;
	}

	private void recover() throws IOException {
		long recoverOffset = indexService.recover();
		logService.recover(recoverOffset);
	}
	
	@Override
	public void putMessage(BrokerMessage message) {
		try {
			logService.writeMessage(message);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public PutResult putMessage(BrokerMessage[] messages) {
		return null;
	}
}
