package pers.zyc.piglet.broker.store;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;
import pers.zyc.tools.utils.SystemMillis;
import pers.zyc.tools.utils.lifecycle.Service;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.Adler32;

/**
 * @author zhangyancheng
 */
@Slf4j
public class Checkpoint extends Service implements Persistently {
	private static final int DATA_LENGTH = 16 + 8;
	private static final int NEXT_POS = 1000;
	
	private final File file;
	private FileChannel fileChannel;
	private ByteBuffer dataBuffer;
	
	@Getter
	@Setter
	private long recoverOffset;
	
	@Getter
	private long timestamp;
	
	Checkpoint(File file) {
		this.file = file;
	}
	
	@Override
	protected void beforeStart() throws Exception {
		if (!file.exists() && !file.createNewFile()) {
			throw new IllegalStateException("Create checkpoint file failed, file: " + file);
		}
		fileChannel = new RandomAccessFile(file, "rw").getChannel();
		dataBuffer = ByteBuffer.allocateDirect(DATA_LENGTH);
	}
	
	@Override
	protected void doStart() throws Exception {
		long size = fileChannel.size();
		if (size >= DATA_LENGTH) {
			readData(0);
			if (!validData()) {
				if (size >= NEXT_POS + DATA_LENGTH) {
					readData(NEXT_POS);
					if (!validData()) {
						throw new SystemException(SystemCode.CHECKSUM_WRONG);
					}
				} else {
					throw new SystemException(SystemCode.CHECKSUM_WRONG);
				}
			}
		}
	}
	
	private boolean validData() {
		dataBuffer.rewind();
		dataBuffer.limit(16);// 读前16字节
		
		Adler32 checksum = new Adler32();
		checksum.update(dataBuffer);
		
		dataBuffer.limit(DATA_LENGTH);// 读最后8字节
		if (checksum.getValue() == dataBuffer.getLong()) {
			dataBuffer.rewind();
			recoverOffset = dataBuffer.getLong();
			timestamp = dataBuffer.getLong();
			return true;
		}
		return false;
	}
	
	@Override
	protected void doStop() throws Exception {
		((DirectBuffer) dataBuffer).cleaner().clean();
		fileChannel.close();
	}
	
	@Override
	public void persistent() {
		serviceLock.lock();
		try {
			doPersistent();
		} finally {
			serviceLock.lock();
		}
	}
	
	private void doPersistent() {
		dataBuffer.clear();
		
		timestamp = SystemMillis.current();
		dataBuffer.putLong(recoverOffset);
		dataBuffer.putLong(timestamp);
		dataBuffer.flip();
		Adler32 checksum = new Adler32();
		checksum.update(dataBuffer);
		dataBuffer.limit(DATA_LENGTH);
		dataBuffer.putLong(checksum.getValue());
		try {
			// 双写，避免写入异常时仍有一份数据不被脏写
			writeData(0);
			writeData(NEXT_POS);
			fileChannel.force(true);
		} catch (IOException e) {
			log.error("Checkpoint persistent error: {}", e.getMessage());
		}
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
