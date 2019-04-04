package pers.zyc.piglet.broker.store.file;

import pers.zyc.tools.utils.SystemMillis;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author zhangyancheng
 */
public class AppendFile implements Closeable {
	final static short HEAD_LENGTH = 8 + 56;
	
	private final long id;
	private final File file;
	private final int fileLength;
	private final AppendDirectory directory;
	private final FileChannel fileChannel;
	private final long createTime;
	
	private int writePos;
	private int flushPos;
	
	AppendFile(long id, int fileLength, AppendDirectory directory) throws IOException {
		this.id = id;
		this.fileLength = fileLength;
		this.directory = directory;
		this.file = new File(directory.getDirectory(), String.valueOf(id));
		
		if (!file.exists() && !file.createNewFile()) {
			throw new IllegalStateException("Create append file failed, file: " + file.getPath());
		}
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		fileChannel = raf.getChannel();
		if (raf.length() < HEAD_LENGTH) {
			// 预申请固定大小
			raf.setLength(fileLength + HEAD_LENGTH);
			createTime = SystemMillis.current();
			writeHeader();
		} else {
			ByteBuffer headerBuffer = readHeader();
			createTime = headerBuffer.getLong();
		}
	}
	
	@Override
	public void close() {
		
	}
	
	private void writeHeader() throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(HEAD_LENGTH);
		buffer.putLong(createTime);
		buffer.put(new byte[56]);
		buffer.flip();
		while (buffer.hasRemaining()) {
			fileChannel.write(buffer);
		}
	}
	
	private ByteBuffer readHeader() throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(HEAD_LENGTH);
		while (buffer.hasRemaining()) {
			fileChannel.read(buffer);
		}
		buffer.flip();
		return buffer;
	}
}
