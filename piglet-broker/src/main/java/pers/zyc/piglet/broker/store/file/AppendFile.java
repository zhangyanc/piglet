package pers.zyc.piglet.broker.store.file;

import lombok.Getter;
import pers.zyc.piglet.broker.store.Persistently;
import pers.zyc.tools.utils.Locks;
import pers.zyc.tools.utils.SystemMillis;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zhangyancheng
 */
public class AppendFile implements Closeable, Persistently {
	private static final short VERSION = 1;

	/**
	 * 头部长度（2字节版本号，8字节创建时间戳，54字节留白）
	 */
	final static short HEAD_LENGTH = 2 + 8 + 54;

	/**
	 * 文件id，就是文件名，也是该文件在顺序写目录的起始偏移量
	 */
	@Getter
	private final long id;

	/**
	 * 文件大小
	 */
	@Getter
	private final int fileLength;

	/**
	 * 所在的顺序写目录
	 */
	@Getter
	private final AppendDir directory;

	/**
	 * 对应磁盘文件
	 */
	@Getter
	private final File file;

	/**
	 * 文件读写通道
	 */
	private final FileChannel fileChannel;

	/**
	 * 版本号（用于区别不同的协议格式）
	 */
	private final short version;

	/**
	 * 文件创建时间
	 */
	private final long createTime;

	/**
	 * 已写入位置
	 */
	@Getter
	private int writePosition;

	/**
	 * 已刷盘位置
	 */
	@Getter
	private int flushPosition;

	private final Lock lock = new ReentrantLock();

	AppendFile(long id, int fileLength, AppendDir directory) throws IOException {
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
			version = VERSION;
			createTime = SystemMillis.current();
			writeHeader();
		} else {
			ByteBuffer headerBuffer = readHeader();
			version = headerBuffer.getShort();
			createTime = headerBuffer.getLong();
			// 写入位置设置到末尾, 等待恢复时矫正
			writePosition = fileLength;
			flushPosition = fileLength;
		}
	}
	
	@Override
	public void close() throws IOException {
		Locks.execute(lock, fileChannel::close);
	}

	@Override
	public void persistent() throws IOException {
		Locks.execute(lock, () -> {
			fileChannel.force(false);
			flushPosition = writePosition;
		});
	}

	@Override
	public String toString() {
		return "AppendFile[" + file + "]";
	}

	/**
	 * 写头部数据
	 *
	 * @throws IOException io异常
	 */
	private void writeHeader() throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(HEAD_LENGTH);
		buffer.putShort(version);
		buffer.putLong(createTime);
		buffer.put(new byte[54]);
		buffer.flip();
		append0(buffer);
	}

	/**
	 * 读取头部数据
	 *
	 * @return 头部数据
	 * @throws IOException io异常
	 */
	private ByteBuffer readHeader() throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(HEAD_LENGTH);
		while (buffer.hasRemaining()) {
			fileChannel.read(buffer);
		}
		buffer.flip();
		return buffer;
	}

	/**
	 * @return 文件剩余可写入大小
	 */
	public int remaining() {
		return fileLength - writePosition;
	}

	public long getMaxOffset() {
		return id + writePosition;
	}

	/**
	 * 追加数据
	 *
	 * @param data 数据
	 * @throws IOException io异常
	 */
	public void append(ByteBuffer data) throws IOException {
		long realFileWritePos = fileChannel.position();
		if (writePosition + HEAD_LENGTH != realFileWritePos) {
			throw new IOException("Incorrect write position: expected " + writePosition + " " +
					"actual " + (realFileWritePos - HEAD_LENGTH));
		}
		int length = data.remaining();
		if (writePosition + length > fileLength) {
			throw new IOException("File is not large enough, required " + length + ", " +
					"remain " + (fileLength - writePosition));
		}
		append0(data);
		writePosition += length;
	}

	private void append0(ByteBuffer data) throws IOException {
		while (data.hasRemaining()) {
			fileChannel.write(data);
		}
	}

	/**
	 * 从指定位置开始读取一定长度的数据，如果没有足够的数据则返回剩余长度数据量
	 *
	 * @param position 起始位置
	 * @param size 读取数据长度
	 * @return 数据缓存区
	 * @throws IOException IO异常
	 */
	public ByteBuffer read(int position, int size) throws IOException {
		if (position < 0 || position > writePosition) {
			throw new IllegalArgumentException("Read position " + position + " not in [0," + writePosition + ")");
		}
		size = Math.min(size, fileLength - position);
		ByteBuffer result = ByteBuffer.allocate(size);
		if (size > 0) {
			fileChannel.read(result, position + HEAD_LENGTH);
			result.flip();
		}
		return result;
	}

	/**
	 * 从指定偏移量开始读取一定长度的数据，如果没有足够的数据则返回剩余长度数据量
	 *
	 * @param offset 偏移量
	 * @param size 读取数据长度
	 * @return 数据缓存区
	 * @throws IOException IO异常
	 */
	public ByteBuffer read(long offset, int size) throws IOException {
		if (offset < id || offset > id + fileLength) {
			throw new IllegalArgumentException("Read offset " + offset + " invalid for " + this);
		}
		return read((int) (offset % fileLength), size);
	}

	/**
	 * 根据恢复出的最后写入位置截断文件（矫正writePosition）
	 *
	 * @param truncateOffset 恢复的位置
	 */
	public void truncate(long truncateOffset) throws IOException {
		if (truncateOffset < id || truncateOffset > id + fileLength) {
			throw new IllegalArgumentException("Truncate offset " + truncateOffset + " invalid for " + this);
		}
		int pos = (int) (truncateOffset % fileLength);
		flushPosition = writePosition = pos;
		fileChannel.position(pos + HEAD_LENGTH);
	}
}
