package pers.zyc.piglet.broker.store.file;

import lombok.Getter;
import pers.zyc.piglet.IOExecutor;
import pers.zyc.piglet.broker.store.MapFile;
import pers.zyc.tools.utils.SystemMillis;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author zhangyancheng
 */
public class AppendFile implements Closeable, MapFile {
	private static final short VERSION = 1;

	/**
	 * 头部长度（2字节版本号，8字节创建时间戳，54字节留白）
	 */
	final static short HEAD_LENGTH = 2 + 8 + 54;

	private final static byte[] BLANK_BYTES = new byte[54];

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

	AppendFile(long id, int fileLength, AppendDir directory) {
		this.id = id;
		this.fileLength = fileLength;
		this.directory = directory;
		this.file = new File(directory.getDirectory(), String.valueOf(id));

		try {
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
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
	
	@Override
	public void close() {
		IOExecutor.execute(fileChannel::close);
	}

	@Override
	public void flush() {
		IOExecutor.execute(() -> {
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
	 * @throws UncheckedIOException io异常
	 */
	private void writeHeader() {
		ByteBuffer buffer = ByteBuffer.allocate(HEAD_LENGTH);
		buffer.putShort(version);
		buffer.putLong(createTime);
		buffer.put(BLANK_BYTES);
		buffer.flip();
		append0(buffer);
	}

	/**
	 * 读取头部数据
	 *
	 * @return 头部数据
	 * @throws UncheckedIOException io异常
	 */
	private ByteBuffer readHeader() {
		ByteBuffer buffer = ByteBuffer.allocate(HEAD_LENGTH);
		readData(0, buffer);
		return buffer;
	}

	private void readData(int position, ByteBuffer buffer) {
		IOExecutor.execute(() -> {
			int pos = position;
			while (buffer.hasRemaining()) {
				pos += fileChannel.read(buffer, pos);
			}
			buffer.flip();
		});
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
	 * @throws UncheckedIOException io异常
	 */
	public void append(ByteBuffer data) {
		long realFileWritePos = IOExecutor.execute((IOExecutor.IOCallAction<Long>) fileChannel::position);
		if (writePosition + HEAD_LENGTH != realFileWritePos) {
			throw new IllegalStateException("Incorrect write position: expected " + writePosition + " " +
					"actual " + (realFileWritePos - HEAD_LENGTH));
		}
		int length = data.remaining();
		if (writePosition + length > fileLength) {
			throw new IllegalStateException("File is not large enough, required " + length + ", " +
					"remain " + (fileLength - writePosition));
		}
		append0(data);
		writePosition += length;
	}

	private void append0(ByteBuffer data) {
		IOExecutor.execute(() -> {
			while (data.hasRemaining()) {
				fileChannel.write(data);
			}
		});
	}

	/**
	 * 从指定位置开始读取一定长度的数据，如果没有足够的数据则返回剩余长度数据量
	 *
	 * @param position 起始位置
	 * @param size 读取数据长度
	 * @return 数据缓存区
	 * @throws UncheckedIOException IO异常
	 * @throws IllegalArgumentException position错误
	 */
	public ByteBuffer read(int position, int size) {
		if (position < 0 || position > writePosition) {
			throw new IllegalArgumentException("Read position " + position + " not in [0," + writePosition + ")");
		}
		size = Math.min(size, fileLength - position);
		ByteBuffer result = ByteBuffer.allocate(size);
		if (size > 0) {
			readData(position + HEAD_LENGTH, result);
		}
		return result;
	}

	/**
	 * 从指定偏移量开始读取一定长度的数据，如果没有足够的数据则返回剩余长度数据量
	 *
	 * @param offset 偏移量
	 * @param size 读取数据长度
	 * @return 数据缓存区
	 * @throws UncheckedIOException IO异常
	 */
	public ByteBuffer read(long offset, int size) {
		if (offset < id || offset > id + fileLength) {
			throw new IllegalArgumentException("Read offset " + offset + " invalid for " + this);
		}
		return read((int) (offset % fileLength), size);
	}

	public void read(long offset, ByteBuffer target) {
		if (offset < id || offset > id + fileLength) {
			throw new IllegalArgumentException("Read offset " + offset + " invalid for " + this);
		}
		int readPosition = (int) (offset % fileLength);
		int limit = Math.min(target.limit(), fileLength - readPosition);
		target.limit(limit);
		readData(readPosition + HEAD_LENGTH, target);
	}

	/**
	 * 根据恢复出的最后写入位置截断文件（矫正writePosition）
	 *
	 * @param truncateOffset 恢复的位置
	 * @throws UncheckedIOException IO异常
	 */
	public void truncate(long truncateOffset) {
		if (truncateOffset < id || truncateOffset > id + fileLength) {
			throw new IllegalArgumentException("Truncate offset " + truncateOffset + " invalid for " + this);
		}
		int pos = (int) (truncateOffset % fileLength);
		flushPosition = writePosition = pos;
		IOExecutor.execute(() -> fileChannel.position(pos + HEAD_LENGTH));
	}
}
