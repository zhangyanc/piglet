package pers.zyc.piglet.broker.store;

import lombok.Getter;
import lombok.Setter;
import pers.zyc.piglet.IOExecutor;
import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;
import pers.zyc.tools.utils.SystemMillis;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.Adler32;

/**
 *
 * @author zhangyancheng
 */
class Checkpoint implements Closeable, MapFile {

	private static final int CP_DATA_LENGTH = 16 + 4;
	private static final int CP_NEXT_POS = 1004;

	@Getter
	private final File file;

	private final ByteBuffer dataBuffer;

	private final FileChannel fileChannel;

	@Getter
	@Setter
	private long checkpointOffset;

	@Getter
	private long timestamp;

	Checkpoint(File checkpointFile) {
		this.file = checkpointFile;
		dataBuffer = ByteBuffer.allocateDirect(CP_DATA_LENGTH);
		try {
			fileChannel = new RandomAccessFile(checkpointFile, "rw").getChannel();
			long size = fileChannel.size();
			if (size >= CP_DATA_LENGTH) {
				if (!validFrom(0)) {
					if (size >= CP_NEXT_POS + CP_DATA_LENGTH) {
						if (!validFrom(CP_NEXT_POS)) {
							throw new SystemException(SystemCode.CHECKSUM_WRONG);
						}
					} else {
						throw new SystemException(SystemCode.CHECKSUM_WRONG);
					}
				}
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public void close() {
		((DirectBuffer) dataBuffer).cleaner().clean();
		IOExecutor.execute(fileChannel::close);
	}

	@Override
	public void flush() {
		dataBuffer.clear();

		timestamp = SystemMillis.current();
		dataBuffer.putLong(checkpointOffset);
		dataBuffer.putLong(timestamp);
		dataBuffer.flip();
		int checksum = getChecksum(dataBuffer);
		dataBuffer.limit(CP_DATA_LENGTH);
		dataBuffer.putInt(checksum);
		IOExecutor.execute(() -> {
			// 双写，避免写入异常时仍有一份数据不被脏写
			writeData(0);
			writeData(CP_NEXT_POS);
			fileChannel.force(false);
		});
	}

	private boolean validFrom(int position) {
		readData(position);

		dataBuffer.rewind();
		dataBuffer.limit(16);// 读前16字节

		int checksum = getChecksum(dataBuffer);

		dataBuffer.limit(CP_DATA_LENGTH);// 读最后4字节
		if (checksum == dataBuffer.getInt()) {
			dataBuffer.rewind();
			checkpointOffset = dataBuffer.getLong();
			timestamp = dataBuffer.getLong();
			return true;
		}
		return false;
	}

	private void readData(int position) {
		dataBuffer.clear();
		IOExecutor.execute(() -> {
			fileChannel.position(position);
			while (dataBuffer.hasRemaining()) {
				fileChannel.read(dataBuffer);
			}
		});
	}

	private void writeData(int position) {
		dataBuffer.rewind();
		IOExecutor.execute(() -> {
			fileChannel.position(position);
			while (dataBuffer.hasRemaining()) {
				fileChannel.write(dataBuffer);
			}
		});
	}

	private static int getChecksum(ByteBuffer buffer) {
		Adler32 checksum = new Adler32();
		checksum.update(buffer);
		return (int) checksum.getValue();
	}
}
