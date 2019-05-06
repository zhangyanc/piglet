package pers.zyc.piglet.broker.store;

import lombok.Getter;
import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;
import pers.zyc.piglet.broker.store.file.AppendDir;
import pers.zyc.piglet.broker.store.file.AppendFile;
import sun.nio.ch.DirectBuffer;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;

/**
 * @author zhangyancheng
 */
@Getter
public class IndexQueue implements Persistently, Closeable{

	/**
	 * 主题
	 */
	private final String topic;

	/**
	 * 队列号
	 */
	private final short queueNum;

	/**
	 * 索引队列文件目录 eg. queue/{topic}/{queueId}
	 */
	private final File directory;

	/**
	 * 顺序写目录
	 */
	private final AppendDir appendDir;

	/**
	 * 索引条目数据缓存区
	 */
	private final ByteBuffer itemBuffer;

	/**
	 * 队列全局偏移量
	 */
	private long offset;

	public IndexQueue(String topic, short queueNum, File directory, int indexFileItems) {
		this.topic = topic;
		this.queueNum = queueNum;
		this.directory = directory;
		if (!directory.exists() && !directory.mkdir()) {
			throw new IllegalStateException("Create append directory failed, directory: " + directory);
		}
		appendDir = new AppendDir(directory, indexFileItems * IndexItem.LENGTH);
		itemBuffer = ByteBuffer.allocateDirect(IndexItem.LENGTH);
	}

	@Override
	public void close() {
		((DirectBuffer) itemBuffer).cleaner().clean();
		persistent();
	}

	@Override
	public File getFile() {
		return directory;
	}

	@Override
	public void persistent() {
		// 因为切换文件时上一个文件已经持久化，因此只需要持久化最后一个文件
		appendDir.getLastFile().persistent();
	}

	boolean delete() {
		appendDir.close();
		return directory.delete();
	}

	/**
	 * 从磁盘恢复，返回当前队列未正确刷盘的索引项的日志偏移量，如果全部正确刷盘返回-1
	 *
	 * @return 当前队列未正确刷盘的索引项的日志偏移量
	 */
	public long recover() {
		// 因为切换文件时上一个文件已经刷盘，因此只需要恢复最后一个文件
		AppendFile lastFile = appendDir.getLastFile();
		offset = lastFile.getId();
		int splits = 10;
		int blockSize = lastFile.getFileLength() / splits;
		long logOffset = -1;
		int readPosition = 0;
		out:while (splits-- > 0) {
			ByteBuffer block = lastFile.read(readPosition, blockSize);
			readPosition += blockSize;
			do {
				IndexItem indexItem = new IndexItem();
				if (indexItem.decodeAndCheck(block)) {
					if (indexItem.isBlank()) {
						// 到达末尾空白
						logOffset = -1;
					}
					break out;
				}
				updateOffset();
				logOffset = indexItem.getLogOffset();// 最后一条成功刷盘的日志偏移量
			} while (block.hasRemaining());
		}
		lastFile.truncate(offset);
		return logOffset;
	}

	public void updateOffset() {
		offset += IndexItem.LENGTH;
	}

	public void append(IndexItem indexItem) {
		itemBuffer.clear();
		indexItem.encode(itemBuffer);
		itemBuffer.flip();

		AppendFile lastFile = appendDir.getLastFile();
		if (lastFile.remaining() < IndexItem.LENGTH) {
			// 切换文件时需要将上一个文件刷盘
			lastFile.persistent();
			lastFile = appendDir.createNewFile();
		}
		if (lastFile.getWritePosition() != indexItem.getIndexOffset() - lastFile.getId()) {
			throw new SystemException(SystemCode.STORE_WRONG_OFFSET.getCode(),
					"Wrong index queue offset: " + indexItem.getIndexOffset() +
							", writePos: " + lastFile.getWritePosition() + ", afId: " + lastFile.getId());
		}
		lastFile.append(itemBuffer);
	}
}
