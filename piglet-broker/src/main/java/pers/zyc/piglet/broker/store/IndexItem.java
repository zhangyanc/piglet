package pers.zyc.piglet.broker.store;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.zip.Adler32;

/**
 * @author zhangyancheng
 */
@Getter
@Setter
public class IndexItem {

	public final static int LENGTH = 4 + 8 + 4;

	/**
	 * 主题
	 */
	private String topic;

	/**
	 * 队列号
	 */
	private short queueNum;

	/**
	 * 索引偏移量
	 */
	private long indexOffset;

	/**
	 * 日志偏移量
	 */
	private long logOffset;

	/**
	 * 消息大小
	 */
	private int messageSize;

	private int checksum;

	/**
	 * @param buf 解码缓冲区
	 */
	public void encode(ByteBuffer buf) {
		ByteBuffer dataBuf = buf.slice();
		buf.putInt(messageSize);
		buf.putLong(logOffset);
		dataBuf.limit(buf.position());
		checksum = checksum(dataBuf);
		buf.putInt(checksum);
	}

	private static int checksum(ByteBuffer buf) {
		Adler32 adler32 = new Adler32();
		adler32.update(buf);
		return (int) adler32.getValue();
	}

	/**
	 * 解码item内容
	 *
	 * @param buf 解码缓存区
	 */
	public void decode(ByteBuffer buf) {
		messageSize = buf.getInt();
		logOffset = buf.getLong();
		buf.getInt();
	}

	/**
	 * 解码item内容并检查校验和
	 *
	 * @param buf 解码缓存区
	 * @return 如果校验和错误返回真否则返回假
	 */
	public boolean decodeAndCheck(ByteBuffer buf) {
		ByteBuffer dataBuf = buf.duplicate();
		messageSize = buf.getInt();
		logOffset = buf.getLong();
		dataBuf.limit(buf.position());
		checksum = buf.getInt();
		// 非空白内容或校验和错误返回真
		return isBlank() || checksum != checksum(dataBuf);
	}

	/**
	 * 当前索引项内容是否空白，未编码和解码后字节数据全为0时则表示内容空白
	 *
	 * @return 当前item内容空白返回真否则返回假
	 */
	public boolean isBlank() {
		return messageSize == 0 && logOffset == 0 && checksum == 0;
	}
}
