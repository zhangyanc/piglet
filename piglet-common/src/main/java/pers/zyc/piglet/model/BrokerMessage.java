package pers.zyc.piglet.model;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import pers.zyc.piglet.ChecksumException;
import pers.zyc.piglet.ChecksumUtil;

import java.util.Optional;

/**
 * @author zhangyancheng
 */
@Getter
@Setter
public class BrokerMessage extends Message {

	public static final int INDEX_OFFSET_WRITE_INDEX = 4 + 1;// 4字节长度 + 1字节队列号
	public static final int LOG_OFFSET_WRITE_INDEX = INDEX_OFFSET_WRITE_INDEX + 8;// + 8字节索引偏移量长度
	public static final int STORE_TIME_WRITE_INDEX = 4 + 1 + 8 + 8 + 4 + 4 + 4;

	/**
	 * 消息大小（字节数）
	 */
	private int size;
	
	/**
	 * 日志位置
	 */
	private long logOffset;

	/**
	 * 队列号
	 */
	private short queueNum;

	/**
	 * 索引位置
	 */
	private long indexOffset;
	
	/**
	 * 客户端地址
	 */
	private byte[] clientAddress;
	
	/**
	 * Broker地址
	 */
	private byte[] serverAddress;

	/**
	 * 服务端接收时间
	 */
	private long receiveTime;

	/**
	 * 存储时间
	 */
	private long storeTime;

	public BrokerMessage() {
	}

	public BrokerMessage(Message message) {
		setTopic(message.getTopic());
		setProducer(message.getProducer());
		setClientSendTime(message.getClientSendTime());
		setBody(message.getBody());
		setProperties(message.getProperties());
	}

	@Override
	public void encode(ByteBuf buf) {
		int begin = buf.writerIndex();

		buf.writeInt(0);// 总大小待全部编码后再回写
		buf.writeByte(queueNum);
		buf.writeLong(indexOffset);
		buf.writeLong(logOffset);
		buf.writeBytes(Optional.ofNullable(clientAddress).orElse(new byte[4]));
		buf.writeBytes(Optional.ofNullable(serverAddress).orElse(new byte[4]));
		buf.writeInt((int) (receiveTime - getClientSendTime()));
		buf.writeInt(0);// 存储时间单独写入
		buf.writeInt(ChecksumUtil.calc(getBody()));
		super.encode(buf);

		int end = buf.writerIndex();
		size = end - begin;
		buf.writerIndex(begin);
		buf.writeInt(size);
		buf.writerIndex(end);
	}

	@Override
	public void decode(ByteBuf buf) {
		size = buf.readInt();

		queueNum = buf.readByte();
		indexOffset = buf.readLong();
		logOffset = buf.readLong();
		clientAddress = serverAddress = new byte[4];
		buf.readBytes(clientAddress);
		buf.readBytes(serverAddress);
		int receiveTimeDiff = buf.readInt();
		int storeTimeDiff = buf.readInt();
		int checksum = buf.readInt();
		super.decode(buf);

		if (checksum != ChecksumUtil.calc(getBody())) {
			throw new ChecksumException();
		}
		receiveTime = getClientSendTime() + receiveTimeDiff;
		storeTime = getClientSendTime() + storeTimeDiff;
	}
}
