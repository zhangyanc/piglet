package pers.zyc.piglet.model;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import pers.zyc.piglet.ChecksumException;
import pers.zyc.piglet.LenType;
import pers.zyc.piglet.Serialization;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.Adler32;

/**
 * @author zhangyancheng
 */
@Getter
public class Message {
	
	/**
	 * 主题
	 */
	@Setter
	private String topic;

	/**
	 * 生产者
	 */
	@Setter
	private String producer;

	/**
	 * 发送时间
	 */
	@Setter
	private long clientSendTime;

	/**
	 * 消息体
	 */
	private byte[] body;

	/**
	 * body数据校验和
	 */
	private int checksum;
	
	/**
	 * 属性
	 */
	@Setter
	private Map<String, String> properties;

	/**
	 * 设置消息体
	 *
	 * @param body 消息体
	 */
	public void setBody(byte[] body) {
		this.body = Objects.requireNonNull(body);
		Adler32 adler32 = new Adler32();
		adler32.update(body);
		this.checksum = (int) adler32.getValue();
	}

	/**
	 * 添加附加属性
	 *
	 * @param key 属性键
	 * @param value 属性值
	 */
	public void addProperty(String key, String value) {
		if (properties == null) {
			properties = new HashMap<>();
		}
		properties.put(key, value);
	}

	/**
	 * 获取属性值
	 *
	 * @param key 属性键
	 * @return 属性值
	 */
	public Optional<String> getProperty(String key) {
		return Optional.ofNullable(properties).map(properties -> properties.get(key));
	}

	public void encode(ByteBuf buf) {
		Serialization.writeString(buf, topic);
		Serialization.writeString(buf, producer);
		buf.writeLong(clientSendTime);
		Serialization.writeBytes(buf, body);
		buf.writeInt(checksum);
		encodeProperties(buf);
	}

	private void encodeProperties(ByteBuf buf) {
		buf.writeInt(properties == null ? -1 : properties.size());// 写入-1长度，区分null和空两种情况
		if (properties != null) {
			properties.forEach((k, v) -> {
				Serialization.writeString(buf, k);
				Serialization.writeString(buf, v, LenType.INT);
			});
		}
	}

	private void decodeProperties(ByteBuf buf) {
		int propertySize = buf.readInt();
		if (propertySize >= 0) {
			properties = new HashMap<>();
			while (propertySize-- > 0) {
				properties.put(Serialization.readString(buf), Serialization.readString(buf, LenType.INT));
			}
		}
	}

	public void decode(ByteBuf buf) {
		topic = Serialization.readString(buf);
		producer = Serialization.readString(buf);
		clientSendTime = buf.readLong();
		setBody(Serialization.readBytes(buf));
		if (checksum != buf.readInt()) {
			throw new ChecksumException();
		}
		decodeProperties(buf);
	}
}
