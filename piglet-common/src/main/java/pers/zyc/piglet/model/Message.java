package pers.zyc.piglet.model;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import pers.zyc.piglet.LenType;
import pers.zyc.piglet.Serialization;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author zhangyancheng
 */
@Getter
@Setter
public class Message {
	
	/**
	 * 主题
	 */
	private String topic;

	/**
	 * 生产者
	 */
	private String producer;

	/**
	 * 发送时间
	 */
	private long clientSendTime;

	/**
	 * 消息体
	 */
	private byte[] body;
	
	/**
	 * 属性
	 */
	private Map<String, String> properties;

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
		body = Serialization.readBytes(buf);
		decodeProperties(buf);
	}
}
