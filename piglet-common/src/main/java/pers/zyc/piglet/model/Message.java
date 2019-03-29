package pers.zyc.piglet.model;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.zip.Adler32;

/**
 * @author zhangyancheng
 */
public class Message {
	
	/**
	 * 主题
	 */
	@Getter
	@Setter
	private String topic;
	
	/**
	 * 接入者
	 */
	@Getter
	@Setter
	private String joiner;
	
	/**
	 * 消息体
	 */
	@Getter
	private byte[] body;
	
	/**
	 * 发送时间
	 */
	@Getter
	@Setter
	private long clientSendTime;
	
	/**
	 * body数据校验和
	 */
	@Getter
	private long checksum;
	
	/**
	 * 属性
	 */
	@Getter
	@Setter
	private Map<String, String> properties;

	public void setBody(byte[] body) {
		this.body = Objects.requireNonNull(body);
		Adler32 adler32 = new Adler32();
		adler32.update(body);
		this.checksum = adler32.getValue();
	}
	
	public void addProperty(String key, String value) {
		if (properties == null) {
			properties = new HashMap<>();
		}
		properties.put(key, value);
	}
	
	public String getProperty(String key) {
		return properties == null ? null : properties.get(key);
	}
}
