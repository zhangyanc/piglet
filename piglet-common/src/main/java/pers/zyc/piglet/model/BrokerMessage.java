package pers.zyc.piglet.model;

import lombok.Getter;
import lombok.Setter;

/**
 * @author zhangyancheng
 */
public class BrokerMessage extends Message {
	
	/**
	 * 消息大小（字节数）
	 */
	@Getter
	@Setter
	private int size;
	
	/**
	 * 日志位置
	 */
	@Getter
	@Setter
	private long logOffset;
	
	/**
	 * 索引位置
	 */
	@Getter
	@Setter
	private long queueOffset;
	
	/**
	 * 客户端地址
	 */
	@Getter
	@Setter
	private byte[] clientAddress;
	
	/**
	 * Broker地址
	 */
	@Getter
	@Setter
	private byte[] serverAddress;
	
	
	@Getter
	@Setter
	private long receiveTime;
}
