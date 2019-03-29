package pers.zyc.piglet.model;

import lombok.Getter;
import lombok.Setter;

/**
 * @author zhangyancheng
 */
public class ConsumePolicy {
	
	@Getter
	@Setter
	private String consumer;
	
	/**
	 * 消费节点
	 */
	@Getter
	@Setter
	private Broker.Role consumeRole;
	
	/**
	 * 应答超时（ms）
	 */
	@Getter
	@Setter
	private long ackTimeout;
	
	/**
	 * 批量拉取条数
	 */
	@Getter
	@Setter
	private int batchSize;
	
	/**
	 * 消费延迟时间（ms）
	 */
	@Getter
	@Setter
	private long delay;
	
	/**
	 * 消息选择器
	 */
	@Getter
	@Setter
	private String selector;
	
	/**
	 * 重试策略
	 */
	@Getter
	@Setter
	private RetryPolicy retryPolicy;
	
	public static class RetryPolicy {
		
	}
}
