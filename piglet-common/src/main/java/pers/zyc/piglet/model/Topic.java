package pers.zyc.piglet.model;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.Set;

/**
 * @author zhangyancheng
 */
@Getter
@Setter
public class Topic {
	
	public enum StoreLevel {
		/**
		 * 写到broker内存
		 */
		MEMORY,
		
		/**
		 * 写入文件
		 */
		FILE,
		
		/**
		 * 从文件刷入磁盘
		 */
		FLUSH
	}
	
	/**
	 * 主题代码
	 */
	private String code;
	
	/**
	 * 名称或者描述
	 */
	private String name;
	
	/**
	 * 存储级别
	 */
	private StoreLevel level;
	
	/**
	 * 是否广播
	 */
	private boolean broadcast;
	
	/**
	 * 队列数
	 */
	private int queues;
	
	/**
	 * 分组
	 */
	private Set<String> groups;
	
	/**
	 * 消费者策略
	 */
	private Map<String, ConsumePolicy> consumers;
	
	/**
	 * 生产者策略
	 */
	private Map<String, ProducePolicy> producers;
}
