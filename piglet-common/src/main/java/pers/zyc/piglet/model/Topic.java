package pers.zyc.piglet.model;

import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;

/**
 * @author zhangyancheng
 */
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
	@Setter
	@Getter
	private String code;
	
	/**
	 * 名称或者描述
	 */
	@Setter
	@Getter
	private String name;
	
	/**
	 * 存储级别
	 */
	@Setter
	@Getter
	private StoreLevel level;
	
	/**
	 * 是否广播
	 */
	@Setter
	@Getter
	private boolean broadcast;
	
	/**
	 * 队列数
	 */
	@Setter
	@Getter
	private int queues;
	
	/**
	 * 分组
	 */
	@Getter
	private final Set<String> groups = new HashSet<>();
}
