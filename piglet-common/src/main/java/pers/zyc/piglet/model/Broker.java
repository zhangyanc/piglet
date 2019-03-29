package pers.zyc.piglet.model;

import lombok.Getter;
import lombok.Setter;

/**
 * @author zhangyancheng
 */
@Setter
@Getter
public class Broker {
	
	public enum Role {
		MASTER, SLAVE, BACKUP
	}
	
	/**
	 * IP地址
	 */
	private String ip;
	
	/**
	 * 消息服务端口
	 */
	private int port;
	
	/**
	 * 名称
	 */
	private String name;
	
	/**
	 * 分组
	 */
	private String group;
	
	/**
	 * 角色
	 */
	private Role role;
	
	/**
	 * 权限
	 */
	private Permission permission;
	
	/**
	 * 机房
	 */
	private DataCenter dataCenter;
}
