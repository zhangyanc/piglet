package pers.zyc.piglet.model;

import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangyancheng
 */
public class BrokerCluster {
	
	/**
	 * 主题
	 */
	@Getter
	private final String topic;
	
	/**
	 * 主题分组列表
	 */
	@Getter
	private final List<BrokerGroup> groups = new ArrayList<>();
	
	public BrokerCluster(@NonNull String topic) {
		this.topic = topic;
	}
	
	public void addGroup(BrokerGroup group) {
		groups.add(group);
	}
	
	public BrokerGroup getGroup(String groupCode) {
		for (BrokerGroup group : groups) {
			if (group.getCode().equals(groupCode)) {
				return group;
			}
		}
		return null;
	}
	
	public boolean contains(String groupCode) {
		return getGroup(groupCode) != null;
	}
}
