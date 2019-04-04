package pers.zyc.piglet.model;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangyancheng
 */
public class BrokerGroup {
	
	/**
	 * 分组
	 */
	@Getter
	private final String code;
	
	@Getter
	@Setter
	private Permission permission;
	
	@Getter
	@Setter
	private short weight;
	
	/**
	 * 组内Broker列表
	 */
	@Getter
	private final List<Broker> brokers = new ArrayList<>(3);
	
	public BrokerGroup(@NonNull String code) {
		this.code = code;
	}
	
	public void addBroker(@NonNull Broker broker) {
		brokers.add(broker);
	}
	
	public Broker getBroker(String brokerName) {
		for (Broker broker : brokers) {
			if (broker.getName().equals(brokerName)) {
				return broker;
			}
		}
		return null;
	}
	
	public boolean contains(String brokerName) {
		return getBroker(brokerName) != null;
	}
}
