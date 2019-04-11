package pers.zyc.piglet.model;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author zhangyancheng
 */
public class BrokerGroup implements Cloneable {
	
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
	private List<Broker> brokers;
	
	public BrokerGroup(@NonNull String code) {
		this.code = code;
	}
	
	public void addBroker(@NonNull Broker broker) {
		if (brokers == null) {
			brokers = new ArrayList<>(3);
		}
		brokers.add(broker);
	}
	
	public Optional<Broker> getBroker(String brokerName) {
		for (Broker broker : brokers) {
			if (broker.getName().equals(brokerName)) {
				return Optional.of(broker);
			}
		}
		return Optional.empty();
	}
	
	public boolean contains(String brokerName) {
		return getBroker(brokerName).isPresent();
	}
	
	@Override
	public final BrokerGroup clone() {
		try {
			BrokerGroup brokerGroup = (BrokerGroup) super.clone();
			brokerGroup.brokers = new ArrayList<>();
			this.brokers.stream().map(Broker::new).forEach(brokerGroup::addBroker);
			return brokerGroup;
		} catch (CloneNotSupportedException e) {
			// this shouldn't happen, since we are Cloneable
			throw new Error(e);
		}
	}
}
