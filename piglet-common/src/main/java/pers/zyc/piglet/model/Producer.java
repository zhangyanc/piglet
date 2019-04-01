package pers.zyc.piglet.model;

import lombok.Getter;
import lombok.Setter;

/**
 * @author zhangyancheng
 */
public class Producer extends Subscription {
	
	@Getter
	@Setter
	private String id;
	
	@Getter
	@Setter
	private String connectionId;
	
	public Producer(String topic, String subscriber) {
		super(topic, subscriber);
	}
}
