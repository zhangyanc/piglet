package pers.zyc.piglet.model;

import lombok.Getter;

/**
 * @author zhangyancheng
 */
public class Subscription {
	
	@Getter
	private final String topic;
	
	@Getter
	private final String subscriber;
	
	Subscription(String topic, String subscriber) {
		this.topic = topic;
		this.subscriber = subscriber;
	}
}
