package pers.zyc.piglet.broker.store;

import lombok.Getter;
import lombok.Setter;

/**
 * @author zhangyancheng
 */
@Getter
public class StoreEvent {

	public enum EventType {
		EXCEPTION
	}

	private final EventType eventType;

	@Setter
	private Object payload;

	public StoreEvent(EventType eventType) {
		this.eventType = eventType;
	}

	public static StoreEvent create(Exception e) {
		StoreEvent exceptionEvent = new StoreEvent(EventType.EXCEPTION);
		exceptionEvent.setPayload(e);
		return exceptionEvent;
	}
}
