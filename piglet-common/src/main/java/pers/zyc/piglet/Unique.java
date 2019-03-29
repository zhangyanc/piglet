package pers.zyc.piglet;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zhangyancheng
 */
public interface Unique {

	AtomicLong SEQUENCE = new AtomicLong();

	default long sequence() {
		return SEQUENCE.getAndIncrement();
	}
}
