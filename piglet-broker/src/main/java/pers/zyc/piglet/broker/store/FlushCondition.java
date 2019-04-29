package pers.zyc.piglet.broker.store;

/**
 * @author zhangyancheng
 */
public interface FlushCondition {

	boolean reachedWhen(int dataSize);
}
