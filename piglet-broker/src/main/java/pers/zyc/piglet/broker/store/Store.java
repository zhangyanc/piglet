package pers.zyc.piglet.broker.store;

import pers.zyc.piglet.model.BrokerMessage;
import pers.zyc.tools.utils.lifecycle.Lifecycle;

/**
 * @author zhangyancheng
 */
public interface Store extends Lifecycle {
	
	StoreConfig getConfig();
	
	void putMessage(BrokerMessage message);
	
	PutResult putMessage(BrokerMessage[] messages);
}
