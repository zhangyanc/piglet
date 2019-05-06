package pers.zyc.piglet.broker.store;

import java.io.File;

/**
 * @author zhangyancheng
 */
public interface Persistently {

	File getFile();

	void persistent();
}
