package pers.zyc.piglet.broker.store;

import java.io.File;
import java.io.IOException;

/**
 * @author zhangyancheng
 */
public interface Persistently {

	File getFile();

	void persistent() throws IOException;
}
