package pers.zyc.piglet.broker.store;

import java.io.File;

/**
 * @author zhangyancheng
 */
public interface MapFile {

	File getFile();

	void flush();
}
