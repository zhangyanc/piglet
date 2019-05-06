package pers.zyc.piglet;

import pers.zyc.tools.utils.CallAction;
import pers.zyc.tools.utils.RunAction;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * @author zhangyancheng
 */
public interface IOExecutor {

	interface IORunAction extends RunAction<IOException> {}

	interface IOCallAction<T> extends CallAction<T, IOException> {}

	static void execute(IORunAction action) {
		try {
			action.run();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	static <T> T execute(IOCallAction<T> action) {
		try {
			return action.call();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
