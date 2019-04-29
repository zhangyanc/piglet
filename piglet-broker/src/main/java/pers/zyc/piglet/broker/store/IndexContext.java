package pers.zyc.piglet.broker.store;

import lombok.Getter;
import lombok.Setter;

/**
 * @author zhangyancheng
 */
@Getter
public class IndexContext extends IndexItem {

	private final boolean recover;

	@Setter
	private IndexQueue queue;

	public IndexContext() {
		this(false);
	}

	public IndexContext(boolean recover) {
		this.recover = recover;
	}
}
