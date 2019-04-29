package pers.zyc.piglet.broker.store;

import lombok.Getter;
import lombok.Setter;
import pers.zyc.tools.utils.SystemMillis;

/**
 * @author zhangyancheng
 */
@Getter
@Setter
public class FlushPolicy {

	private boolean sync;

	private int cumulativeData;

	private int intervalMillis;

	public FlushPolicy() {
		this.sync = true;
	}

	public FlushPolicy(int cumulativeData, int intervalMillis) {
		this.sync = false;
		this.cumulativeData = cumulativeData;
		this.intervalMillis = intervalMillis;
	}

	public FlushCondition createCondition() {
		return new FlushCondition() {

			private int cumulativeDataSize;

			private long lastFlushTime;

			@Override
			public boolean reachedWhen(int dataSize) {
				if (sync) {
					return true;
				}
				long now = SystemMillis.current();
				if (now - lastFlushTime >= intervalMillis) {
					reset(now);
					return true;
				} else {
					cumulativeDataSize += dataSize;
					if (cumulativeDataSize >= cumulativeData) {
						reset(now);
						return true;
					}
				}
				return false;
			}

			private void reset(long flushTime) {
				lastFlushTime = flushTime;
				cumulativeDataSize = 0;
			}
		};
	}
}
