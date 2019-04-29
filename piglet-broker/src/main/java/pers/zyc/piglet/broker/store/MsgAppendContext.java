package pers.zyc.piglet.broker.store;

import io.netty.buffer.ByteBuf;
import pers.zyc.piglet.model.BrokerMessage;

import java.util.concurrent.CountDownLatch;

/**
 * @author zhangyancheng
 */
class MsgAppendContext {
	final BrokerMessage message;
	final ByteBuf buf;
	final boolean flush;
	final CountDownLatch latch;

	MsgAppendContext(BrokerMessage message,
	                 ByteBuf buf,
	                 boolean flush,
	                 CountDownLatch latch) {
		this.message = message;
		this.buf = buf;
		this.flush = flush;
		this.latch = latch;
	}

	void setIndexOffset(long indexOffset) {
		message.setIndexOffset(indexOffset);
		buf.setLong(BrokerMessage.INDEX_OFFSET_WRITE_INDEX, indexOffset);
	}

	void setLogOffset(long logOffset) {
		message.setLogOffset(logOffset);
		buf.setLong(BrokerMessage.LOG_OFFSET_WRITE_INDEX, logOffset);
	}
}
