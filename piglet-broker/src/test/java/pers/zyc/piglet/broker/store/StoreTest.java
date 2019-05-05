package pers.zyc.piglet.broker.store;

import org.junit.Test;
import pers.zyc.piglet.model.BrokerMessage;
import pers.zyc.tools.utils.IPUtil;
import pers.zyc.tools.utils.SystemMillis;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @author zhangyancheng
 */
public class StoreTest {

	@Test
	public void case_recover() throws Exception {
		StoreConfig config = new StoreConfig("E:/export/pt");

		DefaultStore store = new DefaultStore(config);
		store.start();

		System.out.println("");
		store.stop();
	}

	@Test
	public void case_putMessage() throws Exception {
		StoreConfig config = new StoreConfig("E:/export/pt");

		DefaultStore store = new DefaultStore(config);
		store.start();

		store.getIndexService().addTopic("topic_x1", (short) 1);


		Executor executor = Executors.newFixedThreadPool(1);

		int count = 1;

		CountDownLatch latch = new CountDownLatch(count);
		long begin = SystemMillis.current();
		while (count-- > 0) {
			executor.execute(() -> {
				BrokerMessage message = new BrokerMessage();
				message.setTopic("topic_x1");
				message.setProducer("producer_x2");

				LocalDateTime ldt = LocalDateTime.parse("2019-04-03T10:15:30");
				message.setClientSendTime(ldt.toInstant(ZoneOffset.UTC).toEpochMilli());

				message.setBody(new byte[1024 * 1024 * 5]);
				message.setClientAddress(IPUtil.toBytes("172.52.49.30"));
				message.setServerAddress(IPUtil.toBytes("172.52.12.189"));

				store.putMessage(message);
				latch.countDown();
			});
		}
		latch.await();
		System.out.println(SystemMillis.current() - begin);
		store.stop();
	}
}
