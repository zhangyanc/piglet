package pers.zyc.piglet.broker.store;

import org.junit.Test;

/**
 * @author zhangyancheng
 */
public class StoreTest {

	@Test
	public void case_recover() throws Exception {
		StoreConfig config = new StoreConfig("E:/export/pt");

		DefaultStore store = new DefaultStore(config);
		store.start();

		/*BrokerMessage message = new BrokerMessage();
		message.setTopic("topic_x1");
		message.setProducer("producer_x1");

		LocalDateTime ldt = LocalDateTime.parse("2019-04-03T10:15:30");
		message.setClientSendTime(ldt.toEpochSecond(ZoneOffset.UTC));

		message.setBody("test msg x1".getBytes());
		message.setClientAddress(IPUtil.toBytes("172.52.49.30"));
		message.setServerAddress(IPUtil.toBytes("172.52.12.189"));

		store.putMessage(message);*/

		//TimeUnit.SECONDS.sleep(35);

		store.stop();
	}
}
