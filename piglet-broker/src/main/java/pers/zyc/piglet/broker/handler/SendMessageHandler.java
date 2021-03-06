package pers.zyc.piglet.broker.handler;

import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;
import pers.zyc.piglet.broker.store.Store;
import pers.zyc.piglet.model.BrokerMessage;
import pers.zyc.piglet.model.Connection;
import pers.zyc.piglet.model.Producer;
import pers.zyc.piglet.network.CommandFactory;
import pers.zyc.piglet.network.command.BooleanResponse;
import pers.zyc.piglet.network.command.SendMessage;
import pers.zyc.tools.network.Response;
import pers.zyc.tools.network.SingleTypeRequestHandler;
import pers.zyc.tools.utils.SystemMillis;

import java.util.stream.Stream;

/**
 * @author zhangyancheng
 */
public class SendMessageHandler extends SingleTypeRequestHandler<SendMessage> {
	
	private final Store store;
	
	public SendMessageHandler(Store store) {
		this.store = store;
	}
	
	@Override
	public int supportedRequestType() {
		return CommandFactory.SEND_MESSAGE;
	}
	
	@Override
	protected Response handle0(SendMessage request) throws Exception {
		Connection connection = request.getChannel().attr(SessionHandler.CONNECTION_ATTRIBUTE_KEY).get();
		Producer producer = request.getChannel().attr(SessionHandler.PRODUCER_ATTRIBUTE_KEY).get();
		if (connection == null) {
			throw new SystemException(SystemCode.CONNECTION_NOT_EXISTS);
		}
		if (producer == null) {
			throw new SystemException(SystemCode.PRODUCER_NOT_EXISTS);
		}

		Stream.of(request.getMessages()).map(msg -> {
			BrokerMessage bMsg = new BrokerMessage(msg);
			bMsg.setReceiveTime(SystemMillis.current());
			bMsg.setClientAddress(connection.getClientAddress());
			bMsg.setServerAddress(connection.getServerAddress());
			return bMsg;
		}).forEach(store::putMessage);

		return BooleanResponse.create(request.getId());
	}
}
