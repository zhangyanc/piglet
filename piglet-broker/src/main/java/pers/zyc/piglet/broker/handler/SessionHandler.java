package pers.zyc.piglet.broker.handler;

import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import pers.zyc.piglet.*;
import pers.zyc.piglet.broker.SessionManager;
import pers.zyc.piglet.broker.auth.Authentication;
import pers.zyc.piglet.command.AddConnection;
import pers.zyc.piglet.command.AddProducer;
import pers.zyc.piglet.command.BooleanResponse;
import pers.zyc.piglet.model.Connection;
import pers.zyc.piglet.model.Producer;
import pers.zyc.tools.network.BaseRequestHandler;
import pers.zyc.tools.network.Request;
import pers.zyc.tools.network.Response;

/**
 * @author zhangyancheng
 */
@Slf4j
public class SessionHandler extends BaseRequestHandler {
	
	static final AttributeKey<Connection> CONNECTION_ATTRIBUTE_KEY = AttributeKey.valueOf("connection");
	static final AttributeKey<Producer> PRODUCER_ATTRIBUTE_KEY = AttributeKey.valueOf("producer");
	
	private final Authentication authentication;
	private final SessionManager sessionManager;
	
	public SessionHandler(Authentication authentication,
	                      SessionManager sessionManager) {
		this.authentication = authentication;
		this.sessionManager = sessionManager;
	}
	
	@Override
	public Response handle(Request request) throws Exception {
		switch (request.getType()) {
			case CommandTypes.ADD_CONNECTION:
				return addConnection((AddConnection) request);
			case CommandTypes.ADD_PRODUCER:
				return addProducer((AddProducer) request);
		}
		return null;
	}
	
	private BooleanResponse addConnection(AddConnection addConnection) {
		authentication.auth(addConnection.getSubscriber(), addConnection.getToken());
		Connection connection = addConnection.newConnection();
		if (!sessionManager.addConnection(connection)) {
			log.warn("{}, cid: {}", SystemCode.CONNECTION_ALREADY_EXISTS.getMsg(), addConnection.getConnectionId());
			throw new SystemException(SystemCode.CONNECTION_ALREADY_EXISTS);
		}
		addConnection.getChannel().attr(CONNECTION_ATTRIBUTE_KEY).set(connection);
		return BooleanResponse.create(addConnection.getId());
	}
	
	private BooleanResponse addProducer(AddProducer addProducer) {
		String topic = addProducer.getTopic();
		ProducerId producerId = addProducer.getProducerId();
		ConnectionId connectionId = producerId.getConnectionId();
		
		Connection connection = sessionManager.getConnection(connectionId.getConnectionId());
		if (connection == null) {
			log.error("{}, cid: {}, topic: {}", SystemCode.CONNECTION_NOT_EXISTS.getMsg(),
					connectionId.getConnectionId(), topic);
			throw new SystemException(SystemCode.CONNECTION_NOT_EXISTS);
		}
		
		Producer producer = new Producer(topic, connection.getSubscriber());
		producer.setId(producerId.getProducerId());
		producer.setConnectionId(connectionId.getConnectionId());
		if (!sessionManager.addProducer(producer)) {
			log.warn("{}, pid: {}, topic: {}", SystemCode.PRODUCER_ALREADY_EXISTS.getMsg(), producer.getId(), topic);
			throw new SystemException(SystemCode.PRODUCER_ALREADY_EXISTS);
		}
		return BooleanResponse.create(addProducer.getId());
	}
}
