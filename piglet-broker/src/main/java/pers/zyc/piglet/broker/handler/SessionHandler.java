package pers.zyc.piglet.broker.handler;

import pers.zyc.piglet.CommandTypes;
import pers.zyc.piglet.ProducerId;
import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;
import pers.zyc.piglet.broker.SessionManager;
import pers.zyc.piglet.broker.auth.Authentication;
import pers.zyc.piglet.command.AddConnection;
import pers.zyc.piglet.command.AddProducer;
import pers.zyc.piglet.command.BooleanResponse;
import pers.zyc.piglet.model.Connection;
import pers.zyc.tools.network.BaseRequestHandler;
import pers.zyc.tools.network.Request;
import pers.zyc.tools.network.Response;

/**
 * @author zhangyancheng
 */
public class SessionHandler extends BaseRequestHandler {
	
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
				return handleAddConnection((AddConnection) request);
			case CommandTypes.ADD_PRODUCER:
				return handleAddProducer((AddProducer) request);
		}
		return null;
	}
	
	private BooleanResponse handleAddConnection(AddConnection addConnection) {
		authentication.auth(addConnection.getJoiner(), addConnection.getToken());
		if (!sessionManager.addConnection(addConnection.newConnection())) {
			throw new SystemException(SystemCode.CONNECTION_ALREADY_EXISTS);
		}
		return BooleanResponse.create(addConnection.getId());
	}
	
	private BooleanResponse handleAddProducer(AddProducer addProducer) {
		ProducerId producerId = addProducer.getProducerId();
		
		Connection connection = sessionManager.getConnection(producerId.getConnectionId().getConnectionId());
		if (connection == null) {
			throw new SystemException(SystemCode.CONNECTION_NOT_EXISTS);
		}
		
		return null;
	}
}
