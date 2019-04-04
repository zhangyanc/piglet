package pers.zyc.piglet.broker;

import lombok.extern.slf4j.Slf4j;
import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;
import pers.zyc.piglet.network.command.BooleanResponse;
import pers.zyc.tools.network.Request;
import pers.zyc.tools.network.RequestHandleExceptionHandler;
import pers.zyc.tools.network.Response;

/**
 * @author zhangyancheng
 */
@Slf4j
public class BrokerRequestHandleExceptionHandler implements RequestHandleExceptionHandler {
	
	@Override
	public Response handleException(Exception cause, Request request) {
		log.error("Request[:" + request.getType() + "] handle error", cause);
		
		int errorCode = cause instanceof SystemException ?
				((SystemException) cause).getCode() : SystemCode.UNKNOWN_ERROR.getCode();
		return BooleanResponse.create(request.getId(), errorCode, cause.getMessage());
	}
}
