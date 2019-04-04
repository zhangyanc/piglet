package pers.zyc.piglet.network.command;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import pers.zyc.piglet.network.CommandTypes;
import pers.zyc.piglet.Serialization;
import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;
import pers.zyc.tools.network.Header;
import pers.zyc.tools.network.Request;

/**
 * @author zhangyancheng
 */
public class GetCluster extends Request {
	
	@Getter
	@Setter
	private String subscriber;
	
	@Getter
	@Setter
	private String clientId;
	
	public GetCluster() {
		super(CommandTypes.GET_CLUSTER);
	}
	
	public GetCluster(Header header) {
		super(header);
	}
	
	@Override
	public void validate() throws Exception {
		if (StringUtils.isBlank(subscriber) || StringUtils.isBlank(clientId)) {
			throw new SystemException(SystemCode.COMMAND_ARGS_INVALID);
		}
	}
	
	@Override
	protected void encodeBody(ByteBuf byteBuf) throws Exception {
		Serialization.writeString(byteBuf, subscriber);
		Serialization.writeString(byteBuf, clientId);
	}
	
	@Override
	protected void decodeBody(ByteBuf byteBuf) throws Exception {
		subscriber = Serialization.readString(byteBuf);
		clientId = Serialization.readString(byteBuf);
	}
}
