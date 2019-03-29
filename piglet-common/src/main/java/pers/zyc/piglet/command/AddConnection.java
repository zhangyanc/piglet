package pers.zyc.piglet.command;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import pers.zyc.piglet.CommandTypes;
import pers.zyc.piglet.ConnectionId;
import pers.zyc.piglet.Language;
import pers.zyc.piglet.Serialization;
import pers.zyc.piglet.model.Connection;
import pers.zyc.tools.network.Header;
import pers.zyc.tools.network.Request;

/**
 * @author zhangyancheng
 */
public class AddConnection extends Request {
	
	@Getter
	private String joiner;
	
	@Getter
	private String token;
	
	@Getter
	private ConnectionId connectionId;
	
	@Getter
	private Language language;
	
	public AddConnection(String joiner, String token, ConnectionId connectionId) {
		super(CommandTypes.ADD_CONNECTION);
		this.joiner = joiner;
		this.token = token;
		this.connectionId = connectionId;
		language = Language.JAVA;
	}
	
	public AddConnection(Header header) {
		super(header);
	}
	
	@Override
	protected void encodeBody(ByteBuf byteBuf) throws Exception {
		Serialization.writeString(byteBuf, joiner);
		Serialization.writeString(byteBuf, token);
		byteBuf.writeByte(language.ordinal());
		byteBuf.writeBytes(connectionId.getConnectionId().getBytes(UTF_8));
	}
	
	@Override
	protected void decodeBody(ByteBuf byteBuf) throws Exception {
		joiner = Serialization.readString(byteBuf);
		token = Serialization.readString(byteBuf);
		language = Language.values()[byteBuf.readByte()];
		byte[] connectionId = new byte[byteBuf.readableBytes()];
		byteBuf.readBytes(connectionId);
		this.connectionId = new ConnectionId(new String(connectionId, UTF_8));
	}
	
	public Connection newConnection() {
		Connection connection = new Connection();
		connection.setJoiner(joiner);
		connection.setLanguage(language);
		connection.setVersion(connectionId.getClientId().getVersion());
		connection.setId(connectionId.getConnectionId());
		connection.setChannel(getChannel());
		return connection;
	}
}
