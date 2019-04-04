package pers.zyc.piglet.network.command;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import lombok.Getter;
import pers.zyc.piglet.ConnectionId;
import pers.zyc.piglet.IPUtil;
import pers.zyc.piglet.Language;
import pers.zyc.piglet.Serialization;
import pers.zyc.piglet.model.Connection;
import pers.zyc.piglet.network.CommandFactory;
import pers.zyc.tools.network.Header;
import pers.zyc.tools.network.Request;

import java.net.InetSocketAddress;

/**
 * @author zhangyancheng
 */
public class AddConnection extends Request {
	
	@Getter
	private String subscriber;
	
	@Getter
	private String token;
	
	@Getter
	private ConnectionId connectionId;
	
	@Getter
	private Language language;
	
	public AddConnection(String subscriber, String token, ConnectionId connectionId) {
		super(CommandFactory.ADD_CONNECTION);
		this.subscriber = subscriber;
		this.token = token;
		this.connectionId = connectionId;
		language = Language.JAVA;
	}
	
	public AddConnection(Header header) {
		super(header);
	}
	
	@Override
	protected void encodeBody(ByteBuf byteBuf) throws Exception {
		Serialization.writeString(byteBuf, subscriber);
		Serialization.writeString(byteBuf, token);
		byteBuf.writeByte(language.ordinal());
		byteBuf.writeBytes(connectionId.getConnectionId().getBytes(UTF_8));
	}
	
	@Override
	protected void decodeBody(ByteBuf byteBuf) throws Exception {
		subscriber = Serialization.readString(byteBuf);
		token = Serialization.readString(byteBuf);
		language = Language.values()[byteBuf.readByte()];
		byte[] connectionId = new byte[byteBuf.readableBytes()];
		byteBuf.readBytes(connectionId);
		this.connectionId = new ConnectionId(new String(connectionId, UTF_8));
	}
	
	public Connection newConnection() {
		Connection connection = new Connection();
		connection.setSubscriber(subscriber);
		connection.setLanguage(language);
		connection.setVersion(connectionId.getClientId().getVersion());
		connection.setId(connectionId.getConnectionId());
		Channel channel = getChannel();
		connection.setChannel(channel);
		connection.setClientAddress(IPUtil.toBytes((InetSocketAddress) channel.remoteAddress()));
		connection.setServerAddress(IPUtil.toBytes((InetSocketAddress) channel.localAddress()));
		return connection;
	}
}
