package pers.zyc.piglet.command;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import pers.zyc.piglet.CommandTypes;
import pers.zyc.piglet.ProducerId;
import pers.zyc.piglet.Serialization;
import pers.zyc.tools.network.Header;
import pers.zyc.tools.network.Request;

/**
 * @author zhangyancheng
 */
public class AddProducer extends Request {
	
	@Getter
	private String topic;
	
	@Getter
	private ProducerId producerId;
	
	public AddProducer(String topic, ProducerId producerId) {
		super(CommandTypes.ADD_PRODUCER);
		this.topic = topic;
		this.producerId = producerId;
	}
	
	public AddProducer(Header header) {
		super(header);
	}
	
	@Override
	protected void encodeBody(ByteBuf byteBuf) throws Exception {
		Serialization.writeString(byteBuf, topic);
		byteBuf.writeBytes(producerId.getProducerId().getBytes(UTF_8));
	}
	
	@Override
	protected void decodeBody(ByteBuf byteBuf) throws Exception {
		topic = Serialization.readString(byteBuf);
		byte[] producerId = new byte[byteBuf.readableBytes()];
		byteBuf.readBytes(producerId);
		this.producerId = new ProducerId(new String(producerId, UTF_8));
	}
}
