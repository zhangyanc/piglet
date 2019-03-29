package pers.zyc.piglet.command;

import io.netty.buffer.ByteBuf;
import pers.zyc.piglet.CommandTypes;
import pers.zyc.piglet.Serialization;
import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;
import pers.zyc.piglet.model.Message;
import pers.zyc.tools.network.Header;
import pers.zyc.tools.network.Request;

import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author zhangyancheng
 */
public class SendMessage extends Request {
	
	private Message[] messages;
	
	public SendMessage(Message[] messages) {
		super(CommandTypes.SEND_MESSAGE);
		this.messages = messages;
	}
	
	public SendMessage(Header header) {
		super(header);
	}
	
	@Override
	public void validate() throws Exception {
		if (messages == null || messages.length == 0) {
			throw new SystemException(SystemCode.COMMAND_ARGS_INVALID);
		}
	}
	
	@Override
	protected void encodeBody(ByteBuf byteBuf) throws Exception {
		byteBuf.writeInt(messages.length);
		Stream.of(messages).forEach(message -> Serialization.writeMessage(byteBuf, message));
	}
	
	@Override
	protected void decodeBody(ByteBuf byteBuf) throws Exception {
		messages = new Message[byteBuf.readInt()];
		IntStream.range(0, messages.length).forEach(i -> messages[i] = Serialization.readMessage(byteBuf));
	}
}
