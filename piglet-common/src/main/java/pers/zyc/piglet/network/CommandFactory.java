package pers.zyc.piglet.network;

import pers.zyc.piglet.CommandTypes;
import pers.zyc.piglet.command.AddConnection;
import pers.zyc.piglet.command.AddProducer;
import pers.zyc.piglet.command.BooleanResponse;
import pers.zyc.tools.network.Command;
import pers.zyc.tools.network.DefaultCommandFactory;
import pers.zyc.tools.network.Header;

/**
 * @author zhangyancheng
 */
public class CommandFactory implements pers.zyc.tools.network.CommandFactory {
	
	private final DefaultCommandFactory commandFactory = new DefaultCommandFactory();
	
	{
		commandFactory.register(CommandTypes.BOOLEAN_ACK, BooleanResponse.class);
		commandFactory.register(CommandTypes.ADD_CONNECTION, AddConnection.class);
		commandFactory.register(CommandTypes.ADD_PRODUCER, AddProducer.class);
	}
	
	@Override
	public Command createByHeader(Header header) {
		return commandFactory.createByHeader(header);
	}
}
