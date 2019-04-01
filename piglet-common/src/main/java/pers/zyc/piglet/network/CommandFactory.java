package pers.zyc.piglet.network;

import pers.zyc.piglet.CommandTypes;
import pers.zyc.piglet.command.AddConnection;
import pers.zyc.piglet.command.AddProducer;
import pers.zyc.piglet.command.BooleanResponse;
import pers.zyc.tools.network.DefaultCommandFactory;

/**
 * @author zhangyancheng
 */
public class CommandFactory extends DefaultCommandFactory {
	
	public CommandFactory() {
		register(CommandTypes.BOOLEAN_ACK, BooleanResponse.class);
		register(CommandTypes.ADD_CONNECTION, AddConnection.class);
		register(CommandTypes.ADD_PRODUCER, AddProducer.class);
	}
}
