package pers.zyc.piglet.network;

import pers.zyc.piglet.command.*;
import pers.zyc.piglet.network.command.*;
import pers.zyc.tools.network.DefaultCommandFactory;

/**
 * @author zhangyancheng
 */
public class CommandFactory extends DefaultCommandFactory {
	
	public CommandFactory() {
		register(CommandTypes.BOOLEAN_ACK, BooleanResponse.class);
		register(CommandTypes.GET_CLUSTER, GetCluster.class);
		register(CommandTypes.GET_CLUSTER_ACK, GetClusterAck.class);
		register(CommandTypes.ADD_CONNECTION, AddConnection.class);
		register(CommandTypes.ADD_PRODUCER, AddProducer.class);
	}
}
