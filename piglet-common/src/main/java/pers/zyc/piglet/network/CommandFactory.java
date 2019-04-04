package pers.zyc.piglet.network;

import pers.zyc.piglet.network.command.*;
import pers.zyc.tools.network.DefaultCommandFactory;

/**
 * @author zhangyancheng
 */
public class CommandFactory extends DefaultCommandFactory {
	
	public CommandFactory() {
		register(BOOLEAN_ACK, BooleanResponse.class);
		register(GET_CLUSTER, GetCluster.class);
		register(GET_CLUSTER_ACK, GetClusterAck.class);
		register(ADD_CONNECTION, AddConnection.class);
		register(ADD_PRODUCER, AddProducer.class);
	}
	
	private static int ackOf(int requestType) {
		return requestType * 10;
	}
	
	public static final int BOOLEAN_ACK = 10000;
	
	public static final int GET_CLUSTER = 10001;
	public static final int GET_CLUSTER_ACK = ackOf(GET_CLUSTER);
	
	public static final int ADD_CONNECTION = 10002;
	public static final int ADD_PRODUCER = 10003;
	public static final int ADD_CONSUMER = 10004;
	public static final int REMOVE_CONNECTION = 10005;
	public static final int REMOVE_PRODUCER = 10006;
	public static final int REMOVE_CONSUMER = 10007;
	
	public static final int SEND_MESSAGE = 10010;
	public static final int SEND_MESSAGE_ACK = ackOf(SEND_MESSAGE);
}
