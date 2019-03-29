package pers.zyc.piglet;

/**
 * @author zhangyancheng
 */
public interface CommandTypes {

	static int ackOf(int requestType) {
		return requestType * 10;
	}
	
	int BOOLEAN_ACK = 10000;

	int GET_CLUSTER = 10001;
	int GET_CLUSTER_ACK = ackOf(GET_CLUSTER);

	int ADD_CONNECTION = 10002;
	int ADD_PRODUCER = 10003;
	int ADD_CONSUMER = 10004;
	int REMOVE_CONNECTION = 10005;
	int REMOVE_PRODUCER = 10006;
	int REMOVE_CONSUMER = 10007;
	
	int SEND_MESSAGE = 10010;
	int SEND_MESSAGE_ACK = ackOf(SEND_MESSAGE);
}
