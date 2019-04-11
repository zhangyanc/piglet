package pers.zyc.piglet.network.command;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import pers.zyc.piglet.Serialization;
import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;
import pers.zyc.piglet.model.*;
import pers.zyc.piglet.network.CommandFactory;
import pers.zyc.tools.network.Header;
import pers.zyc.tools.network.Response;
import pers.zyc.tools.utils.IPUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * @author zhangyancheng
 */
public class GetClusterAck extends Response {
	
	@Getter
	@Setter
	private List<BrokerCluster> clusterList;
	
	@Getter
	@Setter
	private int getTimeout;
	
	@Getter
	@Setter
	private byte[] cachedBody;
	
	public GetClusterAck(int requestId) {
		super(CommandFactory.GET_CLUSTER_ACK, requestId);
	}
	
	public GetClusterAck(Header header) {
		super(header);
	}
	
	@Override
	public void validate() throws Exception {
		if (clusterList == null) {
			throw new SystemException(SystemCode.COMMAND_ARGS_INVALID);
		}
	}
	
	/**
	 * <i>协议格式：</i>
	 * <pre>
	 *     +----------+----------+      +----------+----------+ ---- +----------+----------+
	 *     |   timeout(4B)       |    / |   topic 1           |      |   code(1BL)         |
	 *     +----------+----------+      +----------+----------+ \__  +----------+----------+
	 *     |   cluster size(4B)  |  /   |   group size(4B)    |    | |   name(1BL)         |
	 *     +----------+----------+      +----------+----------+    \ +----------+----------+
	 *     |   cluster 1         |      |   group 1           |      |   level(2B)         |
	 *     +----------+----------+      +----------+----------+      +----------+----------+
	 *     |   cluster 2         |  \   |   group 2           |      |   queues(2B)        |
	 *     +----------+----------+      +----------+----------+      +----------+----------+
	 *     |     .               |   \  |    .                |      |   group size(4B)    |
	 *     |     .               |      |    .                |      +----------+----------+
	 *     |     .               |      |    .                |      |   group code 1(1BL) |
	 *     +----------+----------+      +----------+----------+      +----------+----------+
	 *     |   cluster n         |   ___|   group n           |      |   group code 2(1BL) |
	 *     +----------+----------+  |   +----------+----------+      +----------+----------+
	 *                              |                                |    .                |
	 *                              |-> +----------+----------+      |    .                |
	 *                                  |   name(1BL)         |      |    .                |
	 *                                  +----------+----------+      +----------+----------+
	 *                                  |   group(1BL)        |      |   group code n(1BL) |
	 *                                  +----------+----------+      +----------+----------+
	 *                                  |   ip(4B)            |
	 *                                  +----------+----------+
	 *                                  |   port(2B)          |
	 *                                  +----------+----------+
	 *                                  |   role(2B)          |
	 *                                  +----------+----------+
	 *                                  |   permission(2B)    |
	 *                                  +----------+----------+
	 * </pre>
	 */
	@Override
	public void encodeBody(ByteBuf byteBuf) throws Exception {
		if (cachedBody != null) {
			byteBuf.writeBytes(cachedBody);
		} else {
			byteBuf.writeInt(getTimeout);
			byteBuf.writeInt(clusterList.size());
			clusterList.forEach(brokerCluster -> encodeBrokerCluster(byteBuf, brokerCluster));
		}
	}
	
	private static void encodeBrokerCluster(ByteBuf byteBuf, BrokerCluster brokerCluster) {
		encodeTopic(byteBuf, brokerCluster.getTopic());
		byteBuf.writeInt(brokerCluster.getGroups().size());
		brokerCluster.getGroups().forEach(group -> encodeBrokerGroup(byteBuf, group));
	}
	
	private static void encodeBrokerGroup(ByteBuf byteBuf, BrokerGroup brokerGroup) {
		Serialization.writeString(byteBuf, brokerGroup.getCode());
		byteBuf.writeShort(brokerGroup.getPermission().ordinal());
		byteBuf.writeShort(brokerGroup.getWeight());
		byteBuf.writeInt(brokerGroup.getBrokers().size());
		brokerGroup.getBrokers().forEach(broker -> encodeBroker(byteBuf, broker));
	}
	
	private static void encodeBroker(ByteBuf byteBuf, Broker broker) {
		Serialization.writeString(byteBuf, broker.getName());
		Serialization.writeString(byteBuf, broker.getGroup());
		byteBuf.writeInt(IPUtil.toInt(broker.getIp()));
		byteBuf.writeShort(broker.getPort());
		byteBuf.writeShort(broker.getRole().ordinal());
		byteBuf.writeShort(broker.getPermission().ordinal());
	}
	
	private static BrokerCluster decodeBrokerCluster(ByteBuf byteBuf) {
		BrokerCluster brokerCluster = new BrokerCluster(decodeTopic(byteBuf));
		IntStream.range(0, byteBuf.readInt()).forEach(i -> brokerCluster.addGroup(decodeBrokerGroup(byteBuf)));
		return brokerCluster;
	}
	
	private static BrokerGroup decodeBrokerGroup(ByteBuf byteBuf) {
		BrokerGroup brokerGroup = new BrokerGroup(Serialization.readString(byteBuf));
		brokerGroup.setPermission(Permission.values()[byteBuf.readShort()]);
		brokerGroup.setWeight(byteBuf.readShort());
		IntStream.range(0, byteBuf.readInt()).forEach(i -> brokerGroup.addBroker(decodeBroker(byteBuf)));
		return brokerGroup;
	}
	
	private static Broker decodeBroker(ByteBuf byteBuf) {
		Broker broker = new Broker();
		broker.setName(Serialization.readString(byteBuf));
		broker.setGroup(Serialization.readString(byteBuf));
		broker.setIp(IPUtil.toIp(byteBuf.readInt()));
		broker.setPort(byteBuf.readShort());
		broker.setRole(Broker.Role.values()[byteBuf.readShort()]);
		broker.setPermission(Permission.values()[byteBuf.readShort()]);
		return broker;
	}
	
	private static void encodeTopic(ByteBuf byteBuf, Topic topic) {
		Serialization.writeString(byteBuf, topic.getCode());
		Serialization.writeString(byteBuf, topic.getName());
		byteBuf.writeShort(topic.getLevel().ordinal());
		byteBuf.writeShort(topic.getQueues());
		byteBuf.writeInt(topic.getGroups().size());
		topic.getGroups().forEach(group -> Serialization.writeString(byteBuf, group));
	}
	
	private static Topic decodeTopic(ByteBuf byteBuf) {
		Topic topic = new Topic();
		topic.setCode(Serialization.readString(byteBuf));
		topic.setName(Serialization.readString(byteBuf));
		topic.setLevel(Topic.StoreLevel.values()[byteBuf.readShort()]);
		topic.setQueues(byteBuf.readShort());
		int groupSize = byteBuf.readInt();
		Set<String> groups = new HashSet<>(groupSize);
		IntStream.range(0, groupSize).forEach(i -> groups.add(Serialization.readString(byteBuf)));
		return topic;
	}
	
	@Override
	protected void decodeBody(ByteBuf byteBuf) throws Exception {
		getTimeout = byteBuf.readInt();
		int brokerClusterSize = byteBuf.readInt();
		clusterList = new ArrayList<>(brokerClusterSize);
		IntStream.range(0, brokerClusterSize).forEach(i -> clusterList.add(decodeBrokerCluster(byteBuf)));
	}
}
