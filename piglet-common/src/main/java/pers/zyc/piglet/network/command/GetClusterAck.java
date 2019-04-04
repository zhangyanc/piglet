package pers.zyc.piglet.network.command;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import pers.zyc.piglet.network.CommandTypes;
import pers.zyc.piglet.Serialization;
import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;
import pers.zyc.piglet.model.*;
import pers.zyc.tools.network.Header;
import pers.zyc.tools.network.Response;

import java.util.*;
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
	private Map<String, Topic> topicMap;
	
	@Getter
	@Setter
	private int getTimeout;
	
	@Getter
	@Setter
	private byte[] cachedBody;
	
	public GetClusterAck(int requestId) {
		super(CommandTypes.GET_CLUSTER_ACK, requestId);
	}
	
	public GetClusterAck(Header header) {
		super(header);
	}
	
	@Override
	public void validate() throws Exception {
		if (clusterList == null || topicMap == null) {
			throw new SystemException(SystemCode.COMMAND_ARGS_INVALID);
		}
	}
	
	/**
	 * <i>协议格式：</i>
	 * <per>
	 *     +----------+----------+      +----------+----------+      +----------+----------+
	 *     |   拉取超时（4字节）   |    / |   主题代码（1字节长度）|    / |  分组代码（1字节长度）|
	 *     +----------+----------+      +----------+----------+      +----------+----------+
	 *     |   集群个数（4字节）   |  /   |   分组个数（4字节）   |  /   |   分组权限（2字节）   |
	 *     +----------+----------+      +----------+----------+      +----------+----------+       +----------+----------+
	 *     |   集群1             |       |  分组1              |      |   分组权重（2字节）   |     / |   name（1字节长度）   |
	 *     +----------+----------+      +----------+----------+      +----------+----------+       +----------+----------+
	 *     |   集群2             |  \    |  分组2              |  \   |   Broker个数（4字节） |  /    |  group（1字节长度）  |
	 *     +----------+----------+      +----------+----------+      +----------+----------+       +----------+----------+
	 *     |     .               |   \  |    .                |    \ |   Broker1           |       |   ip（1字节长度）     |           |
	 *     |     .               |      |    .                |      +----------+----------+       +----------+----------+
	 *     |     .               |      |    .                |      |   Broker2           |   \   |   port（2字节）      |
	 *     +----------+----------+      +----------+----------+      +----------+----------+       +----------+----------+
	 *     |   集群n             |       |  分组n              |      |    .                |     \ |   role（2字节）      |
	 *     +----------+----------+      +----------+----------+      |    .                |       +----------+----------+
	 *     |   主题个数（4字节）   |                                   |    .                |       |   permission（2字节）|
	 *     +----------+----------+ ---- +----------+----------+      +----------+----------+       +----------+----------+
	 *     |   主题1             |       |  代码（1字节长度）    |      |   BrokerN           |
	 *     +----------+----------+ \__  +----------+----------+      +----------+----------+
	 *     |   主题2             |    |  |  名称（1字节长度）    |
	 *     +----------+----------+    \ +----------+----------+
	 *     |     .               |      |  级别（2字节）        |
	 *     |     .               |      +----------+----------+
	 *     |     .               |      |  队列数（2字节）      |
	 *     +----------+----------+      +----------+----------+
	 *     |   主题n             |       |  分组个数（4字节）    |
	 *     +----------+----------+      +----------+----------+
	 *                                  |  分组代码1（1字节长度）|
	 *                                  +----------+----------+
	 *                                  |  分组代码2（1字节长度）|
	 *                                  +----------+----------+
	 *                                  |     .               |
	 *                                  |     .               |
	 *                                  |     .               |
	 *                                  +----------+----------+
	 *                                  |  分组代码n（1字节长度）|
	 *                                  +----------+----------+
	 * </per>
	 */
	@Override
	protected void encodeBody(ByteBuf byteBuf) throws Exception {
		if (cachedBody != null) {
			byteBuf.writeBytes(cachedBody);
		} else {
			byteBuf.writeInt(getTimeout);
			byteBuf.writeInt(clusterList.size());
			clusterList.forEach(brokerCluster -> encodeBrokerCluster(byteBuf, brokerCluster));
			byteBuf.writeInt(topicMap.size());
			topicMap.values().forEach(topic -> encodeTopic(byteBuf, topic));
		}
	}
	
	private static void encodeBrokerCluster(ByteBuf byteBuf, BrokerCluster brokerCluster) {
		Serialization.writeString(byteBuf, brokerCluster.getTopic());
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
		Serialization.writeString(byteBuf, broker.getIp());
		byteBuf.writeShort(broker.getPort());
		byteBuf.writeShort(broker.getRole().ordinal());
		byteBuf.writeShort(broker.getPermission().ordinal());
	}
	
	private static BrokerCluster decodeBrokerCluster(ByteBuf byteBuf) {
		BrokerCluster brokerCluster = new BrokerCluster(Serialization.readString(byteBuf));
		IntStream.range(0, byteBuf.readInt()).forEach(i -> brokerCluster.addGroup(decodeBrokerGroup(byteBuf)));
		return brokerCluster;
	}
	
	private static BrokerGroup decodeBrokerGroup(ByteBuf byteBuf) {
		BrokerGroup brokerGroup = new BrokerGroup(Serialization.readString(byteBuf));
		IntStream.range(0, byteBuf.readInt()).forEach(i -> brokerGroup.addBroker(decodeBroker(byteBuf)));
		return brokerGroup;
	}
	
	private static Broker decodeBroker(ByteBuf byteBuf) {
		Broker broker = new Broker();
		broker.setName(Serialization.readString(byteBuf));
		broker.setGroup(Serialization.readString(byteBuf));
		broker.setIp(Serialization.readString(byteBuf));
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
		int topicSize = byteBuf.readInt();
		topicMap = new HashMap<>(topicSize);
		IntStream.range(0, topicSize).forEach(i -> {
			Topic topic = decodeTopic(byteBuf);
			topicMap.put(topic.getCode(), topic);
		});
	}
}
