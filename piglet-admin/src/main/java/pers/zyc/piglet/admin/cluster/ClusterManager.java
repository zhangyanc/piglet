package pers.zyc.piglet.admin.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.data.Stat;
import pers.zyc.piglet.JSONUtil;
import pers.zyc.piglet.model.*;
import pers.zyc.tools.utils.event.EventBus;
import pers.zyc.tools.utils.event.EventListener;
import pers.zyc.tools.utils.event.EventSource;
import pers.zyc.tools.utils.lifecycle.Service;
import pers.zyc.tools.zkclient.NodeEventWatcher;
import pers.zyc.tools.zkclient.ZKClient;
import pers.zyc.tools.zkclient.listener.DataEventListener;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author zhangyancheng
 */
@Slf4j
public class ClusterManager extends Service implements EventSource<ClusterEvent>, DataEventListener {
	
	private final ClusterConfig clusterConfig;
	private final ZKClient zkClient;
	private final EventBus<ClusterEvent> clusterEventBus;
	private NodeEventWatcher brokerNodeWatcher;
	private NodeEventWatcher topicNodeWatcher;
	
	private volatile Map<String, BrokerGroup> groupMap;
	private volatile Map<Topic, List<BrokerGroup>> topicGroupMap;
	
	ClusterManager(ClusterConfig clusterConfig, ZKClient zkClient) {
		this.clusterConfig = clusterConfig;
		this.zkClient = zkClient;
		this.clusterEventBus = new EventBus.Builder<ClusterEvent>().name("Cluster Event Bus").build();
	}
	
	@Override
	protected void doStart() throws Exception {
		clusterEventBus.start();
		if (zkClient.isConnected()) {
			updateBroker();
			updateTopic();
		}
		brokerNodeWatcher = zkClient.createNodeEventWatcher(clusterConfig.getBrokerPath());
		brokerNodeWatcher.addListener(this);
		topicNodeWatcher = zkClient.createNodeEventWatcher(clusterConfig.getTopicPath());
		topicNodeWatcher.addListener(this);
	}
	
	@Override
	protected void doStop() throws Exception {
		clusterEventBus.stop();
		brokerNodeWatcher.quit();
		topicNodeWatcher.quit();
	}
	
	@Override
	public void addListener(EventListener<ClusterEvent> listener) {
		clusterEventBus.addListener(listener);
	}
	
	@Override
	public void removeListener(EventListener<ClusterEvent> listener) {
		clusterEventBus.removeListener(listener);
	}
	
	@Override
	public void onDataChanged(String path, Stat stat, byte[] data) {
		try {
			if (path.equals(clusterConfig.getBrokerPath())) {
				updateBroker(data);
			} else if (path.equals(clusterConfig.getTopicPath())) {
				updateTopic(data);
			}
		} catch (Exception e) {
			log.error("Node data parse error, path: " + path, e);
		}
	}
	
	@Override
	public void onNodeDeleted(String path) {
		log.error("Node should not be deleted, path: " + path);
	}
	
	private void updateBroker() throws Exception {
		updateBroker(zkClient.getData(clusterConfig.getBrokerPath()));
	}
	
	private void updateBroker(byte[] data) throws Exception {
		List<Broker> brokerList = JSONUtil.parseObject(new String(data, StandardCharsets.UTF_8),
				new TypeReference<List<Broker>>(){});
		
		Map<String, List<Broker>> brokerMap = brokerList.stream().collect(Collectors.groupingBy(Broker::getGroup));
		Map<String, BrokerGroup> groupMap = new HashMap<>(brokerMap.size());
		brokerMap.forEach((group, brokers) -> {
			BrokerGroup brokerGroup = new BrokerGroup(group);
			brokers.forEach(brokerGroup::addBroker);
			groupMap.put(group, brokerGroup);
		});
		this.groupMap = groupMap;
	}
	
	private void updateTopic() throws Exception {
		updateTopic(zkClient.getData(clusterConfig.getTopicPath()));
	}
	
	private void updateTopic(byte[] data) throws Exception {
		List<Topic> topicList = JSONUtil.parseObject(new String(data, StandardCharsets.UTF_8),
				new TypeReference<List<Topic>>() {});
		
		Map<Topic, List<BrokerGroup>> topicGroupMap = new HashMap<>(topicList.size());
		topicList.forEach(topic -> topicGroupMap.put(topic,
				topic.getGroups().stream().map(groupMap::get).collect(Collectors.toList())));
		this.topicGroupMap = topicGroupMap;
	}
	
	List<BrokerCluster> getCluster(String subscriber) {
		List<BrokerCluster> result = new ArrayList<>();
		topicGroupMap.forEach((topic, topicGroup) -> {
			ConsumePolicy consumePolicy = topic.getConsumers().get(subscriber);
			ProducePolicy producePolicy = topic.getProducers().get(subscriber);
			
			Permission clusterPerm = Permission.FULL;
			if (consumePolicy == null) {
				// 未订阅消费
				clusterPerm = clusterPerm.removeRead();
			}
			if (producePolicy == null) {
				// 未订阅生产
				clusterPerm = clusterPerm.removeWrite();
			}
			if (clusterPerm == Permission.NONE) {
				return;
			}
			
			BrokerCluster brokerCluster = new BrokerCluster(topic.getCode());
			
			Permission groupPermUnion = Permission.NONE;
			topicGroup.forEach(group -> {
				Permission groupPerm = Permission.NONE;
				group.getBrokers().forEach(broker -> {
					Permission brokerPerm = broker.getPermission();
					switch (broker.getRole()) {
						case MASTER:
							break;
						case SLAVE:
							brokerPerm = brokerPerm.removeWrite();
							break;
						default:
							brokerPerm = Permission.NONE;
					}
					if (brokerPerm.isWritable()) {
						groupPerm.addWrite();
						groupPermUnion.addWrite();
					}
					if (brokerPerm.isReadable()) {
						groupPerm.addRead();
						groupPermUnion.addRead();
					}
				});
				BrokerGroup brokerGroup = new BrokerGroup(group.getCode());
				brokerGroup.setWeight(group.getWeight());
				brokerGroup.setPermission(groupPerm);
				brokerCluster.addGroup(brokerGroup);
			});
			
		});
		
		return null;
	}
}
