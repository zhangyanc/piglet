package pers.zyc.piglet.admin.cluster;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.data.Stat;
import pers.zyc.tools.utils.event.EventBus;
import pers.zyc.tools.utils.event.EventListener;
import pers.zyc.tools.utils.event.EventSource;
import pers.zyc.tools.utils.lifecycle.Service;
import pers.zyc.tools.zkclient.NodeEventWatcher;
import pers.zyc.tools.zkclient.ZKClient;
import pers.zyc.tools.zkclient.listener.DataEventListener;

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
		if (path.equals(clusterConfig.getBrokerPath())) {
			updateBroker(data);
		} else if (path.equals(clusterConfig.getTopicPath())) {
			updateTopic(data);
		}
	}
	
	@Override
	public void onNodeDeleted(String path) {
		log.error("Node should not be deleted, path: " + path);
	}
	
	private void updateBroker() {
		
	}
	
	private void updateBroker(byte[] data) {
		
	}
	
	private void updateTopic() {
		
	}
	
	private void updateTopic(byte[] data) {
		
	}
	
}
