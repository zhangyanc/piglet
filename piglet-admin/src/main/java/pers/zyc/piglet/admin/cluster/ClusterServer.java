package pers.zyc.piglet.admin.cluster;

import lombok.NonNull;
import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;
import pers.zyc.piglet.network.CommandFactory;
import pers.zyc.tools.network.NetServer;
import pers.zyc.tools.zkclient.ZKClient;

import java.util.concurrent.TimeUnit;

/**
 * @author zhangyancheng
 */
public class ClusterServer extends NetServer {
	
	private final ClusterConfig clusterConfig;
	
	private ClusterManager clusterManager;
	
	private ZKClient zkClient;
	
	public ClusterServer(@NonNull ClusterConfig clusterConfig) {
		this.clusterConfig = clusterConfig;
	}
	
	@Override
	protected void beforeStart() throws Exception {
		zkClient = new ZKClient(clusterConfig.getZkAddress());
		boolean zkConnected = zkClient.waitToConnected(10, TimeUnit.SECONDS);
		if (!zkConnected) {
			// cluster强依赖zookeeper，无法连通则无法工作
			throw new SystemException(SystemCode.ZK_DISCONNECTED);
		}
	}
	
	@Override
	protected void doStart() {
		clusterManager = new ClusterManager(clusterConfig, zkClient);
		GetClusterHandler getClusterHandler = new GetClusterHandler();
		
		setPort(clusterConfig.getServerPort());
		setCommandFactory(new CommandFactory());
		setRequestHandlerFactory(requestType -> {
			switch (requestType) {
				case CommandFactory.GET_CLUSTER:
					return getClusterHandler;
			}
			return null;
		});
		super.doStart();
	}
	
	@Override
	protected void doStop() throws Exception {
		zkClient.destroy();
		super.doStop();
	}
}
