package pers.zyc.piglet.admin.cluster;

import lombok.Getter;
import lombok.Setter;

/**
 * @author zhangyancheng
 */
public class ClusterConfig {
	
	@Getter
	@Setter
	private String zkAddress;
	
	@Getter
	@Setter
	private int serverPort;
	
	@Getter
	@Setter
	private String brokerPath;
	
	@Getter
	@Setter
	private String topicPath;
	
	@Getter
	@Setter
	private int clusterChangeWaitTimeout;
}
