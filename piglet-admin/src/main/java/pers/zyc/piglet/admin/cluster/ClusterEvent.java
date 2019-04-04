package pers.zyc.piglet.admin.cluster;

import pers.zyc.tools.utils.event.SourcedEvent;

/**
 * @author zhangyancheng
 */
public class ClusterEvent extends SourcedEvent<ClusterManager> {
	
	public ClusterEvent(ClusterManager source) {
		super(source);
	}
	
	
}
