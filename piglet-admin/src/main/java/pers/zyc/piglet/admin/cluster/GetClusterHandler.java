package pers.zyc.piglet.admin.cluster;

import pers.zyc.piglet.network.CommandFactory;
import pers.zyc.piglet.network.command.GetCluster;
import pers.zyc.tools.network.Response;
import pers.zyc.tools.network.SingleTypeRequestHandler;

/**
 * @author zhangyancheng
 */
public class GetClusterHandler extends SingleTypeRequestHandler<GetCluster> {
	
	@Override
	public int supportedRequestType() {
		return CommandFactory.GET_CLUSTER;
	}
	
	@Override
	protected Response handle0(GetCluster request) throws Exception {
		return null;
	}
}
