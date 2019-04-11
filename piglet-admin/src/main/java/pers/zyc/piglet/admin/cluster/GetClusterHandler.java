package pers.zyc.piglet.admin.cluster;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import lombok.extern.slf4j.Slf4j;
import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.network.CommandFactory;
import pers.zyc.piglet.network.command.BooleanResponse;
import pers.zyc.piglet.network.command.GetCluster;
import pers.zyc.piglet.network.command.GetClusterAck;
import pers.zyc.tools.network.Response;
import pers.zyc.tools.network.SingleTypeRequestHandler;
import pers.zyc.tools.utils.GeneralThreadFactory;
import pers.zyc.tools.utils.LockUtil;
import pers.zyc.tools.utils.event.EventListener;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zhangyancheng
 */
@Slf4j
public class GetClusterHandler extends SingleTypeRequestHandler<GetCluster> implements EventListener<ClusterEvent> {
	
	private final ClusterManager clusterManager;
	private final int clusterChangeWaitTimeout;
	private final int clientClusterGetTimeout;
	private final ReentrantLock responderLock = new ReentrantLock();
	private final ConcurrentMap<String, Responder> responderMap = new ConcurrentHashMap<>();
	private final ConcurrentMap<String, byte[]> clusterCacheMap = new ConcurrentHashMap<>();
	private final ConcurrentMap<String, Object> clientIdRequestRecord = new ConcurrentHashMap<>();
	
	public GetClusterHandler(ClusterManager clusterManager, int clusterChangeWaitTimeout) {
		this.clusterManager = clusterManager;
		this.clusterChangeWaitTimeout = clusterChangeWaitTimeout;
		this.clientClusterGetTimeout = (int) (clusterChangeWaitTimeout * 1.5);
		clusterManager.addListener(this);
		setExecutor(Executors.newCachedThreadPool(new GeneralThreadFactory("Get Cluster Handler")));
	}
	
	@Override
	public int supportedRequestType() {
		return CommandFactory.GET_CLUSTER;
	}
	
	@Override
	protected Response handle0(GetCluster request) throws Exception {
		return getResponder(request).respond(request);
	}
	
	private Responder getResponder(GetCluster request) {
		String subscriber = request.getSubscriber();
		Responder responder = responderMap.get(subscriber);
		if (responder == null) {
			Responder newResponder = new Responder(new ClusterCalc(subscriber, request.getClientId()));
			if (responderMap.putIfAbsent(subscriber, newResponder) == null) {
				responder = newResponder;
				responder.run();
			}
		}
		return responder;
	}
	
	@Override
	public void onEvent(ClusterEvent event) {
		Set<String> affectedSubscribers = getAffectedSubscriber(event);
		if (affectedSubscribers.isEmpty()) {
			return;
		}
		responderMap.values().stream().map(responder -> responder.clusterCalc)
				.filter(clusterCalc -> affectedSubscribers.contains(clusterCalc.subscriber))
				.forEach(clusterCalc -> LockUtil.runWithLock(responderLock, clusterCalc.condition::signal));
	}
	
	private boolean withClusterChange(ClusterCalc clusterCalc) {
		try {
			// 未按照'虚假唤醒'所建议的while check方式，因为即使发生'虚假唤醒'程序不会出错
			return LockUtil.callWithLock(responderLock, () ->
					clusterCalc.condition.await(clusterChangeWaitTimeout, TimeUnit.MILLISECONDS));
		} catch (Exception interruptedException) {
			return false;
		}
	}
	
	private Set<String> getAffectedSubscriber(ClusterEvent event) {
		return new HashSet<>();
	}
	
	class ClusterCalc implements Callable<byte[]> {
		
		final String subscriber;
		final String clientId;
		final Condition condition = responderLock.newCondition();
		
		ClusterCalc(String subscriber, String clientId) {
			this.subscriber = subscriber;
			this.clientId = clientId;
		}
		
		@Override
		public byte[] call() throws Exception {
			if (clientIdRequestRecord.containsKey(clientId)) {
				boolean changed = withClusterChange(this);
				log.info("Wait cluster changed {}", changed);
			}
			// 等待结束后总是计算一次集群信息
			byte[] clusterBody = calc();
			clusterCacheMap.put(subscriber, clusterBody);
			return clusterBody;
		}
		
		private byte[] calc() throws Exception {
			GetClusterAck getClusterAck = new GetClusterAck(0);
			getClusterAck.setClusterList(clusterManager.getCluster(subscriber));
			getClusterAck.setGetTimeout(clientClusterGetTimeout);
			ByteBuf byteBuf = Unpooled.buffer();
			getClusterAck.encodeBody(byteBuf);
			byte[] clusterBodyBytes = new byte[byteBuf.readableBytes()];
			byteBuf.readBytes(clusterBodyBytes);
			return clusterBodyBytes;
		}
	}
	
	private class Responder extends FutureTask<byte[]> {
		
		final ClusterCalc clusterCalc;
		final List<GetCluster> waiterList = new ArrayList<>();
		
		Responder(ClusterCalc clusterCalc) {
			super(clusterCalc);
			this.clusterCalc = clusterCalc;
		}
		
		Response respond(GetCluster request) {
			//客户端第一次请求需尽快返回，如果订阅者已经计算过集群信息则直接返回旧值
			if (clientIdRequestRecord.putIfAbsent(request.getClientId(), this) == null) {
				byte[] clusterCache = clusterCacheMap.get(request.getSubscriber());
				if (clusterCache != null) {
					return respondSuccess(request.getId(), clusterCache);
				}
			}
			if (!isDone()) {
				synchronized (waiterList) {
					if (!isDone()) {
						waiterList.add(request);
						return null;
					}
				}
			}
			try {
				return respondSuccess(request.getId(), get());
			} catch (Exception e) {
				return BooleanResponse.create(request.getId(), SystemCode.UNKNOWN_ERROR.getCode(), e.getMessage());
			}
		}
		
		GetClusterAck respondSuccess(int requestId, byte[] body) {
			GetClusterAck getClusterAck = new GetClusterAck(requestId);
			getClusterAck.setCachedBody(body);
			return getClusterAck;
		}
		
		void respondAllWaiter() {
			log.info("Respond all {} waiting requests, {}", waiterList.size(), clusterCalc.subscriber);
			waiterList.forEach(request -> request.getChannel().writeAndFlush(respond(request)).
					addListener(ChannelFutureListener.CLOSE_ON_FAILURE));
			waiterList.clear();
		}
		
		@Override
		protected void set(byte[] bytes) {
			synchronized (waiterList) {
				super.set(bytes);
			}
			respondAllWaiter();
		}
		
		@Override
		protected void setException(Throwable t) {
			synchronized (waiterList) {
				super.setException(t);
			}
			respondAllWaiter();
		}
	}
}
