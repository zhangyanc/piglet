package pers.zyc.piglet.broker;

import pers.zyc.piglet.model.Connection;
import pers.zyc.piglet.model.Producer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author zhangyancheng
 */
public class SessionManager {
	
	private final ConcurrentMap<String, Connection> connectionMap = new ConcurrentHashMap<>();
	
	private final ConcurrentMap<String, Producer> producerMap = new ConcurrentHashMap<>();
	
	public boolean addConnection(Connection connection) {
		return connectionMap.putIfAbsent(connection.getId(), connection) == null;
	}
	
	public Connection getConnection(String connectionId) {
		return connectionMap.get(connectionId);
	}
	
	public boolean addProducer(Producer producer) {
		return producerMap.putIfAbsent(producer.getId(), producer) == null;
	}
	
	public Producer getProducer(String producerId) {
		return producerMap.get(producerId);
	}
}
