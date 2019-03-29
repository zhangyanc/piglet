package pers.zyc.piglet.broker;

import pers.zyc.piglet.model.Connection;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author zhangyancheng
 */
public class SessionManager {
	
	private final ConcurrentMap<String, Connection> connectionMap = new ConcurrentHashMap<>();
	
	public boolean addConnection(Connection connection) {
		return connectionMap.putIfAbsent(connection.getId(), connection) == null;
	}
	
	public Connection getConnection(String connectionId) {
		return connectionMap.get(connectionId);
	}
}
