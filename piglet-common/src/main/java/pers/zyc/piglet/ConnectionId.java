package pers.zyc.piglet;

import lombok.Getter;
import lombok.ToString;

import java.util.Objects;

/**
 * @author zhangyancheng
 */
@ToString(of = "connectionId", includeFieldNames = false)
public class ConnectionId implements Unique {
	
	/**
	 * 客户端id
	 */
	@Getter
	private final ClientId clientId;
	
	@Getter
	private final String connectionId;

	public ConnectionId(String connectionId) {
		this.clientId = new ClientId(connectionId.substring(0, connectionId.lastIndexOf("-")));
		this.connectionId = connectionId;
	}

	public ConnectionId(ClientId clientId) {
		this.clientId = clientId;
		this.connectionId = clientId + "-" + sequence();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ConnectionId that = (ConnectionId) o;
		return Objects.equals(connectionId, that.connectionId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(connectionId);
	}
}
