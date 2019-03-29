package pers.zyc.piglet;

import lombok.Getter;
import lombok.ToString;

import java.util.Objects;

/**
 * @author zhangyancheng
 */
@ToString(includeFieldNames = false, of = "producerId")
public class ProducerId implements Unique {
	
	/**
	 * 连接id
	 */
	@Getter
	private final ConnectionId connectionId;
	
	@Getter
	private final String producerId;
	
	public ProducerId(String producerId) {
		this.connectionId = new ConnectionId(producerId.substring(0, producerId.lastIndexOf("-")));
		this.producerId = producerId;
	}
	
	public ProducerId(ConnectionId connectionId) {
		this.connectionId = connectionId;
		this.producerId = connectionId + "-" + sequence();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ProducerId that = (ProducerId) o;
		return Objects.equals(producerId, that.producerId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(producerId);
	}
}
