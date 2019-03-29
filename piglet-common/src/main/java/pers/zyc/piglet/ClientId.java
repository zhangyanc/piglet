package pers.zyc.piglet;

import lombok.Getter;
import lombok.ToString;

import java.util.Objects;

/**
 * @author zhangyancheng
 */
@ToString(includeFieldNames = false, of = "clientId")
public class ClientId implements Unique {
	/**
	 * 客户端版本号
	 */
	@Getter
	private final String version;
	
	/**
	 * 客户端ip地址
	 */
	@Getter
	private final String ip;
	
	/**
	 * 时间戳
	 */
	@Getter
	private final long time;
	
	@Getter
	private final String clientId;

	public ClientId(String clientId) {
		String[] parts = clientId.split("-");
		this.version = parts[0];
		this.ip = parts[1];
		this.time = Long.parseLong(parts[2]);
		this.clientId = clientId;
	}

	public ClientId(String version, String ip, long time) {
		this.version = version;
		this.ip = ip;
		this.time = time;
		this.clientId = version + "-" + ip + "-" + time + "-" + sequence();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ClientId that = (ClientId) o;
		return Objects.equals(clientId, that.clientId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(clientId);
	}
}
