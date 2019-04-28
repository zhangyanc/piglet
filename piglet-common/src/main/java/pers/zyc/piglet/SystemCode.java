package pers.zyc.piglet;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author zhangyancheng
 */
public enum SystemCode {
	SUCCESS(0, "Success"),
	
	UNKNOWN_ERROR(-1, "Unknown error"),
	IO_EXCEPTION(-2, "IO exception"),
	
	COMMAND_ARGS_INVALID(-10, "Command args invalid"),
	CHECKSUM_WRONG(-11, "Checksum error"),
	
	ZK_DISCONNECTED(-50, "ZooKeeper disconnected"),
	AUTH_FAILED(-80, "Auth failed"),
	
	CONNECTION_ALREADY_EXISTS(-101, "Connection already exists"),
	CONNECTION_NOT_EXISTS(-102, "Connection not exists"),
	PRODUCER_ALREADY_EXISTS(-103, "Producer already exists"),
	PRODUCER_NOT_EXISTS(-104, "Producer not exists"),
	CONSUMER_ALREADY_EXISTS(-105, "Consumer already exists"),
	CONSUMER_NOT_EXISTS(-106, "Consumer not exists"),
	TOPIC_NOT_EXISTS(-107, "Topic not exists"),

	STORE_BAD_FILE(-201, "Bad file"),
	STORE_WRONG_OFFSET(-202, "Wrong offset"),
	
	;
	
	@Getter
	private final int code;
	@Getter
	private final String msg;
	
	SystemCode(int code, String msg) {
		this.code = code;
		this.msg = msg;
	}
	
	private static final Map<Integer, SystemCode> CODE_MAP = new HashMap<>();
	static {
		Stream.of(SystemCode.values()).forEach(c -> {
			if (CODE_MAP.put(c.code, c) != null) {
				throw new Error("Duplicated system code: " + c.code);
			}
		});
	}
	
	public static SystemCode of(int code) {
		return CODE_MAP.get(code);
	}
	
	public static boolean isSuccess(int code) {
		return SUCCESS.code == code;
	}
}
