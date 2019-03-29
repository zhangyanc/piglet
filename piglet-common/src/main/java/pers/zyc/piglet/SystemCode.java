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
	
	COMMAND_ARGS_INVALID(-10, "Command args invalid"),
	CHECKSUM_WRONG(-11, "Checksum error"),
	
	AUTH_FAILED(-80, "Auth failed"),
	
	CONNECTION_ALREADY_EXISTS(-101, "Connection already exists"),
	CONNECTION_NOT_EXISTS(-102, "Connection not exists"),
	
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
