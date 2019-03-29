package pers.zyc.piglet;

import lombok.Getter;

/**
 * @author zhangyancheng
 */
public class SystemException extends RuntimeException {
	
	@Getter
	private int code;
	
	public SystemException(String msg, int code) {
		super(msg);
		this.code = code;
	}
	
	public SystemException(SystemCode systemCode) {
		super(systemCode.getMsg());
		this.code = systemCode.getCode();
	}
	
	public SystemException(SystemCode systemCode, Throwable cause) {
		super(systemCode.getMsg(), cause);
		this.code = systemCode.getCode();
	}
}
