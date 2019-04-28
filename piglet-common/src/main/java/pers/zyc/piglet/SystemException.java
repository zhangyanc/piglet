package pers.zyc.piglet;

import lombok.Getter;

/**
 * @author zhangyancheng
 */
public class SystemException extends RuntimeException {
	
	@Getter
	private int code;
	
	public SystemException(int code, String msg) {
		super(msg);
		this.code = code;
	}
	
	public SystemException(int code, Throwable cause) {
		super(cause);
		this.code = code;
	}
	
	public SystemException(int code, String msg, Throwable cause) {
		super(msg, cause);
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

	@Override
	public String getMessage() {
		return "[" + code + "]" + super.getMessage();
	}
}
