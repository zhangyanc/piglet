package pers.zyc.piglet.network.command;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;
import pers.zyc.piglet.network.CommandFactory;
import pers.zyc.tools.network.Header;
import pers.zyc.tools.network.Response;

/**
 * @author zhangyancheng
 */
public class BooleanResponse extends Response {
	
	@Getter
	private int code = 1;
	
	@Getter
	private String msg;
	
	private BooleanResponse(int requestId) {
		super(CommandFactory.BOOLEAN_ACK, requestId);
	}
	
	public BooleanResponse(Header header) {
		super(header);
	}
	
	@Override
	public void validate() throws Exception {
		if (msg == null || code > 0) {
			throw new SystemException(SystemCode.COMMAND_ARGS_INVALID);
		}
	}
	
	@Override
	protected void encodeBody(ByteBuf byteBuf) throws Exception {
		byteBuf.writeInt(code);
		byteBuf.writeBytes(msg.getBytes(UTF_8));
	}
	
	@Override
	protected void decodeBody(ByteBuf byteBuf) throws Exception {
		code = byteBuf.readInt();
		byte[] msgBytes = new byte[byteBuf.readableBytes()];
		byteBuf.readBytes(msgBytes);
		msg = new String(msgBytes, UTF_8);
	}
	
	public boolean isSuccess() {
		return SystemCode.isSuccess(code);
	}
	
	/**
	 * 创建成功应答
	 *
	 * @param requestId 请求id
	 */
	public static BooleanResponse create(int requestId) {
		return create(requestId, SystemCode.SUCCESS);
	}
	
	/**
	 * 创建失败应答
	 *
	 * @param requestId 请求id
	 * @param exception 异常
	 */
	public static BooleanResponse create(int requestId, SystemException exception) {
		return create(requestId, exception.getCode(), exception.getMessage());
	}
	
	/**
	 * 创建应答
	 *
	 * @param requestId 请求id
	 * @param code 响应码
	 * @param msg 响应信息
	 */
	public static BooleanResponse create(int requestId, int code, String msg) {
		BooleanResponse booleanResponse = new BooleanResponse(requestId);
		booleanResponse.code = code;
		booleanResponse.msg = msg;
		return booleanResponse;
	}
	
	/**
	 * 创建应答
	 *
	 * @param requestId 请求id
	 * @param systemCode 响应码
	 */
	public static BooleanResponse create(int requestId, SystemCode systemCode) {
		return create(requestId, systemCode.getCode(), systemCode.getMsg());
	}
}
