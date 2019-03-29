package pers.zyc.piglet.broker.auth;

/**
 * @author zhangyancheng
 */
public interface Authentication {
	
	/**
	 * 为用户创建认证码
	 *
	 * @param user 用户
	 */
	String createToken(String user);
	
	/**
	 * 认证
	 *
	 * @param user 用户
	 * @param token 认证码
	 */
	void auth(String user, String token);
}
