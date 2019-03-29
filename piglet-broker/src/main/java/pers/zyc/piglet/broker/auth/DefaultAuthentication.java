package pers.zyc.piglet.broker.auth;

import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;
import pers.zyc.tools.utils.PasswordEncoder;

/**
 * @author zhangyancheng
 */
public class DefaultAuthentication implements Authentication {
	
	private final PasswordEncoder passwordEncoder;
	
	public DefaultAuthentication() {
		this("PIGLET");
	}
	
	public DefaultAuthentication(String key) {
		passwordEncoder = new PasswordEncoder(key);
	}
	
	@Override
	public String createToken(String user) {
		return passwordEncoder.encode(user).substring(0, 10);
	}
	
	@Override
	public void auth(String user, String token) {
		boolean matched = passwordEncoder.match(user, token);
		if (!matched) {
			throw new SystemException(SystemCode.AUTH_FAILED);
		}
	}
}
