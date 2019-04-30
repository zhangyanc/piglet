package pers.zyc.piglet.model;

import io.netty.channel.Channel;
import lombok.Getter;
import lombok.Setter;
import pers.zyc.piglet.Language;

/**
 * @author zhangyancheng
 */
@Getter
@Setter
public class Connection {

	private String id;
	
	private String version;
	
	private String subscriber;
	
	private Language language;
	
	private Channel channel;
	
	private byte[] clientAddress;
	
	private byte[] serverAddress;
}
