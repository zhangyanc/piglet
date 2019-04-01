package pers.zyc.piglet.model;

import io.netty.channel.Channel;
import lombok.Getter;
import lombok.Setter;
import pers.zyc.piglet.Language;

/**
 * @author zhangyancheng
 */
public class Connection {
	@Getter
	@Setter
	private String id;
	@Getter
	@Setter
	private String version;
	@Getter
	@Setter
	private String subscriber;
	@Getter
	@Setter
	private Language language;
	@Getter
	@Setter
	private Channel channel;
}
