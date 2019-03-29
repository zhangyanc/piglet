package pers.zyc.piglet.model;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * @author zhangyancheng
 */
public class ProducePolicy {
	
	@Getter
	@Setter
	private String producer;
	
	
	private Map<String, Short> weights;
}
