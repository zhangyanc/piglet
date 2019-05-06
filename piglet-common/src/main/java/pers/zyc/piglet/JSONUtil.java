package pers.zyc.piglet;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author zhangyancheng
 */
public class JSONUtil {
	
	private static final ObjectMapper OM = new ObjectMapper();
	
	public static <T> T parseObject(String json, Class<T> type) {
		return IOExecutor.execute(() -> OM.readValue(json, type));
	}
	
	public static <T> T parseObject(String json, TypeReference<T> typeReference) {
		return IOExecutor.execute(() -> OM.readValue(json, typeReference));
	}
}
