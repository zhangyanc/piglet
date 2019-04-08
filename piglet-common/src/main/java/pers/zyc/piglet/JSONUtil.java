package pers.zyc.piglet;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * @author zhangyancheng
 */
public class JSONUtil {
	
	private static final ObjectMapper OM = new ObjectMapper();
	
	public static <T> T parseObject(String json, Class<T> type) throws IOException {
		return OM.readValue(json, type);
	}
	
	public static <T> T parseObject(String json, TypeReference<T> typeReference) throws IOException {
		return OM.readValue(json, typeReference);
	}
}
