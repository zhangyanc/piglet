package pers.zyc.piglet;

import java.nio.ByteBuffer;
import java.util.zip.Adler32;

/**
 * @author zhangyancheng
 */
public class ChecksumUtil {

	private static final ThreadLocal<Adler32> ADLER32_THREAD_LOCAL = ThreadLocal.withInitial(Adler32::new);

	public static int calc(ByteBuffer buffer) {
		Adler32 checksum = ADLER32_THREAD_LOCAL.get();
		checksum.update(buffer);
		try {
			return (int) checksum.getValue();
		} finally {
			checksum.reset();
		}
	}

	public static int calc(byte[] bytes) {
		Adler32 checksum = ADLER32_THREAD_LOCAL.get();
		checksum.update(bytes);
		try {
			return (int) checksum.getValue();
		} finally {
			checksum.reset();
		}
	}

	private ChecksumUtil() {
	}
}
