package pers.zyc.piglet;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Objects;

/**
 * @author zhangyancheng
 */
public class IPUtil {
	
	public static byte[] toBytes(InetSocketAddress socketAddress) {
		InetAddress inetAddress = Objects.requireNonNull(socketAddress.getAddress());
		return toBytes(inetAddress, socketAddress.getPort());
	}
	
	public static byte[] toBytes(String ip, int port) {
		try {
			return toBytes(InetAddress.getByName(ip), port);
		} catch (UnknownHostException e) {
			throw new RuntimeException("Never happen with a ip address: " + ip);
		}
	}
	
	private static byte[] toBytes(InetAddress inetAddress, int port) {
		byte[] ipBytes = inetAddress.getAddress();
		byte[] result = new byte[ipBytes.length + 2];// 加两字节端口
		System.arraycopy(ipBytes, 0, result, 2, ipBytes.length);
		
		result[1] = (byte) (port >> 8 & 0xFF);
		result[0] = (byte) (port & 0xFF);
		return result;
	}
}
