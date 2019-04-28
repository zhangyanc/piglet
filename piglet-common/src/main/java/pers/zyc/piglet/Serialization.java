package pers.zyc.piglet;

import io.netty.buffer.ByteBuf;
import pers.zyc.tools.network.Protocol;

/**
 * @author zhangyancheng
 */
public class Serialization {

	public static void writeBytes(ByteBuf byteBuf, byte[] src) {
		byteBuf.writeInt(src.length);
		byteBuf.writeBytes(src);
	}

	public static byte[] readBytes(ByteBuf byteBuf) {
		byte[] bytes = new byte[byteBuf.readInt()];
		byteBuf.readBytes(bytes);
		return bytes;
	}

	public static void writeString(ByteBuf byteBuf, String value) {
		writeString(byteBuf, value, LenType.BYTE);
	}

	public static void writeString(ByteBuf byteBuf, String value, LenType lenType) {
		byte[] valueBytes = value.getBytes(Protocol.UTF_8);
		int length = valueBytes.length;
		switch (lenType) {
			case BYTE:
				byteBuf.writeByte(length);
				break;
			case SHORT:
				byteBuf.writeShort(length);
				break;
			case MEDIUM:
				byteBuf.writeMedium(length);
				break;
			case INT:
				byteBuf.writeInt(length);
				break;
			default:
				throw new Error("Unknown LenType: " + lenType);
		}
		byteBuf.writeBytes(valueBytes);
	}

	public static String readString(ByteBuf byteBuf) {
		return readString(byteBuf, LenType.BYTE);
	}

	public static String readString(ByteBuf byteBuf, LenType lenType) {
		int length;
		switch (lenType) {
			case BYTE:
				length = byteBuf.readByte();
				break;
			case SHORT:
				length = byteBuf.readShort();
				break;
			case MEDIUM:
				length = byteBuf.readMedium();
				break;
			case INT:
				length = byteBuf.readInt();
				break;
			default:
				throw new Error("Unknown LenType: " + lenType);
		}
		byte[] valueBytes = new byte[length];
		byteBuf.readBytes(valueBytes);
		return new String(valueBytes, Protocol.UTF_8);
	}
}
