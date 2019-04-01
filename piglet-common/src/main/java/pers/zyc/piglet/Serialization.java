package pers.zyc.piglet;

import io.netty.buffer.ByteBuf;
import pers.zyc.piglet.model.BrokerMessage;
import pers.zyc.piglet.model.Message;
import pers.zyc.tools.network.Protocol;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

/**
 * @author zhangyancheng
 */
public class Serialization {

	public static void writeString(ByteBuf byteBuf, String string) {
		writeString(byteBuf, string, LenType.BYTE);
	}

	public static void writeString(ByteBuf byteBuf, String string, LenType lenType) {
		byte[] stringBytes = string.getBytes(Protocol.UTF_8);
		int length = stringBytes.length;
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
		byteBuf.writeBytes(stringBytes);
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
		byte[] stringBytes = new byte[length];
		byteBuf.readBytes(stringBytes);
		return new String(stringBytes, Protocol.UTF_8);
	}

	public static void writeProperties(ByteBuf byteBuf, Map<String, String> properties) {
		Optional.ofNullable(properties).ifPresent((m) -> {
			byteBuf.writeInt(properties.size());
			m.forEach((k, v) -> {
				writeString(byteBuf, k);
				writeString(byteBuf, v);
			});
		});
	}

	public static void writeMessage(ByteBuf byteBuf, Message message) {
		writeString(byteBuf, message.getTopic());
		byteBuf.writeLong(message.getClientSendTime());
		byteBuf.writeInt(message.getBody().length);
		byteBuf.writeBytes(message.getBody());
		byteBuf.writeLong(message.getChecksum());
		writeProperties(byteBuf, message.getProperties());
	}

	public static Map<String, String> readProperties(ByteBuf byteBuf) {
		return Optional.of(byteBuf).filter(ByteBuf::isReadable).map(buf -> {
			int size = byteBuf.readInt();
			Map<String, String> properties = new HashMap<>(size);
			IntStream.range(0, size).forEach(i -> properties.put(readString(byteBuf), readString(byteBuf)));
			return properties;
		}).orElse(null);
	}

	public static Message readMessage(ByteBuf byteBuf) {
		Message message = new BrokerMessage();
		message.setTopic(readString(byteBuf));
		message.setClientSendTime(byteBuf.readLong());
		byte[] body = new byte[byteBuf.readInt()];
		byteBuf.readBytes(body);
		message.setBody(body);
		if (message.getChecksum() != byteBuf.readLong()) {
			throw new SystemException(SystemCode.CHECKSUM_WRONG);
		}
		message.setProperties(readProperties(byteBuf));
		return message;
	}
}
