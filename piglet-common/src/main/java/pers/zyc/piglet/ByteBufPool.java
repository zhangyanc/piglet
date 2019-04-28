package pers.zyc.piglet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author zhangyancheng
 */
public class ByteBufPool {

	private static final int DEFAULT_INITIAL_CAPACITY = 1024;
	private static final int DEFAULT_CAPACITY_LIMIT = 1024 * 100;

	/**
	 * ByteBuf 初始容量
	 */
	private final int bufferInitialCapacity;

	/**
	 * ByteBuf 被池化复用的容量限制（超过限制将不再池中缓存）
	 */
	private final int poolBufferCapacityLimit;

	/**
	 * 缓存的ByteBuf对象队列
	 */
	private final Queue<ByteBuf> byteBufQueue = new ConcurrentLinkedQueue<>();

	public ByteBufPool() {
		this(DEFAULT_INITIAL_CAPACITY, DEFAULT_CAPACITY_LIMIT, 1000);
	}

	public ByteBufPool(int bufferInitialCapacity, int poolBufferCapacityLimit, int prepareSize) {
		this.bufferInitialCapacity = bufferInitialCapacity;
		this.poolBufferCapacityLimit = poolBufferCapacityLimit;
		while (prepareSize-- > 0) {
			byteBufQueue.offer(newByteBuf());
		}
	}

	private ByteBuf newByteBuf() {
		return Unpooled.buffer(bufferInitialCapacity);
	}

	/**
	 * 从池中取出缓存的ByteBuf对象
	 *
	 * @return ByteBuf
	 */
	public ByteBuf borrow() {
		ByteBuf buf = byteBufQueue.poll();
		if (buf == null) {
			buf = newByteBuf();
		}
		return buf;
	}

	/**
	 * 回收ByteBuf，仅当其容量小于限制时才放入池中。否则将应当被GC掉
	 *
	 * @param buf 待还ByteBuf
	 */
	public void recycle(ByteBuf buf) {
		if (buf.capacity() < poolBufferCapacityLimit) {
			buf.clear();
			byteBufQueue.offer(buf);
		} else {
			buf.release();// help gc
		}
	}

	/**
	 * @return 当前池中缓存的ByteBuf个数
	 */
	public int pooledBufferCount() {
		return byteBufQueue.size();
	}

	/**
	 * @return 当前池中缓存的ByteBuf总内存大小
	 */
	public int pooledBufferSize() {
		return byteBufQueue.stream().mapToInt(ByteBuf::capacity).sum();
	}
}
