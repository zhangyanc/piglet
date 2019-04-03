package pers.zyc.piglet.broker.offset;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangyancheng
 */
public class SequenceSet {
	private final int span;

	private Sequence head;
	private int size;

	public SequenceSet() {
		this(1);
	}

	public SequenceSet(int span) {
		this.span = span;
	}

	public boolean add(long value) {
		if (head == null) {
			head = new Sequence(value);
			size++;
			return true;
		}

		Sequence seq = head;
		while (true) {
			if (seq.contains(value)) {
				return false;
			}
			if (seq.first > value) {
				if (seq.firstAdjacent(value)) {
					seq.first = value;
				} else {
					size++;
					Sequence insert = new Sequence(value);
					insert.prev = seq.prev;
					insert.next = seq;
					if (seq == head) {
						insert.prev = head.prev == null ? head : head.prev;
						head = insert;
					} else {
						seq.prev.next = insert;
					}
					seq.prev = insert;
				}
				return true;
			}
			Sequence next = seq.next;
			if (seq.lastAdjacent(value)) {
				seq.last = value;
				if (next != null && next.firstAdjacent(value)) {
					size--;
					seq.last = next.last;
					seq.next = next.next;
					if (next.next != null) {
						next.next.prev = seq;
					} else {
						head.prev = size > 1 ? seq : null;
					}
				}
				return true;
			}
			if (next == null) {
				Sequence last = new Sequence(value);
				last.prev = seq;
				head.prev = seq.next = last;
				size++;
				return true;
			}
			seq = next;
		}
	}

	private void add(Sequence sequence) {
		for (long value = sequence.first; value <= sequence.last; value += span) {
			add(value);
		}
	}

	public void add(String seqStr) {
		add(parseSequence(seqStr));
	}

	public void add(String... sequences) {
		for (String seqStr : sequences) {
			add(seqStr);
		}
	}

	public List<String> getSequences() {
		List<String> sequences = new ArrayList<>();
		for (Sequence seq = head; seq != null; seq = seq.next) {
			sequences.add(seq.toString());
		}
		return sequences;
	}

	private Sequence parseSequence(String str) {
		String[] parts = str.split("-");
		return parts.length == 1 ? new Sequence(Long.parseLong(parts[0])) :
				new Sequence(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
	}

	public long min() {
		if (head == null) {
			throw new IllegalStateException();
		}
		return head.first;
	}

	public long max() {
		if (head == null) {
			throw new IllegalStateException();
		}
		return (head.prev == null ? head : head.prev).last;
	}

	public boolean contains(long value) {
		for (Sequence seq = head; seq != null; seq = seq.next) {
			if (seq.contains(value)) {
				return true;
			}
		}
		return false;
	}

	public long misses() {
		long misses = 0;
		Sequence seq = head;
		while (seq.next != null) {
			Sequence next = seq.next;
			misses += (next.first - seq.last - span) / span;
			seq = next;
		}
		return misses;
	}

	public int size() {
		return size;
	}

	public class Sequence {

		Sequence prev;
		Sequence next;

		long first;
		long last;

		Sequence(long value) {
			first = last = value;
		}

		Sequence(long first, long last) {
			this.first = first;
			this.last = last;
		}

		boolean contains(long value) {
			return first <= value && value <= last;
		}

		boolean lastAdjacent(long value) {
			return value - last == span;
		}

		boolean firstAdjacent(long value) {
			return first - value == span;
		}

		@Override
		public String toString() {
			return first == last ? "" + first : first + "-" + last;
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder("[");
		Sequence seq = head;
		while (seq != null) {
			builder.append(seq.toString());
			seq = seq.next;
			if (seq != null) {
				builder.append(", ");
			}
		}
		builder.append("]");
		return builder.toString();
	}
}
