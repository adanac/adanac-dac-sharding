package com.adanac.framework.dac.route.support.hash;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

import com.adanac.framework.dac.util.DacUtils;

/**
 * 一致性hash
 * @author adanac
 * @version 1.0
 */
public class ConsistentHash {
	private static final int defaultNumberOfReplicas = 16;
	private int numberOfReplicas;
	private SortedMap<Long, String> circle = new TreeMap<Long, String>();

	public ConsistentHash(Collection<String> nodes, int numberOfReplicas) {
		if (numberOfReplicas < 1) {
			throw new IllegalArgumentException("The numberOfReplicas must bigger then 0.");
		}
		if (nodes == null || nodes.isEmpty()) {
			throw new IllegalArgumentException("The shards can't empty.");
		}
		this.numberOfReplicas = numberOfReplicas;
		for (String node : nodes) {
			add(node);
		}
	}

	public ConsistentHash(Collection<String> nodes) {
		this(nodes, defaultNumberOfReplicas);
	}

	public void add(String node) {
		for (int i = 0; i < numberOfReplicas; i++) {
			circle.put(hash(node.toString() + i), node);
		}
	}

	public void remove(String node) {
		for (int i = 0; i < numberOfReplicas; i++) {
			circle.remove(hash(node.toString() + i));
		}
	}

	public String get(Object key) {
		if (circle.isEmpty()) {
			return null;
		}
		long hash = hash(key.toString());
		if (!circle.containsKey(hash)) {
			SortedMap<Long, String> tailMap = circle.tailMap(hash);
			hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
		}
		return circle.get(hash);
	}

	private long hash(final String k) {
		final byte[] bKey = DacUtils.computeMd5(k);
		final long rv = ((long) (bKey[3] & 0xFF) << 24) | ((long) (bKey[2] & 0xFF) << 16)
				| ((long) (bKey[1] & 0xFF) << 8) | (bKey[0] & 0xFF);
		return rv & 0xffffffffL; /* Truncate to 32-bits */
	}

	protected static String decorateWithCounter(final String input, final int counter) {
		return new StringBuilder(input).append('%').append(counter).append('%').toString();
	}
}
