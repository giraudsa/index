package com.chronosave.index.utils;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

public class MultivalueMap<K, V> extends LinkedHashMap<K, LinkedHashSet<V>> {

	/**
	 *
	 */
	private static final long serialVersionUID = -8578758310281413814L;

	public V add(final K key, final V value) {
		if (containsKey(key))
			put(key, new LinkedHashSet<V>());
		get(key).add(value);
		return value;
	}

}
