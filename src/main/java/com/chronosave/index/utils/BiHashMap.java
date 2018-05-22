package com.chronosave.index.utils;

import java.util.HashMap;

public class BiHashMap<K1, K2, V> extends HashMap<Pair<K1, K2>, V> {

	/**
	 *
	 */
	private static final long serialVersionUID = -3017208040141101937L;

	public boolean containsKeys(final K1 k1, final K2 k2) {
		return containsKey(key(k1, k2));
	}

	public V get(final K1 k1, final K2 k2) {
		return get(key(k1, k2));
	}

	private Pair<K1, K2> key(final K1 key1, final K2 key2) {
		return new Pair<>(key1, key2);
	}

	public V put(final K1 k1, final K2 k2, final V value) {
		return put(key(k1, k2), value);
	}

	public V removeKey(final K1 k1, final K2 k2) {
		return remove(key(k1, k2));
	}
}
