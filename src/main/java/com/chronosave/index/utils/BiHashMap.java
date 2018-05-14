package com.chronosave.index.utils;

import java.util.HashMap;

public class BiHashMap<K1, K2, V> extends HashMap<Pair<K1, K2>, V>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -3017208040141101937L;

	public boolean containsKeys(K1 k1, K2 k2) {
		return containsKey(key(k1,k2));
	}

	public V get(K1 k1, K2 k2) {
		return get(key(k1, k2));
	}

	public V put(K1 k1, K2 k2, V value) {
		return put(key(k1, k2), value);
	}

	public V removeKey(K1 k1, K2 k2) {
		return remove(key(k1, k2));
	}

	private Pair<K1,K2> key(K1 key1, K2 key2) {
		return new Pair<>(key1, key2);
	}
}
