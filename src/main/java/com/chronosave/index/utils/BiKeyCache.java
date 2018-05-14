package com.chronosave.index.utils;

import java.util.LinkedHashMap;

public class BiKeyCache<K1, K2, V> extends LinkedHashMap<Pair<K1, K2>, V> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5177848805419929091L;

	public synchronized void put(K1 key1, K2 key2, V value) {
		put(key(key1, key2), value);
	}
	public synchronized V get(K1 key1, K2 key2) {
		return get(key(key1, key2));
	}
	
	public synchronized boolean containKey(K1 key1, K2 key2){
		return containsKey(key(key1, key2));
	}	
	
	private Pair<K1,K2> key(K1 key1, K2 key2) {
		return new Pair<>(key1, key2);
	}
}
