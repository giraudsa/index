package com.chronosave.index.utils;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

public class MultivalueMap<K, V> extends LinkedHashMap<K, LinkedHashSet<V>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8578758310281413814L;

	public V add(K key, V value) {
		if(this.containsKey(key))
			this.put(key, new LinkedHashSet<V>());
		this.get(key).add(value);
		return value;
	}
	
}
