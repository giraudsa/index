package com.chronosave.index.utils;

import java.util.AbstractMap;
import java.util.Map.Entry;

public class Pair<K, V> extends AbstractMap.SimpleImmutableEntry<K, V> {
	private static final long serialVersionUID = 1L;
	public Pair(Entry<? extends K, ? extends V> entry) {
		super(entry);
	}
	public Pair(K key, V value) {
		super(key, value);
	}
}
