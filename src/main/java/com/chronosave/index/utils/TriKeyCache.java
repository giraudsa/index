package com.chronosave.index.utils;

import java.util.LinkedHashMap;
import java.util.Map;

public class TriKeyCache<K1, K2, K3, V> extends LinkedHashMap<Pair<K1, Pair<K2, K3>>, V> {

	private static final long serialVersionUID = 5309238193705099265L;

	private final int taille;

	public TriKeyCache(final int taille) {
		super(taille, 0.75f, true);
		this.taille = taille;
	}

	public synchronized boolean containKey(final K1 key1, final K2 key2, final K3 key3) {
		return containsKey(key(key1, key2, key3));
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		final TriKeyCache<?, ?, ?, ?> other = (TriKeyCache<?, ?, ?, ?>) obj;
		return taille == other.taille;
	}

	public synchronized V get(final K1 key1, final K2 key2, final K3 key3) {
		return get(key(key1, key2, key3));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + taille;
		return result;
	}

	private Pair<K1, Pair<K2, K3>> key(final K1 key1, final K2 key2, final K3 key3) {
		return new Pair<>(key1, new Pair<>(key2, key3));
	}

	public synchronized void put(final K1 key1, final K2 key2, final K3 key3, final V value) {
		put(key(key1, key2, key3), value);
	}

	@Override
	protected boolean removeEldestEntry(final Map.Entry<Pair<K1, Pair<K2, K3>>, V> eldest) {
		return size() > taille;
	}

}
