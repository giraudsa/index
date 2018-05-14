package com.chronosave.index.storage.memory;

abstract class MemoryNode<K,V> {
	protected K key;
	protected final V value;
	/**
	 * @param key
	 * @param value
	 */
	public MemoryNode(K key, V value) {
		super();
		this.key = key;
		this.value = value;
	}
	protected abstract MemoryNode<K, V> addAndBalance(K clef, V objet);
	protected abstract MemoryNode<K, V> deleteAndBalance(K clef);
	protected abstract MemoryNode<K, V> findNoeud(K clef);
	
}
