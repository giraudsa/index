package com.chronosave.index.storage.file;

import java.io.IOException;

import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;

public abstract class AbstractNode<K, V> {
	protected final Class<K> keyType;
	protected final Class<V> valueType;
	protected final AbstractIndex<?,?,?> index;
	protected final long nodePosition;
	protected long keyPositionPosition() { return nodePosition;}
	protected long valuePositionPosition() { return keyPositionPosition() + Long.SIZE / 8;}
	private boolean isOnlyKey() {
		return keyPositionPosition() == valuePositionPosition();
	}
	
	/**
	 * runtime
	 * @param keyType
	 * @param valueType
	 * @param index
	 * @param position
	 * @param modifs
	 */
	public AbstractNode(Class<K> keyType, Class<V> valueType, AbstractIndex<?, ?, ?> index, long position, CacheModifications modifs) {
		super();
		this.keyType = keyType;
		this.valueType = valueType;
		this.index = index;
		this.nodePosition = position;
		modifs.addCache(index.getEndOfFile(), this);
	}
	
	/**
	 * from file
	 * @param keyType
	 * @param valueType
	 * @param index
	 * @param position
	 */
	public AbstractNode(Class<K> keyType, Class<V> valueType, AbstractIndex<?, ?, ?> index, long position) {
		super();
		this.keyType = keyType;
		this.valueType = valueType;
		this.index = index;
		this.nodePosition = position;
	}
	
	abstract AbstractNode<K, V> addAndBalance(K key, long keyPosition, Long valuePosition, CacheModifications modifs) throws IOException, StorageException, SerializationException;
	abstract AbstractNode<K, V> deleteAndBalance(K key, CacheModifications modifs) throws IOException, StorageException, SerializationException;
	abstract AbstractNode<K, V> findNode(K key, CacheModifications modifs) throws IOException, StorageException, SerializationException;
	
	long keyPosition(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(keyPositionPosition(), Long.class, modifs);
	}
	long valuePosition(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(valuePositionPosition(), Long.class, modifs);
	}
	final long getPosition() {
		return nodePosition;
	}

	protected K getKey(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(keyPosition(modifs), keyType, modifs);
	}

	protected V getValue(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(valuePosition(modifs), valueType, modifs);
	}

	protected void init(long keyPosition, long valuePosition, CacheModifications modifs) throws SerializationException {
		modifs.addCache(nodePosition, this);
		index.writeFakeAndCache(keyPosition, modifs);//positionClef
		if(!isOnlyKey()) 
			index.writeFakeAndCache(valuePosition, modifs); //positionValeur
	}

}
