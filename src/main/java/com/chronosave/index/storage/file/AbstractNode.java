package com.chronosave.index.storage.file;

import java.io.IOException;

import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;

public abstract class AbstractNode<K, V> {
	protected final AbstractIndex<?, ?, ?> index;
	protected final Class<K> keyType;
	protected final long nodePosition;
	protected final Class<V> valueType;

	/**
	 * from file
	 * 
	 * @param keyType
	 * @param valueType
	 * @param index
	 * @param position
	 */
	public AbstractNode(final Class<K> keyType, final Class<V> valueType, final AbstractIndex<?, ?, ?> index, final long position) {
		super();
		this.keyType = keyType;
		this.valueType = valueType;
		this.index = index;
		this.nodePosition = position;
	}

	/**
	 * runtime
	 * 
	 * @param keyType
	 * @param valueType
	 * @param index
	 * @param position
	 * @param modifs
	 */
	public AbstractNode(final Class<K> keyType, final Class<V> valueType, final AbstractIndex<?, ?, ?> index, final long position, final CacheModifications modifs) {
		super();
		this.keyType = keyType;
		this.valueType = valueType;
		this.index = index;
		this.nodePosition = position;
		modifs.addCache(index.getEndOfFile(), this);
	}

	private boolean isOnlyKey() {
		return keyPositionPosition() == valuePositionPosition();
	}

	protected K getKey(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(keyPosition(modifs), keyType, modifs);
	}

	protected V getValue(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(valuePosition(modifs), valueType, modifs);
	}

	protected void init(final long keyPosition, final Long valuePosition, final CacheModifications modifs) throws SerializationException {
		modifs.addCache(nodePosition, this);
		index.writeFakeAndCache(keyPosition, modifs);// positionClef
		if (!isOnlyKey())
			index.writeFakeAndCache(valuePosition, modifs); // positionValeur
	}

	protected long keyPositionPosition() {
		return nodePosition;
	}

	protected long valuePositionPosition() {
		return keyPositionPosition() + Long.BYTES;
	}

	abstract AbstractNode<K, V> addAndBalance(K key, long keyPosition, Long valuePosition, CacheModifications modifs) throws IOException, StorageException, SerializationException;

	abstract AbstractNode<K, V> deleteAndBalance(K key, CacheModifications modifs) throws IOException, StorageException, SerializationException;

	abstract AbstractNode<K, V> findNode(K key, CacheModifications modifs) throws IOException, StorageException, SerializationException;

	final long getPosition() {
		return nodePosition;
	}

	long keyPosition(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(keyPositionPosition(), Long.class, modifs);
	}

	long valuePosition(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(valuePositionPosition(), Long.class, modifs);
	}

}
