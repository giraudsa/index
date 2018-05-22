package com.chronosave.index.storage.file;

import java.io.IOException;

import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;

public class SingletonNode<K extends Comparable<K>> extends SimpleNode<K, K> {

	/**
	 * fake node
	 * 
	 * @param keyType
	 * @param valueType
	 * @param index
	 * @param modifs
	 */
	public SingletonNode(final Class<K> keyType, final Class<?> dummy, final AbstractIndex<?, ?, ?> index,
			final CacheModifications modifs) {
		super(keyType, keyType, index, modifs);
	}

	/**
	 * runtime
	 * 
	 * @param keyPosition
	 * @param positionValeur
	 * @param index
	 * @param modifs
	 * @param keyType
	 * @param valueType
	 * @throws StorageException
	 * @throws SerializationException
	 */
	public SingletonNode(final long keyPosition, final AbstractIndex<?, ?, ?> index, final Class<K> keyType,
			final CacheModifications modifs) throws StorageException, SerializationException {
		super(keyPosition, null, index, keyType, keyType, modifs);
	}

	/**
	 * file
	 * 
	 * @param position
	 * @param indexAbstrait
	 * @param keyType
	 * @param valueType
	 * @throws IOException
	 * @throws StorageException
	 * @throws SerializationException
	 */
	public SingletonNode(final long position, final AbstractIndex<?, ?, ?> index, final Class<K> keyType,
			final Class<?> dummy) {
		super(position, index, keyType, keyType);
	}

	@Override
	protected K getValue(final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return getKey(modifs);
	}

	@Override
	protected Node1D<K, K> newNode(final long keyPosition, final Long valuePosition, final AbstractIndex<?, ?, ?> index,
			final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return new SingletonNode<>(keyPosition, index, keyType, modifs);
	}

	@Override
	protected void setValuePosition(final Long dummy1, final CacheModifications dummy2) {
		/* no value ! */}

	@Override
	protected long valuePositionPosition() {
		return keyPositionPosition();
	}

}
