package com.chronosave.index.storage.file;

import java.io.IOException;

import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;

public class SimpleNode<K extends Comparable<K>, V> extends Node1D<K, V> {

	/**
	 * fake node
	 * 
	 * @param keyType
	 * @param valueType
	 * @param index
	 * @param modifs
	 */
	public SimpleNode(final Class<K> keyType, final Class<V> valueType, final AbstractIndex<?, ?, ?> index,
			final CacheModifications modifs) {
		super(keyType, valueType, index, modifs);
	}

	/**
	 * file
	 * 
	 * @param position
	 * @param indexAbstrait
	 * @param keyType
	 * @param valueType
	 */
	public SimpleNode(final long position, final AbstractIndex<?, ?, ?> indexAbstrait, final Class<K> keyType,
			final Class<V> valueType) {
		super(position, indexAbstrait, keyType, valueType);
	}

	/**
	 * runtime
	 * 
	 * @param clef
	 * @param valuePosition
	 * @param index
	 * @param modifs
	 * @param keyType
	 * @param valueType
	 * @throws StorageException
	 * @throws SerializationException
	 */
	public SimpleNode(final long keyPosition, final Long valuePosition, final AbstractIndex<?, ?, ?> index,
			final Class<K> keyType, final Class<V> valueType, final CacheModifications modifs)
			throws StorageException, SerializationException {
		super(keyPosition, valuePosition, index, keyType, valueType, modifs);
	}

	@Override
	protected void keysAreEquals(final Long valuePosition, final CacheModifications modifs) {
		setValuePosition(valuePosition, modifs);
	}

	@Override
	protected Node1D<K, V> newNode(final long keyPosition, final Long valuePosition, final AbstractIndex<?, ?, ?> index,
			final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return new SimpleNode<>(keyPosition, valuePosition, index, keyType, valueType, modifs);
	}

}
