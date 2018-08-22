package com.chronosave.index.storage.file;

import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;

public class SingletonReverseNode<V extends Comparable<V>> extends SingletonNode<V> implements ReverseNode {

	/**
	 * fake node
	 * 
	 * @param keyType
	 * @param index
	 * @param modifs
	 */
	public SingletonReverseNode(final Class<V> valueType, final AbstractIndex<?, ?, ?> index, final CacheModifications modifs) {
		super(valueType, index, modifs);
	}

	/**
	 * Runtime
	 * 
	 * @param keyPosition
	 * @param index
	 * @param keyType
	 * @param modifs
	 * @throws StorageException
	 * @throws SerializationException
	 */
	public SingletonReverseNode(final long keyPosition, final AbstractIndex<?, ?, ?> index, final Class<V> valueType, final CacheModifications modifs) throws StorageException, SerializationException {
		super(keyPosition, index, valueType, modifs);
	}

	/**
	 * File
	 * 
	 * @param position
	 * @param index
	 * @param keyType
	 */
	public SingletonReverseNode(final long position, final AbstractIndex<?, ?, ?> index, final Class<V> valueType) {
		super(position, index, valueType);
	}

}
