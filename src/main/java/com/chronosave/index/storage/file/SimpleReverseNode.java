package com.chronosave.index.storage.file;

import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;

public class SimpleReverseNode<K, V extends Comparable<V>> extends SimpleNode<V, K> implements ReverseNode {

	/**
	 * Fake Node
	 * 
	 * @param keyType
	 * @param valueType
	 * @param index
	 * @param modifs
	 */
	public SimpleReverseNode(final Class<K> keyType, final Class<V> valueType, final AbstractIndex<?, ?, ?> index, final CacheModifications modifs) {
		super(valueType, keyType, index, modifs);
	}

	/**
	 * File reading
	 * 
	 * @param position
	 * @param indexAbstrait
	 * @param keyType
	 * @param valueType
	 */
	public SimpleReverseNode(final long position, final AbstractIndex<?, ?, ?> indexAbstrait, final Class<K> keyType, final Class<V> valueType) {
		super(position, indexAbstrait, valueType, keyType);
	}

	/**
	 * Runtime
	 * 
	 * @param keyPosition
	 * @param valuePosition
	 * @param index
	 * @param keyType
	 * @param valueType
	 * @param modifs
	 * @throws StorageException
	 * @throws SerializationException
	 */
	public SimpleReverseNode(final long keyPosition, final Long valuePosition, final AbstractIndex<?, ?, ?> index, final Class<K> keyType, final Class<V> valueType, final CacheModifications modifs) throws StorageException, SerializationException {
		super(keyPosition, valuePosition, index, valueType, keyType, modifs);
	}

}
