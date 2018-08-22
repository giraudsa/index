package com.chronosave.index.storage.file;

import java.io.IOException;

import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;

public class ComplexReverseNode<K extends Comparable<K>, V extends Comparable<V>> extends ComplexNode<V, K, SingletonReverseNode<K>> implements ReverseNode {

	/**
	 * Fake Node
	 * 
	 * @param keyType
	 * @param index
	 * @param modifs
	 */
	@SuppressWarnings("unchecked")
	public ComplexReverseNode(final Class<V> valueType, final AbstractIndex<?, ?, ?> index, final CacheModifications modifs) {
		super(valueType, (Class<SingletonReverseNode<K>>) (Object) SingletonReverseNode.class, index, modifs);
	}

	/**
	 * File Reading
	 * 
	 * @param position
	 * @param index
	 * @param keyType
	 */
	@SuppressWarnings("unchecked")
	public ComplexReverseNode(final long position, final AbstractIndex<?, ?, ?> index, final Class<V> valueType) {
		super(position, index, valueType, (Class<SingletonReverseNode<K>>) (Object) SingletonReverseNode.class);
	}

	/**
	 * Runtime creation
	 * 
	 * @param keyPosition
	 * @param valuePosition
	 * @param index
	 * @param keyType
	 * @param modifs
	 * @throws SerializationException
	 */
	@SuppressWarnings("unchecked")
	public ComplexReverseNode(final long keyPosition, final Long valuePosition, final AbstractIndex<?, ?, ?> index, final Class<V> valueType, final CacheModifications modifs) throws SerializationException {
		super(keyPosition, valuePosition, index, valueType, (Class<SingletonReverseNode<K>>) (Object) SingletonReverseNode.class, modifs);
	}

	@Override
	protected Node1D<V, SingletonReverseNode<K>> newNode(final long keyPosition, final Long valuePosition, final AbstractIndex<?, ?, ?> index, final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return new ComplexReverseNode<>(keyPosition, valuePosition, index, keyType, modifs);
	}

}
