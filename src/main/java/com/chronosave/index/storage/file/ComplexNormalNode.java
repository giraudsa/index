package com.chronosave.index.storage.file;

import java.io.IOException;

import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;

public class ComplexNormalNode<K extends Comparable<K>, V extends Comparable<V>> extends ComplexNode<K, V, SingletonNode<V>> {

	/**
	 * Fake
	 * 
	 * @param keyType
	 * @param index
	 * @param modifs
	 */
	@SuppressWarnings("unchecked")
	public ComplexNormalNode(final Class<K> keyType, final AbstractIndex<?, ?, ?> index, final CacheModifications modifs) {
		super(keyType, (Class<SingletonNode<V>>) (Object) SingletonNode.class, index, modifs);
	}

	/**
	 * File
	 * 
	 * @param position
	 * @param index
	 * @param keyType
	 */
	@SuppressWarnings("unchecked")
	public ComplexNormalNode(final long position, final AbstractIndex<?, ?, ?> index, final Class<K> keyType) {
		super(position, index, keyType, (Class<SingletonNode<V>>) (Object) SingletonNode.class);
	}

	/**
	 * Runtime
	 * 
	 * @param keyPosition
	 * @param valuePosition
	 * @param index
	 * @param keyType
	 * @param modifs
	 * @throws SerializationException
	 */
	@SuppressWarnings("unchecked")
	public ComplexNormalNode(final long keyPosition, final Long valuePosition, final AbstractIndex<?, ?, ?> index, final Class<K> keyType, final CacheModifications modifs) throws SerializationException {
		super(keyPosition, valuePosition, index, keyType, (Class<SingletonNode<V>>) (Object) SingletonNode.class, modifs);
	}

	@Override
	protected Node1D<K, SingletonNode<V>> newNode(final long keyPosition, final Long valuePosition, final AbstractIndex<?, ?, ?> index, final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return new ComplexNormalNode<>(keyPosition, valuePosition, index, keyType, modifs);
	}

}
