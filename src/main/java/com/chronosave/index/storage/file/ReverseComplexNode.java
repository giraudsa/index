package com.chronosave.index.storage.file;

import java.io.IOException;

import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;

public class ReverseComplexNode<K extends Comparable<K>, V extends Comparable<V>> extends ComplexNode<V, K> {

	/**
	 * fakeNoeud
	 * @param keyType
	 * @param dummy
	 * @param index
	 * @param modifs
	 */
	public ReverseComplexNode(Class<V> valueType, Class<SingletonNode<K>> singletonKeyType, AbstractIndex<?, ?, ?> index, CacheModifications modifs) {
		super(valueType, singletonKeyType, index, modifs);
	}

	/**
	 * from file
	 * @param position
	 * @param abstractIndex
	 * @param keyType
	 * @param dummy
	 */
	public ReverseComplexNode(long position, AbstractIndex<?, ?, ?> index, Class<V> valueType, Class<SingletonNode<K>> singletonKeyType) {
		super(position, index, valueType, singletonKeyType);
	}

	/**
	 * runtime
	 * @param keyPosition
	 * @param valuePosition
	 * @param index
	 * @param modifs
	 * @param keyType
	 * @throws SerializationException
	 */
	public ReverseComplexNode(long position, Long valuePosition, AbstractIndex<?, ?, ?> index, Class<V> valueType, Class<SingletonNode<K>> singletonKeyType, CacheModifications modifs) throws SerializationException {
		super(position, valuePosition, index, valueType, singletonKeyType, modifs);
	}
	
	@Override
	protected Node1D<V, SingletonNode<K>> newNode(long keyPosition, Long valuePosition, AbstractIndex<?, ?, ?> index,
			CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return new ReverseComplexNode<>(keyPosition, valuePosition, index, keyType, valueType, modifs);
	}

}
