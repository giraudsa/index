package com.chronosave.index.storage.file;

import java.io.IOException;

import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;

public class ReverseSimpleNode<K, V  extends Comparable<V>> extends SimpleNode<V,K> {

	/**
	 * fake node
	 * @param keyType
	 * @param valueType
	 * @param index 
	 * @param modifs 
	 */
	public ReverseSimpleNode(Class<K> keyType, Class<V> valueType, AbstractIndex<?, ?, ?> index, CacheModifications modifs) {
		super(valueType, keyType, index, modifs);
	}

	/**
	 * file
	 * @param index
	 * @param keyType
	 * @param valueType
	 * @throws IOException
	 * @throws SerializationException 
	 * @throws StorageException 
	 */
	public ReverseSimpleNode(long position, AbstractIndex<?, ?, ?> index, Class<K> keyType, Class<V> valueType) {
		super(position, index, valueType, keyType);
	}

	/**
	 * runtime
	 * @param clef
	 * @param valuePosition
	 * @param index
	 * @param modifs
	 * @param keyType
	 * @param valueType
	 * @throws StorageException
	 * @throws SerializationException 
	 */
	public ReverseSimpleNode(long keyPosition, long valuePosition, AbstractIndex<?, ?, ?> index, Class<K> keyType, Class<V> valueType,  CacheModifications modifs) throws StorageException, SerializationException {
		super(keyPosition, valuePosition, index, valueType, keyType, modifs);
	}

	@Override
	protected Node1D<V, K> newNode(long keyPosition, Long valuePosition, AbstractIndex<?, ?, ?> index, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return new ReverseSimpleNode<>(keyPosition, valuePosition, index, valueType, keyType, modifs);
	}

}
