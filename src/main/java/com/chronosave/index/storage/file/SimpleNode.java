package com.chronosave.index.storage.file;

import java.io.IOException;

import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;

public class SimpleNode<K extends Comparable<K>, V> extends Node1D<K, V>{



	/**
	 * fake node
	 * @param keyType
	 * @param valueType
	 * @param index 
	 * @param modifs 
	 */
	public SimpleNode(Class<K> keyType, Class<V> valueType, AbstractIndex<?, ?, ?> index, CacheModifications modifs) {
		super(keyType, valueType, index, modifs);
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
	public SimpleNode(long keyPosition, Long valuePosition, AbstractIndex<?, ?, ?> index, Class<K> keyType, Class<V> valueType, CacheModifications modifs) throws StorageException, SerializationException {
		super(keyPosition, valuePosition, index, keyType, valueType, modifs);
	}

	/**
	 * file
	 * @param position
	 * @param indexAbstrait
	 * @param keyType
	 * @param valueType
	 */
	public SimpleNode(long position, AbstractIndex<?, ?, ?> indexAbstrait, Class<K> keyType, Class<V> valueType){
		super(position, indexAbstrait, keyType, valueType);
	}

	@Override
	protected Node1D<K, V> newNode(long keyPosition, Long valuePosition, AbstractIndex<?, ?, ?> index, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return new SimpleNode<>(keyPosition, valuePosition, index, keyType, valueType, modifs);
	}

	@Override
	protected void keysAreEquals(Long valuePosition, CacheModifications modifs) {
		setValuePosition(valuePosition, modifs);
	}
	

}
