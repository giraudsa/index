package com.chronosave.index.storage.file;

import java.io.IOException;

import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;

public class SingletonNode<K extends Comparable<K>> extends SimpleNode<K,K>{

	/**
	 * fake node
	 * @param keyType
	 * @param valueType
	 * @param index
	 * @param modifs 
	 */
	public SingletonNode(Class<K> keyType, Class<?> dummy, AbstractIndex<?, ?, ?> index, CacheModifications modifs) {
		super(keyType, keyType, index, modifs);
	}

	/**
	 * file
	 * @param position
	 * @param indexAbstrait
	 * @param keyType
	 * @param valueType
	 * @throws IOException
	 * @throws StorageException 
	 * @throws SerializationException 
	 */
	public SingletonNode(long position, AbstractIndex<?, ?, ?> index, Class<K> keyType, Class<?> dummy){
		super(position, index, keyType, keyType);
	}

	/**
	 * runtime
	 * @param keyPosition
	 * @param positionValeur
	 * @param index
	 * @param modifs
	 * @param keyType
	 * @param valueType
	 * @throws StorageException
	 * @throws SerializationException 
	 */
	public SingletonNode(long keyPosition, AbstractIndex<?, ?, ?> index, CacheModifications modifs, Class<K> keyType) throws StorageException, SerializationException {
		super(keyPosition, null, index, modifs, keyType, keyType);
	}
	
	@Override
	protected Node1D<K, K> newNode(long keyPosition, Long valuePosition, AbstractIndex<?, ?, ?> index, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return new SingletonNode<>(keyPosition, index, modifs, keyType);
	}
	@Override
	protected void setValuePosition(long dummy1, CacheModifications dummy2) {/* no value ! */}
	
	@Override
	protected K getValue(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return getKey(modifs);
	}
	
	@Override
	protected long valuePositionPosition() {
		return keyPositionPosition();
	}

	
}
