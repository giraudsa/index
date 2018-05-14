package com.chronosave.index.storage.file;

import java.io.IOException;

import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;

public class NodeId extends SingletonNode<String>{

	/**
	 * fake node
	 * @param keyType
	 * @param index
	 * @param modifs 
	 */
	public NodeId(Class<?> dum1, Class<?> dumm2, AbstractIndex<?, ?, ?> index, CacheModifications modifs) {
		super(String.class, dum1, index, modifs);
	}

	/**
	 * runtime
	 * @param keyPosition
	 * @param index
	 * @param modifs
	 * @param keyType
	 * @param valueType
	 * @throws StorageException
	 * @throws SerializationException 
	 */
	public NodeId(long keyPosition, AbstractIndex<?, ?, ?> index, CacheModifications modifs) throws StorageException, SerializationException {
		super(keyPosition, index, modifs, String.class);
	}

	/**
	 * file
	 * @param position
	 * @param indexAbstrait
	 * @param keyType
	 * @param dummy
	 */
	public NodeId(long position, AbstractIndex<?, ?, ?> indexAbstrait, Class<String> keyType, Class<?> dummy) {
		super(position, indexAbstrait, keyType, dummy);
	}
	
	@Override
	protected Node1D<String, String> newNode(long keyPosition, Long valuePOsition, AbstractIndex<?, ?, ?> index, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return new NodeId(keyPosition, index, modifs);
	}


}
