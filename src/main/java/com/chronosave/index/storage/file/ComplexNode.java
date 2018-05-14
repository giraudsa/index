package com.chronosave.index.storage.file;

import java.io.IOException;

import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;

public class ComplexNode<K extends Comparable<K>> extends Node1D<K, NodeId> {

	/**
	 * fake noeud
	 * @param keyType
	 * @param valueType
	 * @param index
	 * @param modifs 
	 */
	public ComplexNode(Class<K> keyType, Class<?> dummy, AbstractIndex<?, ?, ?> index, CacheModifications modifs) {
		super(keyType, NodeId.class, index, modifs);
	}

	/**
	 * lecture fichier
	 * @param position
	 * @param abstractIndex
	 * @param keyType
	 * @param valueType
	 * @param lireValeur
	 * @throws IOException
	 * @throws StorageException 
	 * @throws SerializationException 
	 */
	public ComplexNode(long position, AbstractIndex<?, ?, ?> abstractIndex, Class<K> keyType, Class<?> dummy) {
		super(position, abstractIndex, keyType, NodeId.class);
	}

	/**
	 * au runtime
	 * @param keyPosition
	 * @param valuePosition
	 * @param index
	 * @param modifs
	 * @param keyType
	 * @param valueType
	 * @throws SerializationException 
	 * @throws StorageException
	 */
	public ComplexNode(long keyPosition, Long valuePosition, AbstractIndex<?, ?, ?> index, CacheModifications modifs, Class<K> keyType) throws SerializationException {
		super(keyPosition, valuePosition, index, modifs, keyType, NodeId.class);
	}

	@Override
	protected Node1D<K, NodeId> newNode(long keyPosition, Long valyePosition, AbstractIndex<?, ?, ?> index, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return new ComplexNode<>(keyPosition, valyePosition, index, modifs, keyType);
	}

	@Override
	protected void keysAreEquals(long valuePosition, CacheModifications modifs){
		//rien a faire
	}

	/**
	 * 
	 * @param id
	 * @param modifs
	 * @return true if this node is empty
	 * @throws IOException
	 * @throws StorageException
	 * @throws SerializationException  
	 */
	protected boolean supprimerId(String id, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		if(getValue(modifs) == null) return true;
		boolean isEmpty = false;
		AbstractNode<String, String> newNode =  getValue(modifs).deleteAndBalance(id, modifs);
		if(newNode == null) isEmpty = true;
		else setValuePosition(newNode.valuePosition(modifs), modifs);
		return isEmpty;
	}

	protected void storeValue(String id, long positionId, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		NodeId root = getValue(modifs);
		if(root == null) root = new NodeId(positionId, this.index, modifs);
		else root = (NodeId) root.addAndBalance(id, positionId, null, modifs);
		setValuePosition(root.getPosition(), modifs);
	}
}
