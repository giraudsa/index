package com.chronosave.index.storage.file;

import java.io.IOException;

import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;

public class ComplexNode<K extends Comparable<K>, V  extends Comparable<V>> extends Node1D<K, SingletonNode<V>> {

	/**
	 * fake noeud
	 * @param keyType
	 * @param valueType
	 * @param index
	 * @param modifs 
	 */
	public ComplexNode(Class<K> keyType, Class<SingletonNode<V>> singletonNodeType, AbstractIndex<?, ?, ?> index, CacheModifications modifs) {
		super(keyType, singletonNodeType, index, modifs);
	}

	/**
	 * from file
	 * @param position
	 * @param abstractIndex
	 * @param keyType
	 * @param valueType
	 * @param lireValeur
	 * @throws IOException
	 * @throws StorageException 
	 * @throws SerializationException 
	 */
	public ComplexNode(long position, AbstractIndex<?, ?, ?> index, Class<K> keyType, Class<SingletonNode<V>> singletonNodeType) {
		super(position, index, keyType, singletonNodeType);
	}

	/**
	 * runtime
	 * @param keyPosition
	 * @param valuePosition
	 * @param index
	 * @param modifs
	 * @param keyType
	 * @param valueType
	 * @throws SerializationException 
	 * @throws StorageException
	 */
	public ComplexNode(long keyPosition, Long valuePosition, AbstractIndex<?, ?, ?> index, Class<K> keyType, Class<SingletonNode<V>> singletonNodeType, CacheModifications modifs) throws SerializationException {
		super(keyPosition, valuePosition, index, keyType, singletonNodeType, modifs);
	}

	@Override
	protected Node1D<K, SingletonNode<V>> newNode(long keyPosition, Long valuePosition, AbstractIndex<?, ?, ?> index, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return new ComplexNode<>(keyPosition, valuePosition, index, keyType, valueType, modifs);
	}

	@Override
	protected void keysAreEquals(Long valuePosition, CacheModifications modifs){
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
	protected boolean removeValue(V value, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		if(getValue(modifs) == null) return true;
		boolean isEmpty = false;
		AbstractNode<V, V> newNode =  getValue(modifs).deleteAndBalance(value, modifs);
		if(newNode == null) isEmpty = true;
		else setValuePosition(newNode.valuePosition(modifs), modifs);
		return isEmpty;
	}

	@SuppressWarnings("unchecked")
	protected void storeValue(V id, long positionId, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		AbstractNode<V, V> root = getValue(modifs);
		if(root == null) root = new SingletonNode<>(positionId, this.index, (Class<V>)id.getClass(), modifs);
		else root = root.addAndBalance(id, positionId, null, modifs);
		setValuePosition(root.getPosition(), modifs);
	}
}
