package com.chronosave.index.storage.file;

import java.io.IOException;

import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;

public class ComplexNode<K extends Comparable<K>, V extends Comparable<V>> extends Node1D<K, SingletonNode<V>> {

	/**
	 * fake noeud
	 * 
	 * @param keyType
	 * @param valueType
	 * @param index
	 * @param modifs
	 */
	public ComplexNode(final Class<K> keyType, final Class<SingletonNode<V>> singletonNodeType,
			final AbstractIndex<?, ?, ?> index, final CacheModifications modifs) {
		super(keyType, singletonNodeType, index, modifs);
	}

	/**
	 * from file
	 * 
	 * @param position
	 * @param abstractIndex
	 * @param keyType
	 * @param valueType
	 * @param lireValeur
	 * @throws IOException
	 * @throws StorageException
	 * @throws SerializationException
	 */
	public ComplexNode(final long position, final AbstractIndex<?, ?, ?> index, final Class<K> keyType,
			final Class<SingletonNode<V>> singletonNodeType) {
		super(position, index, keyType, singletonNodeType);
	}

	/**
	 * runtime
	 * 
	 * @param keyPosition
	 * @param valuePosition
	 * @param index
	 * @param modifs
	 * @param keyType
	 * @param valueType
	 * @throws SerializationException
	 * @throws StorageException
	 */
	public ComplexNode(final long keyPosition, final Long valuePosition, final AbstractIndex<?, ?, ?> index,
			final Class<K> keyType, final Class<SingletonNode<V>> singletonNodeType, final CacheModifications modifs)
			throws SerializationException {
		super(keyPosition, valuePosition, index, keyType, singletonNodeType, modifs);
	}

	@Override
	protected void keysAreEquals(final Long valuePosition, final CacheModifications modifs) {
		// rien a faire
	}

	@Override
	protected Node1D<K, SingletonNode<V>> newNode(final long keyPosition, final Long valuePosition,
			final AbstractIndex<?, ?, ?> index, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		return new ComplexNode<>(keyPosition, valuePosition, index, keyType, valueType, modifs);
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
	protected boolean removeValue(final V value, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		if (getValue(modifs) == null)
			return true;
		boolean isEmpty = false;
		final AbstractNode<V, V> newNode = getValue(modifs).deleteAndBalance(value, modifs);
		if (newNode == null)
			isEmpty = true;
		else
			setValuePosition(newNode.valuePosition(modifs), modifs);
		return isEmpty;
	}

	@SuppressWarnings("unchecked")
	protected void storeValue(final V id, final long positionId, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		AbstractNode<V, V> root = getValue(modifs);
		if (root == null)
			root = new SingletonNode<>(positionId, index, (Class<V>) id.getClass(), modifs);
		else
			root = root.addAndBalance(id, positionId, null, modifs);
		setValuePosition(root.getPosition(), modifs);
	}
}
