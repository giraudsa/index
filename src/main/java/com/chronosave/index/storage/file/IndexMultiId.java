package com.chronosave.index.storage.file;

import java.io.IOException;
import java.nio.file.Path;

import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StoreException;

/**
 *
 * @author giraudsa
 *
 * @param <U>
 *            Object
 * @param <K>
 *            Key
 * @param <N>
 *            Value type of Node
 * @param <R>
 *            Value type of Reverse node
 */
public abstract class IndexMultiId<U, K extends Comparable<K>, R>
		extends IndexBiDirectionalId<U, K, SingletonNode<String>, R> {
	/**
	 * runtime
	 * 
	 * @param basePath
	 * @param keyType
	 * @param store
	 * @param extention
	 * @param delegateKey
	 * @throws IOException
	 * @throws StorageException
	 * @throws SerializationException
	 */
	public IndexMultiId(final Path basePath, final Class<K> keyType, final Store<U> store, final String extention,
			final ComputeKey<K, U> delegateKey) throws IOException, StorageException, SerializationException {
		super(basePath, keyType, store, extention, delegateKey);
	}

	/**
	 * from file
	 * 
	 * @param file
	 * @param store
	 * @throws IOException
	 * @throws StorageException
	 * @throws ClassNotFoundException
	 * @throws SerializationException
	 * @throws StoreException
	 */
	public IndexMultiId(final Path file, final Store<U> store)
			throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException {
		super(file, store);
	}

	@Override
	protected void addKeyToValue(final K key, final long keyPosition, final String id, final long idPosition,
			final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setRoot(getRoot(modifs).addAndBalance(key, keyPosition, NULL, modifs), modifs);
		final ComplexNode<K, String> n = (ComplexNode<K, String>) getRoot(modifs).findNode(key, modifs);
		n.storeValue(id, idPosition, modifs);
	}

	@Override
	protected void deleteKtoId(final K key, final String id, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		final ComplexNode<K, String> complexNode = (ComplexNode<K, String>) getRoot(modifs).findNode(key, modifs);
		final boolean aSupprimer = complexNode.removeValue(id, modifs);
		if (aSupprimer)
			setRoot(getRoot(modifs).deleteAndBalance(key, modifs), modifs);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Class<? extends AbstractNode<K, SingletonNode<String>>> getNodeType() {
		return (Class<? extends AbstractNode<K, SingletonNode<String>>>) ComplexNode.class;
	}

	@Override
	protected Class<?> getValueTypeOfNode() {
		return SingletonNode.class;
	}

}
