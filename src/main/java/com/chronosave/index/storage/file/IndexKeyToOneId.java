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
 * @param <K>Key
 */
public class IndexKeyToOneId<U, K extends Comparable<K>> extends IndexBiDirectionalId<U, K, String, K> {
	private static final String EXTENTION = ".idx1to1";

	/**
	 * runtime
	 * 
	 * @param basePath
	 * @param keyType
	 * @param store
	 * @param delegateKey
	 * @throws IOException
	 * @throws StorageException
	 * @throws SerializationException
	 */
	protected IndexKeyToOneId(final Path basePath, final Class<K> keyType, final Store<U> store,
			final ComputeKey<K, U> delegateKey) throws IOException, StorageException, SerializationException {
		super(basePath, keyType, store, EXTENTION, delegateKey);
	}

	/**
	 * file
	 * 
	 * @param file
	 * @param store
	 * @throws IOException
	 * @throws StorageException
	 * @throws ClassNotFoundException
	 * @throws SerializationException
	 * @throws StoreException
	 */
	protected IndexKeyToOneId(final Path file, final Store<U> store)
			throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException {
		super(file, store);
	}

	@Override
	protected void add(final K key, final String id, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		final AbstractNode<String, K> oldReverseNode = getReverseRoot(modifs).findNode(id, modifs);
		if (oldReverseNode != null) {
			final K oldKey = oldReverseNode.getValue(modifs);
			if (isEqual(oldKey, key))
				return;// nothing to do
			deleteKtoId(oldKey, id, modifs);
		}
		add(key, getKeyPosition(key, modifs), id, getIdPosition(id, modifs), modifs);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void delete(final String id, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		if (id == null)
			return;
		final AbstractNode<String, ?> reverseNode = getReverseRoot(modifs).findNode(id, modifs);
		if (reverseNode == null)
			return; // nothing to do
		final K key = (K) reverseNode.getValue(modifs);
		delete(key, id, modifs);
	}

	@Override
	protected void deleteKtoId(final K key, final String value, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		setRoot(getRoot(modifs).deleteAndBalance(key, modifs), modifs);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Class<? extends AbstractNode<K, String>> getNodeType() {
		return (Class<? extends AbstractNode<K, String>>) SimpleNode.class;

	}

	@SuppressWarnings("unchecked")
	@Override
	protected Class<? extends AbstractNode<String, K>> getReverseNodeType() {
		return (Class<? extends AbstractNode<String, K>>) SimpleNode.class;
	}
}
