package com.chronosave.index.storage.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StoreException;

public class IndexMultiKeyToMultiId<U, K extends Comparable<K>> extends IndexMultiId<U, K, SingletonReverseNode<K>> {
	private static final String EXTENTION = ".idxmtom";

	protected static final <U> void feed(final Path basePath, final Map<ComputeKey<?, U>, AbstractIndex<U, ?, ?>> index, final Store<U> store) throws IOException, ClassNotFoundException, StorageException, SerializationException, StoreException {
		final String debutNom = store.debutNomFichier();
		final File[] idxs = IndexedStorageManager.findAllFileThatBeginsWith(basePath, debutNom + EXTENTION);
		for (final File fidx : idxs) {
			final AbstractIndex<U, ?, ?> idx = new IndexMultiKeyToMultiId<>(fidx.toPath(), store);
			final ComputeKey<?, U> fc = idx.getDelegateKey();
			index.put(fc, idx);
		}
	}

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
	public IndexMultiKeyToMultiId(final Path basePath, final Class<K> keyType, final Store<U> store, final ComputeKey<K, U> delegateKey) throws IOException, StorageException, SerializationException {
		super(basePath, keyType, store, EXTENTION, delegateKey);
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
	public IndexMultiKeyToMultiId(final Path file, final Store<U> store) throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException {
		super(file, store);
	}

	private void add(final Set<K> keys, final String id, final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		final AbstractNode<String, SingletonReverseNode<K>> oldReverseNode = getReverseRoot(modifs).findNode(id, modifs);
		final Set<K> nothingToDo = new HashSet<>();
		if (oldReverseNode != null)
			for (final K oldKey : oldReverseNode.getValue(modifs))
				if (keys.contains(oldKey))
					nothingToDo.add(oldKey);
				else
					delete(oldKey, id, modifs);
		final long idPosition = getIdPosition(id, modifs);
		for (final K newKey : keys)
			if (!nothingToDo.contains(newKey))
				add(newKey, getKeyPosition(newKey, modifs), id, idPosition, modifs);
	}

	private Collection<K> getKeys(final U objectToAdd) throws StorageException {
		return getDelegateKey().getKeys(objectToAdd);
	}

	@Override
	protected void add(final U objet, final long version, final CacheModifications modifs) throws StorageException, IOException, SerializationException {
		add(new HashSet<>(getKeys(objet)), computeValue(objet, version), modifs);
	}

	@Override
	protected void addValueToKey(final String id, final long idPosition, final K key, final long keyPosition, final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setReverseRoot(getReverseRoot(modifs).addAndBalance(id, idPosition, keyPosition, modifs), modifs);
		final ComplexReverseNode<K, String> reverse = (ComplexReverseNode<K, String>) getReverseRoot(modifs).findNode(id, modifs);
		reverse.storeValue(key, keyPosition, modifs);
	}

	@Override
	protected AbstractNode<String, SingletonReverseNode<K>> createFakeReverseNode(final CacheModifications modifs) {
		return new ComplexReverseNode<>(getValueType(), this, modifs);
	}

	@Override
	protected void delete(final String id, final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		final ComplexReverseNode<K, String> oldReverseNode = (ComplexReverseNode<K, String>) getReverseRoot(modifs).findNode(id, modifs);
		if (oldReverseNode != null)
			for (final K oldKey : oldReverseNode.getValue(modifs))
				delete(oldKey, id, modifs);
	}

	@Override
	protected void deleteIdtoK(final String id, final K oldKey, final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		final ComplexReverseNode<K, String> reverseComplexNode = (ComplexReverseNode<K, String>) getReverseRoot(modifs).findNode(id, modifs);
		final boolean aSupprimer = reverseComplexNode.removeValue(oldKey, modifs);
		if (aSupprimer)
			setReverseRoot(getReverseRoot(modifs).deleteAndBalance(id, modifs), modifs);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Class<? extends AbstractNode<String, SingletonReverseNode<K>>> getReverseNodeType() {
		return (Class<? extends AbstractNode<String, SingletonReverseNode<K>>>) ComplexReverseNode.class;
	}

	@Override
	protected <N extends AbstractNode<?, ?>> AbstractNode<?, ?> readAbstractNode(final long nodePosition, final Class<N> nodeType) {
		if (ComplexReverseNode.class.isAssignableFrom(nodeType))
			return new ComplexReverseNode<>(nodePosition, this, getValueType());
		if (SingletonReverseNode.class.isAssignableFrom(nodeType))
			return new SingletonReverseNode<>(nodePosition, this, getValueType());
		if (SingletonNode.class.isAssignableFrom(nodeType))
			return new SingletonNode<>(nodePosition, this, getKeyType());
		return new ComplexNormalNode<>(nodePosition, this, getKeyType());
	}
}
