package com.chronosave.index.storage.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StoreException;
import com.chronosave.index.utils.CloseableIterator;
import com.chronosave.index.utils.ReadWriteLock;

public class IndexKeyToMultiId<U, K extends Comparable<K>> extends IndexMultiId<U, K, K> {

	private class NodeIterator implements CloseableIterator<String> {

		private boolean closed = false;
		private final Iterator<SingletonNode<String>> complexNodeIterator;
		private boolean hasNext;
		private Iterator<String> idNodeIterator;
		private final ReadWriteLock locker;
		private String next;

		private NodeIterator(final ComplexNode<K, String> root, final K min, final K max, final ReadWriteLock locker)
				throws InterruptedException {
			locker.lockRead();
			this.locker = locker;
			complexNodeIterator = root.iterator(min, max);
			cacheNext();
		}

		private void cacheNext() {
			if (idNodeIterator != null && idNodeIterator.hasNext()) {
				next = idNodeIterator.next();
				hasNext = true;
				return;
			}
			while (complexNodeIterator.hasNext()) {
				idNodeIterator = complexNodeIterator.next().iterator();
				hasNext = idNodeIterator.hasNext();
				next = hasNext ? idNodeIterator.next() : null;
				if (hasNext)
					return;
			}
		}

		@Override
		public void close() throws IOException {
			locker.unlockRead();
			closed = true;
		}

		@Override
		public boolean hasNext() {
			if (closed)
				throw new IllegalStateException("Iterator has been closed!");
			return hasNext;
		}

		@Override
		public String next() {
			if (next == null)
				throw new NoSuchElementException();
			final String ret = next;
			cacheNext();
			return ret;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

	private static final String EXTENTION = ".idx1tom";

	protected static final <U> void feed(final Path basePath, final Map<ComputeKey<?, U>, AbstractIndex<U, ?, ?>> index,
			final Store<U> store)
			throws IOException, ClassNotFoundException, StorageException, SerializationException, StoreException {
		final String debutNom = store.debutNomFichier();
		final File[] idxs = IndexedStorageManager.findAllFileThatBeginsWith(basePath, debutNom + EXTENTION);
		for (final File fidx : idxs) {
			final AbstractIndex<U, ?, ?> idx = new IndexKeyToMultiId<>(fidx.toPath(), store);
			final ComputeKey<?, U> fc = idx.getDelegateKey();
			index.put(fc, idx);
		}
	}

	/**
	 * runtime
	 * 
	 * @param keyType
	 * @param datas
	 * @param delegateKey
	 * @throws IOException
	 * @throws StorageException
	 * @throws SerializationException
	 */
	public IndexKeyToMultiId(final Path basePath, final Class<K> keyType, final Store<U> store,
			final ComputeKey<K, U> delegateKey) throws IOException, StorageException, SerializationException {
		super(basePath, keyType, store, EXTENTION, delegateKey);
	}

	/**
	 * file
	 * 
	 * @param keyType
	 * @param store
	 * @param datas
	 * @throws IOException
	 * @throws StorageException
	 * @throws ClassNotFoundException
	 * @throws SerializationException
	 * @throws StoreException
	 */
	public IndexKeyToMultiId(final Path file, final Store<U> store)
			throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException {
		super(file, store);
	}

	@Override
	protected void add(final K key, final String id, final CacheModifications modifs)
			throws StorageException, IOException, SerializationException {
		final AbstractNode<String, K> oldReverseNode = getReverseRoot(modifs).findNode(id, modifs);
		if (oldReverseNode != null) {
			final K oldKey = oldReverseNode.getValue(modifs);
			if (isEqual(oldKey, key))
				return;// nothing to do
			final AbstractNode<K, SingletonNode<String>> oldComplexNode = getRoot(modifs).findNode(oldKey, modifs);
			final boolean mustDeleteOldComplexNode = ((ComplexNode<K, String>) oldComplexNode).removeValue(id, modifs);
			if (mustDeleteOldComplexNode)
				setRoot(getRoot(modifs).deleteAndBalance(oldKey, modifs), modifs);
		}
		add(key, getKeyPosition(key, modifs), id, getIdPosition(id, modifs), modifs);
	}

	@Override
	protected void delete(final String id, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		final AbstractNode<String, K> reverseNode = getReverseRoot(modifs).findNode(id, modifs);
		if (reverseNode == null)
			return;// nothing to do
		final K oldKey = reverseNode.getValue(modifs);
		delete(oldKey, id, modifs);
	}

	public CloseableIterator<String> getBetween(final K min, final K max, final ReadWriteLock locker)
			throws IOException, StorageException, SerializationException, InterruptedException {
		return new NodeIterator((ComplexNode<K, String>) getRoot(null), min, max, locker);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Class<? extends AbstractNode<String, K>> getReverseNodeType() {
		return (Class<? extends AbstractNode<String, K>>) SimpleNode.class;
	}
}
