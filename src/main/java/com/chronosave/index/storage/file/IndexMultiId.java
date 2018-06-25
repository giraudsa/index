package com.chronosave.index.storage.file;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StoreException;
import com.chronosave.index.utils.CloseableIterator;
import com.chronosave.index.utils.ReadWriteLock;

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

	@Override
	public CloseableIterator<String> getBetween(final K min, final K max, final ReadWriteLock locker)
			throws IOException, StorageException, SerializationException, InterruptedException {
		return new NodeIterator((ComplexNode<K, String>) getRoot(null), min, max, locker);
	}

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
			if (closed)
				throw new IllegalStateException("Iterator has been closed!");
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

}
