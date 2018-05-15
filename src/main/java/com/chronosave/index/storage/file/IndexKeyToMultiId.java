package com.chronosave.index.storage.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StoreException;
import com.chronosave.index.utils.CloseableIterator;
import com.chronosave.index.utils.ReadWriteLock;

public class IndexKeyToMultiId<U, K extends Comparable<K>> extends IndexBiDirectionalId<U, K>{

	private static final String EXTENTION = ".idx1tom";
	protected static final <U> void feed(Path basePath, Map<ComputeKey<?, U>, AbstractIndex<U, ?, ?>> index, Store<U> store) throws IOException, ClassNotFoundException, StorageException, SerializationException, StoreException{
		String debutNom = store.debutNomFichier();
		File[] idxs = IndexedStorageManager.findAllFileThatBeginsWith(basePath, debutNom + EXTENTION);
		for(File fidx : idxs) {
			AbstractIndex<U,?, ?> idx = new IndexKeyToMultiId<>(fidx.toPath(), store);
			ComputeKey<?, U> fc = (ComputeKey<?, U>) idx.getDelegateKey();
			index.put(fc, idx);
		}
	}
	@SuppressWarnings("rawtypes")
	private Class<ComplexNode> nodeType = ComplexNode.class;	
	
	/**
	 * runtime
	 * @param keyType
	 * @param datas
	 * @param delegateKey
	 * @throws IOException
	 * @throws StorageException
	 * @throws SerializationException 
	 */
	public IndexKeyToMultiId(Path basePath, Class<K> keyType, Store<U> store, ComputeKey<K, U> delegateKey) throws IOException, StorageException, SerializationException {
		super(basePath, keyType, store, EXTENTION, delegateKey);
	}

	/**
	 * file
	 * @param keyType
	 * @param store
	 * @param datas
	 * @throws IOException
	 * @throws StorageException
	 * @throws ClassNotFoundException
	 * @throws SerializationException 
	 * @throws StoreException 
	 */
	public IndexKeyToMultiId(Path file, Store<U> store) throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException {
		super(file, store);
	}
	
	@Override
	protected void add(U objectToAdd, long version, final CacheModifications modifs) throws StorageException, IOException, SerializationException {
		K key = getKey(objectToAdd);
		String id = computeValue(objectToAdd, version);
		add(key, id, modifs);
	}
	
	@SuppressWarnings("unchecked")
	private void add(K key, String id, final CacheModifications modifs) throws StorageException, IOException, SerializationException {
		AbstractNode<String, K> oldReverseNode = getReverseRoot(modifs).findNode(id, modifs);
		long idPosition = NULL;
		if(oldReverseNode != null) {
			idPosition = oldReverseNode.keyPosition(modifs);
			K oldKey = oldReverseNode.getValue(modifs);
			if(isEqual(oldKey,key)) return;//nothing to do
			ComplexNode<K> oldComplexNode = (ComplexNode<K>) getRoot(modifs).findNode(oldKey, modifs);
			boolean mustDeleteOldComplexNode = oldComplexNode.supprimerId(id, modifs);
			if(mustDeleteOldComplexNode) setRoot(getRoot(modifs).deleteAndBalance(oldKey, modifs), modifs);
		}
		idPosition = idPosition == NULL ? writeFakeAndCache(id, modifs) : idPosition;
		long keyPosition = key == null ? NULL : writeFakeAndCache(key, modifs);
		setRoot(getRoot(modifs).addAndBalance(key, keyPosition, NULL, modifs), modifs);
		ComplexNode<K> n = (ComplexNode<K>) getRoot(modifs).findNode(key, modifs);
		n.storeValue(id, idPosition, modifs);
		setReverseRoot((ReverseSimpleNode<K, String>) getReverseRoot(modifs).addAndBalance(id, idPosition, keyPosition, modifs), modifs);
	}

	@SuppressWarnings("unchecked") @Override
	protected void delete(U objet, long version, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		String id = computeValue(objet, store.getVersion());
		AbstractNode<String, K> reverseNode = getReverseRoot(modifs).findNode(id, modifs);
		if(reverseNode == null)
			return;//nothing to do
		setReverseRoot((ReverseSimpleNode<K, String>) getReverseRoot(modifs).deleteAndBalance(id, modifs), modifs);
		K oldKey = reverseNode.getValue(modifs);
		ComplexNode<K> complexNode = (ComplexNode<K>) getRoot(modifs).findNode(oldKey, modifs);
		boolean mustDeleteOldComplexNode = complexNode.supprimerId(id, modifs);
		if(mustDeleteOldComplexNode) setRoot(getRoot(modifs).deleteAndBalance(oldKey, modifs), modifs);
	}

	@SuppressWarnings("rawtypes") @Override
	protected Class<? extends AbstractNode> getNodeType() {
		return nodeType;
	}
	
	@SuppressWarnings("unchecked")
	public CloseableIterator<String> getBetween(K min, K max, ReadWriteLock locker) throws IOException, StorageException, SerializationException, InterruptedException {
		return new NodeIterator((ComplexNode<K>) getRoot(null), min, max, locker);
	}
	
	
	private class NodeIterator implements CloseableIterator<String>{

		private final Iterator<NodeId> complexNodeIterator;
		private final ReadWriteLock locker;
		private Iterator<String> idNodeIterator;
		private String next;
		private boolean hasNext;
		private boolean closed = false;
		
		private NodeIterator(final ComplexNode<K> root, K min, K max, ReadWriteLock locker) throws InterruptedException {
			locker.lockRead();
			this.locker = locker;
			complexNodeIterator = root.iterator(min, max);
			cacheNext();
		}
		
		private void cacheNext() {
			if(idNodeIterator != null && idNodeIterator.hasNext()) {
				next = idNodeIterator.next();
				hasNext = true;
				return;
			}
			while(complexNodeIterator.hasNext()) {
				idNodeIterator = complexNodeIterator.next().iterator();
				hasNext = idNodeIterator.hasNext();
				next = hasNext ? idNodeIterator.next() : null;
				if(hasNext)
					return;
			}
		}

		@Override
		public boolean hasNext() {
			if (closed)
		            throw new IllegalStateException("Iterator has been closed!");
			return hasNext;
		}

		@Override
		public String next() {
			if(next == null)
				throw new NoSuchElementException();
			String ret = next;
			cacheNext();
			return ret;
		}

		@Override
		public void close() throws IOException {
			locker.unlockRead();
			closed = true;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}
}
