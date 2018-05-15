package com.chronosave.index.storage.file;

import java.io.IOException;
import java.nio.file.Path;

import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StoreException;

public class IndexKeyToOneId<U, K extends Comparable<K>> extends IndexBiDirectionalId<U, K>{
	private static final String EXTENTION = ".idx1to1";
	
	@SuppressWarnings("rawtypes")
	private static final Class<SimpleNode> simpleNodeType = SimpleNode.class;
	
	
	/**
	 * file
	 * @param file
	 * @param store
	 * @throws IOException
	 * @throws StorageException
	 * @throws ClassNotFoundException
	 * @throws SerializationException
	 * @throws StoreException
	 */
	protected IndexKeyToOneId(Path file, Store<U> store) throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException{
		super(file, store);
	}

	/**
	 * runtime
	 * @param basePath
	 * @param keyType
	 * @param store
	 * @param delegateKey
	 * @throws IOException
	 * @throws StorageException
	 * @throws SerializationException
	 */
	protected IndexKeyToOneId(Path basePath, Class<K> keyType, Store<U> store, ComputeKey<K, U> delegateKey) throws IOException, StorageException, SerializationException {
		super(basePath, keyType, store, EXTENTION, delegateKey);
	}

	@SuppressWarnings("rawtypes") @Override
	protected Class<? extends AbstractNode> getNodeType() {
		return simpleNodeType;
	}
	
	@Override
	protected void add(U object, long version, final CacheModifications modifs) throws StorageException, IOException, SerializationException {
		 //the value is the id --> never == null
		add(getKey(object), computeValue(object, version), modifs);
	}
	
	private void add(K key, String id, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		AbstractNode<String, K> oldReverseNode = getReverseRoot(modifs).findNode(id, modifs);
		if(oldReverseNode != null) {
			K oldKey = oldReverseNode.getValue(modifs);
			if(isEqual(oldKey, key)) return;//nothing to do
			setRoot(getRoot(modifs).deleteAndBalance(oldKey, modifs), modifs);
		}
		long keyPosition = key == null ? NULL : writeFakeAndCache(key, modifs);
		long valuePosition = writeFakeAndCache(id, modifs);
		setRoot(getRoot(modifs).addAndBalance(key, keyPosition, valuePosition, modifs), modifs);
		setReverseRoot((ReverseSimpleNode<K, String>) getReverseRoot(modifs).addAndBalance(id, valuePosition, keyPosition, modifs), modifs);
	}

	protected void delete(String id, long version, CacheModifications modifs) throws IOException, StorageException, SerializationException{
		if(id == null) return;
		AbstractNode<String, K> reverseNode = getReverseRoot(modifs).findNode(id,modifs);
		if(reverseNode == null)
			return; //nothing to do
		K key = reverseNode.getValue(modifs);
		setRoot(getRoot(modifs).deleteAndBalance(key, modifs), modifs);
		setReverseRoot((ReverseSimpleNode<K, String>) getReverseRoot(modifs).deleteAndBalance(id, modifs), modifs);
	}
}
