package com.chronosave.index.storage.file;

import java.io.IOException;
import java.nio.file.Path;

import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StoreException;

/**
 * 
 * @author giraudsa
 *
 * @param <U> Object
 * @param <K>Key
 */
public class IndexKeyToOneId<U, K extends Comparable<K>> extends IndexBiDirectionalId<U, K, String, K>{
	private static final String EXTENTION = ".idx1to1";

	
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

	@SuppressWarnings("unchecked") @Override
	protected Class<? extends AbstractNode<K, String>> getNodeType() {
		return (Class<? extends AbstractNode<K, String>>) SimpleNode.class;

	}
	
	@Override
	protected void add(K key, String id, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		AbstractNode<String, K> oldReverseNode = getReverseRoot(modifs).findNode(id, modifs);
		if(oldReverseNode != null) {
			K oldKey = oldReverseNode.getValue(modifs);
			if(isEqual(oldKey, key)) return;//nothing to do
			deleteKtoId(oldKey, id, modifs);
		}
		add(key, getKeyPosition(key, modifs), id, getIdPosition(id, modifs), modifs);
	}

	@SuppressWarnings("unchecked")
	protected void delete(String id, CacheModifications modifs) throws IOException, StorageException, SerializationException{
		if(id == null) return;
		AbstractNode<String, ?> reverseNode = getReverseRoot(modifs).findNode(id,modifs);
		if(reverseNode == null)
			return; //nothing to do
		K key = (K) reverseNode.getValue(modifs);
		delete(key, id, modifs);
	}

	@Override
	protected void deleteKtoId(K key, String value, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setRoot(getRoot(modifs).deleteAndBalance(key, modifs), modifs);
	}
	

	@SuppressWarnings("unchecked") @Override
	protected Class<? extends AbstractNode<String, K>> getReverseNodeType() {
		return (Class<? extends AbstractNode<String, K>>) SimpleNode.class;
	}
}
