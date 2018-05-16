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
 * @param <U> Object
 * @param <K> Key
 * @param <N> Value type of Node
 * @param <R> Value type of Reverse node
 */
public abstract class IndexMultiId<U, K extends Comparable<K>, R> extends IndexBiDirectionalId<U, K, SingletonNode<String>, R>{
	/**
	 * runtime
	 * @param basePath
	 * @param keyType
	 * @param store
	 * @param extention
	 * @param delegateKey
	 * @throws IOException
	 * @throws StorageException
	 * @throws SerializationException
	 */
	public IndexMultiId(Path basePath, Class<K> keyType, Store<U> store, String extention, ComputeKey<K, U> delegateKey)
			throws IOException, StorageException, SerializationException {
		super(basePath, keyType, store, extention, delegateKey);
	}

	/**
	 * from file
	 * @param file
	 * @param store
	 * @throws IOException
	 * @throws StorageException
	 * @throws ClassNotFoundException
	 * @throws SerializationException
	 * @throws StoreException
	 */
	public IndexMultiId(Path file, Store<U> store)
			throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException {
		super(file, store);
	}
	
	@SuppressWarnings("unchecked") @Override
	protected Class<? extends AbstractNode<K, SingletonNode<String>>> getNodeType() {
		return (Class<? extends AbstractNode<K, SingletonNode<String>>>) ComplexNode.class;
	}
	
	@Override
	protected void deleteKtoId(K key, String id, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		ComplexNode<K, String> complexNode = (ComplexNode<K, String>) getRoot(modifs).findNode(key, modifs);
		boolean aSupprimer = complexNode.removeValue(id, modifs);
		if(aSupprimer) setRoot(getRoot(modifs).deleteAndBalance(key, modifs), modifs);
	}
	
	@Override
	protected void addKeyToValue(K key, long keyPosition, String id, long idPosition, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setRoot(getRoot(modifs).addAndBalance(key, keyPosition, NULL, modifs), modifs);
		ComplexNode<K, String> n = (ComplexNode<K, String>) getRoot(modifs).findNode(key, modifs);
		n.storeValue(id, idPosition, modifs);
	}
	
}
