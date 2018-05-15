package com.chronosave.index.storage.file;

import java.io.IOException;
import java.nio.file.Path;

import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StoreException;

public abstract class IndexMultiId<U, K extends Comparable<K>> extends IndexBiDirectionalId<U, K>{

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
	
	@SuppressWarnings("rawtypes") @Override
	protected Class<? extends AbstractNode> getNodeType() {
		return ComplexNode.class;
	}
	
	@SuppressWarnings("unchecked") @Override
	protected void deleteKtoId(K key, String id, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		ComplexNode<K, String> complexNode = (ComplexNode<K, String>) getRoot(modifs).findNode(key, modifs);
		boolean aSupprimer = complexNode.removeValue(id, modifs);
		if(aSupprimer) setRoot(getRoot(modifs).deleteAndBalance(key, modifs), modifs);
	}
	
	@SuppressWarnings("unchecked") @Override
	protected void addKeyToValue(K key, long keyPosition, String id, long idPosition, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setRoot(getRoot(modifs).addAndBalance(key, keyPosition, NULL, modifs), modifs);
		ComplexNode<K, String> n = (ComplexNode<K, String>) getRoot(modifs).findNode(key, modifs);
		n.storeValue(id, idPosition, modifs);
	}
	
}
