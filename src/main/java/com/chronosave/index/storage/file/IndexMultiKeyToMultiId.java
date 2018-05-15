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

public class IndexMultiKeyToMultiId<U, K extends Comparable<K>> extends IndexMultiId<U, K> {
	private static final String EXTENTION = ".idxmtom";
	protected static final <U> void feed(Path basePath, Map<ComputeKey<?, U>, AbstractIndex<U, ?, ?>> index, Store<U> store) throws IOException, ClassNotFoundException, StorageException, SerializationException, StoreException{
		String debutNom = store.debutNomFichier();
		File[] idxs = IndexedStorageManager.findAllFileThatBeginsWith(basePath, debutNom + EXTENTION);
		for(File fidx : idxs) {
			AbstractIndex<U,?, ?> idx = new IndexMultiKeyToMultiId<>(fidx.toPath(), store);
			ComputeKey<?, U> fc = (ComputeKey<?, U>) idx.getDelegateKey();
			index.put(fc, idx);
		}
	}
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
	public IndexMultiKeyToMultiId(Path basePath, Class<K> keyType, Store<U> store, String extention, ComputeKey<K, U> delegateKey) throws IOException, StorageException, SerializationException {
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
	public IndexMultiKeyToMultiId(Path file, Store<U> store) throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException {
		super(file, store);
	}
	
	
	@Override
	protected void add(U objet, long version, CacheModifications modifs) throws StorageException, IOException, SerializationException {
		add(new HashSet<>(getKeys(objet)), computeValue(objet, version), modifs);
	}
	
	@SuppressWarnings("unchecked")
	private void add(Set<K> keys, String id, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		ReverseComplexNode<K, String> oldReverseNode = (ReverseComplexNode<K, String>) getReverseRoot(modifs).findNode(id, modifs);
		Set<K> nothingToDo = new HashSet<>();
		long idPosition = NULL;
		if(oldReverseNode != null) {
			idPosition = oldReverseNode.keyPosition(modifs);
			for(K oldKey : oldReverseNode.getValue(modifs)) {
				if(keys.contains(oldKey)) nothingToDo.add(oldKey);
				else delete(oldKey, id, modifs);
			}
		}
		idPosition = idPosition == NULL ? writeFakeAndCache(id, modifs) : idPosition;
		for(K newKey : keys)
			if(!nothingToDo.contains(newKey)) {
				long keyPosition = getKeyPosition(newKey, modifs);
				add(newKey, keyPosition, id, idPosition, modifs);
			}
	}
	
	@SuppressWarnings("unchecked") @Override
	protected void delete(String id, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		ReverseComplexNode<K, String> oldReverseNode = (ReverseComplexNode<K, String>) getReverseRoot(modifs).findNode(id, modifs);
		if(oldReverseNode != null)
			for(K oldKey : oldReverseNode.getValue(modifs)) 
				delete(oldKey, id, modifs);
	}
	
	@SuppressWarnings("unchecked") @Override
	protected void addValueToKey(String id, long idPosition, K key, long keyPosition, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setReverseRoot(getReverseRoot(modifs).addAndBalance(id, idPosition, keyPosition, modifs), modifs);
		ReverseComplexNode<K, String> reverse = (ReverseComplexNode<K, String>) getReverseRoot(modifs).findNode(id, modifs);
		reverse.storeValue(key, keyPosition, modifs);
	}
	
	@SuppressWarnings("unchecked") @Override
	protected void deleteIdtoK(String id, K oldKey, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		ReverseComplexNode<K, String> reverseComplexNode = (ReverseComplexNode<K, String>) getReverseRoot(modifs).findNode(id, modifs);
		boolean aSupprimer = reverseComplexNode.removeValue(oldKey, modifs);
		if(aSupprimer) setReverseRoot(getReverseRoot(modifs).deleteAndBalance(id, modifs), modifs);
	}
	@SuppressWarnings("unchecked") @Override
	protected Class<? extends AbstractNode<String, ?>> getReverseNodeType() {
		return (Class<? extends AbstractNode<String, ?>>) ReverseComplexNode.class;
	}
	private Collection<K> getKeys(U objectToAdd) throws StorageException {
		return getDelegateKey().getKeys(objectToAdd);
	}
}
