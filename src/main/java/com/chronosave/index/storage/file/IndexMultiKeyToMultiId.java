package com.chronosave.index.storage.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;

import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StoreException;

public class IndexMultiKeyToMultiId<U, K extends Comparable<K>> extends IndexMultiId<U, Collection<K>> {
	private static final String EXTENTION = ".idxmtom";
	protected static final <U> void feed(Path basePath, Map<ComputeKey<?, U>, AbstractIndex<U, ?, ?>> index, Store<U> store) throws IOException, ClassNotFoundException, StorageException, SerializationException, StoreException{
		String debutNom = store.debutNomFichier();
		File[] idxs = IndexedStorageManager.findAllFileThatBeginsWith(basePath, debutNom + EXTENTION);
		for(File fidx : idxs) {
			AbstractIndex<U,?, ?> idx = new IndexKeyToMultiId<>(fidx.toPath(), store);
			ComputeKey<?, U> fc = (ComputeKey<?, U>) idx.getDelegateKey();
			index.put(fc, idx);
		}
	}
	private final long genericKeyTypePosition;
	private final Class<K> genericKeyType;
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
	@SuppressWarnings("unchecked")
	public IndexMultiKeyToMultiId(Path basePath, Class<K> genericKeyType, Store<U> store, String extention,
			ComputeKey<Collection<K>, U> delegateKey) throws IOException, StorageException, SerializationException {
		super(basePath, (Class<Collection<K>>)(Object)Collection.class, store, extention, delegateKey);
		this.genericKeyType = genericKeyType;
		this.genericKeyTypePosition = reverseRootPosition + Long.BYTES;
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
	@SuppressWarnings("unchecked")
	public IndexMultiKeyToMultiId(Path file, Store<U> store)
			throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException {
		super(file, store);
		this.genericKeyTypePosition = reverseRootPosition + Long.BYTES;
		this.genericKeyType = (Class<K>) Class.forName(getStuff(genericKeyTypePosition, String.class, null));
	}
	
	@Override
	protected long initFile() throws IOException, StorageException, SerializationException {
		long ret = super.initFile();
		if(genericKeyType != null) write(genericKeyTypePosition, genericKeyType.getName());
		return ret;
	}
	
	@Override
	protected void add(U objectToAdd, long version, CacheModifications modifs) throws StorageException, IOException, SerializationException {
	
	}
	
	@Override
	protected void delete(String id, long version, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		
	}

}
