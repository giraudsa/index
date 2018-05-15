package com.chronosave.index.storage.file;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.condition.ComputeValue;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StoreException;

public abstract class Index1D<U, K, V extends Comparable<V>> extends AbstractIndex<U, K, V> {
	
	protected static Path getPath(Path basePath, String debutNomFichier, String extention, ComputeKey<?, ?> delegateKey) {
		return Paths.get(basePath.toString(), debutNomFichier + extention + "." + delegateKey.hashCode() + ".0");
	}

	/**
	 * runtime
	 * @param keyType
	 * @param valueType
	 * @param fileStore
	 * @param store
	 * @param delegateKey
	 * @param delegateValue
	 * @throws IOException
	 * @throws StorageException
	 * @throws SerializationException 
	 */
	public Index1D(Class<K> keyType, Class<V> valueType, Path fileStore, Store<U> store, ComputeKey<K, U> delegateKey, ComputeValue<V, U> delegateValue) throws IOException, StorageException, SerializationException {
		super(keyType, valueType, fileStore, store, delegateKey, delegateValue);
	}

	/**
	 * from file
	 * @param valueType
	 * @param file
	 * @param store
	 * @param delegateValue
	 * @throws IOException
	 * @throws StorageException
	 * @throws ClassNotFoundException
	 * @throws SerializationException
	 * @throws StoreException
	 */
	public Index1D(Class<V> valueType, Path file, Store<U> store, ComputeValue<V, U> delegateValue)
			throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException {
		super(valueType, file, store, delegateValue);
	}
	
	@Override
	protected void add(U objectToAdd, long version, final CacheModifications modifs) throws StorageException, IOException, SerializationException {
		add(getKey(objectToAdd), computeValue(objectToAdd, version), modifs);
	}
	
	private void add(K key, V value, final CacheModifications modifs) throws SerializationException, IOException, StorageException {
		long keyPosition = writeFakeAndCache(key, modifs);
		long valuePosition = writeFakeAndCache(value, modifs);
		setRoot(getRoot(modifs).addAndBalance(key, keyPosition, valuePosition, modifs), modifs);
	}

	protected void setRoot(AbstractNode<K, ?> abstractNode, CacheModifications modifs){
		rootPosition = abstractNode == null ? NULL : abstractNode.getPosition();
		modifs.add(ROOT_POSITION_POSITION, rootPosition);
	}
	
	@SuppressWarnings("unchecked")
	protected AbstractNode<K, ?> getRoot(CacheModifications modifs) throws IOException, StorageException, SerializationException{
		try {
			if(rootPosition == NULL) //Fake node
				return getNodeType().getConstructor(Class.class, Class.class, AbstractIndex.class, CacheModifications.class).newInstance(keyType, valueType, this, modifs);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException | NoSuchMethodException | SecurityException e) {
			throw new StorageException("impossible to create fake node", e);
		}
		return (AbstractNode<K, ?>) getStuff(rootPosition, getNodeType(), modifs);
	}
	
	@Override
	protected void rebuild(long version) throws IOException, StorageException, SerializationException {
		clear();
		for(String id : store.getPrimaryIndex()) {
			CacheModifications modifs = new CacheModifications(this, version);
			U obj = store.getObjectById(id);
			add(obj, store.getVersion(), modifs);
			modifs.writeWithoutChangingVersion();
		}
		setVersion(version);
	}
}
