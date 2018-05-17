package com.chronosave.index.storage.file;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;

import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.condition.GetId;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StoreException;
/***
 * 
 * @author giraudsa
 *
 * @param <U> Object
 * @param <K> Key
 * @param <N> Value type of Node
 * @param <R> Value type of Reverse node
 */
public abstract class IndexBiDirectionalId<U, K, N, R>  extends Index1D<U, K, String, N> {
	
	private final long reverseRootPositionposition;
	protected long reverseRootPosition;
	
		
	//file
	protected IndexBiDirectionalId(Path file, Store<U> store) throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException{
		super(String.class, file, store, new GetId<>(store.getIdManager()));
		reverseRootPositionposition = positionEndOfHeaderInAbstractIndex;
		reverseRootPosition = getStuff(reverseRootPositionposition, Long.class, null);
		checkVersion(store.getVersion());
	}

	//runtime
	protected IndexBiDirectionalId(Path basePath, Class<K> keyType, Store<U> store, String extention, ComputeKey<K, U> delegateKey) throws IOException, StorageException, SerializationException {
		super(keyType, String.class, getPath(basePath, store.debutNomFichier(), extention, delegateKey), store, delegateKey,new GetId<>(store.getIdManager()));
		reverseRootPositionposition = positionEndOfHeaderInAbstractIndex;
		setReverseRootPosition(NULL);
		rebuild(store.getVersion());
	}
	
	protected void setReverseRoot(AbstractNode<String, R> node, CacheModifications modifs){
		reverseRootPosition = node == null ? NULL : node.getPosition();
		modifs.add(reverseRootPositionposition, reverseRootPosition);
	}
	
	protected AbstractNode<String, R> getReverseRoot(CacheModifications modifs) throws IOException, StorageException, SerializationException{
		try {
			if(reverseRootPosition == NULL) //Fake node
				return (AbstractNode<String, R>) getReverseNodeType().getConstructor(Class.class, Class.class, AbstractIndex.class, CacheModifications.class).newInstance(String.class, keyType, this, modifs);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException | NoSuchMethodException | SecurityException e) {
			throw new StorageException("impossible to create fake node", e);
		}
		return (AbstractNode<String, R>) getStuff(reverseRootPosition, getReverseNodeType(), modifs);
	}
	
	protected abstract Class<? extends AbstractNode<String,R>> getReverseNodeType();

	@Override
	protected long initFile() throws IOException, StorageException, SerializationException {
		long positionEndOfHeaderInAbstractIndex = super.initFile();
		setReverseRootPosition(NULL);
		return positionEndOfHeaderInAbstractIndex;
	}
	@Override
	protected void add(K key, long keyPosition, String id, long idPosition, final CacheModifications modifs) throws StorageException, IOException, SerializationException {
		addKeyToValue(key, keyPosition, id, idPosition, modifs);
		addValueToKey(id, idPosition, key, keyPosition, modifs);
	}
	protected void delete(K key, String id, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		deleteKtoId(key, id, modifs);
		deleteIdtoK(id, key, modifs);
	}

	private void setReverseRootPosition(long positionRacineInverse) throws IOException, StorageException {
		this.reverseRootPosition = positionRacineInverse;
		write(reverseRootPositionposition, positionRacineInverse);
	}
	
	protected void deleteIdtoK(String id, K key, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setReverseRoot(getReverseRoot(modifs).deleteAndBalance(id, modifs), modifs);
	}
	protected void addValueToKey(String id, long idPosition, K key, long keyPosition, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setReverseRoot(getReverseRoot(modifs).addAndBalance(id, idPosition, keyPosition, modifs), modifs);
	}
	
	protected long getIdPosition(String id, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		AbstractNode<String, ?> reverseNode = getReverseRoot(modifs).findNode(id, modifs);
		return reverseNode == null ? writeFakeAndCache(id, modifs) : reverseNode.valuePosition(modifs);
	}
}
