package com.chronosave.index.storage.file;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.condition.GetId;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StoreException;

public abstract class IndexBiDirectionalId<U, K extends Comparable<K>>  extends Index1D<U, K, String> {
	
	private static Path getPath(Path basePath, Class<?> clazz, String extention, ComputeKey<?, ?> delegateKey) {
		return Paths.get(basePath.toString(), clazz.getName() + extention + "." + delegateKey.hashCode() + ".0");
	}
	
	private final long reverseRootPositionposition;
	private long reverseRootPosition;
	
		
	//file
	protected IndexBiDirectionalId(Path file, Store<U> store) throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException{
		super(String.class, file, store, new GetId<>(store.getObjectType(), store.getIdManager()));
		reverseRootPositionposition = positionEndOfHeaderInAbstractIndex;
		reverseRootPosition = getStuff(reverseRootPositionposition, Long.class, null);
		checkVersion(store.getVersion());
	}

	//runtime
	protected IndexBiDirectionalId(Path basePath, Class<K> keyType, Store<U> store, String extention, ComputeKey<K, U> delegateKey) throws IOException, StorageException, SerializationException {
		super(keyType, String.class, getPath(basePath, store.getObjectType(), extention, delegateKey), store, delegateKey,new GetId<>(store.getObjectType(), store.getIdManager()));
		reverseRootPositionposition = positionEndOfHeaderInAbstractIndex;
		setReverseRootPosition(NULL);
		rebuild(store.getVersion());
	}
	
	protected void setReverseRoot(ReverseSimpleNode<K, String> node, CacheModifications modifs){
		reverseRootPosition = node == null ? NULL : node.getPosition();
		modifs.add(reverseRootPositionposition, reverseRootPosition);
	}
	

	@SuppressWarnings("unchecked")
	protected ReverseSimpleNode<K, String> getReverseRoot(CacheModifications modifs) throws IOException, StorageException, SerializationException{
		if(reverseRootPosition == NULL)//Fake noeud
			return new ReverseSimpleNode<>(keyType, String.class, this, modifs);
		return getStuff(reverseRootPosition, ReverseSimpleNode.class, modifs);
	}
	
	@Override
	protected void initFile() throws IOException, StorageException, SerializationException {
		super.initFile();
		setReverseRootPosition(NULL);
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


	private void setReverseRootPosition(long positionRacineInverse) throws IOException, StorageException {
		this.reverseRootPosition = positionRacineInverse;
		write(reverseRootPositionposition, positionRacineInverse);
	}
}
