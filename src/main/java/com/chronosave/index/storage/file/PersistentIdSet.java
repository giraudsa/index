package com.chronosave.index.storage.file;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

import com.chronosave.index.storage.condition.GetId;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;

public class PersistentIdSet<U> extends Index1D<U, String, String, String> implements Closeable{
	
	protected PersistentIdSet(Class<U> type, Store<U> store) throws IOException, StorageException, SerializationException{
		super(String.class, String.class, Paths.get(UUID.randomUUID().toString()), store, new GetId<U>(type, store.getIdManager()), new GetId<U>(type, store.getIdManager()));
	}

	@SuppressWarnings("unchecked") @Override
	protected Class<? extends AbstractNode<String, String>> getNodeType() {
		return (Class<? extends AbstractNode<String, String>>) SingletonNode.class;
	}

	public boolean contains(String id) throws IOException, StorageException, SerializationException {
		return getRoot(null).findNode(id,null) != null;
	}


	public void addId(String id) throws IOException, StorageException, SerializationException {
		CacheModifications modifs = new CacheModifications(this, 0);
		long idPosition = writeFakeAndCache(id, modifs);
		setRoot(getRoot(modifs).addAndBalance(id, idPosition, null, modifs), modifs);
		modifs.writeWithoutChangingVersion();
	}


	public void close() throws IOException {
		raf.close();
		removeFile();
	}


	private void removeFile() throws IOException {
		Files.deleteIfExists(file);
		raf = null;
	}

//NOTHING TO DO

	@Override
	protected void rebuild(long version){
		//nothing to do
	}


	@Override
	protected void delete(String id, CacheModifications modifs) {
		//nothing to do
	}


	@Override
	protected void deleteKtoId(String key, String value, CacheModifications modifs){
		//nothing to do
	}

	@Override
	protected void addKeyToValue(String key, long keyPosition, String value, long valuePosition, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		//nothing to do
	}
}
