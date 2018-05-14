package com.chronosave.index.storage.file;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.UUID;

import com.chronosave.index.storage.condition.GetId;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;

public class PersistentIdSet<U> extends Index1D<U, String, String> {
	
	private static final Class<NodeId> simpleNodeType = NodeId.class;
	

	protected PersistentIdSet(Class<U> type, Store<U> store) throws IOException, StorageException, SerializationException{
		super(String.class, String.class, Paths.get(UUID.randomUUID().toString()), store, new GetId<U>(type, store.getIdManager()), new GetId<U>(type, store.getIdManager()));
	}


	@Override
	protected void rebuild(long version){
		//nothing to do
	}


	@Override
	protected void delete(U object, long version, CacheModifications modifs) {
		//nothing to do
	}


	@SuppressWarnings("rawtypes") @Override
	protected Class<? extends AbstractNode> getNodeType() {
		return simpleNodeType;
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
}
