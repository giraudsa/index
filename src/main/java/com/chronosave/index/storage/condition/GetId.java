package com.chronosave.index.storage.condition;


import java.util.Collection;
import java.util.Collections;

import com.chronosave.index.externe.IdManager;
import com.chronosave.index.storage.exception.StorageException;

public class GetId<U> implements ComputeKey<String, U>, ComputeValue<String, U> {
	private final IdManager idManager;
	
	public GetId(IdManager idManager) {
		this.idManager = idManager;
	}

	@Override
	public String getKey(U object) throws StorageException {
		return getId(object);
	}
	
	@Override
	public String getValue(U object, long version) throws StorageException {
		return getId(object);
	}
	
	private String getId(Object object) throws StorageException {
		return idManager.getId(object);
	}
	@Override
	public Collection<String> getKeys(U objectToAdd) throws StorageException {
		return Collections.singletonList(idManager.getId(objectToAdd));
	}
	@Override
	public Class<String> getKeyType() {
		return String.class;
	}
	
	@Override
	public boolean isMultipleKey() {
		return false;
	}
}
