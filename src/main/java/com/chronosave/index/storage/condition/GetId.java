package com.chronosave.index.storage.condition;

import java.util.Collection;
import java.util.Collections;

import com.chronosave.index.externe.IdManager;
import com.chronosave.index.storage.exception.StorageException;

public class GetId<U> implements ComputeKey<String, U>, ComputeValue<String, U> {
	private final IdManager idManager;

	public GetId(final IdManager idManager) {
		this.idManager = idManager;
	}

	private String getId(final Object object) throws StorageException {
		return idManager.getId(object);
	}

	@Override
	public String getKey(final U object) throws StorageException {
		return getId(object);
	}

	@Override
	public Collection<String> getKeys(final U objectToAdd) throws StorageException {
		return Collections.singletonList(idManager.getId(objectToAdd));
	}

	@Override
	public Class<String> getKeyType() {
		return String.class;
	}

	@Override
	public String getValue(final U object, final long version) throws StorageException {
		return getId(object);
	}

	@Override
	public boolean isMultipleKey() {
		return false;
	}
}
