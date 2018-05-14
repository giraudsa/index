package com.chronosave.index.storage.condition;


import com.chronosave.index.externe.IdManager;
import com.chronosave.index.storage.exception.StorageException;

public class GetId<U> implements ComputeKey<String, U>, ComputeValue<String, U> {
	private final IdManager idManager;
	private final Class<U> classU;
	public Class<U> getClassU(){
		return classU;
	}
	public GetId(Class<U> classU, IdManager idManager) {
		this.classU = classU;
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
	public Class<String> getKeyType() {
		return String.class;
	}
	@Override
	public Class<U> getObjectType() {
		return classU;
	}
}
