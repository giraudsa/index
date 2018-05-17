package com.chronosave.index.storage.file;

import java.io.IOException;

import com.chronosave.index.externe.IdManager;
import com.chronosave.index.storage.condition.ComputeValue;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;

public class GetIndexedObjectInDataFile<U> implements ComputeValue<Long, U>{

	private final Class<U> objectType;
	private final Store<U> store;


	@Override
	public Long getValue(U objet, long version) throws StorageException, IOException, SerializationException {
		return store.writeData(objet, version);
	}
	
	/**
	 * @param datas
	 */
	public GetIndexedObjectInDataFile(Store<U> stockage) {
		super();
		this.store = stockage;
		this.objectType = stockage.getObjectType();
	}
	
	public Class<U> getObjetType(){
		return objectType;
	}
}