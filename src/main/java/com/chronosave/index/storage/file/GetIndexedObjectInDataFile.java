package com.chronosave.index.storage.file;

import java.io.IOException;

import com.chronosave.index.storage.condition.ComputeValue;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;

public class GetIndexedObjectInDataFile<U> implements ComputeValue<Long, U> {

	private final Class<U> objectType;
	private final Store<U> store;

	/**
	 * @param datas
	 */
	public GetIndexedObjectInDataFile(final Store<U> stockage) {
		super();
		this.store = stockage;
		this.objectType = stockage.getObjectType();
	}

	public Class<U> getObjetType() {
		return objectType;
	}

	@Override
	public Long getValue(final U objet, final long version)
			throws StorageException, IOException, SerializationException {
		return store.writeData(objet, version);
	}
}