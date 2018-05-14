package com.chronosave.index.externe;

import com.chronosave.index.storage.exception.StorageException;

public interface IdManager {
	/**
	 * give a unique ID for every objects
	 * @param object
	 * @return
	 * @throws StorageException
	 */
	public String getId(Object object) throws StorageException;
}
