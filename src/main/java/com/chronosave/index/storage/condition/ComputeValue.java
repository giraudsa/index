package com.chronosave.index.storage.condition;

import java.io.IOException;

import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;

public interface ComputeValue<V, U> {
	public V getValue(U object, long version) throws StorageException, IOException, SerializationException;
}
