package com.chronosave.index.externe;

import java.io.DataInput;
import java.io.DataOutput;

import com.chronosave.index.storage.exception.SerializationException;

public interface SerializationStore {
	/**
	 *
	 * @param objet
	 * @param output
	 * @return size in byte of the serialized object
	 * @throws SerializationException
	 */
	public long serialize(Object objet, DataOutput output) throws SerializationException;

	public <U> U unserialize(Class<U> clazz, DataInput input) throws SerializationException;

}
