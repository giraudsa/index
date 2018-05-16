package com.chronosave.index.storage.condition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import com.chronosave.index.externe.SerializationStore;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;

public interface ComputeKey<K, U> {
	
	@SuppressWarnings("unchecked")
	public static <K, U> ComputeKey<K,U> unmarshall(SerializationStore marshaller, DataInput input) throws ClassNotFoundException, IOException, SerializationException{
		Class<? extends ComputeKey<?, ?>> clazz = (Class<? extends ComputeKey<?, ?>>) Class.forName(input.readUTF());
		return (ComputeKey<K, U>) marshaller.unserialize(clazz, input);
	}
	public static <K, U> void marshall(ComputeKey<K, U> computeKey, DataOutput output, SerializationStore marshaller) throws IOException, SerializationException {
		output.writeUTF(computeKey.getClass().getName());
		marshaller.serialize(computeKey, output);
	}
	public static <K, U> boolean isSpatial(ComputeKey<K, U> computeKey) {
		return computeKey instanceof ComputeSpatialKey;
	}
	
	
	public Class<K> getKeyType();
	public Class<U> getObjectType();
	public K getKey(U object) throws StorageException;
	public Collection<K> getKeys(U objectToAdd) throws StorageException;
	
	public boolean isMultipleKey();
}