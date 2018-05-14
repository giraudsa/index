package com.chronosave.index.storage.condition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.chronosave.index.externe.SerializationStore;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;

public interface ComputeKey<K, U> {
	
	@SuppressWarnings("unchecked")
	public static <K1, U1> ComputeKey<K1,U1> unmarshall(SerializationStore marshaller, DataInput input) throws ClassNotFoundException, IOException, SerializationException{
		Class<? extends ComputeKey<?, ?>> clazz = (Class<? extends ComputeKey<?, ?>>) Class.forName(input.readUTF());
		return (ComputeKey<K1, U1>) marshaller.unserialize(clazz, input);
	}
	public static <K1, U1> void marshall(ComputeKey<K1, U1> computeKey, DataOutput output, SerializationStore marshaller) throws IOException, SerializationException {
		output.writeUTF(computeKey.getClass().getName());
		marshaller.serialize(computeKey, output);
	}
	
	public Class<K> getKeyType();
	public Class<U> getObjectType();
	public K getKey(U object) throws StorageException;
}