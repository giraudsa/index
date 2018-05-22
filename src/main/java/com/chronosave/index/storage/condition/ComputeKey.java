package com.chronosave.index.storage.condition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import com.chronosave.index.externe.SerializationStore;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;

public interface ComputeKey<K, U> {

	public static <K, U> boolean isSpatial(final ComputeKey<K, U> computeKey) {
		return computeKey instanceof ComputeSpatialKey;
	}

	public static <K, U> void marshall(final ComputeKey<K, U> computeKey, final DataOutput output,
			final SerializationStore marshaller) throws IOException, SerializationException {
		output.writeUTF(computeKey.getClass().getName());
		marshaller.serialize(computeKey, output);
	}

	@SuppressWarnings("unchecked")
	public static <K, U> ComputeKey<K, U> unmarshall(final SerializationStore marshaller, final DataInput input)
			throws ClassNotFoundException, IOException, SerializationException {
		final Class<? extends ComputeKey<?, ?>> clazz = (Class<? extends ComputeKey<?, ?>>) Class
				.forName(input.readUTF());
		return (ComputeKey<K, U>) marshaller.unserialize(clazz, input);
	}

	public K getKey(U object) throws StorageException;

	public Collection<K> getKeys(U objectToAdd) throws StorageException;

	public Class<K> getKeyType();

	public boolean isMultipleKey();

}