package com.chronosave.index.storage.file;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.utils.BiHashMap;
import com.chronosave.index.utils.Pair;

public class CacheModifications {

	private long endOfFile;

	private final AbstractIndex<?, ?, ?> index;
	private final Map<Long, Object> positionsAndNewValuesToWrite;
	private final BiHashMap<Long, Class<?>, Object> positionsAndTypeToLocalCache;
	private final long version;

	public CacheModifications(final AbstractIndex<?, ?, ?> index, final long version) {
		this.index = index;
		this.version = version;
		positionsAndNewValuesToWrite = new HashMap<>();
		positionsAndTypeToLocalCache = new BiHashMap<>();
		endOfFile = index.getEndOfFile();
	}

	public long getEndOfFile() {
		return endOfFile;
	}

	private void updateEndOfFile(final Object value) throws SerializationException {
		if (value instanceof Date)
			endOfFile += Long.BYTES;
		else if (value instanceof String)
			endOfFile += ((String) value).getBytes().length + Short.BYTES;
		else if (value instanceof Boolean)
			endOfFile += AbstractIndex.BOOLEAN_BYTES;
		else if (value instanceof Byte)
			endOfFile += Byte.BYTES;
		else if (value instanceof Long)
			endOfFile += Long.BYTES;
		else if (value instanceof Integer)
			endOfFile += Integer.BYTES;
		else if (value instanceof Short)
			endOfFile += Short.BYTES;
		else if (value instanceof Double)
			endOfFile += Double.BYTES;
		else if (value instanceof Float)
			endOfFile += Float.BYTES;
		else if (value instanceof Character)
			endOfFile += Character.BYTES;
		else if (value instanceof List<?>) // actually List<Double> for lonlat
			endOfFile += 2 * Double.BYTES;
		else
			endOfFile += index.getStore().getMarshaller().serialize(value, new DataOutputStream(new ByteArrayOutputStream()));

	}

	private void updateGlobalCache() {
		for (final Map.Entry<Pair<Long, Class<?>>, Object> entry : positionsAndTypeToLocalCache.entrySet())
			index.addCache(entry.getKey().getKey(), entry.getValue());
	}

	protected void add(final long position, final Object valueToWrite) {
		positionsAndNewValuesToWrite.put(position, valueToWrite);
		addCache(position, valueToWrite);
	}

	protected void addCache(final long position, final Object value) {
		positionsAndTypeToLocalCache.put(position, value.getClass(), value);
	}

	protected long addToEnd(final Object value) throws SerializationException {
		add(endOfFile, value);
		final long oldEndOfFile = endOfFile;
		updateEndOfFile(value);
		return oldEndOfFile;
	}

	@SuppressWarnings("unchecked")
	protected <U> U getObjectFromRecentModifs(final long position, final Class<U> type) {
		return (U) positionsAndTypeToLocalCache.get(position, type);
	}

	protected void write() throws IOException, StorageException {
		updateGlobalCache();
		for (final Entry<Long, Object> entry : positionsAndNewValuesToWrite.entrySet())
			index.write(entry.getKey(), entry.getValue());
		index.setVersion(version);
	}

	protected void writeWithoutChangingVersion() throws IOException, StorageException {
		updateGlobalCache();
		for (final Entry<Long, Object> entry : positionsAndNewValuesToWrite.entrySet())
			index.write(entry.getKey(), entry.getValue());
	}
}
