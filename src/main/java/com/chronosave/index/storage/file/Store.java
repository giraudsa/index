package com.chronosave.index.storage.file;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.chronosave.index.externe.IdManager;
import com.chronosave.index.externe.SerializationStore;
import com.chronosave.index.storage.condition.AbstractCondition;
import com.chronosave.index.storage.condition.ComputeComparableKey;
import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.condition.ComputeSpatialKey;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StoreException;
import com.chronosave.index.utils.CloseableIterator;
import com.chronosave.index.utils.ReadWriteLock;

public class Store<U> {

	private final Path basePath;
	private final DataFile<U> data;
	private final IdManager idManager;
	private final Map<ComputeKey<?, U>, AbstractIndex<U, ?, ?>> index = new HashMap<>();
	private final SerializationStore marshaller;
	private final Class<U> objectType;
	private final PrimaryIndexFile<U> primaryIndex;

	Store(final Path basePath, final Class<U> clazz, final SerializationStore marshaller, final IdManager idManager,
			final long lastGoodVersion)
			throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException {
		this.objectType = clazz;
		this.basePath = basePath;
		this.marshaller = marshaller;
		this.idManager = idManager;
		data = new DataFile<>(basePath, clazz, marshaller);
		final Path filePrimary = PrimaryIndexFile.getFile(basePath, data);
		if (filePrimary != null) {// file read
			primaryIndex = new PrimaryIndexFile<>(filePrimary, this, lastGoodVersion);
			if (data.getVersion() > lastGoodVersion || primaryIndex.version != data.getVersion())
				clean(lastGoodVersion);
			IndexKeyToMultiId.feed(basePath, index, this);
			SpaceIndex.feed(basePath, index, this);
			IndexMultiKeyToMultiId.feed(basePath, index, this);
			for (final AbstractIndex<U, ?, ?> i : index.values())
				i.rebuild(getVersion());
		} else
			primaryIndex = new PrimaryIndexFile<>(this, basePath);
	}

	protected void clean(final long version) throws IOException, StorageException, SerializationException {
		primaryIndex.rebuild(version);
		for (final AbstractIndex<U, ?, ?> i : index.values())
			i.rebuild(version);
	}

	private void createIndex(final ComputeKey<?, U> computeKey)
			throws IOException, StorageException, SerializationException {
		if (ComputeKey.isSpatial(computeKey))
			createIndexSpatial((ComputeSpatialKey<?, U>) computeKey);
		else if (computeKey.isMultipleKey())
			createIndexMultiKeyToMultiId((ComputeComparableKey<?, U>) computeKey);
		else
			createIndexKeyToMultiId((ComputeComparableKey<?, U>) computeKey);
	}

	private <K extends Comparable<K>> void createIndexKeyToMultiId(final ComputeKey<K, U> computeKey)
			throws IOException, StorageException, SerializationException {
		index.put(computeKey, new IndexKeyToMultiId<>(basePath, computeKey.getKeyType(), this, computeKey));
	}

	private <K extends Comparable<K>> void createIndexMultiKeyToMultiId(final ComputeKey<K, U> computeKey)
			throws IOException, StorageException, SerializationException {
		index.put(computeKey, new IndexMultiKeyToMultiId<>(basePath, computeKey.getKeyType(), this, computeKey));
	}

	private <K extends List<Double>> void createIndexSpatial(final ComputeKey<K, U> computeKey)
			throws IOException, StorageException, SerializationException {
		index.put(computeKey, new SpaceIndex<>(basePath, computeKey.getKeyType(), this, computeKey));
	}

	protected String debutNomFichier() {
		return data.debutNomFichier();
	}

	protected void delete(final Collection<String> ids, final long v, final Collection<CacheModifications> modifs)
			throws IOException, StorageException, SerializationException {
		modifs.add(primaryIndex.deleteObjects(ids, v));
		for (final AbstractIndex<U, ?, ?> indexe : index.values())
			modifs.add(indexe.deleteObjects(ids, v));
	}

	protected void delete(final String id, final long version) throws IOException {
		data.delete(id, version);
	}

	protected CloseableIterator<U> getAllObjectsWithMaxVersionLessThan(final long version)
			throws IOException, StorageException, SerializationException {
		return data.getAllObjectsWithMaxVersionLessThan(version, this);
	}

	private String getId(final U object) throws StorageException {
		return idManager.getId(object);
	}

	protected IdManager getIdManager() {
		return idManager;
	}

	protected SerializationStore getMarshaller() {
		return marshaller;
	}

	protected U getObjectById(final String id) throws IOException, StorageException, SerializationException {
		return primaryIndex.getValue(id);
	}

	protected Class<U> getObjectType() {
		return objectType;
	}

	protected PrimaryIndexFile<U> getPrimaryIndex() {
		return primaryIndex;
	}

	protected long getVersion() {
		return data.getVersion();
	}

	protected U read(final Long position) throws IOException, SerializationException {
		return data.getObject(position);
	}

	protected <K> CloseableIterator<String> selectIdFromClassWhere(final AbstractCondition<K, U> condition,
			final ReadWriteLock locker)
			throws IOException, StorageException, SerializationException, StoreException, InterruptedException {
		final ComputeKey<K, U> computeKey = condition.getDelegate();
		if (!index.containsKey(computeKey))
			try {
				locker.lockWrite();
				createIndex(computeKey);
			} finally {
				locker.unlockWrite();
			}
		return condition.run(index.get(condition.getDelegate()), locker);
	}

	protected void setVersion(final long version) throws IOException {
		data.setVersion(version);
	}

	protected void store(final Collection<U> v, final long version, final Collection<CacheModifications> modifs)
			throws StorageException, IOException, SerializationException {
		modifs.add(primaryIndex.stockpile(v, version));
		for (final AbstractIndex<U, ?, ?> indexe : index.values())
			modifs.add(indexe.stockpile(v, version));
	}

	protected Long writeData(final U object, final long version)
			throws IOException, SerializationException, StorageException {
		return data.writeData(getId(object), object, version);
	}

}
