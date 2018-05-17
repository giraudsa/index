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
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StoreException;
import com.chronosave.index.utils.CloseableIterator;
import com.chronosave.index.utils.ReadWriteLock;

public class Store<U> {
	
	private final Map<ComputeKey<?, U>, AbstractIndex<U, ?, ?>> index = new HashMap<>();
	private final PrimaryIndexFile<U> primaryIndex;
	private final DataFile<U> data;
	private final Class<U> objectType;
	private final Path basePath;
	private final SerializationStore marshaller;
	private final IdManager idManager;
	
	Store(Path basePath, Class<U> clazz, SerializationStore marshaller, IdManager idManager, long lastGoodVersion) throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException{
		this.objectType = clazz;
		this.basePath = basePath;
		this.marshaller = marshaller;
		this.idManager = idManager;
		data = new DataFile<>(basePath, clazz, marshaller);
		Path filePrimary = PrimaryIndexFile.getFile(basePath, data);
		if(filePrimary != null) {//file read
			primaryIndex = new PrimaryIndexFile<>(filePrimary, this, lastGoodVersion);
			IndexKeyToMultiId.feed(basePath, index, this);
			SpaceIndex.feed(basePath, index, this);
		}else {//runtime
			primaryIndex = new PrimaryIndexFile<>(this, basePath);
		}
	}

	protected Class<U> getObjectType(){
		return objectType;
	}
	
	protected String debutNomFichier() {
		return data.debutNomFichier();
	}
	protected PrimaryIndexFile<U> getPrimaryIndex() {
		return primaryIndex;
	}
	protected SerializationStore getMarshaller() {
		return marshaller;
	}

	protected void store(Collection<U> v, long version, Collection<CacheModifications> modifs) throws StorageException, IOException, SerializationException {
		modifs.add(primaryIndex.stockpile(v, version));
		for(AbstractIndex<U, ?, ?> indexe : index.values())
			modifs.add(indexe.stockpile(v, version));
	}
	protected U getObjectById(String id) throws IOException, StorageException, SerializationException {
		return primaryIndex.getValue(id);
	}

	protected void delete(Collection<String> ids, long v, Collection<CacheModifications> modifs) throws IOException, StorageException, SerializationException {
		modifs.add(primaryIndex.deleteObjects(ids, v));
		for(AbstractIndex<U, ?, ?> indexe : index.values())
			modifs.add(indexe.deleteObjects(ids, v));
	}

	protected <K> CloseableIterator<String> selectIdFromClassWhere(AbstractCondition<K, U> condition, ReadWriteLock locker) throws IOException, StorageException, SerializationException, StoreException, InterruptedException {
		ComputeKey<K, U> computeKey = condition.getDelegate();
		if(!index.containsKey(computeKey)) {
			locker.lockWrite();
			createIndex(computeKey);
			locker.unlockWrite();
		}
		return condition.run(index.get(condition.getDelegate()), locker);
	}
	
	private void createIndex(ComputeKey<?, U> computeKey) throws IOException, StorageException, SerializationException {
		if(ComputeKey.isSpatial(computeKey))
			createIndexSpatial((ComputeSpatialKey<?, U>)computeKey);
		else if(computeKey.isMultipleKey())
			createIndexMultiKeyToMultiId((ComputeComparableKey<?, U>)computeKey);
		else
			createIndexKeyToMultiId((ComputeComparableKey<?, U>)computeKey);
	}

	private <K extends Comparable<K>> void createIndexMultiKeyToMultiId(ComputeKey<K, U> computeKey) throws IOException, StorageException, SerializationException {
		index.put(computeKey, new IndexMultiKeyToMultiId<U, K>(basePath, computeKey.getKeyType(), this, computeKey));
	}

	private <K extends Comparable<K>> void createIndexKeyToMultiId(ComputeKey<K, U> computeKey) throws IOException, StorageException, SerializationException {
		index.put(computeKey, new IndexKeyToMultiId<>(basePath, computeKey.getKeyType(), this, computeKey));
	}

	private <K extends List<Double>> void createIndexSpatial(ComputeKey<K, U> computeKey) throws IOException, StorageException, SerializationException {
		index.put(computeKey, new SpaceIndex<>(basePath, computeKey.getKeyType(), this, computeKey));
	}

	protected void setVersion(long version) throws IOException {
		data.setVersion(version);
	}

	protected long getVersion() {
		return data.getVersion();
	}


	protected U read(Long position) throws IOException, SerializationException {
		return data.getObject(position);
	}
	
	protected Long writeData(U object, long version) throws IOException, SerializationException, StorageException {
		return data.writeData(getId(object), object, version);
	}

	private String getId(U object) throws StorageException {
		return idManager.getId(object);
	}

	protected void delete(String id, long version) throws IOException {
		data.delete(id, version);
	}

	protected CloseableIterator<U> getAllObjectsWithMaxVersionLessThan(long version) throws IOException, StorageException, SerializationException {
		return data.getAllObjectsWithMaxVersionLessThan(version, this);
	}

	protected void clean(long version) throws IOException, StorageException, SerializationException {
		primaryIndex.rebuild(version);
		for(AbstractIndex<U, ?, ?> i : index.values()) {
			i.rebuild(version);
		}
	}
	

	protected IdManager getIdManager() {
		return idManager;
	}
	
}
