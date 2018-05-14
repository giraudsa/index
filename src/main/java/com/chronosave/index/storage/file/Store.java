package com.chronosave.index.storage.file;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.chronosave.index.externe.IdManager;
import com.chronosave.index.externe.SerializationStore;
import com.chronosave.index.storage.condition.AbstractCondition;
import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.condition.ConditionBBOX;
import com.chronosave.index.storage.condition.ConditionCompare;
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
		data = new DataFile<>(basePath, clazz, marshaller, idManager);
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
		return objectType.getName();
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

	protected void delete(Collection<U> us, long v, Collection<CacheModifications> modifs) throws IOException, StorageException, SerializationException {
		modifs.add(primaryIndex.deleteObjects(us, v));
		for(AbstractIndex<U, ?, ?> indexe : index.values())
			modifs.add(indexe.deleteObjects(us, v));
	}

	protected <K> CloseableIterator<String> selectIdFromClassWhere(AbstractCondition<K, U> condition, ReadWriteLock locker) throws IOException, StorageException, SerializationException, StoreException, InterruptedException {
		if(!index.containsKey(condition.getDelegate())) {
			locker.lockWrite();
			if(condition instanceof ConditionBBOX)
				createIndexSpatial((ConditionBBOX<?, U>)condition);
			else {
				createIndexKeyToMultiId((ConditionCompare<?, U>)condition);
			}
			locker.unlockWrite();
		}
		return condition.run(index.get(condition.getDelegate()), locker);
	}
	
	private <K extends Comparable<K>> void createIndexKeyToMultiId(ConditionCompare<K, U> condition) throws IOException, StorageException, SerializationException {
		index.put(condition.getDelegate(), new IndexKeyToMultiId<>(basePath, condition.getTypeReturn(), this, condition.getDelegate()));
		
	}

	private <K extends List<Double>> void createIndexSpatial(ConditionBBOX<K, U> condition) throws IOException, StorageException, SerializationException {
		index.put(condition.getDelegate(), new SpaceIndex<>(basePath, condition.getTypeReturn(), this, condition.getDelegate()));
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
	
	protected Long write(U object, long version) throws IOException, SerializationException {
		return data.writeData(object, version);
	}

	protected void delete(U objet, long version) throws IOException {
		data.delete(objet, version);
	}

	protected Iterator<U> getAllObjectsWithMaxVersionLessThan(long version) throws IOException, StorageException, SerializationException {
		return data.getAllObjectsWithMaxVersionLessThan(version, this);
	}

	public void clean(long version) throws IOException, StorageException, SerializationException {
		primaryIndex.rebuild(version);
		for(AbstractIndex<U, ?, ?> i : index.values()) {
			i.rebuild(version);
		}
	}

	protected IdManager getIdManager() {
		return idManager;
	}
	
}
