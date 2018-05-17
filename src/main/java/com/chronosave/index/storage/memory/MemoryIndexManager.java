package com.chronosave.index.storage.memory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.chronosave.index.externe.AllObjectByType;
import com.chronosave.index.externe.IdManager;
import com.chronosave.index.storage.condition.AbstractCondition;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.utils.ReadWriteLock;

public class MemoryIndexManager {
	private final Map<Class<?>, ClassIndex<?>> classToIndexes;
	private final ReadWriteLock locker;
	private final IdManager idManager;
	private final AllObjectByType allObjectByType;
	public MemoryIndexManager(IdManager idManager, AllObjectByType allObjectByType) {
		this.classToIndexes = new HashMap<>();
		this.locker = new ReadWriteLock();
		this.idManager = idManager;
		this.allObjectByType = allObjectByType;
	}
	@SuppressWarnings("unchecked")
	public <U> void indexOrUpdate(Collection<U> objects) throws InterruptedException, StorageException {
		locker.lockWrite();
		for(U o : objects) {
			Class<U> t = (Class<U>)o.getClass();
			getClassIndexes(t).addOrUpdate(o);
		}
		locker.unlockWrite();
	}
	@SuppressWarnings("unchecked")
	public <U> void unindex(Collection<U> toBeDeleted) throws StorageException, InterruptedException {
		locker.lockWrite();
		for(U o : toBeDeleted) {
			Class<U> t = (Class<U>)o.getClass();
			getClassIndexes(t).supprimer(o);
		}
		locker.unlockWrite();
	}
	public <U> Collection<U> selectStarFromClassWhere(AbstractCondition<?, U> condition) throws InterruptedException, StorageException{
		return getClassIndexes(condition.getTypeObjet()).selectStarFromClassWhere(condition, locker);
	}
	@SuppressWarnings("unchecked")
	private <U> ClassIndex<U> getClassIndexes(Class<U> objectType) {
		if(!classToIndexes.containsKey(objectType))
			classToIndexes.put(objectType, new ClassIndex<U>(objectType, idManager, allObjectByType));
		return (ClassIndex<U>) classToIndexes.get(objectType);
	}
}
