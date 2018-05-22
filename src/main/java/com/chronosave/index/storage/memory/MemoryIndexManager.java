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
	private final AllObjectByType allObjectByType;
	private final Map<Class<?>, ClassIndex<?>> classToIndexes;
	private final IdManager idManager;
	private final ReadWriteLock locker;

	public MemoryIndexManager(final IdManager idManager, final AllObjectByType allObjectByType) {
		classToIndexes = new HashMap<>();
		locker = new ReadWriteLock();
		this.idManager = idManager;
		this.allObjectByType = allObjectByType;
	}

	@SuppressWarnings("unchecked")
	private <U> ClassIndex<U> getClassIndexes(final Class<U> objectType) {
		if (!classToIndexes.containsKey(objectType))
			classToIndexes.put(objectType, new ClassIndex<>(objectType, idManager, allObjectByType));
		return (ClassIndex<U>) classToIndexes.get(objectType);
	}

	@SuppressWarnings("unchecked")
	public <U> void indexOrUpdate(final Collection<U> objects) throws InterruptedException, StorageException {
		locker.lockWrite();
		for (final U o : objects) {
			final Class<U> t = (Class<U>) o.getClass();
			getClassIndexes(t).addOrUpdate(o);
		}
		locker.unlockWrite();
	}

	public <U> Collection<U> selectStarFromClassWhere(final AbstractCondition<?, U> condition)
			throws InterruptedException, StorageException {
		return getClassIndexes(condition.getTypeObjet()).selectStarFromClassWhere(condition, locker);
	}

	@SuppressWarnings("unchecked")
	public <U> void unindex(final Collection<U> toBeDeleted) throws StorageException, InterruptedException {
		locker.lockWrite();
		for (final U o : toBeDeleted) {
			final Class<U> t = (Class<U>) o.getClass();
			getClassIndexes(t).supprimer(o);
		}
		locker.unlockWrite();
	}
}
