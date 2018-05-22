package com.chronosave.index.storage.memory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.chronosave.index.externe.AllObjectByType;
import com.chronosave.index.externe.IdManager;
import com.chronosave.index.storage.condition.AbstractCondition;
import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.condition.ConditionBBOX;
import com.chronosave.index.storage.condition.ConditionCompare;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.utils.ReadWriteLock;

public class ClassIndex<U> {

	private final AllObjectByType allObjectByType;
	private final IdManager idManager;
	private final Map<ComputeKey<?, U>, MemoryIndex<?, U>> keyToIndex;
	private final Class<U> objectType;

	protected ClassIndex(final Class<U> objectType, final IdManager idManager, final AllObjectByType allObjectByType) {
		this.objectType = objectType;
		this.keyToIndex = new HashMap<>();
		this.idManager = idManager;
		this.allObjectByType = allObjectByType;
	}

	protected void addOrUpdate(final U o) throws StorageException {
		for (final MemoryIndex<?, U> i : keyToIndex.values())
			i.addOrUpdate(o);
	}

	private <K extends Comparable<K>> void createMemoryIndexAVL(final ConditionCompare<K, U> condition)
			throws StorageException {
		keyToIndex.put(condition.getDelegate(),
				new MemoryIndexAVL<>(objectType, condition.getDelegate(), idManager, allObjectByType));
	}

	private <K extends List<Double>> void createMemorySpatialIndex(final ConditionBBOX<K, U> condition)
			throws StorageException {
		keyToIndex.put(condition.getDelegate(),
				new MemoryIndexQuadTree<>(objectType, condition.getDelegate(), idManager, allObjectByType));
	}

	@SuppressWarnings("unchecked")
	private <K> MemoryIndex<K, U> getMemoryIndex(final AbstractCondition<K, U> condition, final ReadWriteLock locker)
			throws InterruptedException, StorageException {
		if (!keyToIndex.containsKey(condition.getDelegate())) {
			locker.lockWrite();
			if (condition instanceof ConditionBBOX)
				createMemorySpatialIndex((ConditionBBOX<?, U>) condition);
			else
				createMemoryIndexAVL((ConditionCompare<?, U>) condition);
			locker.unlockWrite();
		}
		return (MemoryIndex<K, U>) keyToIndex.get(condition.getDelegate());
	}

	protected Collection<U> selectStarFromClassWhere(final AbstractCondition<?, U> condition,
			final ReadWriteLock locker) throws InterruptedException, StorageException {
		return condition.runMemory(getMemoryIndex(condition, locker), locker);
	}

	protected void supprimer(final U o) throws StorageException {
		for (final MemoryIndex<?, U> i : keyToIndex.values())
			i.delete(o);
	}

}
