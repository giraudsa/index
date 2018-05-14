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

	private final Class<U> objectType;
	private final Map<ComputeKey<?, U>, MemoryIndex<?,U>> keyToIndex;
	private final IdManager idManager;
	private final AllObjectByType allObjectByType;
	protected ClassIndex(Class<U> objectType, IdManager idManager, AllObjectByType allObjectByType) {
		this.objectType = objectType;
		this.keyToIndex = new HashMap<>();
		this.idManager = idManager;
		this.allObjectByType = allObjectByType;
	}
	protected Collection<U> selectStarFromClassWhere(AbstractCondition<?, U> condition, ReadWriteLock locker) throws InterruptedException, StorageException {
		return condition.runMemory(getMemoryIndex(condition, locker), locker);
	}
	protected void addOrUpdate(U o) throws StorageException {
		for(MemoryIndex<?, U> i : keyToIndex.values()) {
			i.addOrUpdate(o);
		}
	}
	protected void supprimer(U o) throws StorageException {
		for(MemoryIndex<?, U> i : keyToIndex.values()) {
			i.delete(o);
		}
	}
	
	@SuppressWarnings("unchecked")
	private <K> MemoryIndex<K,U> getMemoryIndex(AbstractCondition<K, U> condition, ReadWriteLock locker) throws InterruptedException, StorageException{
		if(!keyToIndex.containsKey(condition.getDelegate())) {
			locker.lockWrite();
			if(condition instanceof ConditionBBOX)
				createMemorySpatialIndex((ConditionBBOX<?, U>)condition);
			else {
				createMemoryIndexAVL((ConditionCompare<?, U>)condition);
			}
			locker.unlockWrite();
		}
		return (MemoryIndex<K, U>) keyToIndex.get(condition.getDelegate());
	}
	private <K extends Comparable<K>> void createMemoryIndexAVL(ConditionCompare<K, U> condition) throws StorageException {
		keyToIndex.put(condition.getDelegate(), new MemoryIndexAVL<>(objectType, condition.getDelegate(), idManager, allObjectByType));
	}
	private <K extends List<Double>> void createMemorySpatialIndex(ConditionBBOX<K, U> condition) throws StorageException {
		keyToIndex.put(condition.getDelegate(), new MemoryIndexQuadTree<>(objectType, condition.getDelegate(), idManager, allObjectByType));
	}
	

}
