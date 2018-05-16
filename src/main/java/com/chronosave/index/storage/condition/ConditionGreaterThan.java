package com.chronosave.index.storage.condition;

import java.io.IOException;
import java.util.Collection;

import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StoreException;
import com.chronosave.index.storage.file.AbstractIndex;
import com.chronosave.index.storage.file.IndexKeyToMultiId;
import com.chronosave.index.storage.memory.MemoryIndex;
import com.chronosave.index.storage.memory.MemoryIndexAVL;
import com.chronosave.index.utils.CloseableIterator;
import com.chronosave.index.utils.ReadWriteLock;

public class ConditionGreaterThan<K extends  Comparable<K>, U> extends ConditionCompare<K,U> {

	public ConditionGreaterThan(Class<U> typeObjet, Class<K> typeReturn, ComputeComparableKey<K, U> delegate, K valeur) {
		super(typeObjet, typeReturn, delegate);
		this.valeur = valeur;
	}



	private K valeur;
	
	
	
	@SuppressWarnings("unchecked") @Override
	public CloseableIterator<String> run(AbstractIndex<U,?,?> index, ReadWriteLock locker) throws StoreException, InterruptedException {
		try {
			return ((IndexKeyToMultiId<U,K>)index).getBetween(valeur, null, locker);
		} catch (IOException | StorageException | SerializationException e) {
			throw new StoreException(e);
		}
	}



	@SuppressWarnings("unchecked") @Override
	public <V> Collection<U> runMemory(MemoryIndex<?, V> memoryIndex, ReadWriteLock locker) throws InterruptedException {
		locker.lockRead();
		Collection<U> ret = (Collection<U>) ((MemoryIndexAVL<K,V>)memoryIndex).getBetween(valeur, null);
		locker.unlockRead();
		return ret;
	}


}
