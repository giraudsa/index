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

public class ConditionEquals<K extends  Comparable<K>, U> extends ConditionCompare<K, U> {

	private final K valeurCible;
	public ConditionEquals(Class<U> typeObjet, Class<K> typeReturn, ComputeKey<K, U> delegate, K valeurcible) {
		super(typeObjet, typeReturn, delegate);
		this.valeurCible = valeurcible;
	}
	@SuppressWarnings("unchecked") @Override
	public CloseableIterator<String> run(AbstractIndex<U,?,?> index, ReadWriteLock locker) throws StoreException, InterruptedException {
		try {
			return ((IndexKeyToMultiId<U,K>)index).getBetween(valeurCible, valeurCible, locker);
		} catch (IOException | StorageException | SerializationException e) {
			throw new StoreException(e);
		}
	}
	@SuppressWarnings("unchecked") @Override
	public <V> Collection<U> runMemory(MemoryIndex<?, V> memoryIndex, ReadWriteLock locker) throws InterruptedException {
		locker.lockRead();
		Collection<U> ret = (Collection<U>) ((MemoryIndexAVL<K,V>)memoryIndex).getBetween(valeurCible, valeurCible);
		locker.unlockRead();
		return ret;
	}


}
