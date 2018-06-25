package com.chronosave.index.storage.condition;

import java.io.IOException;
import java.util.Collection;

import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StoreException;
import com.chronosave.index.storage.file.AbstractIndex;
import com.chronosave.index.storage.file.Index1D;
import com.chronosave.index.storage.memory.MemoryIndex;
import com.chronosave.index.storage.memory.MemoryIndexAVL;
import com.chronosave.index.utils.CloseableIterator;
import com.chronosave.index.utils.ReadWriteLock;

public class ConditionLessThan<K extends Comparable<K>, U> extends ConditionCompare<K, U> {

	private final K valeur;

	public ConditionLessThan(final Class<U> typeObjet, final Class<K> typeReturn,
			final ComputeComparableKey<K, U> delegate, final K valeur) {
		super(typeObjet, typeReturn, delegate);
		this.valeur = valeur;
	}

	@SuppressWarnings("unchecked")
	@Override
	public CloseableIterator<String> run(final AbstractIndex<U, ?, ?> index, final ReadWriteLock locker)
			throws StoreException, InterruptedException {
		try {
			return ((Index1D<U, K, String, ?>) index).getBetween(null, valeur, locker);
		} catch (IOException | StorageException | SerializationException e) {
			throw new StoreException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> Collection<U> runMemory(final MemoryIndex<?, V> memoryIndex, final ReadWriteLock locker)
			throws InterruptedException {
		locker.lockRead();
		final Collection<U> ret = (Collection<U>) ((MemoryIndexAVL<K, V>) memoryIndex).getBetween(null, valeur);
		locker.unlockRead();
		return ret;
	}
}
