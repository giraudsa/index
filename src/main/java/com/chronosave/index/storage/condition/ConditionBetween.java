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

public class ConditionBetween<K extends Comparable<K>, U> extends ConditionCompare<K, U> {

	private final K max;
	private final K min;

	public ConditionBetween(final Class<U> typeObjet, final Class<K> typeReturn,
			final ComputeComparableKey<K, U> delegate, final K min, final K max) {
		super(typeObjet, typeReturn, delegate);
		this.min = max.compareTo(min) > 0 ? min : max;
		this.max = this.min == min ? max : min;
	}

	@SuppressWarnings("unchecked")
	@Override
	public CloseableIterator<String> run(final AbstractIndex<U, ?, ?> index, final ReadWriteLock locker)
			throws StoreException, InterruptedException {
		try {
			return ((Index1D<U, K, String, ?>) index).getBetween(min, max, locker);
		} catch (IOException | StorageException | SerializationException e) {
			throw new StoreException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> Collection<U> runMemory(final MemoryIndex<?, V> memoryIndex, final ReadWriteLock locker)
			throws InterruptedException {
		locker.lockRead();
		final Collection<U> ret = (Collection<U>) ((MemoryIndexAVL<K, V>) memoryIndex).getBetween(min, max);
		locker.unlockRead();
		return ret;
	}
}
