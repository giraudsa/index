package com.chronosave.index.storage.condition;

import java.util.Collection;

import com.chronosave.index.storage.exception.StoreException;
import com.chronosave.index.storage.file.AbstractIndex;
import com.chronosave.index.storage.memory.MemoryIndex;
import com.chronosave.index.utils.CloseableIterator;
import com.chronosave.index.utils.ReadWriteLock;

public abstract class AbstractCondition<K, U> {
	private final ComputeKey<K, U> delegate;
	private final Class<U> typeObjet;
	private final Class<K> typeReturn;
	public abstract CloseableIterator<String> run(AbstractIndex<U,?,?> index, ReadWriteLock locker) throws StoreException, InterruptedException;
	public abstract <V> Collection<U> runMemory(MemoryIndex<?, V> memoryIndex, ReadWriteLock locker) throws InterruptedException;
	/**
	 * @param delegate
	 */
	protected AbstractCondition(Class<U> typeObjet, Class<K> typeReturn, ComputeKey<K, U> delegate) {
		super();
		this.delegate = delegate;
		this.typeObjet = typeObjet;
		this.typeReturn = typeReturn;
	}

	public ComputeKey<K, U> getDelegate() {
		return delegate;
	}

	public Class<U> getTypeObjet() {
		return typeObjet;
	}

	public Class<K> getTypeReturn() {
		return typeReturn;
	}
}
