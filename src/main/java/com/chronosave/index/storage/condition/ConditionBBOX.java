package com.chronosave.index.storage.condition;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StoreException;
import com.chronosave.index.storage.file.AbstractIndex;
import com.chronosave.index.storage.file.SpaceIndex;
import com.chronosave.index.storage.memory.MemoryIndex;
import com.chronosave.index.storage.memory.MemoryIndexQuadTree;
import com.chronosave.index.utils.CloseableIterator;
import com.chronosave.index.utils.ReadWriteLock;

public class ConditionBBOX<K extends List<Double>, U> extends AbstractCondition<K, U> {

	private final double xmin;
	private final double ymin;
	private final double xmax;
	private final double ymax;
	
	

	/**
	 * @param typeObjet
	 * @param typeReturn
	 * @param delegate
	 * @param xmin
	 * @param ymin
	 * @param xmax
	 * @param ymax
	 */
	public ConditionBBOX(Class<U> typeObjet, Class<K> typeReturn, ComputeKey<K, U> delegate, double xmin, double ymin, double xmax, double ymax) {
		super(typeObjet, typeReturn, delegate);
		this.xmin = xmin <= xmax ? xmin : xmax;
		this.ymin = ymin <= ymax ? ymin : ymax;
		this.xmax = xmax >= xmin ? xmax : xmin;
		this.ymax = ymax >= ymin ? ymax : ymin;
	}



	@SuppressWarnings("unchecked") @Override
	public CloseableIterator<String> run(AbstractIndex<U, ?, ?> index, ReadWriteLock locker) throws StoreException, InterruptedException {
		try {
			return ((SpaceIndex<U, K>)index).inTheBox(xmin, ymin, xmax, ymax, locker);
		} catch (IOException | StorageException | SerializationException e) {
			throw new StoreException(e);
		}
	}



	@SuppressWarnings("unchecked") @Override
	public <V> Collection<U> runMemory(MemoryIndex<?, V> memoryIndex, ReadWriteLock locker) throws InterruptedException {
	    locker.lockRead();
		Collection<U> ret = (Collection<U>) ((MemoryIndexQuadTree<?,V>)memoryIndex).inTheBox(xmin, ymin, xmax, ymax);
		locker.unlockRead();
		return ret;
	}
	
}
