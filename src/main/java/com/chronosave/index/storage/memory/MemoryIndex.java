package com.chronosave.index.storage.memory;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.chronosave.index.externe.AllObjectByType;
import com.chronosave.index.externe.IdManager;
import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.exception.StorageException;

public abstract class MemoryIndex<K, U> {

	private final AllObjectByType allObjectByType;
	protected final ComputeKey<K, U> delegate;
	private final IdManager idManager;
	protected MemoryNode<K, Set<U>> index;
	private final Class<U> objectType;
	private final Map<String, K> reverse = new HashMap<>();

	protected MemoryIndex(final Class<U> objectType, final ComputeKey<K, U> delegate, final IdManager idManager,
			final AllObjectByType allObjectByType) throws StorageException {
		super();
		this.idManager = idManager;
		this.objectType = objectType;
		this.delegate = delegate;
		this.allObjectByType = allObjectByType;
		initializes();
	}

	protected void addOrUpdate(final U o) throws StorageException {
		delete(o);
		stocker(o);
	}

	protected void delete(final U o) throws StorageException {
		if (!reverse.containsKey(idManager.getId(o)))
			return; // no last version
		final K oldK = reverse.get(idManager.getId(o));
		supprimer(oldK, o);
	}

	protected K getClef(final U obj) throws StorageException {
		return delegate.getKey(obj);
	}

	private void initializes() throws StorageException {
		for (final U u : allObjectByType.getAll(objectType))
			stocker(u);
	}

	protected abstract MemoryNode<K, Set<U>> newMemoryNoeud(K clef);

	private void stocker(final U o) throws StorageException {
		final LinkedHashSet<U> i = new LinkedHashSet<>();
		i.add(o);
		final K clef = getClef(o);
		if (index == null)
			index = newMemoryNoeud(clef);
		else
			index = index.addAndBalance(clef, i);
	}

	private void supprimer(final K k, final U o) throws StorageException {
		reverse.remove(idManager.getId(o));
		final MemoryNode<K, Set<U>> n = index.findNoeud(k);
		if (n != null) {
			n.value.remove(o);
			if (n.value.isEmpty())
				index = index.deleteAndBalance(k);
		}

	}

}
