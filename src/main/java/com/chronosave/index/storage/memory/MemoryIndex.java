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

	private final Class<U> objectType;
	protected final ComputeKey<K, U> delegate;
	private final Map<String, K> reverse = new HashMap<>();
	protected MemoryNode<K, Set<U>> index;
	private final IdManager idManager;
	private final AllObjectByType allObjectByType;
	
	
	protected MemoryIndex(Class<U> objectType, ComputeKey<K, U> delegate, IdManager idManager, AllObjectByType allObjectByType) throws StorageException {
		super();
		this.idManager = idManager;
		this.objectType = objectType;
		this.delegate = delegate;
		this.allObjectByType = allObjectByType;
		initializes();
	}
	private void initializes() throws StorageException {
		for(U u : allObjectByType.getAll(objectType)) {
			stocker(u);
		}
	}
	protected K getClef(U obj) throws StorageException {
		return delegate.getKey(obj);
	}

	protected void addOrUpdate(U o) throws StorageException {
		delete(o);
		stocker(o);
	}
	protected void delete(U o) throws StorageException {
		if(!reverse.containsKey(idManager.getId(o)))
			return; //no last version
		K oldK = reverse.get(idManager.getId(o));
		supprimer(oldK, o);
	}
	
	
	private void supprimer(K k, U o) throws StorageException {
		reverse.remove(idManager.getId(o));
		MemoryNode<K, Set<U>> n = index.findNoeud(k);
		if(n != null) {
			n.value.remove(o);
			if(n.value.isEmpty()) index = index.deleteAndBalance(k);
		}
			
	}

	private void stocker(U o) throws StorageException {
		LinkedHashSet<U> i = new LinkedHashSet<>();
		i.add(o);
		K clef = getClef(o);
		if(index == null)
			index = newMemoryNoeud(clef);
		else index = index.addAndBalance(clef, i);
	}
	protected abstract MemoryNode<K, Set<U>> newMemoryNoeud(K clef);
	
}
