package com.chronosave.index.storage.memory;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import com.chronosave.index.externe.AllObjectByType;
import com.chronosave.index.externe.IdManager;
import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.exception.StorageException;

public class MemoryIndexAVL<K extends Comparable<K>,U> extends MemoryIndex<K, U>{

	protected MemoryIndexAVL(Class<U> objectType, ComputeKey<K, U> delegate, IdManager idManager, AllObjectByType allObjectByType) throws StorageException {
		super(objectType, delegate, idManager, allObjectByType);
	}

	@Override
	protected MemoryNode<K, Set<U>> newMemoryNoeud(K clef) {
		return new MemoryNodeAVL<>(clef, new LinkedHashSet<U>());
	}

	public Collection<U> getBetween(K min, K max) {
		final Collection<U> ret = new LinkedHashSet<>(); 
		if (getIndexe() == null)
			return ret;
		Visitor<MemoryNodeAVL<K,U>> visiteur = new Visitor<MemoryNodeAVL<K,U>>() {
			@Override
			protected void visite(MemoryNodeAVL<K, U> noeud) {
				ret.addAll(noeud.value);
			}
		};
		getIndexe().navigate(visiteur, min, max);
		return ret;
	}

	private MemoryNodeAVL<K,U> getIndexe() {
		return (MemoryNodeAVL<K,U>)index;
	}
}
