package com.chronosave.index.storage.memory;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import com.chronosave.index.externe.AllObjectByType;
import com.chronosave.index.externe.IdManager;
import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.exception.StorageException;

public class MemoryIndexAVL<K extends Comparable<K>, U> extends MemoryIndex<K, U> {

	protected MemoryIndexAVL(final Class<U> objectType, final ComputeKey<K, U> delegate, final IdManager idManager,
			final AllObjectByType allObjectByType) throws StorageException {
		super(objectType, delegate, idManager, allObjectByType);
	}

	public Collection<U> getBetween(final K min, final K max) {
		final Collection<U> ret = new LinkedHashSet<>();
		if (getIndexe() == null)
			return ret;
		final Visitor<MemoryNodeAVL<K, U>> visiteur = new Visitor<MemoryNodeAVL<K, U>>() {
			@Override
			protected void visite(final MemoryNodeAVL<K, U> noeud) {
				ret.addAll(noeud.value);
			}
		};
		getIndexe().navigate(visiteur, min, max);
		return ret;
	}

	private MemoryNodeAVL<K, U> getIndexe() {
		return (MemoryNodeAVL<K, U>) index;
	}

	@Override
	protected MemoryNode<K, Set<U>> newMemoryNoeud(final K clef) {
		return new MemoryNodeAVL<>(clef, new LinkedHashSet<U>());
	}
}
