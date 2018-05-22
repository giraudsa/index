package com.chronosave.index.storage.memory;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.chronosave.index.externe.AllObjectByType;
import com.chronosave.index.externe.IdManager;
import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.exception.StorageException;

public class MemoryIndexQuadTree<K extends List<Double>, U> extends MemoryIndex<K, U> {

	protected MemoryIndexQuadTree(final Class<U> typeObject, final ComputeKey<K, U> delegate, final IdManager idManager,
			final AllObjectByType allObjectByType) throws StorageException {
		super(typeObject, delegate, idManager, allObjectByType);
	}

	private MemoryNodeQuadTree<K, U> getIndexe() {
		return (MemoryNodeQuadTree<K, U>) index;
	}

	public Collection<U> inTheBox(final double xmin, final double ymin, final double xmax, final double ymax) {
		final Collection<U> ret = new LinkedHashSet<>();
		if (getIndexe() == null)
			return ret;
		final Visitor<MemoryNodeQuadTree<K, U>> visiteur = new Visitor<MemoryNodeQuadTree<K, U>>() {
			@Override
			protected void visite(final MemoryNodeQuadTree<K, U> noeud) {
				ret.addAll(noeud.value);
			}
		};
		getIndexe().navigate(visiteur, xmin, ymin, xmax, ymax);
		return ret;
	}

	@Override
	protected MemoryNode<K, Set<U>> newMemoryNoeud(final K xy) {
		final double x0 = xy.get(0);
		final double y0 = xy.get(1);
		double x;
		double y;
		double hw;
		double hh;
		if (x0 == 0) {
			x = -0.5;
			hw = 1;
		} else {
			hw = 2 * Math.abs(x0);
			x = x0 < 0 ? -hw : 0;
		}
		if (y0 == 0) {
			y = -0.5;
			hh = 1;
		} else {
			hh = 2 * Math.abs(y0);
			y = y0 < 0 ? -hh : 0;
		}
		return new MemoryNodeQuadTree<>(x, y, hw, hh, null);
	}
}
