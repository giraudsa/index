package com.chronosave.index.storage.memory;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.chronosave.index.externe.AllObjectByType;
import com.chronosave.index.externe.IdManager;
import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.exception.StorageException;

public class MemoryIndexQuadTree<K extends List<Double>,U> extends MemoryIndex<K, U> {

	protected MemoryIndexQuadTree(Class<U> typeObject, ComputeKey<K, U> delegate, IdManager idManager, AllObjectByType allObjectByType) throws StorageException {
		super(typeObject, delegate, idManager, allObjectByType);
	}

	@Override
	protected MemoryNode<K, Set<U>> newMemoryNoeud(K xy) {
		double x0 = xy.get(0);
		double y0 = xy.get(1);
		double x;
		double y;
		double hw;
		double hh;
		if(x0 == 0) {
			x = -0.5;
			hw = 1;
		}else {
			hw = 2 * Math.abs(x0);
			x = x0 < 0 ? -hw : 0;
		}
		if(y0 == 0) {
			y = -0.5;
			hh = 1;
		}else {
			hh = 2 * Math.abs(y0);
			y = y0 < 0 ? -hh : 0;
		}
		return new MemoryNodeQuadTree<>(x, y, hw, hh, null);
	}
	
	public Collection<U> inTheBox(double xmin, double ymin, double xmax, double ymax) {
		final Collection<U> ret = new LinkedHashSet<>(); 
		if (getIndexe() == null)
			return ret;
		Visitor<MemoryNodeQuadTree<K,U>> visiteur = new Visitor<MemoryNodeQuadTree<K,U>>() {
			@Override
			protected void visite(MemoryNodeQuadTree<K, U> noeud) {
				ret.addAll(noeud.value);
			}
		};
		getIndexe().navigate(visiteur, xmin, ymin, xmax, ymax);
		return ret;
	}

	private MemoryNodeQuadTree<K, U> getIndexe() {
		return (MemoryNodeQuadTree<K, U>)index;
	}
}
