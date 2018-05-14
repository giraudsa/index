package com.chronosave.index.storage.memory;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class MemoryNodeQuadTree<K extends List<Double>, U> extends MemoryNode<K, Set<U>> {

	private static final byte EMPTY = 0;
	private static final byte LEAF = 1;
	private static final byte POINTER = 2;
	
	private final double x;
	private final double y;
	private final double w;
	private final double h;
	private byte nodeType;
	private MemoryNodeQuadTree<K,U> daddy;
	private MemoryNodeQuadTree<K,U> ne;
	private MemoryNodeQuadTree<K,U> nw;
	private MemoryNodeQuadTree<K,U> se;
	private MemoryNodeQuadTree<K,U> sw;
	public MemoryNodeQuadTree(double x, double y, double w, double h, MemoryNodeQuadTree<K,U> daddy) {
		super(null, new LinkedHashSet<U>());
		this.nodeType = EMPTY;
		this.daddy = daddy;
		this.x = x;
		this.y = y;
		this.w = w;
		this.h = h;
	}
	@Override
	protected MemoryNode<K, Set<U>> addAndBalance(K key, Set<U> objects) {
		if(isRoot() && !contains(key)) {
			MemoryNodeQuadTree<K, U> root = newMemoryNodeQuadTreeRoot(key);
        	return root.addAndBalance(key, objects);
        }
		
		if (nodeType == EMPTY) {
			this.key = key;
			this.value.addAll(objects);
			nodeType = LEAF;
		} else if (nodeType == LEAF) {
			K k = this.key;
			if (k.get(0).equals(key.get(0)) && k.get(1).equals(key.get(1))) {//
				this.value.addAll(objects);
			} else {
			    split();
			    addAndBalance(key, objects);
			}
		} else {// (typeNoeud == POINTER)
			getQuandrantForPoint(key).addAndBalance(key, objects);
		}
        return this;
	}

	private MemoryNode<K, Set<U>> getQuandrantForPoint(K clef) {
		double mx = x + w / 2;
		double my = y + h / 2;
		if (clef.get(0) < mx) return clef.get(1) < my ? nw : sw;
		return clef.get(1) < my ? ne : se;
	}
	private void split() {
		

		double hw = w / 2;
		double hh = h / 2;

		nw = new MemoryNodeQuadTree<>(x, y, hw, hh, this);
		ne = new MemoryNodeQuadTree<>(x + hw, y, hw, hh, this);
		sw = new MemoryNodeQuadTree<>(x, y + hh, hw, hh, this);
		se = new MemoryNodeQuadTree<>(x + hw, y + hh, hw, hh, this);
		nodeType = POINTER;
		addAndBalance(key, value);
	}
	private MemoryNodeQuadTree<K, U> newMemoryNodeQuadTreeRoot(K key) {
		double x0 = key.get(0);
		double y0 = key.get(1);
		double nx = (x0 < x) ? x - w : x;
		double ny = (y0 < y) ? y - h : y;
		double hw = 2 * w;
		double hh = 2 * h;
		boolean east = (nx < x) ? true : false;
		boolean north = (ny < y) ? true : false; 
		MemoryNodeQuadTree<K,U> root = new MemoryNodeQuadTree<>(nx, ny, hw, hh, null);
		daddy = root;
		root.nodeType = POINTER;
		MemoryNodeQuadTree<K,U> nnw;
		MemoryNodeQuadTree<K,U> nne;
		MemoryNodeQuadTree<K,U> nsw;
		MemoryNodeQuadTree<K,U> nse;
		if(north && east) {//NE
			nne = this;
			nnw = new MemoryNodeQuadTree<>(x - w, y, w, h, root);
			nsw = new MemoryNodeQuadTree<>(x - w, y - h, w, h, root);
			nse = new MemoryNodeQuadTree<>(x, y - h, w, h, root);
		}else if(north && !east){ //NW
			nne = new MemoryNodeQuadTree<>(x + w, y, w, h, root);
			nnw = this;
			nsw = new MemoryNodeQuadTree<>(x, y - h, w, h, root);
			nse = new MemoryNodeQuadTree<>(x + w, y - h, w, h, root);
		}else if(!north && east) { //SE
			nne = new MemoryNodeQuadTree<>(x, y + h, w, h, root);
			nnw = new MemoryNodeQuadTree<>(x - w, y + h, w, h, root);
			nsw = new MemoryNodeQuadTree<>(x - w, y, w, h, root);
			nse = this;
		}else{//SW
			nne = new MemoryNodeQuadTree<>(x + h, y + h, w, h, root);
			nnw = new MemoryNodeQuadTree<>(x, y + h, w, h, root);
			nsw = this;
			nse = new MemoryNodeQuadTree<>(x + h, y, w, h, root);
		}

		ne = nne;
		nw = nnw;
		se = nse;
		sw = nsw;
		return root;
	}
	private boolean contains(K key) {
		return intersects(key.get(0), key.get(1), key.get(0), key.get(1));
	}
	private boolean intersects(double left, double bottom, double right, double top){
        return !(x > right ||
                (x + w) < left ||
                y > bottom ||
                (y + h) < top);
    }
	private boolean isRoot() {
		return daddy == null;
	}
	@Override
	protected MemoryNode<K, Set<U>> deleteAndBalance(K key) {
		//useless
		return this;
	}
	@Override
	protected MemoryNode<K, Set<U>> findNoeud(K key) {
		MemoryNode<K, Set<U>> response = null;
		if (nodeType == LEAF) response = this.key.get(0) == key.get(0) && this.key.get(1) == key.get(1) ? this : null;
		else if (nodeType == POINTER) response = getQuandrantForPoint(key).findNoeud(key);
		return response;
	}
	
	protected void navigate(Visitor<MemoryNodeQuadTree<K,U>> visitor, double xmin, double ymin, double xmax, double ymax) {
        if (nodeType == LEAF) {
			visitor.visite(this);
		} else if (nodeType == POINTER) {
			if (ne.intersects(xmin, ymax, xmax, ymin))
			    ne.navigate(visitor, xmin, ymin, xmax, ymax);
			if (se.intersects(xmin, ymax, xmax, ymin))
			    se.navigate(visitor, xmin, ymin, xmax, ymax);
			if (sw.intersects(xmin, ymax, xmax, ymin))
			    sw.navigate(visitor, xmin, ymin, xmax, ymax);
			if (nw.intersects(xmin, ymax, xmax, ymin))
			    nw.navigate(visitor, xmin, ymin, xmax, ymax);
		}
    }
}
