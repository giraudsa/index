package com.chronosave.index.storage.memory;

import java.util.Set;


public class MemoryNodeAVL<K extends Comparable<? super K>, U> extends MemoryNode<K, Set<U>> {

	private MemoryNodeAVL<K, U> left;
	private MemoryNodeAVL<K,U> right;
	private int height;
	public MemoryNodeAVL(K key, Set<U> value) {
		super(key, value);
		height = 0;
	}

	@Override
	protected MemoryNode<K, Set<U>> addAndBalance(K key, Set<U> us) {
		if(areEquals(key, this.key)) value.addAll(us);
		else if(k1IsSmallerThank2(key, this.key))
			left = (left == null) ? new MemoryNodeAVL<>(key, us) :(MemoryNodeAVL<K, U>)left.addAndBalance(key, us);
		else //key est plus grand que this.getkey()
			right = right == null ? new MemoryNodeAVL<>(key, us) : (MemoryNodeAVL<K, U>)right.addAndBalance(key, us);
		return balance(); 
	}

	private MemoryNode<K, Set<U>> balance() {
		updateHeight();
		if(left.height - right.height == 2){
			if(left.left.height < left.right.height) left = left.leftRotation();
			return rightRotation();
		}
		if(left.height - right.height == -2){
			if(right.right.height < right.left.height) right = right.rightRotation();
			return leftRotation();
		}
		return this;
	}


	private MemoryNodeAVL<K, U> rightRotation() {
		MemoryNodeAVL<K,U> temp = left;
		left = temp.right;
		this.updateHeight();
		temp.right = this;
		temp.updateHeight();
		return temp;
	}

	private MemoryNodeAVL<K, U> leftRotation() {
		MemoryNodeAVL<K,U> temp = right;
		right = temp.left;
		this.updateHeight();
		temp.left = this;
		temp.updateHeight();
		return temp;
	}

	private void updateHeight() {
		height = 1 + Math.max(left == null? -1 : left.height, right == null ? -1 : height);
	}

	@Override
	protected MemoryNode<K, Set<U>> deleteAndBalance(K key) {
		if(areEquals(key, this.key)) return supprimeNoeud();
		if(k1IsSmallerThank2(key, this.key)) {
			if(left == null) return this;
			left = (MemoryNodeAVL<K, U>) left.deleteAndBalance(key);
		}else { //key est plus grand que this.key
			if(right == null) return this;
			else right= (MemoryNodeAVL<K, U>) right.deleteAndBalance(key);
		}
		return balance();
	}

	private MemoryNode<K, Set<U>> supprimeNoeud() {
		if(right == null && left == null) return null;
		if(right == null) return left;
		if(left == null) return right;
		MemoryNodeAVL<K, U> goLeft = right;
		while(goLeft.left != null)
			goLeft = goLeft.left;
		goLeft.right = right.deleteLeft();
		goLeft.left = left;
		return goLeft.balance();
	}

	private MemoryNodeAVL<K, U> deleteLeft() {
		if (left == null) return right;
		left = left.deleteLeft();
		return (MemoryNodeAVL<K, U>) balance();
	}

	@Override
	protected MemoryNode<K, Set<U>> findNoeud(K key) {
		if(areEquals(key, this.key)) return this;
		if(k1IsSmallerThank2(key, this.key)) return left == null ? null : left.findNoeud(key);
		return right == null ? null : left.findNoeud(key);
	}
	
	private boolean areEquals(K k1, K k2) {
		return k1 == null && k2 == null || (k2 != null && k1 != null && k1.compareTo(k2) == 0);
	}
	//null est la valeure la plus grande
	private boolean k1IsSmallerThank2(K k1, K k2) {
		return k2 == null || (k1 != null && k2 != null && k2.compareTo(k1) > 0);
	}
	
	private boolean k1IsSmallerThanOrEqualsk2(K k1, K k2) {
		if(areEquals(k1, k2)) return true;
		return k2 == null || (k1 != null && k2 != null && k2.compareTo(k1) > 0);
	}
	
	protected void navigate(Visitor<MemoryNodeAVL<K, U>> visitor, K min, K max) {
		if(min == null || left != null && k1IsSmallerThanOrEqualsk2(min, key)) left.navigate(visitor, min, max);
		if((min == null || k1IsSmallerThanOrEqualsk2(min, key)) && k1IsSmallerThanOrEqualsk2(key, max)) visitor.visite(this);
		if(right != null && k1IsSmallerThanOrEqualsk2(key, max)) right.navigate(visitor, min, max);
	}
}
