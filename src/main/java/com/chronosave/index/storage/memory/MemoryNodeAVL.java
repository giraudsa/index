package com.chronosave.index.storage.memory;

import java.util.Set;

public class MemoryNodeAVL<K extends Comparable<? super K>, U> extends MemoryNode<K, Set<U>> {

	private int height;
	private MemoryNodeAVL<K, U> left;
	private MemoryNodeAVL<K, U> right;

	public MemoryNodeAVL(final K key, final Set<U> value) {
		super(key, value);
		height = 0;
	}

	@Override
	protected MemoryNode<K, Set<U>> addAndBalance(final K key, final Set<U> us) {
		if (areEquals(key, this.key))
			value.addAll(us);
		else if (k1IsSmallerThank2(key, this.key))
			left = left == null ? new MemoryNodeAVL<>(key, us) : (MemoryNodeAVL<K, U>) left.addAndBalance(key, us);
		else // key est plus grand que this.getkey()
			right = right == null ? new MemoryNodeAVL<>(key, us) : (MemoryNodeAVL<K, U>) right.addAndBalance(key, us);
		return balance();
	}

	private boolean areEquals(final K k1, final K k2) {
		return k1 == null && k2 == null || k2 != null && k1 != null && k1.compareTo(k2) == 0;
	}

	private MemoryNode<K, Set<U>> balance() {
		updateHeight();
		if (left.height - right.height == 2) {
			if (left.left.height < left.right.height)
				left = left.leftRotation();
			return rightRotation();
		}
		if (left.height - right.height == -2) {
			if (right.right.height < right.left.height)
				right = right.rightRotation();
			return leftRotation();
		}
		return this;
	}

	@Override
	protected MemoryNode<K, Set<U>> deleteAndBalance(final K key) {
		if (areEquals(key, this.key))
			return supprimeNoeud();
		if (k1IsSmallerThank2(key, this.key)) {
			if (left == null)
				return this;
			left = (MemoryNodeAVL<K, U>) left.deleteAndBalance(key);
		} else if (right == null)
			return this;
		else
			right = (MemoryNodeAVL<K, U>) right.deleteAndBalance(key);
		return balance();
	}

	private MemoryNodeAVL<K, U> deleteLeft() {
		if (left == null)
			return right;
		left = left.deleteLeft();
		return (MemoryNodeAVL<K, U>) balance();
	}

	@Override
	protected MemoryNode<K, Set<U>> findNoeud(final K key) {
		if (areEquals(key, this.key))
			return this;
		if (k1IsSmallerThank2(key, this.key))
			return left == null ? null : left.findNoeud(key);
		return right == null ? null : left.findNoeud(key);
	}

	// null est la valeure la plus grande
	private boolean k1IsSmallerThank2(final K k1, final K k2) {
		return k2 == null || k1 != null && k2 != null && k2.compareTo(k1) > 0;
	}

	private boolean k1IsSmallerThanOrEqualsk2(final K k1, final K k2) {
		if (areEquals(k1, k2))
			return true;
		return k2 == null || k1 != null && k2 != null && k2.compareTo(k1) > 0;
	}

	private MemoryNodeAVL<K, U> leftRotation() {
		final MemoryNodeAVL<K, U> temp = right;
		right = temp.left;
		this.updateHeight();
		temp.left = this;
		temp.updateHeight();
		return temp;
	}

	protected void navigate(final Visitor<MemoryNodeAVL<K, U>> visitor, final K min, final K max) {
		if (min == null || left != null && k1IsSmallerThanOrEqualsk2(min, key))
			left.navigate(visitor, min, max);
		if ((min == null || k1IsSmallerThanOrEqualsk2(min, key)) && k1IsSmallerThanOrEqualsk2(key, max))
			visitor.visite(this);
		if (right != null && k1IsSmallerThanOrEqualsk2(key, max))
			right.navigate(visitor, min, max);
	}

	private MemoryNodeAVL<K, U> rightRotation() {
		final MemoryNodeAVL<K, U> temp = left;
		left = temp.right;
		this.updateHeight();
		temp.right = this;
		temp.updateHeight();
		return temp;
	}

	private MemoryNode<K, Set<U>> supprimeNoeud() {
		if (right == null && left == null)
			return null;
		if (right == null)
			return left;
		if (left == null)
			return right;
		MemoryNodeAVL<K, U> goLeft = right;
		while (goLeft.left != null)
			goLeft = goLeft.left;
		goLeft.right = right.deleteLeft();
		goLeft.left = left;
		return goLeft.balance();
	}

	private void updateHeight() {
		height = 1 + Math.max(left == null ? -1 : left.height, right == null ? -1 : height);
	}
}
