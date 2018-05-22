package com.chronosave.index.storage.file;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.chronosave.index.storage.exception.IOError;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StorageRuntimeException;

public abstract class Node1D<K extends Comparable<K>, V> extends AbstractNode<K, V> implements Iterable<V> {

	protected class KeyNodeIterator implements Iterator<K> {

		private Node1D<K, V> current;
		private final Deque<Node1D<K, V>> nodesStack = new ArrayDeque<>();

		protected KeyNodeIterator(final Node1D<K, V> racine) {
			current = racine.isFake() ? null : racine;
		}

		@Override
		public boolean hasNext() {
			return !nodesStack.isEmpty() || current != null;
		}

		@Override
		public K next() {
			if (!hasNext())
				throw new NoSuchElementException();
			try {
				while (current != null) {
					nodesStack.push(current);
					current = current.getLeft(null);
				}

				current = nodesStack.pop();
				final Node1D<K, V> node = current;
				current = current.getRight(null);

				return node.getKey(null);
			} catch (final IOException e) {
				throw new IOError(e);
			} catch (StorageException | SerializationException e) {
				throw new StorageRuntimeException(e);
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	protected class ValueNodeIterator implements Iterator<V> {
		private Node1D<K, V> current;
		private boolean hasNext = false;
		private final K max;
		private final K min;
		private V next;
		private final Deque<Node1D<K, V>> nodesStack = new ArrayDeque<>();

		protected ValueNodeIterator(final K min, final K max) {
			current = Node1D.this.isFake() ? null : Node1D.this;
			this.min = min;
			this.max = max;
			cacheNext();
		}

		private void cacheNext() {
			try {
				hasNext = false;
				while (!hasNext && !(nodesStack.isEmpty() && current == null))
					searchNext();
			} catch (final IOException e) {
				throw new IOError(e);
			} catch (StorageException | SerializationException e) {
				throw new StorageRuntimeException(e);
			}
		}

		private boolean constraintsRespected(final K key) {
			if (min == null && max == null)
				return true;
			if (min == null)
				return max.compareTo(key) > 0;
			if (max == null)
				return min.compareTo(key) < 0;
			if (max.compareTo(min) == 0)
				return key != null && key.compareTo(max) == 0;
			return min.compareTo(key) < 0 && max.compareTo(key) > 0;
		}

		@Override
		public boolean hasNext() {
			return hasNext;
		}

		@Override
		public V next() {
			if (!hasNext())
				throw new NoSuchElementException();
			final V ret = next;
			cacheNext();
			return ret;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		private void searchNext() throws IOException, StorageException, SerializationException {
			while (current != null) {
				nodesStack.push(current);
				if (min != null) {
					final K key = current.getKey(null);
					current = k1IsLessThanK2(min, key) || areEquals(min, key) ? current.getLeft(null) : null;
				} else
					current = current.getLeft(null);
			}

			current = nodesStack.pop();
			final Node1D<K, V> node = current;
			if (max != null)
				current = k1IsLessThanK2(current.getKey(null), max) || areEquals(current.getKey(null), max)
						? current.getRight(null)
						: null;
			else
				current = current.getRight(null);
			if (constraintsRespected(node.getKey(null))) {
				hasNext = true;
				next = node.getValue(null);
			}
		}
	}

	private static final String IO_PROBLEM = "IO problem !";

	private static final long NULL = -1L;

	private static final <K extends Comparable<K>, V> int height(final Node1D<K, V> node,
			final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return node != null ? node.height(modifs) : (int) NULL;
	}

	private boolean isFake = false;

	// fake noeud
	protected Node1D(final Class<K> keyType, final Class<V> valueType, final AbstractIndex<?, ?, ?> index,
			final CacheModifications modifs) {
		super(keyType, valueType, index, modifs.getEndOfFile());
		isFake = true;
	}

	/**
	 * from file
	 * 
	 * @param position
	 * @param index
	 * @param keyType
	 * @param valueType
	 */
	protected Node1D(final long position, final AbstractIndex<?, ?, ?> index, final Class<K> keyType,
			final Class<V> valueType) {
		super(keyType, valueType, index, position);
	}

	/**
	 * runtime subtilité, si positionValeur est null, on ne l'écrit pas
	 * 
	 * @param keyPosition
	 * @param valuePosition
	 * @param index
	 * @param modifs
	 * @param keyType
	 * @param valueType
	 * @throws SerializationException
	 */
	protected Node1D(final long keyPosition, final Long valuePosition, final AbstractIndex<?, ?, ?> index,
			final Class<K> keyType, final Class<V> valueType, final CacheModifications modifs)
			throws SerializationException { // a ecrire sur le disque
		super(keyType, valueType, index, modifs.getEndOfFile(), modifs);
		nodeInit(keyPosition, valuePosition, modifs);
	}

	@Override
	AbstractNode<K, V> addAndBalance(final K key, final long keyPosition, final Long valuePosition,
			final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		if (this.isFake()) {
			nodeInit(keyPosition, valuePosition, modifs);
			return this;
		}
		if (areEquals(key, getKey(modifs)))
			keysAreEquals(valuePosition, modifs);
		else if (k1IsLessThanK2(key, getKey(modifs))) {
			final AbstractNode<K, V> l = getLeft(modifs);
			if (l == null)
				setLeft(newNode(keyPosition, valuePosition, index, modifs), modifs);
			else
				setLeft((Node1D<K, V>) l.addAndBalance(key, keyPosition, valuePosition, modifs), modifs);
		} else { // key is bigger than this.getKey()
			final AbstractNode<K, V> r = getRight(modifs);
			if (r == null)
				setRight(newNode(keyPosition, valuePosition, index, modifs), modifs);
			else
				setRight((Node1D<K, V>) r.addAndBalance(key, keyPosition, valuePosition, modifs), modifs);
		}
		return balance(modifs);
	}

	private boolean areEquals(final K k1, final K k2) {
		return k1 == null && k2 == null || k2 != null && k1 != null && k1.compareTo(k2) == 0;
	}

	private Node1D<K, V> balance(final CacheModifications modifs)
			throws StorageException, IOException, SerializationException {
		updateHeight(modifs);
		final Node1D<K, V> r = getRight(modifs);
		final Node1D<K, V> l = getLeft(modifs);
		if (height(l, modifs) - height(r, modifs) == 2) {
			final Node1D<K, V> ll = l.getLeft(modifs);
			final Node1D<K, V> lr = l.getRight(modifs);
			if (height(ll, modifs) < height(lr, modifs))
				setLeft(l.leftRotation(modifs), modifs);
			return rightRotation(modifs);
		}
		if (height(l, modifs) - height(r, modifs) == -2) {
			final Node1D<K, V> rr = r.getRight(modifs);
			final Node1D<K, V> rl = r.getLeft(modifs);
			if (height(rr, modifs) < height(rl, modifs))
				setRight(r.rightRotation(modifs), modifs);
			return leftRotation(modifs);
		}
		return this;
	}

	public int compareTo(final Node1D<K, V> o, final CacheModifications modifs) {
		try {
			return getKey(modifs).compareTo(o.getKey(modifs));
		} catch (final IOException e) {
			throw new IOError(IO_PROBLEM, e);
		} catch (StorageException | SerializationException e) {
			throw new StorageRuntimeException(e);
		}
	}

	protected boolean containKey(final K key, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		return findNode(key, modifs) != null;
	}

	@Override
	AbstractNode<K, V> deleteAndBalance(final K key, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		if (this.isFake())
			return this;
		final AbstractNode<K, V> r = getRight(modifs);
		final AbstractNode<K, V> l = getLeft(modifs);
		if (areEquals(key, getKey(modifs)))
			return deleteNode(modifs);
		else if (k1IsLessThanK2(key, getKey(modifs))) {
			if (l == null)
				return this; // nothing to delete
			setLeft((Node1D<K, V>) l.deleteAndBalance(key, modifs), modifs);
		} else { // key is bigger than this.getClef()
			if (r == null)
				return this;
			setRight((Node1D<K, V>) r.deleteAndBalance(key, modifs), modifs);
		}
		return balance(modifs);
	}

	private Node1D<K, V> deleteLeft(final CacheModifications modifs)
			throws StorageException, IOException, SerializationException {
		final Node1D<K, V> g = getLeft(modifs);
		if (g == null)
			return getRight(modifs);
		setLeft(g.deleteLeft(modifs), modifs);
		return balance(modifs);
	}

	private AbstractNode<K, V> deleteNode(final CacheModifications modifs)
			throws StorageException, IOException, SerializationException {
		final Node1D<K, V> r = getRight(modifs);
		final Node1D<K, V> l = getLeft(modifs);
		if (r == null && l == null)
			return null;
		if (r == null)
			return getLeft(modifs);
		if (l == null)
			return getRight(modifs);
		Node1D<K, V> left = r;
		while (left.getLeft(modifs) != null)
			left = left.getLeft(modifs);
		left.setRight(r.deleteLeft(modifs), modifs);
		left.setLeft(l, modifs);
		return left.balance(modifs);
	}

	public boolean equals(final Object obj, final CacheModifications modifs) {
		try {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final Node1D<?, ?> other = (Node1D<?, ?>) obj;
			if (getKey(modifs) == null) {
				if (other.getKey(modifs) != null)
					return false;
			} else if (!getKey(modifs).equals(other.getKey(modifs)))
				return false;
			return true;
		} catch (final IOException e) {
			throw new IOError(IO_PROBLEM, e);
		} catch (StorageException | SerializationException e) {
			throw new StorageRuntimeException(e);
		}
	}

	@Override
	protected AbstractNode<K, V> findNode(final K key, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		if (this.isFake())
			return null;
		if (areEquals(key, getKey(modifs)))
			return this;
		if (k1IsLessThanK2(key, getKey(modifs)))
			return getLeft(modifs) == null ? null : getLeft(modifs).findNode(key, modifs);
		return getRight(modifs) == null ? null : getRight(modifs).findNode(key, modifs);
	}

	@SuppressWarnings("unchecked")
	protected Node1D<K, V> getLeft(final CacheModifications modifs)
			throws StorageException, IOException, SerializationException {
		return index.getStuff(leftPosition(modifs), this.getClass(), modifs);
	}

	@SuppressWarnings("unchecked")
	protected Node1D<K, V> getRight(final CacheModifications modifs)
			throws StorageException, IOException, SerializationException {
		return index.getStuff(rightPosition(modifs), this.getClass(), modifs);
	}

	protected int height(final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(heightPosition(), Integer.class, modifs);
	}

	protected long heightPosition() {
		return valuePositionPosition() + Long.SIZE / 8;
	}

	private boolean isFake() {
		return isFake;
	}

	@Override
	public Iterator<V> iterator() {
		return new ValueNodeIterator(null, null);
	}

	public Iterator<V> iterator(final K min, final K max) {
		return new ValueNodeIterator(min, max);
	}

	private boolean k1IsLessThanK2(final K k1, final K k2) {
		return k2 == null || k1 != null && k2 != null && k2.compareTo(k1) > 0;
	}

	public Iterator<K> keyIterator() {
		return new KeyNodeIterator(this);
	}

	protected abstract void keysAreEquals(Long valuePosition, CacheModifications modifs) throws StorageException;

	protected long leftPosition(final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		return index.getStuff(leftPositionPosition(), Long.class, modifs);
	}

	private long leftPositionPosition() {
		return heightPosition() + Integer.SIZE / 8;
	}

	private Node1D<K, V> leftRotation(final CacheModifications modifs)
			throws StorageException, IOException, SerializationException {
		final Node1D<K, V> r = this.getRight(modifs);
		final Node1D<K, V> rl = r.getLeft(modifs);
		setRight(rl, modifs);
		updateHeight(modifs);
		r.setLeft(this, modifs);
		r.updateHeight(modifs);
		return r;
	}

	protected abstract Node1D<K, V> newNode(long keyPosition, Long valuePosition, AbstractIndex<?, ?, ?> index,
			CacheModifications modifs) throws IOException, StorageException, SerializationException;

	private void nodeInit(final long keyPosition, final Long valuePosition, final CacheModifications modifs)
			throws SerializationException {
		isFake = false;
		super.init(keyPosition, valuePosition, modifs);
		index.writeFakeAndCache(0, modifs); // height
		index.writeFakeAndCache(NULL, modifs);// leftPosition
		index.writeFakeAndCache(NULL, modifs);// rightPosition
	}

	protected long rightPosition(final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		return index.getStuff(rightPositionPosition(), Long.class, modifs);
	}

	private long rightPositionPosition() {
		return leftPositionPosition() + Long.SIZE / 8;
	}

	private Node1D<K, V> rightRotation(final CacheModifications modifs)
			throws StorageException, IOException, SerializationException {
		final Node1D<K, V> l = this.getLeft(modifs);
		final Node1D<K, V> lr = l.getRight(modifs);
		setLeft(lr, modifs);
		updateHeight(modifs);
		l.setRight(this, modifs);
		l.updateHeight(modifs);
		return l;
	}

	protected void setHeight(final int height, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		if (height(modifs) == height)
			return; // nothing to do
		modifs.add(heightPosition(), height);
	}

	protected void setLeft(final Node1D<K, V> gauche, final CacheModifications modifs) {
		setLeftPosition(gauche == null ? NULL : gauche.getPosition(), modifs);
	}

	protected void setLeftPosition(final long leftPosition, final CacheModifications modifs) {
		modifs.add(leftPositionPosition(), leftPosition);
	}

	protected void setRight(final Node1D<K, V> droit, final CacheModifications modifs) {
		setRightPosition(droit == null ? NULL : droit.nodePosition, modifs);
	}

	protected void setRightPosition(final long rightPosition, final CacheModifications modifs) {
		modifs.add(rightPositionPosition(), rightPosition);
	}

	protected void setValuePosition(final Long valuePosition, final CacheModifications modifs) {
		modifs.add(valuePositionPosition(), valuePosition);
	}

	private void updateHeight(final CacheModifications modifs)
			throws StorageException, IOException, SerializationException {
		setHeight(Math.max(height(getRight(modifs), modifs), height(getLeft(modifs), modifs)) + 1, modifs);
	}

}
