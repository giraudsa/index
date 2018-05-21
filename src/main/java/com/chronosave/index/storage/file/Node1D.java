package com.chronosave.index.storage.file;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StorageRuntimeException;
import com.chronosave.index.storage.exception.IOError;
import com.chronosave.index.storage.exception.SerializationException;

public abstract class Node1D<K extends Comparable<K>, V> extends AbstractNode<K, V> implements Iterable<V> {

	private static final long NULL = -1L;
	private static final String IO_PROBLEM = "IO problem !";
	
	private static final <KS extends Comparable<KS>, VS > int height(Node1D<KS, VS> node, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return node != null ? node.height(modifs) : (int)NULL;
	}
	
	private boolean isFake = false;
	
	protected long heightPosition() {return valuePositionPosition() + Long.SIZE / 8;}
	private long leftPositionPosition() { return heightPosition() + Integer.SIZE / 8;}
	private long rightPositionPosition() { return leftPositionPosition() + Long.SIZE / 8;}
	
	protected int height(CacheModifications modifs) throws IOException, StorageException, SerializationException { 
		return index.getStuff(heightPosition(), Integer.class, modifs);
	}
	protected long leftPosition(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(leftPositionPosition(), Long.class, modifs);
	}
	protected long rightPosition(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(rightPositionPosition(), Long.class, modifs);
	}

	//fake noeud
	protected Node1D(Class<K> keyType, Class<V> valueType, AbstractIndex<?, ?, ?> index, CacheModifications modifs){
		super(keyType, valueType, index, modifs.getEndOfFile());
		isFake = true;
	}
	
	/**
	 * from file
	 * @param position
	 * @param index
	 * @param keyType
	 * @param valueType
	 */
	protected Node1D(long position, AbstractIndex<?,?,?> index, Class<K> keyType, Class<V> valueType){ 
		super(keyType, valueType, index, position);
	}


	/**
	 * runtime
	 * subtilité, si positionValeur est null, on ne l'écrit pas 
	 * @param keyPosition
	 * @param valuePosition
	 * @param index
	 * @param modifs
	 * @param keyType
	 * @param valueType
	 * @throws SerializationException
	 */
	protected Node1D(long keyPosition, Long valuePosition, AbstractIndex<?,?,?> index, Class<K> keyType, Class<V> valueType, CacheModifications modifs) throws SerializationException { //a ecrire sur le disque 
		super(keyType, valueType, index, modifs.getEndOfFile(), modifs);
		nodeInit(keyPosition, valuePosition, modifs);
	}

	private void nodeInit(long keyPosition, Long valuePosition, CacheModifications modifs) throws SerializationException {
		isFake = false;
		super.init(keyPosition, valuePosition, modifs);
		index.writeFakeAndCache(0, modifs); //height
		index.writeFakeAndCache(NULL, modifs);//leftPosition
		index.writeFakeAndCache(NULL, modifs);//rightPosition
	}

	protected void setHeight(int height, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		if(height(modifs) == height) return; //nothing to do
		modifs.add(heightPosition(), height);
	}


	protected void setLeftPosition(long leftPosition, CacheModifications modifs) {
		modifs.add(leftPositionPosition(), leftPosition);
	}


	protected void setRightPosition(long rightPosition, CacheModifications modifs) {
		modifs.add(rightPositionPosition(), rightPosition);
	}

	protected void setValuePosition(Long valuePosition, CacheModifications modifs) {
		modifs.add(valuePositionPosition(), valuePosition);
	}

	@SuppressWarnings("unchecked")
	protected Node1D<K, V> getRight(CacheModifications modifs) throws StorageException, IOException, SerializationException {
		return (Node1D<K, V>)index.getStuff(rightPosition(modifs), this.getClass(), modifs);
	}

	protected void setRight(Node1D<K, V> droit, CacheModifications modifs) {
		setRightPosition(droit == null ? NULL : droit.nodePosition, modifs);
	}

	@SuppressWarnings("unchecked")
	protected Node1D<K, V> getLeft(CacheModifications modifs) throws  StorageException, IOException, SerializationException {
		return (Node1D<K, V>)index.getStuff(leftPosition(modifs), this.getClass(), modifs);
	}

	protected void setLeft(Node1D<K, V> gauche, CacheModifications modifs) {
		setLeftPosition(gauche == null ? NULL : gauche.getPosition(), modifs);
	}

	@Override
	protected AbstractNode<K, V> findNode(K key, CacheModifications modifs) throws IOException, StorageException, SerializationException{
		if(this.isFake()) return null;
		if(areEquals(key, getKey(modifs))) return this;
		if(k1IsLessThanK2(key, getKey(modifs))) return getLeft(modifs) == null ? null : getLeft(modifs).findNode(key, modifs);
		return getRight(modifs) == null ? null : getRight(modifs).findNode(key, modifs);
	}
	
	protected boolean containKey(K key, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return findNode(key, modifs) != null;
	}

	private Node1D<K, V> balance(CacheModifications modifs) throws StorageException, IOException, SerializationException{
		updateHeight(modifs);
		Node1D<K, V> r = getRight(modifs);
		Node1D<K, V> l = getLeft(modifs);
		if(height(l, modifs) - height(r, modifs) == 2){
			Node1D<K,V> ll = l.getLeft(modifs);
			Node1D<K, V> lr = l.getRight(modifs);
			if(height(ll, modifs) < height(lr, modifs)) setLeft(l.leftRotation(modifs), modifs);
			return rightRotation(modifs);
		}
		if(height(l, modifs) - height(r, modifs) == -2){
			Node1D<K, V> rr = r.getRight(modifs);
			Node1D<K, V> rl = r.getLeft(modifs);
			if(height(rr, modifs) < height(rl, modifs)) setRight(r.rightRotation(modifs), modifs);
			return leftRotation(modifs);
		}
		return this;
	}

	private void updateHeight(CacheModifications modifs) throws StorageException, IOException, SerializationException {
		setHeight(Math.max(height(getRight(modifs), modifs), height(getLeft(modifs), modifs)) + 1, modifs);
	}

	private Node1D<K, V> rightRotation(CacheModifications modifs) throws StorageException, IOException, SerializationException {
		Node1D<K, V> l = this.getLeft(modifs);
		Node1D<K, V> lr = l.getRight(modifs);
		setLeft(lr, modifs);
		updateHeight(modifs);
		l.setRight(this, modifs);
		l.updateHeight(modifs);
		return l;
	}

	private Node1D<K, V> leftRotation(CacheModifications modifs) throws StorageException, IOException, SerializationException {
		Node1D<K, V> r = this.getRight(modifs);
		Node1D<K, V> rl = r.getLeft(modifs);
		setRight(rl, modifs);
		updateHeight(modifs);
		r.setLeft(this, modifs);
		r.updateHeight(modifs);
		return r;
	}

	@Override
	AbstractNode<K, V> addAndBalance(K key, long keyPosition, Long valuePosition, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		if(this.isFake()) {
			nodeInit(keyPosition, valuePosition, modifs);
			return this;
		}
		if(areEquals(key, getKey(modifs))) {
			keysAreEquals(valuePosition, modifs);
		}else if(k1IsLessThanK2(key, this.getKey(modifs))){
			AbstractNode<K, V> l = getLeft(modifs);
			if(l == null) setLeft(newNode(keyPosition, valuePosition, index, modifs), modifs);
			else setLeft((Node1D<K, V>)l.addAndBalance(key, keyPosition, valuePosition, modifs), modifs);
		}else { //key is bigger than this.getKey()
			AbstractNode<K,V> r = getRight(modifs);
			if(r == null) setRight(newNode(keyPosition, valuePosition, index, modifs), modifs);
			else setRight((Node1D<K, V>)r.addAndBalance(key, keyPosition, valuePosition, modifs), modifs);
		}
		return balance(modifs);
	}
	
	private boolean isFake() {
		return isFake;
	}

	@Override
	 AbstractNode<K, V> deleteAndBalance(K key, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		if(this.isFake()) {
			return this;
		}
		AbstractNode<K,V> r = getRight(modifs);
		AbstractNode<K, V> l = getLeft(modifs);
		if(areEquals(key, getKey(modifs))) {
			return deleteNode(modifs);
		}else if(k1IsLessThanK2(key, getKey(modifs))){
			if(l == null) return this; //nothing to delete
			setLeft((Node1D<K, V>)l.deleteAndBalance(key, modifs), modifs);
		}else { //key is bigger than this.getClef()
			if(r == null) return this;
			setRight((Node1D<K, V>)r.deleteAndBalance(key, modifs), modifs);
		}
		return balance(modifs);
	}
	
	private AbstractNode<K, V> deleteNode(CacheModifications modifs)
			throws StorageException, IOException, SerializationException {
		Node1D<K,V> r = getRight(modifs);
		Node1D<K, V> l = getLeft(modifs);
		if(r == null && l == null) return null;
		if(r == null) return getLeft(modifs);
		if(l == null) return getRight(modifs);
		Node1D<K, V> left = r;
		while(left.getLeft(modifs) != null)
			left = left.getLeft(modifs);
		left.setRight(r.deleteLeft(modifs), modifs);
		left.setLeft(l, modifs);
		return left.balance(modifs);
	}
	
	private Node1D<K, V> deleteLeft(CacheModifications modifs) throws StorageException, IOException, SerializationException {
		Node1D<K, V> g = getLeft(modifs);
		if(g == null) 
			return getRight(modifs);
		setLeft(g.deleteLeft(modifs), modifs);
		return balance(modifs);
	}
	protected abstract void keysAreEquals(Long valuePosition, CacheModifications modifs) throws StorageException;

	private boolean areEquals(K k1, K k2) {
		return k1 == null && k2 == null || (k2 != null && k1 != null && k1.compareTo(k2) == 0);
	}
	
	private boolean k1IsLessThanK2(K k1, K k2) {
		return k2 == null || (k1 != null && k2 != null && k2.compareTo(k1) > 0);
	}

	protected abstract Node1D<K,V> newNode(long keyPosition, Long valuePosition, AbstractIndex<?,?,?> index, CacheModifications modifs) throws IOException, StorageException, SerializationException;

	
	public int compareTo(Node1D<K, V> o, CacheModifications modifs){
		try {
			return this.getKey(modifs).compareTo(o.getKey(modifs));
		} catch (IOException e) {
			throw new IOError(IO_PROBLEM, e);
		} catch (StorageException  | SerializationException e) {
			throw new StorageRuntimeException(e);
		}
	}

	
	public boolean equals(Object obj, CacheModifications modifs) {
		try{
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Node1D<?,?> other = (Node1D<?,?>) obj;
			if (getKey(modifs) == null) {
				if (other.getKey(modifs) != null)
					return false;
			} else if (!getKey(modifs).equals(other.getKey(modifs)))
				return false;
			return true;
		} catch (IOException e){
			throw new IOError(IO_PROBLEM, e);
		} catch (StorageException  | SerializationException e) {
			throw new StorageRuntimeException(e);
		}
	}
	
	@Override
	public Iterator<V> iterator() {
		return new ValueNodeIterator(null, null);
	}
	
	public Iterator<V> iterator(K min, K max) {
		return new ValueNodeIterator(min, max);
	}
	
	public Iterator<K> keyIterator(){
		return new KeyNodeIterator(this);
	}
	
	
	protected class ValueNodeIterator implements Iterator<V>{
		private Node1D<K, V> current;
		private Deque<Node1D<K, V>> nodesStack = new ArrayDeque<>();
		private final K min;
		private final K max;
		private V next;
		private boolean hasNext = false;
		
		protected ValueNodeIterator(K min, K max) {
			current = Node1D.this.isFake() ? null : Node1D.this;
			this.min = min;
			this.max = max;
			cacheNext();
		}
		
		private void cacheNext() {
			try{
				hasNext = false;
				while (!hasNext && !(nodesStack.isEmpty() && current == null)) {
					searchNext();
				}
			}catch (IOException e){
				throw new IOError(e);
			} catch (StorageException  | SerializationException e) {
				throw new StorageRuntimeException(e);
			}
		}

		private void searchNext() throws IOException, StorageException, SerializationException {
			while (current != null) {
				nodesStack.push(current);
				if(min != null && current.getLeft(null) != null) {
					K leftKey = current.getLeft(null).getKey(null);
					current = k1IsLessThanK2(min, leftKey) || areEquals(min, leftKey) ? current.getLeft(null) : null;
				}
				else current = current.getLeft(null);
			}

			current = nodesStack.pop();
			Node1D<K, V> node = current;
			if(max != null) 
				current = k1IsLessThanK2(current.getKey(null), max) || areEquals(current.getKey(null), max) ? current.getRight(null) : null;
			else current = current.getRight(null);
			if(constraintsRespected(node.getKey(null))) {
				hasNext = true;
				next = node.getValue(null);
			}
		}

		private boolean constraintsRespected(K key) {
			if(min == null && max == null) return true;
			if(min == null) return max.compareTo(key) > 0;
			if(max == null) return min.compareTo(key) < 0;
			if(max.compareTo(min) == 0) return key != null && key.compareTo(max) == 0;
			return min.compareTo(key) < 0 && max.compareTo(key) > 0;
		}

		@Override
		public boolean hasNext() {
			return hasNext;
		}

		@Override
		public V next() {
			if(!hasNext())
				throw new NoSuchElementException();
			V ret = next;
			cacheNext();
			return ret;
		}
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	protected class KeyNodeIterator implements Iterator<K>{
		
		

		private Node1D<K, V> current;
		private Deque<Node1D<K, V>> nodesStack = new ArrayDeque<>();
		
		protected KeyNodeIterator(Node1D<K, V> racine){
			current = racine.isFake() ? null : racine;
		}
		
		@Override
		public boolean hasNext() {
			return !nodesStack.isEmpty() || current != null;
		}

		@Override
		public K next() {
			if(!hasNext())
				throw new NoSuchElementException();
			try{
				while (current != null) {
		            nodesStack.push(current);
	            	current = current.getLeft(null);
		        }

				current = nodesStack.pop();
				Node1D<K, V> node = current;
				current = current.getRight(null);

		        return node.getKey(null);
			}catch (IOException e){
				throw new IOError(e);
			} catch (StorageException  | SerializationException e) {
				throw new StorageRuntimeException(e);
			}
		}
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
	
}
