package com.chronosave.index.storage.file;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.chronosave.index.storage.exception.IOError;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StorageRuntimeException;

/**
 * un noeudXY ecrit sur disque donne positionClef -> long positionValeur -> long
 * x -> double y -> double w -> double h -> double positionNoeudParent -> long
 * typeNoeud -> byte positionNW -> long positionNE -> long positionSW -> long
 * positionSE -> long
 */
public class XYNode<K extends List<Double>> extends AbstractNode<K, SingletonNode<String>> {

	protected class BoxIterator implements Iterator<SingletonNode<String>> {
		private Iterator<SingletonNode<String>> currentUnderIterator;
		private boolean dejaVu = false;
		private SingletonNode<String> next;
		private final Deque<XYNode<K>> toScout = new ArrayDeque<>();
		private final double xmax;
		private final double xmin;
		private final double ymax;
		private final double ymin;

		protected BoxIterator(final double xmin, final double ymin, final double xmax, final double ymax) throws IOException, StorageException, SerializationException {
			this.next = null;
			this.xmin = xmin;
			this.ymin = ymin;
			this.xmax = xmax;
			this.ymax = ymax;
			if (getNodeType(null) == POINTER) {
				if (getNE(null).intersects(xmin, ymin, xmax, ymax, null))
					toScout.push(getNE(null));
				if (getNW(null).intersects(xmin, ymin, xmax, ymax, null))
					toScout.push(getNW(null));
				if (getSE(null).intersects(xmin, ymin, xmax, ymax, null))
					toScout.push(getSE(null));
				if (getSW(null).intersects(xmin, ymin, xmax, ymax, null))
					toScout.push(getSW(null));
			}
			cacheNext();
		}

		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public SingletonNode<String> next() {
			if (!hasNext())
				throw new NoSuchElementException();
			try {
				final SingletonNode<String> ret = next;
				cacheNext();
				return ret;
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

		private void cacheNext() throws IOException, StorageException, SerializationException {
			if (getNodeType(null) == EMPTY)
				next = null;
			else if (getNodeType(null) == LEAF)
				cacheNextNodeLeaf();
			else if (getNodeType(null) == POINTER)
				cacheNextPointer();
		}

		private void cacheNextNodeLeaf() throws StorageException, IOException, SerializationException {
			if (!dejaVu) {
				next = isPositionInBox(xmin, ymin, xmax, ymax, null) ? getValue(null) : null;
				dejaVu = true;
			} else {
				next = null;
			}
		}

		private void cacheNextPointer() throws IOException, StorageException, SerializationException {
			while (next == null && stillNodeToScout()) {
				if (currentUnderIterator == null)
					currentUnderIterator = toScout.pop().inTheBox(xmin, ymin, xmax, ymax);
				if (currentUnderIterator.hasNext())
					next = currentUnderIterator.next();
				else
					currentUnderIterator = null;
			}
		}

		private boolean isPositionInBox(final double left, final double bottom, final double right, final double top, final CacheModifications modifs) throws StorageException, IOException, SerializationException {
			if (getNodeType(modifs) != LEAF)
				throw new StorageException("this method must not be called since we are in a Leaf !");
			final List<Double> xy = getKey(modifs);
			final double x = xy.get(0);
			final double y = xy.get(1);
			return x >= left && x <= right && y >= bottom && y <= top;
		}

		private boolean stillNodeToScout() {
			return !toScout.isEmpty() || currentUnderIterator != null;
		}
	}

	private static final byte EMPTY = 0;
	private static final byte LEAF = 1;
	private static final long NULL = -1L;
	private static final byte POINTER = 2;

	/**
	 * root creation
	 * 
	 * @param index
	 * @param clefTYpe
	 * @param x
	 * @param y
	 * @param hw
	 * @param hh
	 * @param modifs
	 * @throws SerializationException
	 */
	@SuppressWarnings("unchecked")
	protected XYNode(final AbstractIndex<?, ?, ?> index, final Class<K> keyType, final double x, final double y, final double hw, final double hh, final CacheModifications modifs) throws SerializationException {
		super(keyType, (Class<SingletonNode<String>>) (Object) SingletonNode.class, index, index.getEndOfFile());
		init(x, y, hw, hh, null, modifs);
	}

	/**
	 * runtime
	 * 
	 * @param index
	 * @param x
	 * @param y
	 * @param hw
	 * @param hh
	 * @param daddy
	 * @param modifs
	 * @throws SerializationException
	 */
	protected XYNode(final AbstractIndex<?, ?, ?> index, final double x, final double y, final double hw, final double hh, final XYNode<K> daddy, final CacheModifications modifs) throws SerializationException {
		super(daddy.keyType, daddy.valueType, index, index.getEndOfFile());
		init(x, y, hw, hh, daddy, modifs);
	}

	/**
	 * file
	 * 
	 * @param position
	 * @param index
	 * @param keyType
	 * @param valueType
	 * @param lireValeur
	 * @throws IOException
	 * @throws StorageException
	 * @throws SerializationException
	 */
	@SuppressWarnings("unchecked")
	protected XYNode(final long position, final AbstractIndex<?, ?, ?> index, final Class<K> keyType) {
		super(keyType, (Class<SingletonNode<String>>) (Object) SingletonNode.class, index, position);
	}

	private boolean contains(final K key, final CacheModifications modifs) throws IOException, SerializationException {
		return intersects(key.get(0), key.get(1), key.get(0), key.get(1), modifs);
	}

	private Double getH(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(hPosition(), Double.class, modifs);
	}

	@SuppressWarnings("unchecked")
	private XYNode<K> getNE(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(nePosition(modifs), XYNode.class, modifs);
	}

	private byte getNodeType(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(noeudTypePosition(), Byte.class, modifs);
	}

	@SuppressWarnings("unchecked")
	private XYNode<K> getNW(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(nwPosition(modifs), XYNode.class, modifs);
	}

	@SuppressWarnings("unchecked")
	private XYNode<K> getOptParent(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(optParentPosition(modifs), XYNode.class, modifs);
	}

	private AbstractNode<K, SingletonNode<String>> getQuandrantForPoint(final K key, final CacheModifications modifs) throws IOException, SerializationException {
		final double mx = getX(modifs) + getW(modifs) / 2;
		final double my = getY(modifs) + getH(modifs) / 2;
		if (key.get(0) < mx)
			return key.get(1) < my ? getNW(modifs) : getSW(modifs);
		return key.get(1) < my ? getNE(modifs) : getSE(modifs);
	}

	@SuppressWarnings("unchecked")
	private XYNode<K> getSE(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(sePosition(modifs), XYNode.class, modifs);
	}

	@SuppressWarnings("unchecked")
	private XYNode<K> getSW(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(swPosition(modifs), XYNode.class, modifs);
	}

	private Double getW(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(wPosition(), Double.class, modifs);
	}

	private Double getX(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(xPosition(), Double.class, modifs);
	}

	private Double getY(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(yPosition(), Double.class, modifs);
	}

	private long hPosition() {
		return wPosition() + Double.BYTES;
	}

	private void init(final double x, final double y, final double hw, final double hh, final XYNode<K> daddy, final CacheModifications modifs) throws SerializationException {
		super.init(NULL, NULL, modifs);
		index.writeFakeAndCache(x, modifs);
		index.writeFakeAndCache(y, modifs);
		index.writeFakeAndCache(hw, modifs);
		index.writeFakeAndCache(hh, modifs);
		index.writeFakeAndCache(daddy == null ? NULL : daddy.getPosition(), modifs);// dad node
		index.writeFakeAndCache(EMPTY, modifs);// node type
		index.writeFakeAndCache(NULL, modifs);// NW
		index.writeFakeAndCache(NULL, modifs);// NE
		index.writeFakeAndCache(NULL, modifs);// SW
		index.writeFakeAndCache(NULL, modifs);// SE
	}

	private boolean intersects(final double left, final double bottom, final double right, final double top, final CacheModifications modifs) throws IOException, SerializationException {
		return !(getX(modifs) > right || getX(modifs) + getW(modifs) < left || getY(modifs) > bottom || getY(modifs) + getH(modifs) < top);
	}

	private boolean isRoot(final CacheModifications modifs) throws IOException, SerializationException {
		return getOptParent(modifs) == null;
	}

	private long nePosition(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(nePositionPosition(), Long.class, modifs);
	}

	private long nePositionPosition() {
		return nwPositionPosition() + Long.BYTES;
	}

	private XYNode<K> newXYRootNode(final K key, final CacheModifications modifs) throws SerializationException, IOException {
		final double x0 = key.get(0);
		final double y0 = key.get(1);
		final double x = x0 < getX(modifs) ? getX(modifs) - getW(modifs) : getX(modifs);
		final double y = y0 < getY(modifs) ? getY(modifs) - getH(modifs) : getY(modifs);
		final double hw = 2 * getW(modifs);
		final double hh = 2 * getH(modifs);
		final boolean east = x < getX(modifs);
		final boolean west = !east;
		final boolean north = y < getY(modifs);
		final boolean south = !north;
		final XYNode<K> root = new XYNode<>(index, keyType, x, y, hw, hh, modifs);
		this.setOptParent(root, modifs);
		root.setNodeType(POINTER, modifs);
		XYNode<K> nw;
		XYNode<K> ne;
		XYNode<K> sw;
		XYNode<K> se;
		if (north && east) {
			ne = this;
			nw = new XYNode<>(index, getX(modifs) - getW(modifs), getY(modifs), getW(modifs), getH(modifs), root, modifs);
			sw = new XYNode<>(index, getX(modifs) - getW(modifs), getY(modifs) - getH(modifs), getW(modifs), getH(modifs), root, modifs);
			se = new XYNode<>(index, getX(modifs), getY(modifs) - getH(modifs), getW(modifs), getH(modifs), root, modifs);
		} else if (north && west) {
			ne = new XYNode<>(index, getX(modifs) + getW(modifs), getY(modifs), getW(modifs), getH(modifs), root, modifs);
			nw = this;
			sw = new XYNode<>(index, getX(modifs), getY(modifs) - getH(modifs), getW(modifs), getH(modifs), root, modifs);
			se = new XYNode<>(index, getX(modifs) + getW(modifs), getY(modifs) - getH(modifs), getW(modifs), getH(modifs), root, modifs);
		} else if (south && east) {
			ne = new XYNode<>(index, getX(modifs), getY(modifs) + getH(modifs), getW(modifs), getH(modifs), root, modifs);
			nw = new XYNode<>(index, getX(modifs) - getW(modifs), getY(modifs) + getH(modifs), getW(modifs), getH(modifs), root, modifs);
			sw = new XYNode<>(index, getX(modifs) - getW(modifs), getY(modifs), getW(modifs), getH(modifs), root, modifs);
			se = this;
		} else {// SW
			ne = new XYNode<>(index, getX(modifs) + getH(modifs), getY(modifs) + getH(modifs), getW(modifs), getH(modifs), root, modifs);
			nw = new XYNode<>(index, getX(modifs), getY(modifs) + getH(modifs), getW(modifs), getH(modifs), root, modifs);
			sw = this;
			se = new XYNode<>(index, getX(modifs) + getH(modifs), getY(modifs), getW(modifs), getH(modifs), root, modifs);
		}

		root.setNe(ne, modifs);
		root.setNw(nw, modifs);
		root.setSe(se, modifs);
		root.setSw(sw, modifs);
		return root;
	}

	private long noeudTypePosition() {
		return optParentPositionPosition() + Long.BYTES;
	}

	private long nwPosition(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(nwPositionPosition(), Long.class, modifs);
	}

	private long nwPositionPosition() {
		return noeudTypePosition() + Byte.BYTES;
	}

	// variable
	private long optParentPosition(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(optParentPositionPosition(), Long.class, modifs);
	}

	private long optParentPositionPosition() {
		return hPosition() + Double.BYTES;
	}

	private long sePosition(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(sePositionPosition(), Long.class, modifs);
	}

	private long sePositionPosition() {
		return swPositionPosition() + Long.BYTES;
	}

	private void setKeyPosition(final long keyPosition, final CacheModifications modifs) {
		modifs.add(keyPositionPosition(), keyPosition);
	}

	private void setNe(final XYNode<K> node, final CacheModifications modifs) throws IOException, SerializationException {
		setNEPosition(node.getPosition(), modifs);
	}

	private void setNEPosition(final long position, final CacheModifications modifs) throws IOException, SerializationException {
		modifs.add(nePosition(modifs), position);
	}

	private void setNodeType(final Byte nodeType, final CacheModifications modifs) {
		modifs.add(noeudTypePosition(), nodeType);
	}

	private void setNw(final XYNode<K> node, final CacheModifications modifs) throws IOException, SerializationException {
		setNWPosition(node.getPosition(), modifs);
	}

	private void setNWPosition(final long position, final CacheModifications modifs) throws IOException, SerializationException {
		modifs.add(nwPosition(modifs), position);
	}

	private void setOptParent(final XYNode<K> daddy, final CacheModifications modifs) {
		setOptParentPosition(daddy.getPosition(), modifs);
	}

	private void setOptParentPosition(final long optParentPosition, final CacheModifications modifs) {
		modifs.add(optParentPositionPosition(), optParentPosition);
	}

	private void setSe(final XYNode<K> node, final CacheModifications modifs) throws IOException, SerializationException {
		setSEPosition(node.getPosition(), modifs);
	}

	private void setSEPosition(final long position, final CacheModifications modifs) throws IOException, SerializationException {
		modifs.add(sePosition(modifs), position);
	}

	private void setSw(final XYNode<K> node, final CacheModifications modifs) throws IOException, SerializationException {
		setSWPosition(node.getPosition(), modifs);
	}

	private void setSWPosition(final long position, final CacheModifications modifs) throws IOException, SerializationException {
		modifs.add(swPosition(modifs), position);
	}

	private void setValuePosition(final Long valuePosition, final CacheModifications modifs) {
		modifs.add(valuePositionPosition(), valuePosition);
	}

	private void split(final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		final double x = getX(modifs);
		final double y = getY(modifs);
		final double hw = getW(modifs) / 2;
		final double hh = getH(modifs) / 2;

		setNw(new XYNode<>(index, x, y, hw, hh, this, modifs), modifs);
		setNe(new XYNode<>(index, x + hw, y, hw, hh, this, modifs), modifs);
		setSw(new XYNode<>(index, x, y + hh, hw, hh, this, modifs), modifs);
		setSe(new XYNode<>(index, x + hw, y + hh, hw, hh, this, modifs), modifs);

		setNodeType(POINTER, modifs);
		addAndBalance(getKey(modifs), keyPosition(modifs), valuePosition(modifs), modifs);
	}

	private long swPosition(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(sePositionPosition(), Long.class, modifs);
	}

	private long swPositionPosition() {
		return nePositionPosition() + Long.BYTES;
	}

	private long wPosition() {
		return yPosition() + Double.BYTES;
	}

	// fixe
	private long xPosition() {
		return valuePositionPosition() + Long.BYTES;
	}

	private long yPosition() {
		return xPosition() + Double.BYTES;
	}

	protected boolean deleteId(final String id, final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		if (getValue(modifs) == null)
			return true;
		boolean isEmpty = false;
		final AbstractNode<String, String> newNode = getValue(modifs).deleteAndBalance(id, modifs);
		if (newNode == null)
			isEmpty = true;
		else
			setValuePosition(newNode.valuePosition(modifs), modifs);
		return isEmpty;
	}

	protected void insertValue(final String id, final long positionId, final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		AbstractNode<String, String> root = getValue(modifs);
		if (root == null)
			root = new SingletonNode<>(positionId, index, String.class, modifs);
		else
			root = root.addAndBalance(id, positionId, null, modifs);
		setValuePosition(root.getPosition(), modifs);
	}

	protected Iterator<SingletonNode<String>> inTheBox(final double xmin, final double ymin, final double xmax, final double ymax) throws IOException, StorageException, SerializationException {
		return new BoxIterator(xmin, ymin, xmax, ymax);
	}

	@Override
	AbstractNode<K, SingletonNode<String>> addAndBalance(final K key, final long keyPosition, final Long valuePosition, final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		if (isRoot(modifs) && !contains(key, modifs)) {
			final XYNode<K> root = newXYRootNode(key, modifs);
			return root.addAndBalance(key, keyPosition, valuePosition, modifs);
		}

		final byte nodeType = getNodeType(modifs);
		if (nodeType == EMPTY) {
			setKeyPosition(keyPosition, modifs);
			setValuePosition(valuePosition, modifs);
			setNodeType(LEAF, modifs);
		} else if (nodeType == LEAF) {
			final K k = getKey(modifs);
			if (k.get(0).equals(key.get(0)) && k.get(1).equals(key.get(1)))
				throw new StorageException("bug : not possible to insert an object of same key on an NodeXY");
			else {
				split(modifs);
				addAndBalance(key, keyPosition, valuePosition, modifs);
			}
		} else if (nodeType == POINTER) {
			getQuandrantForPoint(key, modifs).addAndBalance(key, keyPosition, valuePosition, modifs);
		} else {
			throw new StorageException("Invalid nodeType in parent");
		}
		return this;
	}

	@Override
	AbstractNode<K, SingletonNode<String>> deleteAndBalance(final K clef, final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		// useless
		return null;
	}

	@Override
	AbstractNode<K, SingletonNode<String>> findNode(final K key, final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		AbstractNode<K, SingletonNode<String>> ret = null;
		final byte nodeType = getNodeType(modifs);
		if (nodeType == LEAF) {
			final Double x = key.get(0);
			final Double y = key.get(1);
			ret = getKey(modifs).get(0) == x && getKey(modifs).get(1) == y ? this : null;
		} else if (nodeType == POINTER) {
			ret = getQuandrantForPoint(key, modifs).findNode(key, modifs);
		} else if (nodeType != EMPTY) {
			throw new StorageException("Invalid nodeType");
		}
		return ret;
	}

	@Override
	long keyPosition(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(keyPositionPosition(), Long.class, modifs);
	}

	@Override
	long valuePosition(final CacheModifications modifs) throws IOException, SerializationException {
		return index.getStuff(valuePositionPosition(), Long.class, modifs);
	}

}
