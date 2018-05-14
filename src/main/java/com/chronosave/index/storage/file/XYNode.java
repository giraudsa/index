package com.chronosave.index.storage.file;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StorageRuntimeException;
import com.chronosave.index.storage.exception.IOError;
import com.chronosave.index.storage.exception.SerializationException;

/**
 * un noeudXY ecrit sur disque donne
 * positionClef -> long
 * positionValeur -> long
 * x -> double
 * y -> double
 * w -> double
 * h -> double
 * positionNoeudParent -> long
 * typeNoeud -> byte
 * positionNW -> long
 * positionNE -> long
 * positionSW -> long
 * positionSE -> long
 */
public class XYNode<K extends List<Double>> extends AbstractNode<K, NodeId>{

	private static final byte EMPTY= 0;
	private static final byte LEAF = 1;
	private static final byte POINTER = 2;
	private static final long NULL = -1L;
	//fixe
	private long xPosition() {return valuePositionPosition() + Long.SIZE / 8;}
	private long yPosition() {return xPosition() + Double.SIZE / 8;}
	private long wPosition() {return yPosition() + Double.SIZE / 8;}
	private long hPosition() {return wPosition() + Double.SIZE / 8;}
	private long optParentPositionPosition() {return hPosition() + Double.SIZE / 8;}
	private long noeudTypePosition() {return optParentPositionPosition() + Long.SIZE / 8;}
	private long nwPositionPosition() {return noeudTypePosition() + Byte.SIZE / 8;}
	private long nePositionPosition() {return nwPositionPosition() + Long.SIZE / 8;}
	private long swPositionPosition() {return nePositionPosition() + Long.SIZE / 8;}
	private long sePositionPosition() {return swPositionPosition() + Long.SIZE / 8;}
	//variable
	private long optParentPosition(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(optParentPositionPosition(), Long.class, modifs);
	}
	@Override long keyPosition(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(keyPositionPosition(), Long.class, modifs);
	}
	@Override long valuePosition(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(valuePositionPosition(), Long.class, modifs);
	}
	private long nwPosition(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(nwPositionPosition(), Long.class, modifs);
	}
	private long nePosition(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(nePositionPosition(), Long.class, modifs);
	}
	private long swPosition(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(sePositionPosition(), Long.class, modifs);
	}
	private long sePosition(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(sePositionPosition(), Long.class, modifs);
	}
	private void setKeyPosition(long keyPosition, CacheModifications modifs) {
		modifs.add(keyPositionPosition(), keyPosition);
	}
	private void setValuePosition(Long valuePosition, CacheModifications modifs) {
		modifs.add(valuePositionPosition(), valuePosition);
	}
	private void setSEPosition(long position, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		modifs.add(sePosition(modifs), position);
	}
	private void setSWPosition(long position, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		modifs.add(swPosition(modifs), position);
	}
	private void setNEPosition(long position, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		modifs.add(nePosition(modifs), position);
	}
	private void setNWPosition(long position, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		modifs.add(nwPosition(modifs), position);
	}
	private Double getX(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(xPosition(), Double.class, modifs);
	}
	private Double getY(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(yPosition(), Double.class, modifs);
	}
	private Double getW(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(wPosition(), Double.class, modifs);
	}
	private Double getH(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(hPosition(), Double.class, modifs);
	}
	@SuppressWarnings("unchecked")
	private XYNode<K> getOptParent(CacheModifications modifs) throws IOException, StorageException, SerializationException{
		return index.getStuff(optParentPosition(modifs), XYNode.class, modifs);
	}
	private byte getNodeType(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(noeudTypePosition(), Byte.class, modifs);
	}
	@SuppressWarnings("unchecked")
	private XYNode<K> getNW(CacheModifications modifs) throws IOException, StorageException, SerializationException{
		return index.getStuff(nwPosition(modifs), XYNode.class, modifs);
	}
	@SuppressWarnings("unchecked")
	private XYNode<K> getNE(CacheModifications modifs) throws IOException, StorageException, SerializationException{
		return index.getStuff(nePosition(modifs), XYNode.class, modifs);
	}
	@SuppressWarnings("unchecked")
	private XYNode<K> getSW(CacheModifications modifs) throws IOException, StorageException, SerializationException{
		return index.getStuff(swPosition(modifs), XYNode.class, modifs);
	}
	@SuppressWarnings("unchecked")
	private XYNode<K> getSE(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return index.getStuff(sePosition(modifs), XYNode.class, modifs);
	}
	private void setOptParent(XYNode<K> daddy, CacheModifications modifs) {
		setOptParentPosition(daddy.getPosition(), modifs);
	}
	private void setOptParentPosition(long optParentPosition, CacheModifications modifs) {
		modifs.add(optParentPositionPosition(), optParentPosition);
	}
	private void setNodeType(Byte nodeType, CacheModifications modifs) {
		modifs.add(noeudTypePosition(), nodeType);
	}
	private void setSe(XYNode<K> node, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setSEPosition(node.getPosition(), modifs);
	}
	private void setSw(XYNode<K> node, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setSWPosition(node.getPosition(), modifs);
	}
	private void setNe(XYNode<K> node, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setNEPosition(node.getPosition(), modifs);
	}
	private void setNw(XYNode<K> node, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setNWPosition(node.getPosition(), modifs);
	}
	/**
	 * file
	 * @param position
	 * @param index
	 * @param keyType
	 * @param valueType
	 * @param lireValeur
	 * @throws IOException
	 * @throws StorageException
	 * @throws SerializationException
	 */
	protected XYNode(long position, AbstractIndex<?,?,?> index, Class<K> keyType, Class<NodeId> valueType) {
		super(keyType, NodeId.class, index, position);
	}


	/**
	 * runtime
	 * @param index
	 * @param x
	 * @param y
	 * @param hw
	 * @param hh
	 * @param daddy
	 * @param modifs
	 * @throws SerializationException
	 */
	protected XYNode(AbstractIndex<?, ?, ?> index, double x, double y, double hw, double hh, XYNode<K> daddy, CacheModifications modifs) throws SerializationException {
		super(daddy.keyType, NodeId.class, index, index.getEndOfFile());
		init(x, y, hw, hh, daddy, modifs);
	}
	private void init(double x, double y, double hw, double hh, XYNode<K> daddy, CacheModifications modifs) throws SerializationException {
		super.init(NULL, NULL, modifs);
		index.writeFakeAndCache(x, modifs);
		index.writeFakeAndCache(y, modifs);
		index.writeFakeAndCache(hw, modifs);
		index.writeFakeAndCache(hh, modifs);
		index.writeFakeAndCache(daddy == null ? NULL : daddy.getPosition() , modifs);//dad node
		index.writeFakeAndCache(EMPTY, modifs);//node type
		index.writeFakeAndCache(NULL, modifs);//NW
		index.writeFakeAndCache(NULL, modifs);//NE
		index.writeFakeAndCache(NULL, modifs);//SW
		index.writeFakeAndCache(NULL, modifs);//SE
	}

	/**
	 *  runtime
	 * @param index
	 * @param clefTYpe
	 * @param x
	 * @param y
	 * @param hw
	 * @param hh
	 * @param modifs
	 * @throws SerializationException
	 */
	protected XYNode(AbstractIndex<?, ?, ?> index, Class<K> keyType, double x, double y, double hw, double hh, CacheModifications modifs) throws SerializationException {
		super(keyType, NodeId.class, index, index.getEndOfFile());
		init(x, y, hw, hh, null, modifs);
	}

	private boolean isRoot(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return getOptParent(modifs) == null;
	}

	@Override
	AbstractNode<K, NodeId> addAndBalance(K key, long keyPosition, Long valuePosition, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		if(isRoot(modifs) && !contains(key, modifs)) {
			XYNode<K> root = newXYRootNode(key, modifs);
			return root.addAndBalance(key, keyPosition, valuePosition, modifs);
		}

		byte nodeType = getNodeType(modifs);
		if (nodeType == EMPTY) {
			setKeyPosition(keyPosition, modifs);
			setValuePosition(valuePosition, modifs);
			setNodeType(LEAF, modifs);
		} else if (nodeType == LEAF) {
			K k = getKey(modifs);
			if (k.get(0).equals(key.get(0)) && k.get(1).equals(key.get(1))) {
				throw new StorageException("bug : not possible to insert an object of same key on an NodeXY"); //il ne peut pas y avoir insertion d'un objet de meme clef sur un NoeudXY
			} else {
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

	private XYNode<K> newXYRootNode(K key, CacheModifications modifs) throws SerializationException, IOException, StorageException {
		double x0 = key.get(0);
		double y0 = key.get(1);
		double x = (x0 < getX(modifs)) ? getX(modifs) - getW(modifs) : getX(modifs);
		double y = (y0 < getY(modifs)) ? getY(modifs) - getH(modifs) : getY(modifs);
		double hw = 2 * getW(modifs);
		double hh = 2 * getH(modifs);
		boolean east = (x < getX(modifs)) ? true : false;
		boolean north = (y < getY(modifs)) ? true : false; 
		XYNode<K> root = new XYNode<>(index, keyType, x, y, hw, hh, modifs);
		this.setOptParent(root, modifs);
		root.setNodeType(POINTER, modifs);
		XYNode<K> nw;
		XYNode<K> ne;
		XYNode<K> sw;
		XYNode<K> se;
		if(north && east) {//NE
			ne = this;
			nw = new XYNode<>(index, getX(modifs) - getW(modifs), getY(modifs), getW(modifs), getH(modifs), root, modifs);
			sw = new XYNode<>(index, getX(modifs) - getW(modifs), getY(modifs) - getH(modifs), getW(modifs), getH(modifs), root, modifs);
			se = new XYNode<>(index, getX(modifs), getY(modifs) - getH(modifs), getW(modifs), getH(modifs), root, modifs);
		}else if(north && !east){ //NW
			ne = new XYNode<>(index, getX(modifs) + getW(modifs), getY(modifs), getW(modifs), getH(modifs), root, modifs);
			nw = this;
			sw = new XYNode<>(index, getX(modifs), getY(modifs) - getH(modifs), getW(modifs), getH(modifs), root, modifs);
			se = new XYNode<>(index, getX(modifs) + getW(modifs), getY(modifs) - getH(modifs), getW(modifs), getH(modifs), root, modifs);
		}else if(!north && east) { //SE
			ne = new XYNode<>(index, getX(modifs), getY(modifs) + getH(modifs), getW(modifs), getH(modifs), root, modifs);
			nw = new XYNode<>(index, getX(modifs) - getW(modifs), getY(modifs) + getH(modifs), getW(modifs), getH(modifs), root, modifs);
			sw = new XYNode<>(index, getX(modifs) - getW(modifs), getY(modifs), getW(modifs), getH(modifs), root, modifs);
			se = this;
		}else{//SW
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
	private void split(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		double x = getX(modifs);
		double y = getY(modifs);
		double hw = getW(modifs) / 2;
		double hh = getH(modifs) / 2;

		setNw(new XYNode<>(index, x, y, hw, hh, this, modifs), modifs);
		setNe(new XYNode<>(index, x + hw, y, hw, hh, this, modifs), modifs);
		setSw(new XYNode<>(index, x, y + hh, hw, hh, this, modifs), modifs);
		setSe(new XYNode<>(index, x + hw, y + hh, hw, hh, this, modifs), modifs);

		setNodeType(POINTER, modifs);
		addAndBalance(getKey(modifs), keyPosition(modifs), valuePosition(modifs), modifs);
	}

	@Override
	AbstractNode<K, NodeId> deleteAndBalance(K clef, CacheModifications modifs)throws IOException, StorageException, SerializationException {
		//useless
		return null;
	}

	@Override
	AbstractNode<K, NodeId> findNode(K key, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		AbstractNode<K, NodeId> ret = null;
		byte nodeType = getNodeType(modifs);
		if (nodeType == LEAF) {
			Double x = key.get(0);
			Double y = key.get(1);
			ret = getKey(modifs).get(0) == x && getKey(modifs).get(1) == y ? this : null;
		} else if (nodeType == POINTER) {
			ret = getQuandrantForPoint(key, modifs).findNode(key,modifs);
		} else if (nodeType != EMPTY){
			throw new StorageException("Invalid nodeType");
		}
		return ret;
	}
	private AbstractNode<K, NodeId> getQuandrantForPoint(K key, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		double mx = getX(modifs) + getW(modifs) / 2;
		double my = getY(modifs) + getH(modifs) / 2;
		if (key.get(0) < mx) return key.get(1) < my ? getNW(modifs) : getSW(modifs);
		return key.get(1) < my ? getNE(modifs) : getSE(modifs);
	}

	private boolean contains(K key, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return intersects(key.get(0), key.get(1), key.get(0), key.get(1), modifs);
	}

	private boolean intersects(double left, double bottom, double right, double top, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return !(getX(modifs) > right ||
				(getX(modifs) + getW(modifs)) < left ||
				getY(modifs) > bottom ||
				(getY(modifs) + getH(modifs)) < top);
	}

	private boolean isPositionInBox(double left, double bottom, double right, double top, CacheModifications modifs) throws StorageException, IOException, SerializationException {
		if(getNodeType(modifs) != LEAF)
			throw new StorageException("cette méthode ne doit pas etre appelé si le noeud n'est pas une feuille");
		List<Double> xy = getKey(modifs);
		double x = xy.get(0);
		double y = xy.get(1);
		return x >= left && x <= right && y >= bottom && y <= top;
	}
	protected boolean deleteId(String id, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		if(getValue(modifs) == null) return true;
		boolean isEmpty = false;
		AbstractNode<String, String> newNode =  getValue(modifs).deleteAndBalance(id, modifs);
		if(newNode == null) isEmpty = true;
		else setValuePosition(newNode.valuePosition(modifs), modifs);
		return isEmpty;
	}
	protected void insertValue(String id, long positionId, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		NodeId root = getValue(modifs);
		if(root == null) root = new NodeId(positionId, this.index, modifs);
		else root = (NodeId) root.addAndBalance(id, positionId, null, modifs);
		setValuePosition(root.getPosition(), modifs);
	}
	protected Iterator<NodeId> inTheBox(double xmin, double ymin, double xmax, double ymax) throws IOException, StorageException, SerializationException {
		return new BoxIterator(xmin, ymin, xmax, ymax);
	}

	protected class BoxIterator implements Iterator<NodeId>{
		private Deque<XYNode<K>> toScout = new ArrayDeque<>();
		private NodeId next;
		private Iterator<NodeId> currentUnderIterator;
		private boolean dejaVu = false;
		private final double xmin;
		private final double xmax;
		private final double ymin;
		private final double ymax;

		protected BoxIterator(double xmin, double ymin, double xmax, double ymax) throws IOException, StorageException, SerializationException{
			this.next = null;
			this.xmin = xmin;
			this.ymin = ymin;
			this.xmax = xmax;
			this.ymax = ymax;
			if(getNodeType(null) == POINTER) {
				if(getNE(null).intersects(xmin, ymin, xmax, ymax, null)) toScout.push(getNE(null));
				if(getNW(null).intersects(xmin, ymin, xmax, ymax, null)) toScout.push(getNW(null));
				if(getSE(null).intersects(xmin, ymin, xmax, ymax, null)) toScout.push(getSE(null));
				if(getSW(null).intersects(xmin, ymin, xmax, ymax, null)) toScout.push(getSW(null));
			}
			cacheNext();
		}

		@Override
		public boolean hasNext() {
			return next != null;
		}

		private void cacheNext() throws IOException, StorageException, SerializationException {
			if(getNodeType(null) == EMPTY)
				next = null;
			else if(getNodeType(null) == LEAF) {
				cacheNextNodeLeaf();
			}else if(getNodeType(null) == POINTER) {
				cacheNextPointer();
			}
		}

		private void cacheNextPointer() throws IOException, StorageException, SerializationException {
			while (next == null && stillNodeToScout()) {
				if(currentUnderIterator == null) {
					currentUnderIterator = toScout.pop().inTheBox(xmin, ymin, xmax, ymax);
				}
				if(currentUnderIterator.hasNext())
					next = currentUnderIterator.next();
				else
					currentUnderIterator = null;
			}
		}

		private void cacheNextNodeLeaf() throws StorageException, IOException, SerializationException {
			if(!dejaVu) {
				next = isPositionInBox(xmin, ymin, xmax, ymax, null) ? getValue(null) : null;
				dejaVu = true;
			}else next = null;
		}

		private boolean stillNodeToScout() {
			return !toScout.isEmpty() || currentUnderIterator != null;
		}

		@Override
		public NodeId next() {
			if(!hasNext())
				throw new NoSuchElementException();
			try{
				NodeId ret = next;
				cacheNext();
				return ret;
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
