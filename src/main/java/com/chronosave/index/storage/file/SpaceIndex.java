package com.chronosave.index.storage.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.condition.GetId;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StoreException;
import com.chronosave.index.utils.CloseableIterator;
import com.chronosave.index.utils.ReadWriteLock;

public class SpaceIndex<U, K extends List<Double>>  extends AbstractIndex<U, K, String>{

	private static final String EXTENTION = ".spatial";
	private static Path getPath(Path basePath, String debutNom, ComputeKey<?, ?> delegateKey) {
		return Paths.get(basePath.toString(), debutNom + EXTENTION + "." + delegateKey.hashCode() + ".0");
	}
	protected static final <U> void feed(Path basePath, Map<ComputeKey<?, U>, AbstractIndex<U, ?, ?>> indexes, Store<U> stockage) throws IOException, ClassNotFoundException, StorageException, SerializationException, StoreException{
		String debutNom = stockage.debutNomFichier();
		File[] idxs = IndexedStorageManager.findAllFileThatBeginsWith(basePath, debutNom + EXTENTION);
		for(File fidx : idxs) {
			AbstractIndex<U,?, ?> idx = new SpaceIndex<>(fidx.toPath(), stockage);
			ComputeKey<?, U> fc = idx.getDelegateKey();
			indexes.put(fc, idx);
		}
	}
	
	private long reverseRootPositionPosition() {
		return positionEndOfHeaderInAbstractIndex;
	}
	private long reverseRootPosition(CacheModifications modifs) throws IOException, StorageException, SerializationException {
		return getStuff(reverseRootPositionPosition(), Long.class, modifs);
	}
	
	/**
	 * runtime
	 * @param keyType
	 * @param valueType
	 * @param fileStockage
	 * @param store
	 * @param delegateKey
	 * @throws IOException
	 * @throws StorageException
	 * @throws SerializationException 
	 */
	public SpaceIndex(Path basePath, Class<K> keyType, Store<U> store, ComputeKey<K, U> delegateKey)
			throws IOException, StorageException, SerializationException {
		super(keyType, String.class, getPath(basePath, store.debutNomFichier(), delegateKey), store, delegateKey, new GetId<>(store.getObjectType(), store.getIdManager()));
		setReverseRootPosition(NULL);
		rebuild(store.getVersion());
	}
	
	/**
	 * file
	 * @param valueType
	 * @param file
	 * @param store
	 * @throws IOException
	 * @throws StorageException
	 * @throws ClassNotFoundException
	 * @throws SerializationException
	 * @throws StoreException
	 */
	public SpaceIndex(Path file, Store<U> store) throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException {
		super(String.class, file, store, new GetId<>(store.getObjectType(), store.getIdManager()));
		checkVersion(store.getVersion());
	}
	
	protected void add(K key, String id, CacheModifications modifs) throws StorageException, IOException, SerializationException {
		if(key == null) return;
		AbstractNode<String, K> oldReverseNode = getReverseRoot(modifs).findNode(id, modifs);
		if(oldReverseNode != null) {
			K oldKey = oldReverseNode.getValue(modifs);
			if(isEqual(oldKey,key)) return;//nothing to do
			XYNode<K> oldComplexNode = (XYNode<K>) getRoot(modifs).findNode(oldKey, modifs);//getRacine() is not null
			oldComplexNode.deleteId(id, modifs);
		}
		if(getRoot(modifs) == null) initRootWithCoordinates(key, modifs);
		add(key, writeFakeAndCache(key, modifs), id, getIdPosition(id, modifs), modifs);
	}
	
	private void add(K key, long keyPosition, String id, long idPosition, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		addKeyToValue(key, keyPosition, id, idPosition, modifs);
		addValueToKey(key, keyPosition, id, idPosition, modifs);
	}
	protected long getIdPosition(String id, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		AbstractNode<String, ?> reverseNode = getReverseRoot(modifs).findNode(id, modifs);
		return reverseNode == null ? writeFakeAndCache(id, modifs) : reverseNode.valuePosition(modifs);
	}
		
	@Override
	protected void addKeyToValue(K key, long keyPosition, String id, long idPosition, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setRoot((XYNode<K>) getRoot(modifs).addAndBalance(key, keyPosition, NULL, modifs), modifs);
		XYNode<K> n = (XYNode<K>) getRoot(modifs).findNode(key, modifs);
		n.insertValue(id, idPosition, modifs);
	}
	
	private void addValueToKey(K key, long keyPosition, String id, long idPosition, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setReverseRoot((ReverseSimpleNode<K, String>) getReverseRoot(modifs).addAndBalance(id, idPosition, keyPosition, modifs), modifs);
	}
	/**
	 * We are the first. Need to decide the scale.
	 * si x0=0, la largeur de la bbox est de 1 sinon la largeur = [0, 2*x]
	 * idem pour y
	 * @param key
	 * @param modifs 
	 * @throws SerializationException 
	 */
	private void initRootWithCoordinates(K key, CacheModifications modifs) throws SerializationException {
		double x0 = key.get(0);
		double y0 = key.get(1);
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
		setRoot(new XYNode<>(this, keyType, x, y, hw, hh, modifs), modifs);
	}
	private void setRoot(XYNode<K> noeudXY, CacheModifications modifs){
		rootPosition = noeudXY == null ? NULL : noeudXY.getPosition();
		modifs.add(ROOT_POSITION_POSITION, rootPosition);
	}
	
	private void setReverseRootPosition(long reverseRootPosition) throws IOException, StorageException {
		write(reverseRootPositionPosition(), reverseRootPosition);
	}

	
	@SuppressWarnings("unchecked")
	protected XYNode<K> getRoot(CacheModifications modifs) throws IOException, StorageException, SerializationException{
		return getStuff(rootPosition, XYNode.class, modifs); //rootPosition may be null
	}
	
	protected void setReverseRoot(ReverseSimpleNode<K, String> node, CacheModifications modifs){
		long reverseRootPosition = node == null ? NULL : node.getPosition();
		modifs.add(reverseRootPositionPosition(), reverseRootPosition);
	}
	

	@SuppressWarnings("unchecked")
	protected ReverseSimpleNode<K, String> getReverseRoot(CacheModifications modifs) throws IOException, StorageException, SerializationException{
		if(reverseRootPosition(modifs) == NULL)//Fake node
			return new ReverseSimpleNode<>(keyType, String.class, this, modifs);
		return getStuff(reverseRootPosition(modifs), ReverseSimpleNode.class, modifs);
	}

	@Override
	protected void rebuild(long version) throws IOException, StorageException, SerializationException {
		clear();
		for(String id : store.getPrimaryIndex()) {
			CacheModifications modifs = new CacheModifications(this, version);
			U obj = store.getObjectById(id);
			add(obj, version, modifs);
			modifs.writeWithoutChangingVersion();
		}
		setVersion(version);
	}
	
	@Override
	protected long initFile() throws IOException, StorageException, SerializationException {
		long positionEndOfHeaderInAbstractIndex = super.initFile();
		setReverseRootPosition(NULL);
		return positionEndOfHeaderInAbstractIndex;
	}

	@Override
	protected void delete(String id, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		if(id == null) return;
		AbstractNode<String, K> reverseNode = getReverseRoot(modifs).findNode(id, modifs);
		if(reverseNode == null) return;//nothing to do
		K oldKey = reverseNode.getValue(modifs);
		delete(oldKey, id, modifs);
		deleteKtoId(oldKey, id, modifs);
	}
	private void delete(K key, String id, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		deleteKtoId(key, id, modifs);
		deleteId(id, modifs);
	}
	private void deleteId(String id, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setReverseRoot((ReverseSimpleNode<K, String>) getReverseRoot(modifs).deleteAndBalance(id, modifs), modifs);
	}
	@Override
	protected void deleteKtoId(K key, String id, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		XYNode<K> nodeXY = (XYNode<K>) getRoot(modifs).findNode(key, modifs);//not null since reverseNode != null
		nodeXY.deleteId(id, modifs);
	}

	@SuppressWarnings("rawtypes")
	protected Class<? extends AbstractNode> getNodeType() {
		return XYNode.class;
	}
	public CloseableIterator<String> inTheBox(double xmin, double ymin, double xmax, double ymax, ReadWriteLock locker) throws InterruptedException, IOException, StorageException, SerializationException {
		return new NodeIterator((XYNode<K>) getRoot(null), xmin, ymin, xmax, ymax, locker);
	}
	
	private class NodeIterator implements CloseableIterator<String>{

		private final Iterator<SingletonNode<String>> nodeXYIterator;
		private final ReadWriteLock locker;
		private Iterator<String> nodeIdIterator;
		private String next;
		private boolean hasNext = false;
		private boolean closed = false;
		
		private NodeIterator(final XYNode<K> root, double xmin, double ymin, double xmax, double ymax, ReadWriteLock locker) throws InterruptedException, IOException, StorageException, SerializationException {
			locker.lockRead();
			this.locker = locker;
			nodeXYIterator = root == null ? null : root.inTheBox(xmin, ymin, xmax, ymax);
			cacheNext();
		}
		
		private void cacheNext() {
			if(nodeIdIterator != null && nodeIdIterator.hasNext()) {
				next = nodeIdIterator.next();
				hasNext = true;
			}
			while(!hasNext && nodeXYIterator != null && nodeXYIterator.hasNext()) {
				nodeIdIterator = nodeXYIterator.next().iterator();
				hasNext = nodeIdIterator.hasNext();
				next = hasNext ? nodeIdIterator.next() : null;
			}
		}

		@Override
		public boolean hasNext() {
			 if (closed)
		            throw new IllegalStateException("Iterator has been closed!");
			return hasNext;
		}

		@Override
		public String next() {
			if(next == null)
				throw new NoSuchElementException();
			String ret = next;
			cacheNext();
			return ret;
		}

		@Override
		public void close() throws IOException {
			closed = true;
			locker.unlockRead();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	protected long getKeyPosition(K key, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		if(key == null) return NULL;
		AbstractNode<K, SingletonNode<String>> node = getRoot(modifs);
		node = node == null ? null : node.findNode(key, modifs);
		return node == null ? writeFakeAndCache(key, modifs) : node.valuePosition(modifs);
	}
}
