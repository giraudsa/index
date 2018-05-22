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
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StoreException;
import com.chronosave.index.utils.CloseableIterator;
import com.chronosave.index.utils.ReadWriteLock;

public class SpaceIndex<U, K extends List<Double>> extends AbstractIndex<U, K, String> {

	private class NodeIterator implements CloseableIterator<String> {

		private boolean closed = false;
		private boolean hasNext = false;
		private final ReadWriteLock locker;
		private String next;
		private Iterator<String> nodeIdIterator;
		private final Iterator<SingletonNode<String>> nodeXYIterator;

		private NodeIterator(final XYNode<K> root, final double xmin, final double ymin, final double xmax,
				final double ymax, final ReadWriteLock locker)
				throws InterruptedException, IOException, StorageException, SerializationException {
			locker.lockRead();
			this.locker = locker;
			nodeXYIterator = root == null ? null : root.inTheBox(xmin, ymin, xmax, ymax);
			cacheNext();
		}

		private void cacheNext() {
			if (nodeIdIterator != null && nodeIdIterator.hasNext()) {
				next = nodeIdIterator.next();
				hasNext = true;
			}
			while (!hasNext && nodeXYIterator != null && nodeXYIterator.hasNext()) {
				nodeIdIterator = nodeXYIterator.next().iterator();
				hasNext = nodeIdIterator.hasNext();
				next = hasNext ? nodeIdIterator.next() : null;
			}
		}

		@Override
		public void close() throws IOException {
			closed = true;
			locker.unlockRead();
		}

		@Override
		public boolean hasNext() {
			if (closed)
				throw new IllegalStateException("Iterator has been closed!");
			return hasNext;
		}

		@Override
		public String next() {
			if (next == null)
				throw new NoSuchElementException();
			final String ret = next;
			cacheNext();
			return ret;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	private static final String EXTENTION = ".spatial";

	protected static final <U> void feed(final Path basePath,
			final Map<ComputeKey<?, U>, AbstractIndex<U, ?, ?>> indexes, final Store<U> stockage)
			throws IOException, ClassNotFoundException, StorageException, SerializationException, StoreException {
		final String debutNom = stockage.debutNomFichier();
		final File[] idxs = IndexedStorageManager.findAllFileThatBeginsWith(basePath, debutNom + EXTENTION);
		for (final File fidx : idxs) {
			final AbstractIndex<U, ?, ?> idx = new SpaceIndex<>(fidx.toPath(), stockage);
			final ComputeKey<?, U> fc = idx.getDelegateKey();
			indexes.put(fc, idx);
		}
	}

	private static Path getPath(final Path basePath, final String debutNom, final ComputeKey<?, ?> delegateKey) {
		return Paths.get(basePath.toString(), debutNom + EXTENTION + "." + delegateKey.hashCode() + ".0");
	}

	/**
	 * runtime
	 * 
	 * @param keyType
	 * @param valueType
	 * @param fileStockage
	 * @param store
	 * @param delegateKey
	 * @throws IOException
	 * @throws StorageException
	 * @throws SerializationException
	 */
	public SpaceIndex(final Path basePath, final Class<K> keyType, final Store<U> store,
			final ComputeKey<K, U> delegateKey) throws IOException, StorageException, SerializationException {
		super(keyType, String.class, getPath(basePath, store.debutNomFichier(), delegateKey), store, delegateKey,
				new GetId<>(store.getIdManager()));
		setReverseRootPosition(NULL);
		rebuild(store.getVersion());
	}

	/**
	 * file
	 * 
	 * @param valueType
	 * @param file
	 * @param store
	 * @throws IOException
	 * @throws StorageException
	 * @throws ClassNotFoundException
	 * @throws SerializationException
	 * @throws StoreException
	 */
	public SpaceIndex(final Path file, final Store<U> store)
			throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException {
		super(String.class, file, store, new GetId<>(store.getIdManager()));
	}

	private void add(final K key, final long keyPosition, final String id, final long idPosition,
			final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		addKeyToValue(key, keyPosition, id, idPosition, modifs);
		addValueToKey(keyPosition, id, idPosition, modifs);
	}

	@Override
	protected void add(final K key, final String id, final CacheModifications modifs)
			throws StorageException, IOException, SerializationException {
		if (key == null)
			return;
		final AbstractNode<String, K> oldReverseNode = getReverseRoot(modifs).findNode(id, modifs);
		if (oldReverseNode != null) {
			final K oldKey = oldReverseNode.getValue(modifs);
			if (isEqual(oldKey, key))
				return;// nothing to do
			final XYNode<K> oldComplexNode = (XYNode<K>) getRoot(modifs).findNode(oldKey, modifs);// getRacine() is not
																									// null
			oldComplexNode.deleteId(id, modifs);
		}
		if (getRoot(modifs) == null)
			initRootWithCoordinates(key, modifs);
		add(key, writeFakeAndCache(key, modifs), id, getIdPosition(id, modifs), modifs);
	}

	@Override
	protected void addKeyToValue(final K key, final long keyPosition, final String id, final long idPosition,
			final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setRoot((XYNode<K>) getRoot(modifs).addAndBalance(key, keyPosition, NULL, modifs), modifs);
		final XYNode<K> n = (XYNode<K>) getRoot(modifs).findNode(key, modifs);
		n.insertValue(id, idPosition, modifs);
	}

	private void addValueToKey(final long keyPosition, final String id, final long idPosition,
			final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setReverseRoot(
				(SimpleNode<String, K>) getReverseRoot(modifs).addAndBalance(id, idPosition, keyPosition, modifs),
				modifs);
	}

	private void delete(final K key, final String id, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		deleteKtoId(key, id, modifs);
		deleteId(id, modifs);
	}

	@Override
	protected void delete(final String id, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		if (id == null)
			return;
		final AbstractNode<String, K> reverseNode = getReverseRoot(modifs).findNode(id, modifs);
		if (reverseNode == null)
			return;// nothing to do
		final K oldKey = reverseNode.getValue(modifs);
		delete(oldKey, id, modifs);
	}

	private void deleteId(final String id, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		setReverseRoot((SimpleNode<String, K>) getReverseRoot(modifs).deleteAndBalance(id, modifs), modifs);
	}

	@Override
	protected void deleteKtoId(final K key, final String id, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		final XYNode<K> nodeXY = (XYNode<K>) getRoot(modifs).findNode(key, modifs);// not null since reverseNode != null
		nodeXY.deleteId(id, modifs);
	}

	protected long getIdPosition(final String id, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		final AbstractNode<String, ?> reverseNode = getReverseRoot(modifs).findNode(id, modifs);
		return reverseNode == null ? writeFakeAndCache(id, modifs) : reverseNode.valuePosition(modifs);
	}

	@Override
	protected long getKeyPosition(final K key, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		if (key == null)
			return NULL;
		AbstractNode<K, SingletonNode<String>> node = getRoot(modifs);
		node = node == null ? null : node.findNode(key, modifs);
		return node == null ? writeFakeAndCache(key, modifs) : node.valuePosition(modifs);
	}

	@SuppressWarnings("unchecked")
	protected SimpleNode<String, K> getReverseRoot(final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		if (reverseRootPosition(modifs) == NULL)// Fake node
			return new SimpleNode<>(String.class, keyType, this, modifs);
		return getStuff(reverseRootPosition(modifs), SimpleNode.class, modifs);
	}

	@SuppressWarnings("unchecked")
	protected XYNode<K> getRoot(final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		return getStuff(rootPosition, XYNode.class, modifs); // rootPosition may be null
	}

	@Override
	protected Class<?> getValueTypeOfNode() {
		return SingletonNode.class;
	}

	@Override
	protected long initFile() throws IOException, StorageException, SerializationException {
		final long positionEndOfHeaderInAbstractIndex = super.initFile();
		setReverseRootPosition(NULL);
		return positionEndOfHeaderInAbstractIndex;
	}

	/**
	 * We are the first. Need to decide the scale. si x0=0, la largeur de la bbox
	 * est de 1 sinon la largeur = [0, 2*x] idem pour y
	 * 
	 * @param key
	 * @param modifs
	 * @throws SerializationException
	 */
	private void initRootWithCoordinates(final K key, final CacheModifications modifs) throws SerializationException {
		final double x0 = key.get(0);
		final double y0 = key.get(1);
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
		setRoot(new XYNode<>(this, keyType, x, y, hw, hh, modifs), modifs);
	}

	public CloseableIterator<String> inTheBox(final double xmin, final double ymin, final double xmax,
			final double ymax, final ReadWriteLock locker)
			throws InterruptedException, IOException, StorageException, SerializationException {
		return new NodeIterator(getRoot(null), xmin, ymin, xmax, ymax, locker);
	}

	private long reverseRootPosition(final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		return getStuff(reverseRootPositionPosition(), Long.class, modifs);
	}

	private long reverseRootPositionPosition() {
		return positionEndOfHeaderInAbstractIndex;
	}

	protected void setReverseRoot(final SimpleNode<String, K> node, final CacheModifications modifs) {
		final long reverseRootPosition = node == null ? NULL : node.getPosition();
		modifs.add(reverseRootPositionPosition(), reverseRootPosition);
	}

	private void setReverseRootPosition(final long reverseRootPosition) throws IOException, StorageException {
		write(reverseRootPositionPosition(), reverseRootPosition);
	}

	private void setRoot(final XYNode<K> noeudXY, final CacheModifications modifs) {
		rootPosition = noeudXY == null ? NULL : noeudXY.getPosition();
		modifs.add(ROOT_POSITION_POSITION, rootPosition);
	}
}
