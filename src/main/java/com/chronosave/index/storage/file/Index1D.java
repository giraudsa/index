package com.chronosave.index.storage.file;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.condition.ComputeValue;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StoreException;
import com.chronosave.index.utils.CloseableIterator;
import com.chronosave.index.utils.ReadWriteLock;

/**
 *
 * @author giraudsa
 *
 * @param <U>
 *            Object
 * @param <K>
 *            Key
 * @param <V>
 *            Value
 * @param <N>
 *            Value type of Node
 */
public abstract class Index1D<U, K, V extends Comparable<V>, N> extends AbstractIndex<U, K, V> {

	protected static Path getPath(final Path basePath, final String debutNomFichier, final String extention,
			final ComputeKey<?, ?> delegateKey) {
		return Paths.get(basePath.toString(), debutNomFichier + extention + "." + delegateKey.hashCode() + ".0");
	}

	/**
	 * runtime
	 * 
	 * @param keyType
	 * @param valueType
	 * @param fileStore
	 * @param store
	 * @param delegateKey
	 * @param delegateValue
	 * @throws IOException
	 * @throws StorageException
	 * @throws SerializationException
	 */
	public Index1D(final Class<K> keyType, final Class<V> valueType, final Path fileStore, final Store<U> store,
			final ComputeKey<K, U> delegateKey, final ComputeValue<V, U> delegateValue)
			throws IOException, StorageException, SerializationException {
		super(keyType, valueType, fileStore, store, delegateKey, delegateValue);
	}

	/**
	 * from file
	 * 
	 * @param valueType
	 * @param file
	 * @param store
	 * @param delegateValue
	 * @throws IOException
	 * @throws StorageException
	 * @throws ClassNotFoundException
	 * @throws SerializationException
	 * @throws StoreException
	 */
	public Index1D(final Class<V> valueType, final Path file, final Store<U> store,
			final ComputeValue<V, U> delegateValue)
			throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException {
		super(valueType, file, store, delegateValue);
	}

	protected void add(final K key, final long keyPosition, final V value, final long valuePosition,
			final CacheModifications modifs) throws StorageException, IOException, SerializationException {
		addKeyToValue(key, keyPosition, value, valuePosition, modifs);
	}

	@Override
	protected void add(final K key, final V value, final CacheModifications modifs)
			throws SerializationException, IOException, StorageException {
		final long keyPosition = getKeyPosition(key, modifs);
		final long valuePosition = writeFakeAndCache(value, modifs);
		add(key, keyPosition, value, valuePosition, modifs);
	}

	@Override
	protected void addKeyToValue(final K key, final long keyPosition, final V value, final long idPosition,
			final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setRoot(getRoot(modifs).addAndBalance(key, keyPosition, idPosition, modifs), modifs);
	}

	@Override
	protected long getKeyPosition(final K key, final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		if (key == null)
			return NULL;
		final AbstractNode<K, ?> node = getRoot(modifs).findNode(key, modifs);
		return node == null ? writeFakeAndCache(key, modifs) : node.valuePosition(modifs);
	}

	protected abstract Class<? extends AbstractNode<K, N>> getNodeType();

	protected AbstractNode<K, N> getRoot(final CacheModifications modifs)
			throws IOException, StorageException, SerializationException {
		try {
			if (rootPosition == NULL) // Fake node
				return getNodeType()
						.getConstructor(Class.class, Class.class, AbstractIndex.class, CacheModifications.class)
						.newInstance(keyType, getValueTypeOfNode(), this, modifs);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			throw new StorageException("impossible to create fake node", e);
		}
		return getStuff(rootPosition, getNodeType(), modifs);
	}

	protected void setRoot(final AbstractNode<K, ?> abstractNode, final CacheModifications modifs) {
		rootPosition = abstractNode == null ? NULL : abstractNode.getPosition();
		modifs.add(ROOT_POSITION_POSITION, rootPosition);
	}

	public abstract CloseableIterator<V> getBetween(K min, K max, ReadWriteLock locker)
			throws IOException, StorageException, SerializationException, InterruptedException;
}
