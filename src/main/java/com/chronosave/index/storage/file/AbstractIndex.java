package com.chronosave.index.storage.file;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.condition.ComputeValue;
import com.chronosave.index.storage.condition.GetId;
import com.chronosave.index.storage.exception.IndexInstanciationException;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StoreException;
import com.chronosave.index.utils.TriKeyCache;

/**
 *
 * @author giraudsa
 *
 * @param <U>
 *            type of object
 * @param <K>
 *            type of key
 * @param <V>
 *            type of value
 */
public abstract class AbstractIndex<U, K, V> {
	public static final long BOOLEAN_BYTES = 1;

	private static final TriKeyCache<AbstractIndex<?, ?, ?>, Long, Class<?>, Object> cache = new TriKeyCache<>(100000);
	private static final long IS_PRIMARY_POSITION;
	private static final long KEY_TYPE_POSITION;
	private static final long ROOT_POSITION_POSITION;
	protected static final long NULL = -1L;

	static {
		ROOT_POSITION_POSITION = 0L;
		IS_PRIMARY_POSITION = ROOT_POSITION_POSITION + Long.BYTES;
		KEY_TYPE_POSITION = IS_PRIMARY_POSITION + BOOLEAN_BYTES;
	}

	private final ComputeKey<K, U> delegateKey;
	private final ComputeValue<V, U> delegateValue;
	private long endOfFile;
	private Path file;
	private final boolean isPrimary;
	private final Class<K> keyType;
	private final long positionComputeKey;
	private final long positionEndOfHeaderInAbstractIndex;
	private RandomAccessFile raf;
	private long rootPosition;
	private final Store<U> store;
	private final Class<V> valueType;
	private long version;

	/**
	 * runtime creation
	 *
	 * @param keyType
	 * @param valueType
	 * @param raf
	 * @param datas
	 * @param delegateKey
	 * @param delegateValeur
	 * @throws IOException
	 * @throws StorageException
	 * @throws SerializationException
	 */
	protected AbstractIndex(final Class<K> keyType, final Class<V> valueType, final Path fileStockage, final Store<U> store, final ComputeKey<K, U> delegateKey, final ComputeValue<V, U> delegateValeur) throws IOException, StorageException, SerializationException {
		this.file = fileStockage;
		Files.deleteIfExists(fileStockage);
		Files.createFile(file);
		newRandomAccessFile();
		this.keyType = keyType;
		this.valueType = valueType;
		this.delegateValue = delegateValeur;
		this.delegateKey = delegateKey;
		this.store = store;
		this.isPrimary = this instanceof PrimaryIndexFile;
		positionComputeKey = KEY_TYPE_POSITION + Short.BYTES + keyType.getName().length();
		this.positionEndOfHeaderInAbstractIndex = initFile();
	}

	@SuppressWarnings("unchecked")
	/**
	 * from file
	 *
	 * @param keyType
	 * @param valueType
	 * @param stockage
	 * @param datas
	 * @param delegateValeur
	 * @throws IOException
	 * @throws ForsException
	 * @throws ClassNotFoundException
	 */
	protected AbstractIndex(final Class<V> valueType, final Path file, final Store<U> store, final ComputeValue<V, U> delegateValue) throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException {
		this.file = file;
		newRandomAccessFile();
		this.endOfFile = raf.length();
		if (raf.length() < KEY_TYPE_POSITION + Short.BYTES)
			throw new IndexInstanciationException("the file " + raf.toString() + " is corrupted");
		this.valueType = valueType;
		this.delegateValue = delegateValue;

		this.store = store;
		version = IndexedStorageManager.readVersion(file);
		this.rootPosition = getStuff(ROOT_POSITION_POSITION, Long.class, null);
		isPrimary = getStuff(IS_PRIMARY_POSITION, Boolean.class, null);
		this.keyType = (Class<K>) Class.forName(getStuff(KEY_TYPE_POSITION, String.class, null));
		positionComputeKey = KEY_TYPE_POSITION + Short.BYTES + keyType.getName().length();
		if (isPrimary) {
			this.delegateKey = (ComputeKey<K, U>) new GetId<>(store.getIdManager());
			positionEndOfHeaderInAbstractIndex = positionComputeKey;
		} else {
			seek(positionComputeKey);
			this.delegateKey = ComputeKey.unmarshall(store.getMarshaller(), raf);
			positionEndOfHeaderInAbstractIndex = raf.getFilePointer();
		}
	}

	private void newRandomAccessFile() throws IOException, StorageException {
		if (!file.toFile().exists())
			throw new StorageException("the file doesn't exist, permission pb ? : " + file.toString());
		raf = new RandomAccessFile(file.toFile(), "rw");
		endOfFile = raf.length();
	}

	@SuppressWarnings("unchecked")
	private <W> W readCache(final long positionStuff, final Class<W> typeStuff) {
		synchronized (cache) {
			return (W) cache.get(this, positionStuff, typeStuff);
		}
	}

	@SuppressWarnings("unchecked")
	private synchronized <T> T readClass(final Class<T> type, final long position) throws IOException, SerializationException {
		seek(position);
		if (type == String.class)
			return (T) raf.readUTF();
		if (type == boolean.class || type == Boolean.class)
			return (T) (Object) raf.readBoolean();
		if (type == byte.class || type == Byte.class)
			return (T) (Object) raf.readByte();
		if (type == long.class || type == Long.class)
			return (T) (Object) raf.readLong();
		if (type == int.class || type == Integer.class)
			return (T) (Object) raf.readInt();
		if (type == short.class || type == Short.class)
			return (T) (Object) raf.readShort();
		if (type == double.class || type == Double.class)
			return (T) (Object) raf.readDouble();
		if (type == float.class || type == Float.class)
			return (T) (Object) raf.readFloat();
		if (type == char.class || type == Character.class)
			return (T) (Object) raf.readChar();
		if (type == Date.class)
			return (T) new Date(raf.readLong());
		if (List.class.isAssignableFrom(type)) {// List<Double> for lonlat
			final List<Double> l = new ArrayList<>();
			l.add(raf.readDouble());
			l.add(raf.readDouble());
		}
		if (AbstractNode.class.isAssignableFrom(type)) {
			return (T) readAbstractNode(position, (Class<? extends AbstractNode<?, ?>>) type);
		}
		return store.getMarshaller().unserialize(type, raf);
	}

	private void setRootPosition(final long positionRacine) throws IOException, StorageException {
		this.rootPosition = positionRacine;
		write(ROOT_POSITION_POSITION, positionRacine);
	}

	private void writeNumber(final Number value) throws IOException {
		// Byte, Double, Float, Integer, Long, and Short.
		if (value instanceof Byte)
			raf.writeByte(value.byteValue());
		else if (value instanceof Double)
			raf.writeDouble(value.doubleValue());
		else if (value instanceof Float)
			raf.writeFloat(value.floatValue());
		else if (value instanceof Integer)
			raf.writeInt(value.intValue());
		else if (value instanceof Long)
			raf.writeLong(value.longValue());
		else if (value instanceof Short)
			raf.writeShort(value.shortValue());
	}

	protected abstract void add(K key, V value, CacheModifications modifs) throws StorageException, IOException, SerializationException;

	protected void add(final U objectToAdd, final long version, final CacheModifications modifs) throws StorageException, IOException, SerializationException {
		add(getKey(objectToAdd), computeValue(objectToAdd, version), modifs);// version is used in override method in
																				// primary index
	}

	protected void addCache(final Long position, final Object o) {
		synchronized (cache) {
			cache.put(this, position, o.getClass(), o);
		}
	}

	protected abstract void addKeyToValue(K key, long keyPosition, V value, long valuePosition, CacheModifications modifs) throws IOException, StorageException, SerializationException;

	protected void clear() throws IOException, StorageException, SerializationException {
		raf.setLength(0);
		cache.clear();
		initFile();
	}

	protected V computeValue(final U object, final long version) throws StorageException, IOException, SerializationException {
		return delegateValue.getValue(object, version);

	}

	protected abstract void delete(String id, CacheModifications modifs) throws IOException, StorageException, SerializationException;

	protected void delete(final String id, final long version, final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		if (this.version > version)
			return;
		delete(id, modifs);
	}

	protected abstract void deleteKtoId(K key, V value, CacheModifications modifs) throws IOException, StorageException, SerializationException;

	protected CacheModifications deleteObjects(final Collection<String> ids, final long v) throws IOException, StorageException, SerializationException {
		final CacheModifications modifs = new CacheModifications(this, v);
		for (final String id : ids)
			delete(id, v, modifs);
		return modifs;
	}

	protected ComputeKey<K, U> getDelegateKey() {
		return delegateKey;
	}

	protected ComputeValue<V, U> getDelegateValeur() {
		return delegateValue;
	}

	protected long getEndOfFile() {
		return endOfFile;
	}

	protected K getKey(final U object) throws StorageException {
		return delegateKey.getKey(object);
	}

	protected abstract long getKeyPosition(K key, CacheModifications modifs) throws IOException, StorageException, SerializationException;

	protected Class<K> getKeyType() {
		return keyType;
	}

	protected long getPositionEndOfHeaderInAbstractIndex() {
		return positionEndOfHeaderInAbstractIndex;
	}

	protected long getRootPosition() {
		return rootPosition;
	}

	protected Store<U> getStore() {
		return store;
	}

	protected <W> W getStuff(final long stuffPosition, final Class<W> stuffType, final CacheModifications modifs) throws IOException, SerializationException {
		if (stuffPosition < 0)
			return null;
		W stuff = null;
		if (modifs != null)
			stuff = modifs.getObjectFromRecentModifs(stuffPosition, stuffType);
		if (stuff == null)
			stuff = readCache(stuffPosition, stuffType);
		if (stuff == null) {
			stuff = readClass(stuffType, stuffPosition);
			addCache(stuffPosition, stuff);
		}
		return stuff;
	}

	protected Class<V> getValueType() {
		return valueType;
	}

	protected Class<?> getValueTypeOfNode() {
		return valueType;
	}

	protected long getVersion() {
		return version;
	}

	protected long initFile() throws IOException, StorageException, SerializationException {
		setRootPosition(NULL);
		write(IS_PRIMARY_POSITION, isPrimary);
		write(KEY_TYPE_POSITION, keyType.getName());
		if (!isPrimary)
			ComputeKey.marshall(delegateKey, raf, store.getMarshaller());
		endOfFile = raf.length();
		return raf.getFilePointer();
	}

	protected boolean isEqual(final K ancienneClef, final K key) {
		if (ancienneClef != null && key != null)
			return ancienneClef.equals(key);
		return ancienneClef == null && key == null;

	}

	protected abstract <N extends AbstractNode<?, ?>> AbstractNode<?, ?> readAbstractNode(final long nodePosition, final Class<N> nodeType);

	protected void rebuild(final long version) throws IOException, SerializationException, StorageException {
		if (version == this.version)
			return; // rien a faire
		clear();
		for (final String id : store.getPrimaryIndex()) {
			final CacheModifications modifs = new CacheModifications(this, version);
			final U obj = store.getObjectById(id);
			add(obj, store.getVersion(), modifs);
			modifs.writeWithoutChangingVersion();
		}
		setVersion(version);
	}

	protected void removeFile() throws IOException {
		raf.close();
		Files.deleteIfExists(file);
		raf = null;
	}

	protected void seek(final long position) throws IOException {
		raf.seek(position);
	}

	protected void setRootPosition(final long rootPosition, final CacheModifications modifs) {
		this.rootPosition = rootPosition;
		modifs.add(ROOT_POSITION_POSITION, getRootPosition());
	}

	protected void setVersion(final long ver) throws IOException, StorageException {
		if (store.getVersion() < ver)
			store.setVersion(ver);
		this.version = ver;
		raf.close();
		file = Files.move(file, IndexedStorageManager.nextVersion(file, version), ATOMIC_MOVE);
		newRandomAccessFile();
	}

	protected CacheModifications stockpile(final Collection<U> us, final long version) throws StorageException, IOException, SerializationException {
		final CacheModifications modifs = new CacheModifications(this, version);
		for (final U u : us)
			add(u, version, modifs);
		return modifs;
	}

	protected synchronized void write(final long position, final Object value) throws IOException, StorageException {
		seek(position);
		if (value instanceof Date)
			raf.writeLong(((Date) value).getTime());
		else if (value instanceof String)
			raf.writeUTF((String) value);
		else if (value instanceof Number)
			writeNumber((Number) value);
		else if (value instanceof Boolean)
			raf.writeBoolean((Boolean) value);
		else if (value instanceof Character)
			raf.writeChar((char) value);
		else if (value instanceof List) { // List<Double> for lonlat
			raf.writeDouble((Double) ((List<?>) value).get(0));
			raf.writeDouble((Double) ((List<?>) value).get(0));
		} else {
			throw new StorageException("type not implemented " + value.getClass().getName());
		}
		endOfFile = raf.length();
	}

	/**
	 * return positionBeforeWritting
	 *
	 * @param value
	 * @param modifs
	 * @return
	 * @throws SerializationException
	 */
	protected long writeFakeAndCache(final Object value, final CacheModifications modifs) throws SerializationException {
		return modifs.addToEnd(value);
	}

}
