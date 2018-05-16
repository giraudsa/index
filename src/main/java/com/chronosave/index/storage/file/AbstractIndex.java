package com.chronosave.index.storage.file;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.condition.ComputeValue;
import com.chronosave.index.storage.condition.GetId;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.IndexInstanciationException;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StoreException;
import com.chronosave.index.utils.BiKeyCache;
import com.chronosave.index.utils.TriKeyCache;

/**
 * 
 * @author giraudsa
 *
 * @param <U> type of object
 * @param <K> type of key
 * @param <V> type of value
 */
public abstract class AbstractIndex<U, K, V> {
	protected boolean isEqual(K ancienneClef, K key) {
		if(ancienneClef != null && key != null)
			return ancienneClef.equals(key);
		return ancienneClef == null && key == null;

	}
	private static final TriKeyCache<AbstractIndex<?,?,?>, Long, Class<?>, Object> cache = new TriKeyCache<>(100000);

	private final BiKeyCache<Long, Class<?>, Object> cacheLocal =  new BiKeyCache<>();
	public static final long BOOLEAN_BYTES = 1;
	protected static final long NULL = -1L;
	protected static final long ROOT_POSITION_POSITION = 0L;
	private static final long IS_PRIMARY_POSITION = ROOT_POSITION_POSITION + Long.SIZE / 8;
	private static final long KEY_TYPE_POSITION = IS_PRIMARY_POSITION + BOOLEAN_BYTES;
	protected void addCache(Long position, Object o) {
		synchronized (cache) {
			cache.put(this, position, o.getClass(), o);
		}
	}
	@SuppressWarnings("unchecked")
	private <W> W readCache(long positionStuff, Class<W> typeStuff) {
		synchronized (cache) {
			W w = (W) cacheLocal.get(positionStuff, typeStuff);
			if(w == null)
				w = (W) cache.get(this, positionStuff, typeStuff);
			return w;
		}
	}
	
	
	private final boolean isPrimary;
	protected Path file;
	protected RandomAccessFile raf;
	protected final Class<K> keyType;
	protected final Class<V> valueType;
	protected final Class<U> objectType;
	protected final Store<U> store;
	private long endOfFile;
	protected long version;
	protected long rootPosition;
	private final ComputeKey<K, U> delegateKey;
	private final ComputeValue<V, U> delegateValue;
	private final long positionComputeKey; 
	protected final long positionEndOfHeaderInAbstractIndex;
	
	protected ComputeKey<K, U> getDelegateKey() {
		return delegateKey;
	}
	
	protected ComputeValue<V, U> getDelegateValeur() {
		return delegateValue;
	}
	
	
	@SuppressWarnings("unchecked")
	/**
	 * from file
	 * @param keyType
	 * @param valueType
	 * @param stockage
	 * @param datas
	 * @param delegateValeur
	 * @throws IOException
	 * @throws ForsException
	 * @throws ClassNotFoundException
	 */
	protected AbstractIndex(Class<V> valueType, Path file, Store<U> store, ComputeValue<V, U> delegateValue) throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException{
		this.file = file;
		newRandomAccessFile();
		this.endOfFile = raf.length();
		if(raf.length() < KEY_TYPE_POSITION + Short.SIZE / 8) throw new IndexInstanciationException("the file " + raf.toString() +" is corrupted");
		this.valueType = valueType;
		this.delegateValue = delegateValue;
		
		this.store = store;
		this.objectType = store.getObjectType();
		this.version = IndexedStorageManager.readVersion(file);
		this.rootPosition = getStuff(ROOT_POSITION_POSITION, Long.class, null);
		isPrimary = getStuff(IS_PRIMARY_POSITION, Boolean.class, null);
		this.keyType = (Class<K>) Class.forName(getStuff(KEY_TYPE_POSITION, String.class, null));
		positionComputeKey = KEY_TYPE_POSITION + Short.SIZE / 8 + keyType.getName().length();
		if(isPrimary) {
			this.delegateKey = (ComputeKey<K, U>)new GetId<>(objectType, store.getIdManager());
			positionEndOfHeaderInAbstractIndex = positionComputeKey;
		}else {
			seek(positionComputeKey);
			this.delegateKey =  ComputeKey.unmarshall(store.getMarshaller(), raf);
			positionEndOfHeaderInAbstractIndex = raf.getFilePointer();
		}
	}
	
	
	/**
	 * runtime creation
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
	protected AbstractIndex(Class<K> keyType, Class<V> valueType, Path fileStockage, Store<U> store, ComputeKey<K, U> delegateKey, ComputeValue<V, U> delegateValeur) throws IOException, StorageException, SerializationException{
		this.file = fileStockage;
		Files.createFile(file);
		newRandomAccessFile();
		this.keyType = keyType;
		this.valueType = valueType;
		this.delegateValue = delegateValeur;
		this.delegateKey = delegateKey;
		this.store = store;
		this.objectType = store.getObjectType();
		this.isPrimary = this instanceof PrimaryIndexFile;
		positionComputeKey = KEY_TYPE_POSITION + Short.SIZE / 8 + keyType.getName().length();
		this.positionEndOfHeaderInAbstractIndex = initFile();
	}
	
	protected abstract void rebuild(long version) throws IOException, StorageException, SerializationException;
	protected abstract void delete(String id, CacheModifications modifs) throws IOException, StorageException, SerializationException;
	protected abstract long getKeyPosition(K key,  CacheModifications modifs) throws IOException, StorageException, SerializationException;
	protected abstract void add(K key, V value, CacheModifications modifs) throws StorageException, IOException, SerializationException;
	protected abstract void addKeyToValue(K key, long keyPosition, V value, long valuePosition, CacheModifications modifs) throws IOException, StorageException, SerializationException;
	protected abstract void deleteKtoId(K key, V value, CacheModifications modifs) throws IOException, StorageException, SerializationException;
	protected void delete(String id, long version, CacheModifications modifs) throws IOException, StorageException, SerializationException{
		if(this.version > version)
			return;
		delete(id, modifs);
	}
	
	protected void add(U objectToAdd, long version, final CacheModifications modifs) throws StorageException, IOException, SerializationException{
		add(getKey(objectToAdd), computeValue(objectToAdd, version), modifs);//version is used in override method in primary index
	}
	protected void clear() throws IOException, StorageException, SerializationException {
		raf.setLength(0);
		cache.clear();
		initFile();
	}
	protected long initFile() throws IOException, StorageException, SerializationException {
		setRootPosition(NULL);
		write(IS_PRIMARY_POSITION, isPrimary);
		write(KEY_TYPE_POSITION, keyType.getName());
		if(!isPrimary)
			ComputeKey.marshall(delegateKey, raf, store.getMarshaller());
		return raf.getFilePointer();
	}
	
	protected CacheModifications deleteObjects(Collection<String> ids, long v) throws IOException, StorageException, SerializationException {
		CacheModifications modifs = new CacheModifications(this, v);
		for(String id : ids)
			delete(id, v, modifs);
		return modifs;
	}
	
	protected CacheModifications stockpile(Collection<U> us, long version) throws StorageException, IOException, SerializationException {
		CacheModifications modifs = new CacheModifications(this, version);
		for(U u : us)
			add(u, version, modifs);
		return modifs;
	}
	
	protected V computeValue(U object, long version) throws StorageException, IOException, SerializationException {
		return delegateValue.getValue(object, version);

	}

	protected K getKey(U object) throws StorageException {
		return delegateKey.getKey(object);
	}
	
	protected void checkVersion(long lastConsistentVersion) throws IOException, StorageException, SerializationException{
		if(version > lastConsistentVersion || store.getVersion() > version){
			rebuild(lastConsistentVersion);
		}
	}
	
	
	protected void seek(long position) throws IOException {
		raf.seek(position);
	}

	
	protected long getEndOfFile() {
		return endOfFile;
	}

	/**
	 * return positionBeforeWritting
	 * @param value
	 * @param modifs
	 * @return
	 * @throws SerializationException
	 */
	protected long writeFakeAndCache(Object value, CacheModifications modifs) throws SerializationException{
		return modifs.addToEnd(value); 
	}
	
	protected <W> W getStuff(long stuffPosition, Class<W> stuffType, CacheModifications modifs) throws IOException, StorageException, SerializationException{
		if(stuffPosition < 0)
			return null;
		W stuff = null;
		if(modifs != null) 
			stuff = modifs.getObjectFromRecentModifs(stuffPosition, stuffType);
		if(stuff == null) 
			stuff = readCache(stuffPosition, stuffType);
		if(stuff == null){
			stuff = readClass(stuffType, stuffPosition);
			addCache(stuffPosition, stuff);
		}
		return stuff;
	}
	
	@SuppressWarnings({"rawtypes" })
	private <N extends AbstractNode> AbstractNode<?, ?> readAbstractNode(long nodePosition, Class<N> nodeType) throws StorageException{
		try {
			Constructor<N> constr = nodeType.getConstructor(long.class, AbstractIndex.class, Class.class, Class.class);
			return constr.newInstance(nodePosition, this, keyType, valueType);
		} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | SecurityException | IllegalArgumentException | InvocationTargetException e) {
			throw new StorageException("the node type " + nodeType.getName() + " is not instanciable :"  + e.getMessage());
		}
	}
	
	protected synchronized void write(long position, Object value) throws IOException, StorageException{
		seek(position);
		Class<?> type = value.getClass();
		if (value instanceof Date)
			raf.writeLong(((Date)value).getTime());	
		else if (value instanceof String)
			raf.writeUTF((String)value);
		else if (value instanceof Number)
			writeNumber((Number)value);
		else if (value instanceof Boolean)
			raf.writeBoolean((Boolean)value);
		else if (value instanceof Character)
			raf.writeChar((char)value);
		else if (List.class.isAssignableFrom(type)) { //List<Double> for lonlat
			raf.writeDouble((Double)((List<?>)value).get(0));
			raf.writeDouble((Double)((List<?>)value).get(0));
		}
		else
			throw new StorageException("type not implemented " + type.getName());
		endOfFile = raf.length();
	}
	
	private void writeNumber(Number value) throws IOException {
		//Byte, Double, Float, Integer, Long, and Short.
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
	@SuppressWarnings("unchecked")
	private synchronized <T> T readClass(Class<T> type, long position) throws IOException, StorageException, SerializationException {
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
		if (List.class.isAssignableFrom(type)) {//List<Double> for lonlat
			List<Double> l = new ArrayList<>();
			l.add(raf.readDouble());
			l.add(raf.readDouble());
		}
		if (AbstractNode.class.isAssignableFrom(type))
			return (T)readAbstractNode(position, (Class<? extends AbstractNode<?, ?>>) type);
		return store.getMarshaller().unserialize(type, raf);
	}
	
	protected synchronized void setVersion(long ver) throws IOException, StorageException{
		if(store.getVersion() < ver)
			store.setVersion(ver);
		this.version = ver;
		raf.close();
		file = Files.move(file, IndexedStorageManager.nextVersion(file, version), ATOMIC_MOVE);
		newRandomAccessFile();
	}
	private void newRandomAccessFile() throws IOException, StorageException {
		if(!file.toFile().exists())
			throw new StorageException("the file doesn't exist, permission pb ? : " + file.toString());
		raf = new RandomAccessFile(file.toFile(), "rw");
		endOfFile = raf.length();
	}
	
	
	private void setRootPosition(long positionRacine) throws IOException, StorageException {
		this.rootPosition = positionRacine;
		write(ROOT_POSITION_POSITION, positionRacine);
	}

}
