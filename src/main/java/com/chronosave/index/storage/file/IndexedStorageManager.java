package com.chronosave.index.storage.file;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.chronosave.index.externe.IdManager;
import com.chronosave.index.externe.SerializationStore;
import com.chronosave.index.storage.condition.AbstractCondition;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StorageRuntimeException;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StoreException;
import com.chronosave.index.utils.CloseableIterator;
import com.chronosave.index.utils.MultivalueMap;
import com.chronosave.index.utils.ReadWriteLock;

public class IndexedStorageManager {
	public static final String POINT = ".";
	private static final String FILE_VERSION = "version.stockage";
	private static final Set<Path> ALLREADY_USED_BASE_PATH = new HashSet<>();
	
	protected static Path findFirstFileThatBeginsWith(Path basePath, final String prefix) {
		File[] candidates = basePath.toFile().listFiles(new FilenameFilter()
		{
			  public boolean accept(File dir, String name)
			  {
			     return name.startsWith(prefix);
			  }
		});
		if(candidates.length > 0)
			return candidates[0].toPath();
		else return null;
	}
	
	protected static File[] findAllFileThatBeginsWith(Path basePath, final String prefix) {
		return basePath.toFile().listFiles(new FilenameFilter()
		{
			  public boolean accept(File dir, String name)
			  {
			     return name.startsWith(prefix);
			  }
		});
	}
	
	protected static long readVersion(Path file) throws StorageException {
		if(!file.toFile().exists())
			throw new StorageException("cannot find the version of a file that does not exist " + file.getFileName().toString());
		String name = file.getFileName().toString();
		String versionString =name.substring(name.lastIndexOf(POINT )+1);
		return Long.valueOf(versionString);
	}
	
	protected static synchronized Path nextVersion(Path origine, long newVersion) {
		String name = origine.getFileName().toString();
		String base = name.substring(0, name.lastIndexOf(POINT));
		String newNameString = base + POINT + newVersion;
		Path dir = origine.getParent();
		Path fn = origine.getFileSystem().getPath(newNameString);
		return (dir == null) ? fn : dir.resolve(fn);
	}
	
	protected static Path nextVersion(Path origine, long fileSize, long newVersion) {
		String name = origine.getFileName().toString();
		String base = name.substring(0, name.lastIndexOf(POINT));
		String subBase = base.substring(0, base.lastIndexOf(POINT));
		String newNameString = subBase + POINT + fileSize + POINT + newVersion;
		Path dir = origine.getParent();
		Path fn = origine.getFileSystem().getPath(newNameString);
		return (dir == null) ? fn : dir.resolve(fn);
	}
	
	private Set<Class<?>> findAllClasses() {
		File[] files = basePath.toFile().listFiles(new FilenameFilter()
		{
			  public boolean accept(File dir, String name)
			  {
			     return name.contains(PrimaryIndexFile.EXTENTION);
			  }
		});
		Set<Class<?>> ret = new HashSet<>();
		for(File file : files) {
			String name = file.getName();
			name = name.substring(0, name.lastIndexOf(PrimaryIndexFile.EXTENTION));
			try{
				Class<?> clazz = Class.forName(name);
				ret.add(clazz);
			}catch(ClassNotFoundException e) {
				throw new StorageRuntimeException("Here, the " + file.getName() + " file doesn't match any known class.", e);
			}
		}
		return ret;
	}
	private final ReadWriteLock locker = new ReadWriteLock();
	protected final Path basePath;
	private final IdManager idManager;
	private Path version;
	private long lastVersion;
	private final SerializationStore marshaller;
	private final Map<Class<?>, Store<?>> classToStores = new HashMap<>();
	
	public IndexedStorageManager(SerializationStore marshaller, IdManager idManager, Path directory) throws StoreException {
		try {
			
			if(ALLREADY_USED_BASE_PATH.contains(directory))
				throw new StoreException(directory.toString() + " directory is allready in use !");
			ALLREADY_USED_BASE_PATH.add(directory);
			this.idManager = idManager;
			this.basePath = directory;
			this.marshaller = marshaller;
			if(!Files.exists(basePath))
				Files.createDirectory(basePath);
			version = findFirstFileThatBeginsWith(basePath, FILE_VERSION);
			if(version == null) {
				version = Paths.get(basePath.toString(), FILE_VERSION + ".0");
				Files.createFile(version);
			}
			lastVersion = readVersion(version);
			for(Class<?> clazz : findAllClasses()) {
				classToStores.put(clazz, new Store<>(basePath, clazz, marshaller, idManager, lastVersion));
			}
		}catch(Exception e) {
			throw new StoreException("impossible to create the StorageManager", e);
		}
	}

	/**
	 * allows objects to persist on the disk. This method waits until the current read or write is complete before writing.
	 * @param objets
	 * @throws StoreException
	 */
	@SuppressWarnings("unchecked")
	public <U> void store(Collection<U> objets) throws StoreException {
		try {
			locker.lockWrite();
			++lastVersion;
			Collection<CacheModifications> aFaire = new ArrayList<>();
			MultivalueMap<Store<?>, Object> listeTrier = trier(objets);
			for(Map.Entry<Store<?>, LinkedHashSet<Object>> l : listeTrier.entrySet()) {
				Store<U> store = (Store<U>) l.getKey();
				Collection<U> v = (Collection<U>) l.getValue();
				store.store(v, lastVersion, aFaire);
			}
			for(CacheModifications modifs : aFaire) {
				modifs.write();
			}
			version = Files.move(version, nextVersion(version, lastVersion), ATOMIC_MOVE);
			locker.unlockWrite();
		}catch(Exception e) {
			throw new StoreException(e);
		}
	}
	/**
	 * recovers a persisted object in the file with its id
	 * allows concurrent read access.
	 * @param id
	 * @param type
	 * @return
	 * @throws StoreException
	 */
	@SuppressWarnings("unchecked")
	public <U> U getObject(String id, Class<U> type) throws StoreException {
		try{
			locker.lockRead();
			Store<?> store = getStore(type);
			U u = (U) store.getObjectById(id);
			locker.unlockRead();
			return u;
		}catch(Exception e) {
			throw new StoreException(e);
		}
	}
	
	/**
	 * remove objects from files and indexes
	 * @param objects
	 * @throws StoreException
	 */
	@SuppressWarnings("unchecked")
	public <U extends Object> void delete(Map<String, Class<?>> objects) throws StoreException {
		try {
			locker.lockWrite();
			Collection<CacheModifications> toDo = new ArrayList<>();
			++lastVersion;
			MultivalueMap<Store<?>, String> sortedList = trier(objects);
			for(Map.Entry<Store<?>, LinkedHashSet<String>> l : sortedList.entrySet()) {
				Store<U> store = (Store<U>) l.getKey();
				Collection<String> us = (Collection<String>) l.getValue();
				store.delete(us, lastVersion, toDo);
			}
			for(CacheModifications modifs : toDo)
				modifs.write();
			version = Files.move(version, nextVersion(version, lastVersion), ATOMIC_MOVE);
			locker.unlockWrite();
		}catch(Exception e) {
			throw new StoreException(e);
		}
	}
	
	/**
	 * queries files via indexes.
	 * Allows concurrent read access.
	 * Do not forget to close the iterator at the end of its use (locks the files in writing)
	 * @param condition
	 * @return
	 * @throws StoreException
	 */
	public <U> CloseableIterator<String> selectIdFromClassWhere(AbstractCondition<?,U> condition) throws StoreException{
		try {
			locker.lockRead();
			Store<U> store = getStore(condition.getTypeObjet());
			return store.selectIdFromClassWhere(condition, locker);
		}catch(Exception e) {
			throw new StoreException(e);
		}
	}
	
	/**
	 * cleaning of storage and index files.
	 * @throws StoreException
	 */
	public void cleanFiles() throws StoreException {
		try {
			locker.lockWrite();
			++lastVersion;
			for(Store<?> store : classToStores.values()) {
				store.clean(lastVersion);
			}
			version = Files.move(version, nextVersion(version, lastVersion), ATOMIC_MOVE);
			locker.unlockWrite();
		}catch(Exception e) {
			throw new StoreException(e);
		}
	}
	
	private MultivalueMap<Store<?>, String> trier(Map<String, Class<?>> objects) throws ClassNotFoundException, IOException, StorageException, SerializationException, StoreException {
		MultivalueMap<Store<?>, String> ret = new MultivalueMap<>();
		for(String id : objects.keySet()) {
			ret.add(getStore(objects.get(id)), id);
		}
		return ret;
	}

	private <U extends Object> MultivalueMap<Store<?>, Object> trier(Collection<U> objets) throws ClassNotFoundException, IOException, StorageException, SerializationException, StoreException {
		MultivalueMap<Store<?>, Object> ret = new MultivalueMap<>();
		for(Object mo : objets) {
			ret.add(getStore(mo.getClass()), mo);
		}
		return ret;
	}
	
	@SuppressWarnings("unchecked")
	private synchronized <U> Store<U> getStore(Class<U> clazz) throws ClassNotFoundException, IOException, StorageException, SerializationException, StoreException{
		if(!classToStores.containsKey(clazz))
			classToStores.put(clazz, new Store<>(basePath, clazz, marshaller, idManager, lastVersion));
		return (Store<U>) classToStores.get(clazz);
	}
	
	
}
