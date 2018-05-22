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
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StorageRuntimeException;
import com.chronosave.index.storage.exception.StoreException;
import com.chronosave.index.utils.CloseableIterator;
import com.chronosave.index.utils.MultivalueMap;
import com.chronosave.index.utils.ReadWriteLock;

public class IndexedStorageManager {
	private static final Set<Path> ALLREADY_USED_BASE_PATH = new HashSet<>();
	private static final String FILE_VERSION = "version.stockage";
	public static final String POINT = ".";

	protected static File[] findAllFileThatBeginsWith(final Path basePath, final String prefix) {
		return basePath.toFile().listFiles(new FilenameFilter() {
			@Override
			public boolean accept(final File dir, final String name) {
				return name.startsWith(prefix);
			}
		});
	}

	protected static Path findFirstFileThatBeginsWith(final Path basePath, final String prefix) {
		final File[] candidates = basePath.toFile().listFiles(new FilenameFilter() {
			@Override
			public boolean accept(final File dir, final String name) {
				return name.startsWith(prefix);
			}
		});
		if (candidates.length > 0)
			return candidates[0].toPath();
		else
			return null;
	}

	protected static synchronized Path nextVersion(final Path origine, final long newVersion) {
		final String name = origine.getFileName().toString();
		final String base = name.substring(0, name.lastIndexOf(POINT));
		final String newNameString = base + POINT + newVersion;
		final Path dir = origine.getParent();
		final Path fn = origine.getFileSystem().getPath(newNameString);
		return dir == null ? fn : dir.resolve(fn);
	}

	protected static Path nextVersion(final Path origine, final long fileSize, final long newVersion) {
		final String name = origine.getFileName().toString();
		final String base = name.substring(0, name.lastIndexOf(POINT));
		final String subBase = base.substring(0, base.lastIndexOf(POINT));
		final String newNameString = subBase + POINT + fileSize + POINT + newVersion;
		final Path dir = origine.getParent();
		final Path fn = origine.getFileSystem().getPath(newNameString);
		return dir == null ? fn : dir.resolve(fn);
	}

	protected static long readVersion(final Path file) throws StorageException {
		if (!file.toFile().exists())
			throw new StorageException(
					"cannot find the version of a file that does not exist " + file.getFileName().toString());
		final String name = file.getFileName().toString();
		final String versionString = name.substring(name.lastIndexOf(POINT) + 1);
		return Long.valueOf(versionString);
	}

	protected final Path basePath;
	private final Map<Class<?>, Store<?>> classToStores = new HashMap<>();
	private final IdManager idManager;
	private long lastVersion;
	private final ReadWriteLock locker = new ReadWriteLock();
	private final SerializationStore marshaller;
	private Path version;

	public IndexedStorageManager(final SerializationStore marshaller, final IdManager idManager, final Path directory)
			throws StoreException {
		try {

			if (ALLREADY_USED_BASE_PATH.contains(directory))
				throw new StoreException(directory.toString() + " directory is allready in use !");
			ALLREADY_USED_BASE_PATH.add(directory);
			this.idManager = idManager;
			basePath = directory;
			this.marshaller = marshaller;
			if (!Files.exists(basePath))
				Files.createDirectory(basePath);
			version = findFirstFileThatBeginsWith(basePath, FILE_VERSION);
			if (version == null) {
				version = Paths.get(basePath.toString(), FILE_VERSION + ".0");
				Files.createFile(version);
			}
			lastVersion = readVersion(version);
			for (final Class<?> clazz : findAllClasses())
				classToStores.put(clazz, new Store<>(basePath, clazz, marshaller, idManager, lastVersion));
		} catch (final Exception e) {
			throw new StoreException("impossible to create the StorageManager", e);
		}
	}

	/**
	 * cleaning of storage and index files.
	 * 
	 * @throws StoreException
	 */
	public void cleanFiles() throws StoreException {
		try {
			locker.lockWrite();
			++lastVersion;
			for (final Store<?> store : classToStores.values())
				store.clean(lastVersion);
			version = Files.move(version, nextVersion(version, lastVersion), ATOMIC_MOVE);
			locker.unlockWrite();
		} catch (final Exception e) {
			throw new StoreException(e);
		}
	}

	/**
	 * remove objects from files and indexes
	 * 
	 * @param objects
	 * @throws StoreException
	 */
	@SuppressWarnings("unchecked")
	public <U extends Object> void delete(final Map<String, Class<?>> objects) throws StoreException {
		try {
			locker.lockWrite();
			final Collection<CacheModifications> toDo = new ArrayList<>();
			++lastVersion;
			final MultivalueMap<Store<?>, String> sortedList = trier(objects);
			for (final Map.Entry<Store<?>, LinkedHashSet<String>> l : sortedList.entrySet()) {
				final Store<U> store = (Store<U>) l.getKey();
				final Collection<String> us = l.getValue();
				store.delete(us, lastVersion, toDo);
			}
			for (final CacheModifications modifs : toDo)
				modifs.write();
			version = Files.move(version, nextVersion(version, lastVersion), ATOMIC_MOVE);
			locker.unlockWrite();
		} catch (final Exception e) {
			throw new StoreException(e);
		}
	}

	private Set<Class<?>> findAllClasses() {
		final File[] files = basePath.toFile().listFiles(new FilenameFilter() {
			@Override
			public boolean accept(final File dir, final String name) {
				return name.contains(PrimaryIndexFile.EXTENTION);
			}
		});
		final Set<Class<?>> ret = new HashSet<>();
		for (final File file : files) {
			String name = file.getName();
			name = name.substring(0, name.lastIndexOf(PrimaryIndexFile.EXTENTION));
			try {
				final Class<?> clazz = Class.forName(name);
				ret.add(clazz);
			} catch (final ClassNotFoundException e) {
				throw new StorageRuntimeException(
						"Here, the " + file.getName() + " file doesn't match any known class.", e);
			}
		}
		return ret;
	}

	/**
	 * recovers a persisted object in the file with its id allows concurrent read
	 * access.
	 * 
	 * @param id
	 * @param type
	 * @return
	 * @throws StoreException
	 */
	@SuppressWarnings("unchecked")
	public <U> U getObject(final String id, final Class<U> type) throws StoreException {
		try {
			locker.lockRead();
			final Store<?> store = getStore(type);
			final U u = (U) store.getObjectById(id);
			locker.unlockRead();
			return u;
		} catch (final Exception e) {
			throw new StoreException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private synchronized <U> Store<U> getStore(final Class<U> clazz)
			throws ClassNotFoundException, IOException, StorageException, SerializationException, StoreException {
		if (!classToStores.containsKey(clazz))
			classToStores.put(clazz, new Store<>(basePath, clazz, marshaller, idManager, lastVersion));
		return (Store<U>) classToStores.get(clazz);
	}

	/**
	 * queries files via indexes. Allows concurrent read access. Do not forget to
	 * close the iterator at the end of its use (locks the files in writing)
	 * 
	 * @param condition
	 * @return
	 * @throws StoreException
	 */
	public <U> CloseableIterator<String> selectIdFromClassWhere(final AbstractCondition<?, U> condition)
			throws StoreException {
		try {
			locker.lockRead();
			final Store<U> store = getStore(condition.getTypeObjet());
			return store.selectIdFromClassWhere(condition, locker);
		} catch (final Exception e) {
			throw new StoreException(e);
		}
	}

	/**
	 * allows objects to persist on the disk. This method waits until the current
	 * read or write is complete before writing.
	 * 
	 * @param objets
	 * @throws StoreException
	 */
	@SuppressWarnings("unchecked")
	public <U> void store(final Collection<U> objets) throws StoreException {
		try {
			locker.lockWrite();
			++lastVersion;
			final Collection<CacheModifications> aFaire = new ArrayList<>();
			final MultivalueMap<Store<?>, Object> listeTrier = trier(objets);
			for (final Map.Entry<Store<?>, LinkedHashSet<Object>> l : listeTrier.entrySet()) {
				final Store<U> store = (Store<U>) l.getKey();
				final Collection<U> v = (Collection<U>) l.getValue();
				store.store(v, lastVersion, aFaire);
			}
			for (final CacheModifications modifs : aFaire)
				modifs.write();
			version = Files.move(version, nextVersion(version, lastVersion), ATOMIC_MOVE);
			locker.unlockWrite();
		} catch (final Exception e) {
			throw new StoreException(e);
		}
	}

	private <U extends Object> MultivalueMap<Store<?>, Object> trier(final Collection<U> objets)
			throws ClassNotFoundException, IOException, StorageException, SerializationException, StoreException {
		final MultivalueMap<Store<?>, Object> ret = new MultivalueMap<>();
		for (final Object mo : objets)
			ret.add(getStore(mo.getClass()), mo);
		return ret;
	}

	private MultivalueMap<Store<?>, String> trier(final Map<String, Class<?>> objects)
			throws ClassNotFoundException, IOException, StorageException, SerializationException, StoreException {
		final MultivalueMap<Store<?>, String> ret = new MultivalueMap<>();
		for (final String id : objects.keySet())
			ret.add(getStore(objects.get(id)), id);
		return ret;
	}

}
