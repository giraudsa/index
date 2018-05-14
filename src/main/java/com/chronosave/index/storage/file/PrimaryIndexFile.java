package com.chronosave.index.storage.file;

import java.io.IOError;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.chronosave.index.storage.condition.GetId;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StorageRuntimeException;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StoreException;

public class PrimaryIndexFile<U> extends Index1D<U, String, Long> implements Iterable<String> {
	
	public static final String EXTENTION = ".primaryIdx";
	
	public static Path getFile(Path basePath, DataFile<?> data) {
		if(data == null) return null;
		return IndexedStorageManager.findFirstFileThatBeginsWith(basePath, data.debutNomFichier() + EXTENTION);
	}
	
	private static Path getPath(Path basePath, String debutNom) {
		return Paths.get(basePath.toString(), debutNom + EXTENTION + ".0");
	}
	
	@SuppressWarnings("rawtypes")
	private static final Class<SimpleNode> simpleNodeType = SimpleNode.class;
	
	/**
	 * file
	 * @param store
	 * @param datas
	 * @throws IOException
	 * @throws StorageException
	 * @throws ClassNotFoundException
	 * @throws SerializationException 
	 * @throws StoreException 
	 */
	protected PrimaryIndexFile(Path file, Store<U> store, long lastGoodVersion) throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException {
		super(Long.class, file, store, new GetIndexedObjectInDataFile<>(store));
		checkVersion(lastGoodVersion);
	}
	
	/**
	 * Creation au runtime
	 * @param datas
	 * @throws IOException
	 * @throws StorageException
	 * @throws SerializationException 
	 */
	protected PrimaryIndexFile(Store<U> store, Path basePath) throws IOException, StorageException, SerializationException{
		super(String.class, Long.class, getPath(basePath, store.debutNomFichier()), store, new GetId<>(store.getObjectType(), store.getIdManager()),new GetIndexedObjectInDataFile<>(store));
	}
	
	@SuppressWarnings("unchecked")
	private Long getPositionInDataFile(String id, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		AbstractNode<String, Long> node = (AbstractNode<String, Long>)getRoot(modifs).findNode(id, modifs);
		if(node == null) return null;
		return node.getValue(modifs);
	}
	
	protected U getValue(String id) throws IOException, StorageException, SerializationException {
		Long position = getPositionInDataFile(id, null);
		if(position == null) return null;
		return store.read(position);
	}

	
	@SuppressWarnings("unchecked") @Override
	public Iterator<String> iterator() {
		try {
			return new NodeIterator((Node1D<String, Long>)getRoot(null));
		} catch (IOException e) {
			throw new IOError(e);
		} catch (StorageException | SerializationException e) {
			throw new StorageRuntimeException(e);
		}
	}
	
	@Override
	protected void rebuild(long version) throws IOException, StorageException, SerializationException {
		clear();
		Iterator<U> iterator = store.getAllObjectsWithMaxVersionLessThan(version);
		while(iterator.hasNext()) {
			CacheModifications modifs = new CacheModifications(this, version);
			add(iterator.next(), version, modifs);
			modifs.writeWithoutChangingVersion();
		}
		setVersion(version);
	}
	
	@SuppressWarnings("rawtypes")
	protected Class<? extends AbstractNode> getNodeType() {
		return simpleNodeType;
	}

	@Override
	protected void delete(U object, long version, CacheModifications modifs) throws IOException, StorageException, SerializationException {
		store.delete(object, version);
		setRoot(getRoot(modifs).deleteAndBalance(getKey(object), modifs), modifs);
	}
	
	private class NodeIterator implements Iterator<String>{

		private final Iterator<String> idIterator;
		
		private String next;
		
		private NodeIterator(final Node1D<String, Long> root) {
			idIterator = root.keyIterator();
			cacheNext();
		}
		
		private void cacheNext() {
			while(idIterator.hasNext()){
				next = idIterator.next();
				if(next != null)
					break;
			}
		}

		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public String next() {
			if(next == null)
				throw new NoSuchElementException();
			String u = next;
			cacheNext();
			return u;
		}
		
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}		
	}
}
