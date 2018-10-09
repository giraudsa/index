package com.chronosave.index.storage.file;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.UUID;

import com.chronosave.index.storage.condition.GetId;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.utils.CloseableIterator;
import com.chronosave.index.utils.ReadWriteLock;

public class PersistentIdSet<U> extends Index1D<U, String, String, String> implements Closeable {

	protected PersistentIdSet(final Store<U> store) throws IOException, StorageException, SerializationException {
		super(String.class, String.class, Paths.get(UUID.randomUUID().toString()), store, new GetId<U>(store.getIdManager()), new GetId<U>(store.getIdManager()));
	}

	public void addId(final String id) throws IOException, StorageException, SerializationException {
		final CacheModifications modifs = new CacheModifications(this, 0);
		final long idPosition = writeFakeAndCache(id, modifs);
		setRoot(getRoot(modifs).addAndBalance(id, idPosition, null, modifs), modifs);
		modifs.writeWithoutChangingVersion();
	}

	@Override
	public void close() throws IOException {
		removeFile();
	}

	public boolean contains(final String id) throws IOException, StorageException, SerializationException {
		return getRoot(null).findNode(id, null) != null;
	}

	@Override
	public CloseableIterator<String> getBetween(final String min, final String max, final ReadWriteLock locker) throws IOException, StorageException, SerializationException, InterruptedException {
		throw new InterruptedException("internal use only");
	}

	@Override
	protected void addKeyToValue(final String key, final long keyPosition, final String value, final long valuePosition, final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		// nothing to do
	}

	@Override
	protected AbstractNode<String, String> createFakeNode(final CacheModifications modifs) {
		return new SingletonNode<>(getKeyType(), this, modifs);
	}

	// NOTHING TO DO
	@Override
	protected void delete(final String id, final CacheModifications modifs) {
		// nothing to do
	}

	@Override
	protected void deleteKtoId(final String key, final String value, final CacheModifications modifs) {
		// nothing to do
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Class<? extends AbstractNode<String, String>> getNodeType() {
		return (Class<? extends AbstractNode<String, String>>) SingletonNode.class;
	}

	@Override
	protected <N extends AbstractNode<?, ?>> AbstractNode<?, ?> readAbstractNode(final long nodePosition, final Class<N> nodeType) {
		return new SingletonNode<>(nodePosition, this, getKeyType());
	}
}
