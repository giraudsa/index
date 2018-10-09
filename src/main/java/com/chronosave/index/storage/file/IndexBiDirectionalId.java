package com.chronosave.index.storage.file;

import java.io.IOException;
import java.nio.file.Path;

import com.chronosave.index.storage.condition.ComputeKey;
import com.chronosave.index.storage.condition.GetId;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StoreException;

/***
 *
 * @author giraudsa
 *
 * @param <U>
 *            Object
 * @param <K>
 *            Key
 * @param <N>
 *            Value type of Node
 * @param <R>
 *            Value type of Reverse node
 */
public abstract class IndexBiDirectionalId<U, K, N, R> extends Index1D<U, K, String, N> {

	private final long reverseRootPositionposition;
	protected long reverseRootPosition;

	// runtime
	protected IndexBiDirectionalId(final Path basePath, final Class<K> keyType, final Store<U> store, final String extention, final ComputeKey<K, U> delegateKey) throws IOException, StorageException, SerializationException {
		super(keyType, String.class, getPath(basePath, store.debutNomFichier(), extention, delegateKey), store, delegateKey, new GetId<>(store.getIdManager()));
		reverseRootPositionposition = getPositionEndOfHeaderInAbstractIndex();
		setReverseRootPosition(NULL);
		rebuild(store.getVersion());
	}

	// file
	protected IndexBiDirectionalId(final Path file, final Store<U> store) throws IOException, StorageException, ClassNotFoundException, SerializationException, StoreException {
		super(String.class, file, store, new GetId<>(store.getIdManager()));
		reverseRootPositionposition = getPositionEndOfHeaderInAbstractIndex();
		reverseRootPosition = getStuff(reverseRootPositionposition, Long.class, null);
	}

	private void setReverseRootPosition(final long positionRacineInverse) throws IOException, StorageException {
		this.reverseRootPosition = positionRacineInverse;
		write(reverseRootPositionposition, positionRacineInverse);
	}

	@Override
	protected void add(final K key, final long keyPosition, final String id, final long idPosition, final CacheModifications modifs) throws StorageException, IOException, SerializationException {
		addKeyToValue(key, keyPosition, id, idPosition, modifs);
		addValueToKey(id, idPosition, key, keyPosition, modifs);
	}

	protected void addValueToKey(final String id, final long idPosition, final K key, final long keyPosition, final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setReverseRoot(getReverseRoot(modifs).addAndBalance(id, idPosition, keyPosition, modifs), modifs);
	}

	protected abstract AbstractNode<String, R> createFakeReverseNode(CacheModifications modifs);

	protected void delete(final K key, final String id, final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		deleteKtoId(key, id, modifs);
		deleteIdtoK(id, key, modifs);
	}

	protected void deleteIdtoK(final String id, final K key, final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		setReverseRoot(getReverseRoot(modifs).deleteAndBalance(id, modifs), modifs);
	}

	protected long getIdPosition(final String id, final CacheModifications modifs) throws IOException, StorageException, SerializationException {
		final AbstractNode<String, ?> reverseNode = getReverseRoot(modifs).findNode(id, modifs);
		return reverseNode == null ? writeFakeAndCache(id, modifs) : reverseNode.valuePosition(modifs);
	}

	protected abstract Class<? extends AbstractNode<String, R>> getReverseNodeType();

	protected AbstractNode<String, R> getReverseRoot(final CacheModifications modifs) throws IOException, SerializationException {
		if (reverseRootPosition == NULL) // Fake node
			return createFakeReverseNode(modifs);

		return getStuff(reverseRootPosition, getReverseNodeType(), modifs);
	}

	@Override
	protected long initFile() throws IOException, StorageException, SerializationException {
		final long positionEndOfHeaderInAbstractIndex = super.initFile();
		setReverseRootPosition(NULL);
		return positionEndOfHeaderInAbstractIndex;
	}

	protected void setReverseRoot(final AbstractNode<String, R> node, final CacheModifications modifs) {
		reverseRootPosition = node == null ? NULL : node.getPosition();
		modifs.add(reverseRootPositionposition, reverseRootPosition);
	}
}
