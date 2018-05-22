package com.chronosave.index.storage.file;

import static com.chronosave.index.storage.file.IndexedStorageManager.POINT;
import static com.chronosave.index.storage.file.IndexedStorageManager.nextVersion;
import static com.chronosave.index.storage.file.IndexedStorageManager.readVersion;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;

import com.chronosave.index.externe.SerializationStore;
import com.chronosave.index.storage.exception.SerializationException;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StorageRuntimeException;
import com.chronosave.index.utils.CloseableIterator;

public class DataFile<U> {

	protected class ParcoursObjectsMaxVersion implements CloseableIterator<U> {
		private final PersistentIdSet<U> dejaVu;
		private U next;
		private Path oldFile;
		private Long position;
		private RandomAccessFile raf;
		private final long versionMax;

		public ParcoursObjectsMaxVersion(final long maxVersion, final Store<U> s)
				throws IOException, StorageException, SerializationException {
			setOld();
			this.versionMax = maxVersion;
			this.dejaVu = new PersistentIdSet<>(s.getObjectType(), s);
			position = readPosition(file);
			cacheNext();
		}

		private void cacheNext() throws IOException, StorageException, SerializationException {
			next = null;
			while (next == null && position >= 0) {
				if (position > Long.BYTES) {
					raf.seek(position - Long.BYTES);
					position = raf.readLong();
				} else {
					raf.seek(position);
					position = -1L;
				}
				final long v = raf.readLong();
				if (v > versionMax)
					continue;
				final boolean suppr = raf.readBoolean();
				final String id = raf.readUTF();
				final boolean djv = dejaVu.contains(id);
				if (!djv)
					dejaVu.addId(id);
				if (!djv && !suppr)
					next = marshaller.unserialize(getObjectType(), raf);
			}
		}

		@Override
		public void close() throws IOException {
			dejaVu.close();
		}

		private void deleteFileWhenFinish() throws IOException {
			if (next != null)
				return;
			raf.close();
			setVersion(versionMax);
			Files.delete(oldFile);
		}

		@Override
		public boolean hasNext() {
			try {
				deleteFileWhenFinish();
			} catch (final IOException e) {
				throw new StorageRuntimeException("impossible to delete the old file", e);
			}
			return next == null;
		}

		@Override
		public U next() {
			try {
				if (next == null)
					throw new NoSuchElementException("when there's no more...");
				final U ret = next;
				cacheNext();
				return ret;
			} catch (IOException | StorageException | SerializationException e) {
				try {
					Files.move(oldFile, file, ATOMIC_MOVE);
					throw new StorageRuntimeException(e);
				} catch (final IOException e1) {
					throw new StorageRuntimeException(
							"impossible to rename the old File " + oldFile.toString() + " in " + file.toString(), e1);
				}
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		public void setOld() throws IOException {
			ra.close();
			oldFile = Paths.get(file.toString().replaceAll(".data", ".old"));
			Files.move(file, oldFile, ATOMIC_MOVE);
			Files.createFile(file);
			ra = new RandomAccessFile(file.toFile(), "rw");
			raf = new RandomAccessFile(oldFile.toFile(), "r");
		}
	}

	protected static long readPosition(final Path dataFile) throws StorageException {
		if (!dataFile.toFile().exists())
			throw new StorageException(
					"impossible to find the version of a file that doesn't exist " + dataFile.getFileName().toString());
		final String name = dataFile.getFileName().toString();
		final String base = name.substring(0, name.lastIndexOf(POINT));
		final String stringPosition = base.substring(base.lastIndexOf(POINT) + 1);
		return Long.valueOf(stringPosition);
	}

	private Path file;
	private final SerializationStore marshaller;
	private final Class<U> objectType;
	private RandomAccessFile ra;

	private long version;

	public DataFile(final Path basePath, final Class<U> u, final SerializationStore marshaller)
			throws IOException, StorageException {
		this.objectType = u;
		this.marshaller = marshaller;
		file = IndexedStorageManager.findFirstFileThatBeginsWith(basePath, u.getName() + ".data");
		if (file == null) {
			file = Paths.get(basePath.toString(), u.getName() + ".datas.0.0");
			Files.createFile(file);
		}
		ra = new RandomAccessFile(file.toFile(), "rw");
		version = readVersion(file);
	}

	protected String debutNomFichier() {
		return objectType.getName();
	}

	public void delete(final String id, final long version) throws IOException {
		final long debutSerialisation = ra.length();
		ra.writeLong(version);
		ra.writeBoolean(true); // is deleted
		ra.writeUTF(id);
		ra.writeLong(debutSerialisation);
	}

	public CloseableIterator<U> getAllObjectsWithMaxVersionLessThan(final long version, final Store<U> s)
			throws IOException, StorageException, SerializationException {
		return new ParcoursObjectsMaxVersion(version, s);
	}

	public U getObject(final long beginingOfSerializedObject) throws IOException, SerializationException {
		ra.skipBytes(Long.BYTES); // We jump the version
		final boolean suppr = ra.readBoolean();
		if (suppr)
			return null;
		ra.readUTF();// id
		return marshaller.unserialize(getObjectType(), ra);// the id is read in the description
	}

	protected Class<U> getObjectType() {
		return objectType;
	}

	long getVersion() {
		return version;
	}

	void setVersion(final long version) throws IOException {
		this.version = version;
		final long size = ra.length();
		ra.close();
		file = Files.move(file, nextVersion(file, size, version), ATOMIC_MOVE);
		ra = new RandomAccessFile(file.toFile(), "rw");
	}

	public long writeData(final String id, final U obj, final long version) throws IOException, SerializationException {
		final long beginingOfSerialisation = ra.length();
		ra.seek(beginingOfSerialisation);
		ra.writeLong(version);
		ra.writeBoolean(false); // is not delete
		ra.writeUTF(id);
		marshaller.serialize(obj, ra);
		ra.writeLong(beginingOfSerialisation);
		return beginingOfSerialisation;
	}
}
