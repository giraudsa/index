package com.chronosave.index.storage.file;

import static com.chronosave.index.storage.file.IndexedStorageManager.*;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;

import com.chronosave.index.externe.SerializationStore;
import com.chronosave.index.storage.exception.StorageException;
import com.chronosave.index.storage.exception.StorageRuntimeException;
import com.chronosave.index.utils.CloseableIterator;
import com.chronosave.index.storage.exception.SerializationException;

public class DataFile<U>{
	
	protected static long readPosition(Path dataFile) throws StorageException {
		if(!dataFile.toFile().exists())
			throw new StorageException("impossible to find the version of a file that doesn't exist " + dataFile.getFileName().toString());
		String name = dataFile.getFileName().toString();
		String base =name.substring(0, name.lastIndexOf(POINT));
		String stringPosition = base.substring(base.lastIndexOf(POINT) + 1);
		return Long.valueOf(stringPosition);
	}
	private final Class<U> objectType;
	private final SerializationStore marshaller;
	private Path file;
	private RandomAccessFile ra;
	private long version;
	
	public DataFile(Path basePath, final Class<U> u, SerializationStore marshaller) throws IOException, StorageException{
		this.objectType = u;
		this.marshaller = marshaller;
		file = IndexedStorageManager.findFirstFileThatBeginsWith(basePath, u.getName() + ".data");
		if(file == null){
			file =  Paths.get(basePath.toString(), u.getName() + ".datas.0.0");
			Files.createFile(file);
		}
		ra = new RandomAccessFile(file.toFile(), "rw");
		version = readVersion(file);
	}
	
	void  setVersion(long version) throws IOException {
		this.version = version;
		long size = ra.length();
		ra.close();
		file = Files.move(file, nextVersion(file, size, version), ATOMIC_MOVE);
		ra = new RandomAccessFile(file.toFile(), "rw");
	}

	public U getObject(long beginingOfSerializedObject) throws IOException, SerializationException{
		ra.skipBytes(Long.BYTES); //We jump the version
		boolean suppr = ra.readBoolean();
		if(suppr) return null;
		return marshaller.unserialize(getObjectType(), ra);//the id is read in the description
	}
	
	public long writeData(U obj, long version) throws IOException, SerializationException {
		long beginingOfSerialisation = ra.length();
		ra.seek(beginingOfSerialisation);
		ra.writeLong(version);
		ra.writeBoolean(false); //is not delete
		marshaller.serialize(obj, ra);//l'id est contenu dans l'objet
		ra.writeLong(beginingOfSerialisation);
		return beginingOfSerialisation;
	}

	long getVersion() {
		return version;
	}
	
	public void delete(String id, long version) throws IOException {
		long debutSerialisation = ra.length();
		ra.writeLong(version);
		ra.writeBoolean(true); //is deleted
		ra.writeUTF(id);
		ra.writeLong(debutSerialisation);
	}
	

	public CloseableIterator<U> getAllObjectsWithMaxVersionLessThan(long version, Store<U> s) throws IOException, StorageException, SerializationException {
		return new ParcoursObjectsMaxVersion(version, s);
	}
	
	protected Class<U> getObjectType() {
		return objectType;
	}
	
	protected String debutNomFichier() {
		return objectType.getName();
	}
	
	
protected class ParcoursObjectsMaxVersion implements CloseableIterator<U>{
		private final long versionMax;
		private final PersistentIdSet<U> dejaVu;
		private U next;
		private Long position;
		private Path oldFile;
		private RandomAccessFile raf;

		public ParcoursObjectsMaxVersion(long maxVersion, Store<U> s) throws IOException, StorageException, SerializationException {
			setOld();
			this.versionMax = maxVersion;
			this.dejaVu = new PersistentIdSet<>(s.getObjectType(), s);
			position = readPosition(file);
			cacheNext();
		}

		private void cacheNext() throws IOException, StorageException, SerializationException {
			next = null;
			while(next == null && position >= 0) {
				if(position > Long.BYTES) {
					raf.seek(position - Long.BYTES);
					position = raf.readLong();
				}else {
					raf.seek(position);
					position = -1L;
				}
				long v = raf.readLong();
				if(v > versionMax)
					 continue;
				boolean suppr = raf.readBoolean();
				String id = raf.readUTF();
				boolean djv = dejaVu.contains(id);
				long avantId = raf.getFilePointer();
				if(!djv) dejaVu.addId(id);
				if(!djv && !suppr) {
					raf.seek(avantId);
					next = marshaller.unserialize(getObjectType(), raf);
				}
			}
		}

		@Override
		public boolean hasNext() {
			try {
				deleteFileWhenFinish();
			} catch (IOException e) {
				throw new StorageRuntimeException("impossible to delete the old file", e);
			}
			return next == null;
		}

		private void deleteFileWhenFinish() throws IOException {
			if(next != null)
				return;
			raf.close();
			setVersion(versionMax);
			Files.delete(oldFile);
		}

		@Override
		public U next() {
			try {
				if(next == null)
					throw new NoSuchElementException("when there's no more...");
				U ret = next;
				cacheNext();
				return ret;
			} catch (IOException | StorageException| SerializationException e) {
				try {
					Files.move(oldFile, file, ATOMIC_MOVE);
					throw new StorageRuntimeException(e);
				} catch (IOException e1) {
					throw new StorageRuntimeException("impossible to rename the old File " + oldFile.toString() + " in " + file.toString(), e1);
				}
			}
		}
		
		public void setOld() throws IOException {
			ra.close();
			oldFile = Paths.get(file.toString().replaceAll(".data", ".old"));
			Files.move(file, oldFile, ATOMIC_MOVE);
			Files.createFile(file);
			ra = new RandomAccessFile(file.toFile(), "rw");
			raf = new RandomAccessFile(oldFile.toFile(), "r");
		}
		
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void close() throws IOException {
			dejaVu.close();
		}
	}
}
