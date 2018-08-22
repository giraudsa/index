package com.chronosave.index.utils;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReadWriteLock {
	private final java.util.concurrent.locks.ReadWriteLock locker = new ReentrantReadWriteLock();

	public ReadWriteLock() {
		super();
	}

	public void lockRead() {
		locker.readLock().lock();
	}

	public void unlockRead() {
		locker.readLock().unlock();
	}

	public void lockWrite() {
		locker.writeLock().lock();
	}

	public void unlockWrite() {
		locker.writeLock().unlock();
	}
}
