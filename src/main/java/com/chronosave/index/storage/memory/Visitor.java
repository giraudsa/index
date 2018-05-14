package com.chronosave.index.storage.memory;

public abstract class Visitor<T> {
	protected abstract void visite(T t);
}
