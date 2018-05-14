package com.chronosave.index.utils;

import java.util.LinkedHashMap;
import java.util.Map;

public class DynamicCache<K,V> extends LinkedHashMap<K, V> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3353027275115444016L;
	private int taille;
	
	public void setTaille(int newTaille){
		if(newTaille <= 0) 
			throw new UnsupportedOperationException();
		taille = newTaille;
	}
	
	/**
	 * initialise un cache ne dépassant pas la taille indiquée en paramètre
	 * @param taille
	 */
	public DynamicCache(int taille){
		super(256, 0.75f, true);
		setTaille(taille);
	}
	
	@Override
	protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
	    return  size() >= taille;
	  }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + taille;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		DynamicCache<?,?> other = (DynamicCache<?,?>) obj;
		return taille == other.taille;
	}

	
	
}
