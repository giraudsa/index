package com.chronosave.index.externe;

import java.util.Collection;

public interface AllObjectByType {

	/**
	 * give all instance of a given type
	 * 
	 * @param objectType
	 * @return
	 */
	public <U> Collection<U> getAll(Class<U> type);

}
