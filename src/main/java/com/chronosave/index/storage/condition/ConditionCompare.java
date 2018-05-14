package com.chronosave.index.storage.condition;


public abstract class ConditionCompare<K extends  Comparable<K>, U> extends AbstractCondition<K, U> {

	public ConditionCompare(Class<U> typeObjet, Class<K> typeReturn, ComputeKey<K, U> delegate) {
		super(typeObjet, typeReturn, delegate);
	}

}
