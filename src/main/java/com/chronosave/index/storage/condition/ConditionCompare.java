package com.chronosave.index.storage.condition;

public abstract class ConditionCompare<K extends Comparable<K>, U> extends AbstractCondition<K, U> {

	public ConditionCompare(final Class<U> typeObjet, final Class<K> typeReturn,
			final ComputeComparableKey<K, U> delegate) {
		super(typeObjet, typeReturn, delegate);
	}

}
