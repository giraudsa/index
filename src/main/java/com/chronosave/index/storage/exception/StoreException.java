package com.chronosave.index.storage.exception;

public class StoreException extends Exception {

	/**
	 *
	 */
	private static final long serialVersionUID = 6805865571597461737L;

	/**
	 *
	 */
	public StoreException() {
		super();
	}

	/**
	 * @param arg0
	 */
	public StoreException(final String arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public StoreException(final String arg0, final Throwable arg1) {
		super(arg0, arg1);
	}

	/**
	 * @param arg0
	 * @param arg1
	 * @param arg2
	 * @param arg3
	 */
	public StoreException(final String arg0, final Throwable arg1, final boolean arg2, final boolean arg3) {
		super(arg0, arg1, arg2, arg3);
	}

	/**
	 * @param arg0
	 */
	public StoreException(final Throwable arg0) {
		super(arg0);
	}

}
