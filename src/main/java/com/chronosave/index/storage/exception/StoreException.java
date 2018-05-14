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
	 * @param arg1
	 * @param arg2
	 * @param arg3
	 */
	public StoreException(String arg0, Throwable arg1, boolean arg2, boolean arg3) {
		super(arg0, arg1, arg2, arg3);
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public StoreException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	/**
	 * @param arg0
	 */
	public StoreException(String arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 */
	public StoreException(Throwable arg0) {
		super(arg0);
	}

}
