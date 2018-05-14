package com.chronosave.index.storage.exception;

public class StorageRuntimeException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3924686938819731890L;

	/**
	 * 
	 */
	public StorageRuntimeException() {
		super();
	}

	/**
	 * @param arg0
	 * @param arg1
	 * @param arg2
	 * @param arg3
	 */
	public StorageRuntimeException(String arg0, Throwable arg1, boolean arg2, boolean arg3) {
		super(arg0, arg1, arg2, arg3);
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public StorageRuntimeException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	/**
	 * @param arg0
	 */
	public StorageRuntimeException(String arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 */
	public StorageRuntimeException(Throwable arg0) {
		super(arg0);
	}


}
