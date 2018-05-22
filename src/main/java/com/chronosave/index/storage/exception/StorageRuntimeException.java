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
	 */
	public StorageRuntimeException(final String arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public StorageRuntimeException(final String arg0, final Throwable arg1) {
		super(arg0, arg1);
	}

	/**
	 * @param arg0
	 * @param arg1
	 * @param arg2
	 * @param arg3
	 */
	public StorageRuntimeException(final String arg0, final Throwable arg1, final boolean arg2, final boolean arg3) {
		super(arg0, arg1, arg2, arg3);
	}

	/**
	 * @param arg0
	 */
	public StorageRuntimeException(final Throwable arg0) {
		super(arg0);
	}

}
