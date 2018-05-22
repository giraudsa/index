package com.chronosave.index.storage.exception;

public class StorageException extends Exception {

	/**
	 *
	 */
	private static final long serialVersionUID = 7336496489226150659L;

	/**
	 *
	 */
	public StorageException() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 */
	public StorageException(final String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public StorageException(final String message, final Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public StorageException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public StorageException(final Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

}
