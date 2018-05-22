package com.chronosave.index.storage.exception;

public class ConflictException extends Exception {

	/**
	 *
	 */
	private static final long serialVersionUID = 1892550092634269209L;

	public ConflictException() {
		super();
	}

	public ConflictException(final String message) {
		super(message);
	}

	public ConflictException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ConflictException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ConflictException(final Throwable cause) {
		super(cause);
	}

}
