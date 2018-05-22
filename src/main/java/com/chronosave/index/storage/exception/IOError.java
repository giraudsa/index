package com.chronosave.index.storage.exception;

public class IOError extends RuntimeException {

	/**
	 *
	 */
	private static final long serialVersionUID = 5804973090631367613L;

	public IOError() {
		super();
	}

	public IOError(final String message) {
		super(message);
	}

	public IOError(final String message, final Throwable cause) {
		super(message, cause);
	}

	public IOError(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public IOError(final Throwable cause) {
		super(cause);
	}

}
