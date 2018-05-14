package com.chronosave.index.storage.exception;

public class IOError extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5804973090631367613L;

	public IOError() {
		super();
	}

	public IOError(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public IOError(String message, Throwable cause) {
		super(message, cause);
	}

	public IOError(String message) {
		super(message);
	}

	public IOError(Throwable cause) {
		super(cause);
	}

	
	
}
