package com.chronosave.index.storage.exception;

public class ConflictException extends Exception {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1892550092634269209L;

	public ConflictException() {
		super();
	}

	public ConflictException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ConflictException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConflictException(String message) {
		super(message);
	}

	public ConflictException(Throwable cause) {
		super(cause);
	}
	

}
