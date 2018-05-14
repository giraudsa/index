package com.chronosave.index.storage.exception;

public class SerializationException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = -1530254739821738318L;

	/**
	 * 
	 */
	public SerializationException() {
		super();
	}

	/**
	 * @param arg0
	 * @param arg1
	 * @param arg2
	 * @param arg3
	 */
	public SerializationException(String arg0, Throwable arg1, boolean arg2, boolean arg3) {
		super(arg0, arg1, arg2, arg3);
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public SerializationException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	/**
	 * @param arg0
	 */
	public SerializationException(String arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 */
	public SerializationException(Throwable arg0) {
		super(arg0);
	}
	
}
