package com.cloud.dataflow.realtime.exceptions;

public class InvalidDataException extends Exception {
	private static final long serialVersionUID = 1L;
	final String msg;

	public InvalidDataException(String msg) {
		super();
		this.msg = msg;
	}

	@Override
	public String toString() {
		return msg;

	}
}