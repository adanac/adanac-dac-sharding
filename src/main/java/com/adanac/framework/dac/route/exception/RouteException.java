package com.adanac.framework.dac.route.exception;

/**
 * Dal异常类
 * @author adanac
 * @version 1.0
 */
public class RouteException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 9036525297209826035L;

	public RouteException() {
	}

	public RouteException(String msg) {
		super(msg);
	}

	public RouteException(Throwable exception) {
		super(exception);
	}

	public RouteException(String mag, Exception exception) {
		super(mag, exception);
	}
}
