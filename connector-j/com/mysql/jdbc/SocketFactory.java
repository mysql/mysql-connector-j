package com.mysql.jdbc;

import java.io.IOException;

import java.net.Socket;
import java.net.SocketException;

import java.util.Properties;

/**
 * @author Owner
 *
 * To change this generated comment edit the template variable "typecomment":
 * Window>Preferences>Java>Templates.
 * To enable and disable the creation of type comments go to
 * Window>Preferences>Java>Code Generation.
 */
public interface SocketFactory {
	
/**
 * @author Owner
 *
 * To change this generated comment edit the template variable "typecomment":
 * Window>Preferences>Java>Templates.
 * To enable and disable the creation of type comments go to
 * Window>Preferences>Java>Code Generation.
 */
public class StandardSocketFactory{

}
	/**
	 * Creates a new socket using the given properties.
	 * 
	 * Properties are parsed by the driver from the URL.
	 * 
	 * All properties other than sensitive ones (user
	 * and password) are passed to this method.
	 * 
	 * The driver will instantiate the socket factory
	 * with the class name given in the property
	 * &quot;socketFactory&quot;, where the standard is
	 * <code>com.mysql.jdbc.StandardSocketFactory</code>
	 * 
	 * Implementing classes are responsible for handling
	 * synchronization of this method (if needed).
	 */
	
	public Socket createSocket(Properties props)
		throws SocketException, IOException;

}
