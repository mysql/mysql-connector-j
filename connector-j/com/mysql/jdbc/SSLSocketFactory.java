package com.mysql.jdbc;

import java.io.IOException;

import java.net.Socket;
import java.net.SocketException;

import java.util.Properties;

//import javax.net.SSLSocketFactory;

/**
 * @author Owner
 *
 * To change this generated comment edit the template variable "typecomment":
 * Window>Preferences>Java>Templates.
 * To enable and disable the creation of type comments go to
 * Window>Preferences>Java>Code Generation.
 */
public class SSLSocketFactory extends StandardSocketFactory {
	
	/**
	 * @see com.mysql.jdbc.SocketFactory#createSocket(Properties)
	 */
	/*
	public Socket createSocket(Properties props)
		throws SocketException, IOException {
		
		if (props != null)
		{
			String host = props.getProperty("host");
			String portStr = props.getProperty("port");
			
			int port = 3306;
			
			if (portStr != null)
			{
				port = Integer.parseInt(portStr);
			}
			
			Socket sock = super.createSocket(props);
			
			
		}
	}
	*/
}
