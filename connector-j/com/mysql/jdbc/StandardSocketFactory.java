package com.mysql.jdbc;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Properties;

/**
 * Socket factory for vanilla TCP/IP sockets (the standard)
 */
public class StandardSocketFactory implements SocketFactory {

	/**
	 * @see com.mysql.jdbc.SocketFactory#createSocket(Properties)
	 */
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
				
			if (host != null)
			{
				Socket sock = new Socket(host, port);
			
				try {
					sock.setTcpNoDelay(true);
				}
				catch (Exception ex) {
				/* Ignore */
				}
				
				return sock;
			}	
			
		}
		
		throw new SocketException("Unable to create socket");
	}

}
