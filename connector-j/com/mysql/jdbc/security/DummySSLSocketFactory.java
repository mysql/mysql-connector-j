/*
   Copyright (C) 2002 MySQL AB
   
      This program is free software; you can redistribute it and/or modify
      it under the terms of the GNU General Public License as published by
      the Free Software Foundation; either version 2 of the License, or
      (at your option) any later version.
   
      This program is distributed in the hope that it will be useful,
      but WITHOUT ANY WARRANTY; without even the implied warranty of
      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
      GNU General Public License for more details.
   
      You should have received a copy of the GNU General Public License
      along with this program; if not, write to the Free Software
      Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
      
 */
package com.mysql.jdbc.security;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

/**
 * An SSLSocketFactory that does not check certs.
 * 
 * It is strongly advised NOT to use this in production!
 * 
 * @author Mark Matthews
 */
public class DummySSLSocketFactory extends SSLSocketFactory {
	/**
	 * Instance for singleton
	 */
	
	private static DummySSLSocketFactory instance;
	
	static {
		instance = new DummySSLSocketFactory();
	}
	
	/**
	 * Underlying socket factory to actually create sockets
	 */
		
    private SSLSocketFactory factory;

    private DummySSLSocketFactory() throws RuntimeException {
      
      try {
        SSLContext sslcontext = SSLContext.getInstance( "TLS");
        sslcontext.init( null, // No KeyManager required
            new TrustManager[] { new DummyTrustManager()},
            new java.security.SecureRandom());
        factory = ( SSLSocketFactory) sslcontext.getSocketFactory();

      } catch( Exception ex) {
        throw new RuntimeException(ex.getMessage());
      }
    }

    public static SocketFactory getDefault() {
      return instance;
    }

    public Socket createSocket( Socket socket, String s, int i, boolean
flag)
        throws IOException {
      return factory.createSocket( socket, s, i, flag);
    }

    public Socket createSocket( InetAddress inaddr, int i,
        InetAddress inaddr1, int j) throws IOException {
      return factory.createSocket( inaddr, i, inaddr1, j);
    }

    public Socket createSocket( InetAddress inaddr, int i) throws
IOException {
      return factory.createSocket( inaddr, i);
    }

    public Socket createSocket( String s, int i, InetAddress inaddr, int j)
        throws IOException {
      return factory.createSocket( s, i, inaddr, j);
    }

    public Socket createSocket( String s, int i) throws IOException {
      return factory.createSocket( s, i);
    }

    public String[] getDefaultCipherSuites() {
      return factory.getSupportedCipherSuites();
    }

    public String[] getSupportedCipherSuites() {
      return factory.getSupportedCipherSuites();
    }
  }