/*
 Copyright (C) 2002-2004 MySQL AB

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA



 */
package com.mysql.jdbc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;

/**
 * Holds functionality that falls under export-control regulations.
 * 
 * @author Mark Matthews
 * 
 * @version $Id: ExportControlled.java,v 1.1.2.1 2005/05/13 18:58:38 mmatthews
 *          Exp $
 */
public class ExportControlled {
	protected static boolean enabled() {
		// we may wish to un-static-ify this class
		// this static method call may be removed entirely by the compiler
		return true;
	}

	/**
	 * Converts the socket being used in the given MysqlIO to an SSLSocket by
	 * performing the SSL/TLS handshake.
	 * 
	 * @param mysqlIO
	 *            the MysqlIO instance containing the socket to convert to an
	 *            SSLSocket.
	 * 
	 * @throws CommunicationsException
	 *             if the handshake fails, or if this distribution of
	 *             Connector/J doesn't contain the SSL crytpo hooks needed to
	 *             perform the handshake.
	 */
	protected static void transformSocketToSSLSocket(MysqlIO mysqlIO)
			throws CommunicationsException {
		javax.net.ssl.SSLSocketFactory sslFact = (javax.net.ssl.SSLSocketFactory) javax.net.ssl.SSLSocketFactory
				.getDefault();

		try {
			mysqlIO.mysqlConnection = sslFact.createSocket(
					mysqlIO.mysqlConnection, mysqlIO.host, mysqlIO.port, true);

			// need to force TLSv1, or else JSSE tries to do a SSLv2 handshake
			// which MySQL doesn't understand
			((javax.net.ssl.SSLSocket) mysqlIO.mysqlConnection)
					.setEnabledProtocols(new String[] { "TLSv1" }); //$NON-NLS-1$
			((javax.net.ssl.SSLSocket) mysqlIO.mysqlConnection)
					.startHandshake();

			if (mysqlIO.connection.getUseUnbufferedInput()) {
				mysqlIO.mysqlInput = mysqlIO.mysqlConnection.getInputStream();
			} else {
				mysqlIO.mysqlInput = new BufferedInputStream(
						mysqlIO.mysqlConnection.getInputStream(), 16384);
			}

			mysqlIO.mysqlOutput = new BufferedOutputStream(
					mysqlIO.mysqlConnection.getOutputStream(), 16384);

			mysqlIO.mysqlOutput.flush();
		} catch (IOException ioEx) {
			throw new CommunicationsException(mysqlIO.connection,
					mysqlIO.lastPacketSentTimeMs, ioEx);
		}
	}

	private ExportControlled() { /* prevent instantiation */
	}
}