/*
 Copyright  2002-2004 MySQL AB, 2008 Sun Microsystems

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

/**
 * Thrown when a client requests a connection-level feature that isn't available
 * for this particular distribution of Connector/J (currently only used by code
 * that is export-controlled).
 * 
 * @author Mark Matthews
 * 
 * @version $Id: ConnectionFeatureNotAvailableException.java,v 1.1.2.1
 *          2005/05/13 18:58:38 mmatthews Exp $
 */
public class ConnectionFeatureNotAvailableException extends
		CommunicationsException {

	/**
	 * @param conn
	 * @param lastPacketSentTimeMs
	 * @param underlyingException
	 */
	public ConnectionFeatureNotAvailableException(ConnectionImpl conn,
			long lastPacketSentTimeMs, Exception underlyingException) {
		super(conn, lastPacketSentTimeMs, 0, underlyingException);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Throwable#getMessage()
	 */
	public String getMessage() {
		return "Feature not available in this distribution of Connector/J";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.SQLException#getSQLState()
	 */
	public String getSQLState() {
		return SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE;
	}
}
