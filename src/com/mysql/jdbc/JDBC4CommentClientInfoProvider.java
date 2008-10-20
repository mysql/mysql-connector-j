/*
 Copyright  2002-2007 MySQL AB, 2008 Sun Microsystems

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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLClientInfoException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * An implementation of JDBC4ClientInfoProvider that exposes
 * the client info as a comment prepended to all statements issued
 * by the driver.
 * 
 * Client information is <i>never</i> read from the server with this
 * implementation, it is always cached locally.
 * 
 * @version $Id: $
 */

public class JDBC4CommentClientInfoProvider implements JDBC4ClientInfoProvider {
	private Properties clientInfo;
	
	public synchronized void initialize(java.sql.Connection conn,
			Properties configurationProps) throws SQLException {
		this.clientInfo = new Properties();
	}

	public synchronized void destroy() throws SQLException {
		this.clientInfo = null;
	}

	public synchronized Properties getClientInfo(java.sql.Connection conn)
			throws SQLException {
		return this.clientInfo;
	}

	public synchronized String getClientInfo(java.sql.Connection conn,
			String name) throws SQLException {
		return this.clientInfo.getProperty(name);
	}

	public synchronized void setClientInfo(java.sql.Connection conn,
			Properties properties) throws SQLClientInfoException {
		this.clientInfo = new Properties();
		
		Enumeration propNames = properties.propertyNames();
		
		while (propNames.hasMoreElements()) {
			String name = (String)propNames.nextElement();
			
			this.clientInfo.put(name, properties.getProperty(name));
		}
		
		setComment(conn);
	}

	public synchronized void setClientInfo(java.sql.Connection conn,
			String name, String value) throws SQLClientInfoException {
		this.clientInfo.setProperty(name, value);
		setComment(conn);
	}
	
	private synchronized void setComment(java.sql.Connection conn) {
		StringBuffer commentBuf = new StringBuffer();
		Iterator elements = this.clientInfo.entrySet().iterator();
		
		while (elements.hasNext()) {
			if (commentBuf.length() > 0) {
				commentBuf.append(", ");
			}
			
			Map.Entry entry = (Map.Entry)elements.next();
			commentBuf.append("" + entry.getKey());
			commentBuf.append("=");
			commentBuf.append("" + entry.getValue());
		}
		
		((com.mysql.jdbc.Connection)conn).setStatementComment(
				commentBuf.toString());
	}
}