/*
 Copyright (C) 2002-2004 MySQL AB

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as
 published by the Free Software Foundation.
 

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * Driver that opens two connections, one two a replication master, and another
 * to one or more slaves, and decides to use master when the connection is not
 * read-only, and use slave(s) when the connection is read-only.
 * 
 * @version $Id: NonRegisteringReplicationDriver.java,v 1.1.2.1 2005/05/13
 *          18:58:37 mmatthews Exp $
 */
public class NonRegisteringReplicationDriver extends NonRegisteringDriver {
	public NonRegisteringReplicationDriver() throws SQLException {
		super();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
	 */
	public Connection connect(String url, Properties info) throws SQLException {
		Properties parsedProps = parseURL(url, info);

		if (parsedProps == null) {
			return null;
		}
		
		Properties masterProps = (Properties)parsedProps.clone();
		Properties slavesProps = (Properties)parsedProps.clone();

		// Marker used for further testing later on, also when
		// debugging
		slavesProps.setProperty("com.mysql.jdbc.ReplicationConnection.isSlave", "true");
		
		String hostValues = parsedProps.getProperty(HOST_PROPERTY_KEY);

		if (hostValues != null) {
			StringTokenizer st = new StringTokenizer(hostValues, ",");

			StringBuffer masterHost = new StringBuffer();
			StringBuffer slaveHosts = new StringBuffer();

			if (st.hasMoreTokens()) {
				String[] hostPortPair = parseHostPortPair(st.nextToken());

				if (hostPortPair[HOST_NAME_INDEX] != null) {
					masterHost.append(hostPortPair[HOST_NAME_INDEX]);
				}

				if (hostPortPair[PORT_NUMBER_INDEX] != null) {
					masterHost.append(":");
					masterHost.append(hostPortPair[PORT_NUMBER_INDEX]);
				}
			}

			boolean firstSlaveHost = true;

			while (st.hasMoreTokens()) {
				String[] hostPortPair = parseHostPortPair(st.nextToken());

				if (!firstSlaveHost) {
					slaveHosts.append(",");
				} else {
					firstSlaveHost = false;
				}

				if (hostPortPair[HOST_NAME_INDEX] != null) {
					slaveHosts.append(hostPortPair[HOST_NAME_INDEX]);
				}

				if (hostPortPair[PORT_NUMBER_INDEX] != null) {
					slaveHosts.append(":");
					slaveHosts.append(hostPortPair[PORT_NUMBER_INDEX]);
				}
			}

			if (slaveHosts.length() == 0) {
				throw SQLError.createSQLException(
						"Must specify at least one slave host to connect to for master/slave replication load-balancing functionality",
						SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE);
			}

			masterProps.setProperty(HOST_PROPERTY_KEY, masterHost.toString());
			slavesProps.setProperty(HOST_PROPERTY_KEY, slaveHosts.toString());
		}

		return new ReplicationConnection(masterProps, slavesProps);
	}
}
