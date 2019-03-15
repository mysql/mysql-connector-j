/*
  Copyright (c) 2011, 2019, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */

package com.mysql.jdbc;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface SocketMetadata {

    public boolean isLocallyConnected(ConnectionImpl conn) throws SQLException;

    /*
     * Provides a standard way of determining whether a socket connection is local.
     * 
     * This ensures socket factories (e.g. StandardSocketFactory, StandardSSLSocketFactory) which need to implement this interface, can delegate to a generic
     * implementation.
     */
    static class Helper {

        public static final String IS_LOCAL_HOSTNAME_REPLACEMENT_PROPERTY_NAME = "com.mysql.jdbc.test.isLocalHostnameReplacement";

        public static boolean isLocallyConnected(com.mysql.jdbc.ConnectionImpl conn) throws SQLException {
            long threadId = conn.getId();
            java.sql.Statement processListStmt = conn.getMetadataSafeStatement();
            ResultSet rs = null;
            String processHost = null;

            // "inject" for tests
            if (System.getProperty(IS_LOCAL_HOSTNAME_REPLACEMENT_PROPERTY_NAME) != null) {
                processHost = System.getProperty(IS_LOCAL_HOSTNAME_REPLACEMENT_PROPERTY_NAME);

            } else if (conn.getProperties().getProperty(IS_LOCAL_HOSTNAME_REPLACEMENT_PROPERTY_NAME) != null) {
                processHost = conn.getProperties().getProperty(IS_LOCAL_HOSTNAME_REPLACEMENT_PROPERTY_NAME);

            } else { // get it from server
                try {
                    processHost = findProcessHost(threadId, processListStmt);

                    if (processHost == null) {
                        // http://bugs.mysql.com/bug.php?id=44167 - connection ids on the wire wrap at 4 bytes even though they're 64-bit numbers
                        conn.getLog()
                                .logWarn(String.format(
                                        "Connection id %d not found in \"SHOW PROCESSLIST\", assuming 32-bit overflow, using SELECT CONNECTION_ID() instead",
                                        threadId));

                        rs = processListStmt.executeQuery("SELECT CONNECTION_ID()");

                        if (rs.next()) {
                            threadId = rs.getLong(1);

                            processHost = findProcessHost(threadId, processListStmt);
                        } else {
                            conn.getLog().logError(
                                    "No rows returned for statement \"SELECT CONNECTION_ID()\", local connection check will most likely be incorrect");
                        }
                    }
                } finally {
                    processListStmt.close();
                }
            }

            if (processHost != null) {
                conn.getLog().logDebug(Messages.getString("SocketMetadata.0", new Object[] { processHost }));

                int endIndex = processHost.lastIndexOf(":");
                if (endIndex != -1) {
                    processHost = processHost.substring(0, endIndex);
                }

                try {

                    InetAddress[] allHostAddr = InetAddress.getAllByName(processHost);

                    SocketAddress remoteSocketAddr = conn.getIO().mysqlConnection.getRemoteSocketAddress(); // mysqlConnection should be the raw socket

                    if (remoteSocketAddr instanceof InetSocketAddress) {
                        InetAddress whereIConnectedTo = ((InetSocketAddress) remoteSocketAddr).getAddress();

                        for (InetAddress hostAddr : allHostAddr) {
                            if (hostAddr.equals(whereIConnectedTo)) {
                                conn.getLog().logDebug(Messages.getString("SocketMetadata.1", new Object[] { hostAddr, whereIConnectedTo }));
                                return true;
                            }
                            conn.getLog().logDebug(Messages.getString("SocketMetadata.2", new Object[] { hostAddr, whereIConnectedTo }));
                        }

                    } else {
                        conn.getLog().logDebug(Messages.getString("SocketMetadata.3", new Object[] { remoteSocketAddr }));
                    }

                    return false;
                } catch (UnknownHostException e) {
                    conn.getLog().logWarn(Messages.getString("Connection.CantDetectLocalConnect", new Object[] { processHost }), e);

                    return false;
                }

            }

            return false;
        }

        private static String findProcessHost(long threadId, java.sql.Statement processListStmt) throws SQLException {
            String processHost = null;
            String ps = ((MySQLConnection) processListStmt.getConnection()).getServerVariable("performance_schema");

            ResultSet rs = ((MySQLConnection) processListStmt.getConnection()).versionMeetsMinimum(5, 6, 0) // performance_schema.threads in MySQL 5.5 does not contain PROCESSLIST_HOST column
                    && ps != null && ("1".contentEquals(ps) || "ON".contentEquals(ps))
                            ? processListStmt.executeQuery(
                                    "select PROCESSLIST_ID, PROCESSLIST_USER, PROCESSLIST_HOST from performance_schema.threads where PROCESSLIST_ID="
                                            + threadId)
                            : processListStmt.executeQuery("SHOW PROCESSLIST");

            while (rs.next()) {
                long id = rs.getLong(1);

                if (threadId == id) {
                    processHost = rs.getString(3);
                    break;
                }
            }

            return processHost;
        }
    }
}
