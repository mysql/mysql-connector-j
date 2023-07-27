/*
 * Copyright (c) 2007, 2023, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.jdbc.ha;

import java.lang.reflect.InvocationHandler;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.mysql.cj.jdbc.JdbcConnection;

/**
 * Implement this interface to provide a new load balancing strategy for URLs of the form "jdbc:mysql:loadbalance://..", and provide the implementation class
 * name as the configuration parameter "loadBalanceStrategy".
 *
 * The driver will not pass in a Connection instance when calling init(), but it will pass in the Properties, otherwise it acts like a normal Extension.
 *
 * One instance of a strategy *per* JDBC connection instance will be created. If you need singleton-like behavior, you're on your own to provide it.
 */
public interface BalanceStrategy {

    /**
     * Called by the driver to pick a new connection to route requests over.
     * See LoadBalancedConnectionProxy.createConnectionForHost(String)
     *
     * @param proxy
     *            the InvocationHandler that deals with actual method calls to
     *            the JDBC connection, and serves as a factory for new
     *            connections for this strategy via the
     *            createConnectionForHost() method.
     *
     *            This proxy takes care of maintaining the response time list, map of
     *            host/ports to live connections, and taking connections out of the live
     *            connections map if they receive a network-related error while they are in
     *            use by the application.
     * @param configuredHosts
     *            the list of hosts/ports (in "host:port" form) as passed in by
     *            the user.
     * @param liveConnections
     *            a map of host/ports to "live" connections to them.
     * @param responseTimes
     *            the list of response times for a <strong>transaction</strong>
     *            for each host in the configured hosts list.
     * @param numRetries
     *            the number of times the driver expects this strategy to re-try
     *            connection attempts if creating a new connection fails.
     * @return the physical JDBC connection for the application to use, based
     *         upon the strategy employed.
     * @throws SQLException
     *             if a new connection can not be found or created by this
     *             strategy.
     */
    abstract JdbcConnection pickConnection(InvocationHandler proxy, List<String> configuredHosts, Map<String, JdbcConnection> liveConnections,
            long[] responseTimes, int numRetries) throws SQLException;

}
