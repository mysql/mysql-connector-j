/*
 Copyright  2007 MySQL AB, 2008 Sun Microsystems

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

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Implement this interface to provide a new load balancing strategy for URLs of
 * the form "jdbc:mysql:loadbalance://..", and provide the implementation class
 * name as the configuration parameter "loadBalanceStrategy".
 * 
 * The driver will not pass in a Connection instance when calling init(), but it
 * will pass in the Properties, otherwise it acts like a normal Extension.
 * 
 * One instance of a strategy *per* JDBC connection instance will be created. If
 * you need singleton-like behavior, you're on your own to provide it.
 */
public interface BalanceStrategy extends Extension {
	/**
	 * Called by the driver to pick a new connection to route requests over.
	 * 
	 * @param proxy
	 *            the InvocationHandler that deals with actual method calls to
	 *            the JDBC connection, and serves as a factory for new
	 *            connections for this strategy via the
	 *            createConnectionForHost() method.
	 * 
	 * This proxy takes care of maintaining the response time list, map of
	 * host/ports to live connections, and taking connections out of the live
	 * connections map if they receive a network-related error while they are in
	 * use by the application.
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
	 * 
	 * @see LoadBalancingConnectionProxy#createConnectionForHost(String)
	 */
	public abstract Connection pickConnection(
			LoadBalancingConnectionProxy proxy, List configuredHosts,
			Map liveConnections, long[] responseTimes, int numRetries)
			throws SQLException;
}