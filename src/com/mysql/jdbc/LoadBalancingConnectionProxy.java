/*
 Copyright (C) 2007 MySQL AB

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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * An implementation of java.sql.Connection that load balances requests across a
 * series of MySQL JDBC connections, where the balancing takes place at
 * transaction commit.
 * 
 * Therefore, for this to work (at all), you must use transactions, even if only
 * reading data.
 * 
 * This implementation will invalidate connections that it detects have had
 * communication errors when processing a request. A new connection to the
 * problematic host will be attempted the next time it is selected by the load
 * balancing algorithm.
 * 
 * This implementation is thread-safe, but it's questionable whether sharing a
 * connection instance amongst threads is a good idea, given that transactions
 * are scoped to connections in JDBC.
 * 
 * @version $Id: $
 * 
 */
public class LoadBalancingConnectionProxy implements InvocationHandler, PingTarget {

	private static Method getLocalTimeMethod;

	static {
		try {
			getLocalTimeMethod = System.class.getMethod("nanoTime",
					new Class[0]);
		} catch (SecurityException e) {
			// ignore
		} catch (NoSuchMethodException e) {
			// ignore
		}
	}

	// Lifted from C/J 5.1's JDBC-2.0 connection pool classes, let's merge this
	// if/when this gets into 5.1
	protected class ConnectionErrorFiringInvocationHandler implements
			InvocationHandler {
		Object invokeOn = null;

		public ConnectionErrorFiringInvocationHandler(Object toInvokeOn) {
			invokeOn = toInvokeOn;
		}

		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			Object result = null;

			try {
				result = method.invoke(invokeOn, args);

				if (result != null) {
					result = proxyIfInterfaceIsJdbc(result, result.getClass());
				}
			} catch (InvocationTargetException e) {
				dealWithInvocationException(e);
			}

			return result;
		}
	}

	private Connection currentConn;

	private List hostList;

	private Map liveConnections;

	private Map connectionsToHostsMap;

	private long[] responseTimes;

	private Map hostsToListIndexMap;

	private boolean inTransaction = false;

	private long transactionStartTime = 0;

	private Properties localProps;

	private boolean isClosed = false;

	private BalanceStrategy balancer;

	private int retriesAllDown;

	/**
	 * Creates a proxy for java.sql.Connection that routes requests between the
	 * given list of host:port and uses the given properties when creating
	 * connections.
	 * 
	 * @param hosts
	 * @param props
	 * @throws SQLException
	 */
	LoadBalancingConnectionProxy(List hosts, Properties props)
			throws SQLException {
		this.hostList = hosts;

		int numHosts = this.hostList.size();

		this.liveConnections = new HashMap(numHosts);
		this.connectionsToHostsMap = new HashMap(numHosts);
		this.responseTimes = new long[numHosts];
		this.hostsToListIndexMap = new HashMap(numHosts);

		for (int i = 0; i < numHosts; i++) {
			this.hostsToListIndexMap.put(this.hostList.get(i), new Integer(i));
		}

		this.localProps = (Properties) props.clone();
		this.localProps.remove(NonRegisteringDriver.HOST_PROPERTY_KEY);
		this.localProps.remove(NonRegisteringDriver.PORT_PROPERTY_KEY);
		this.localProps.setProperty("useLocalSessionState", "true");

		String strategy = this.localProps.getProperty("loadBalanceStrategy",
				"random");

		String retriesAllDownAsString = this.localProps.getProperty("retriesAllDown", "120");
		
		try {
			this.retriesAllDown = Integer.parseInt(retriesAllDownAsString);
		} catch (NumberFormatException nfe) {
			throw SQLError.createSQLException(Messages.getString(
					"LoadBalancingConnectionProxy.badValueForRetriesAllDown",
					new Object[] { retriesAllDownAsString }),
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
		}
		
		if ("random".equals(strategy)) {
			this.balancer = (BalanceStrategy) Util.loadExtensions(null, props,
					"com.mysql.jdbc.RandomBalanceStrategy",
					"InvalidLoadBalanceStrategy").get(0);
		} else if ("bestResponseTime".equals(strategy)) {
			this.balancer = (BalanceStrategy) Util.loadExtensions(null, props,
					"com.mysql.jdbc.BestResponseTimeBalanceStrategy",
			"InvalidLoadBalanceStrategy").get(0);
		} else {
			this.balancer = (BalanceStrategy) Util.loadExtensions(null, props,
					strategy, "InvalidLoadBalanceStrategy").get(0);
		}

		this.balancer.init(null, props);
		
		pickNewConnection();
	}

	/**
	 * Creates a new physical connection for the given host:port and updates
	 * required internal mappings and statistics for that connection.
	 * 
	 * @param hostPortSpec
	 * @return
	 * @throws SQLException
	 */
	public synchronized Connection createConnectionForHost(String hostPortSpec)
			throws SQLException {
		Properties connProps = (Properties) this.localProps.clone();

		String[] hostPortPair = NonRegisteringDriver
				.parseHostPortPair(hostPortSpec);

		if (hostPortPair[1] == null) {
			hostPortPair[1] = "3306";
		}

		connProps.setProperty(NonRegisteringDriver.HOST_PROPERTY_KEY,
				hostPortSpec);
		connProps.setProperty(NonRegisteringDriver.PORT_PROPERTY_KEY,
				hostPortPair[1]);

		Connection conn = ConnectionImpl.getInstance(hostPortSpec, Integer
				.parseInt(hostPortPair[1]), connProps, connProps
				.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY),
				"jdbc:mysql://" + hostPortPair[0] + ":" + hostPortPair[1] + "/");

		this.liveConnections.put(hostPortSpec, conn);
		this.connectionsToHostsMap.put(conn, hostPortSpec);

		return conn;
	}

	/**
	 * @param e
	 * @throws SQLException
	 * @throws Throwable
	 * @throws InvocationTargetException
	 */
	void dealWithInvocationException(InvocationTargetException e)
			throws SQLException, Throwable, InvocationTargetException {
		Throwable t = e.getTargetException();

		if (t != null) {
			if (t instanceof SQLException) {
				String sqlState = ((SQLException) t).getSQLState();

				if (sqlState != null) {
					if (sqlState.startsWith("08")) {
						// connection error, close up shop on current
						// connection
						invalidateCurrentConnection();
					}
				}
			}

			throw t;
		}

		throw e;
	}

	/**
	 * Closes current connection and removes it from required mappings.
	 * 
	 * @throws SQLException
	 */
	synchronized void invalidateCurrentConnection() throws SQLException {
		try {
			if (!this.currentConn.isClosed()) {
				this.currentConn.close();
			}

		} finally {
			this.liveConnections.remove(this.connectionsToHostsMap
					.get(this.currentConn));
			this.connectionsToHostsMap.remove(this.currentConn);
		}
	}

	/**
	 * Proxies method invocation on the java.sql.Connection interface, trapping
	 * "close", "isClosed" and "commit/rollback" (to switch connections for load
	 * balancing).
	 */
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {

		String methodName = method.getName();

		if ("close".equals(methodName)) {
			synchronized (this.liveConnections) {
				// close all underlying connections
				Iterator allConnections = this.liveConnections.values()
						.iterator();

				while (allConnections.hasNext()) {
					((Connection) allConnections.next()).close();
				}

				if (!this.isClosed) {
					this.balancer.destroy();
				}
				
				this.liveConnections.clear();
				this.connectionsToHostsMap.clear();
			}

			return null;
		}

		if ("isClosed".equals(methodName)) {
			return Boolean.valueOf(this.isClosed);
		}

		if (this.isClosed) {
			throw SQLError.createSQLException(
					"No operations allowed after connection closed.",
					SQLError.SQL_STATE_CONNECTION_NOT_OPEN);
		}

		if (!inTransaction) {
			this.inTransaction = true;
			this.transactionStartTime = getLocalTimeBestResolution();
		}

		Object result = null;

		try {
			result = method.invoke(this.currentConn, args);

			if (result != null) {
				if (result instanceof com.mysql.jdbc.Statement) {
					((com.mysql.jdbc.Statement)result).setPingTarget(this);
				}
				
				result = proxyIfInterfaceIsJdbc(result, result.getClass());
			}
		} catch (InvocationTargetException e) {
			dealWithInvocationException(e);
		} finally {
			if ("commit".equals(methodName) || "rollback".equals(methodName)) {
				this.inTransaction = false;

				// Update stats
				int hostIndex = ((Integer) this.hostsToListIndexMap
						.get(this.connectionsToHostsMap.get(this.currentConn)))
						.intValue();

				synchronized (this.responseTimes) {
					this.responseTimes[hostIndex] = getLocalTimeBestResolution()
							- this.transactionStartTime;
				}

				pickNewConnection();
			}
		}

		return result;
	}

	/**
	 * Picks the "best" connection to use for the next transaction based on the
	 * BalanceStrategy in use.
	 * 
	 * @throws SQLException
	 */
	private synchronized void pickNewConnection() throws SQLException {
		if (this.currentConn == null) {
			this.currentConn = this.balancer.pickConnection(this,
					Collections.unmodifiableList(this.hostList),
					Collections.unmodifiableMap(this.liveConnections),
					(long[]) this.responseTimes.clone(),
					this.retriesAllDown);

			return;
		}

		Connection newConn = this.balancer.pickConnection(this,
				Collections.unmodifiableList(this.hostList),
				Collections.unmodifiableMap(this.liveConnections),
				(long[]) this.responseTimes.clone(),
				this.retriesAllDown);

		newConn.setTransactionIsolation(this.currentConn
				.getTransactionIsolation());
		newConn.setAutoCommit(this.currentConn.getAutoCommit());
		this.currentConn = newConn;
	}

	/**
	 * Recursively checks for interfaces on the given object to determine if it
	 * implements a java.sql interface, and if so, proxies the instance so that
	 * we can catch and fire SQL errors.
	 * 
	 * @param toProxy
	 * @param clazz
	 * @return
	 */
	Object proxyIfInterfaceIsJdbc(Object toProxy, Class clazz) {
		Class[] interfaces = clazz.getInterfaces();

		for (int i = 0; i < interfaces.length; i++) {
			String packageName = interfaces[i].getPackage().getName();

			if ("java.sql".equals(packageName)
					|| "javax.sql".equals(packageName)) {
				return Proxy.newProxyInstance(toProxy.getClass()
						.getClassLoader(), interfaces,
						new ConnectionErrorFiringInvocationHandler(toProxy));
			}

			return proxyIfInterfaceIsJdbc(toProxy, interfaces[i]);
		}

		return toProxy;
	}

	/**
	 * Returns best-resolution representation of local time, using nanoTime() if
	 * availble, otherwise defaulting to currentTimeMillis().
	 */
	private static long getLocalTimeBestResolution() {
		if (getLocalTimeMethod != null) {
			try {
				return ((Long) getLocalTimeMethod.invoke(null, null))
						.longValue();
			} catch (IllegalArgumentException e) {
				// ignore - we fall through to currentTimeMillis()
			} catch (IllegalAccessException e) {
				// ignore - we fall through to currentTimeMillis()
			} catch (InvocationTargetException e) {
				// ignore - we fall through to currentTimeMillis()
			}
		}

		return System.currentTimeMillis();
	}

	public synchronized void doPing() throws SQLException {
		Iterator allConns = this.liveConnections.values().iterator();
		
		while (allConns.hasNext()) {
			((Connection)allConns.next()).ping();
		}
	}
}