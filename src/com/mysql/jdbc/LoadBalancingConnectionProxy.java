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
import java.util.Set;

/**
 * An implementation of java.sql.Connection that load balances requests across a
 * series of MySQL JDBC connections, where the balancing takes place at
 * transaction commit.
 * 
 * Therefore, for this to work (at all), you must use transactions, even if only
 * reading data.
 * 
 * This implementation will invalidate connections that it detects have had
 * communication errors when processing a request.  Problematic hosts will
 * be added to a global blacklist for loadBalanceBlacklistTimeout ms, after
 * which they will be removed from the blacklist and made eligible once again
 * to be selected for new connections.
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
	
	public static final String BLACKLIST_TIMEOUT_PROPERTY_KEY = "loadBalanceBlacklistTimeout";

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
	
	private static Map globalBlacklist = new HashMap();

	private int globalBlacklistTimeout = 0;

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
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
		}
		String blacklistTimeoutAsString = this.localProps.getProperty(BLACKLIST_TIMEOUT_PROPERTY_KEY, "0");
		
		try {
			this.globalBlacklistTimeout = Integer.parseInt(blacklistTimeoutAsString);
		} catch (NumberFormatException nfe) {
			throw SQLError.createSQLException(Messages.getString(
					"LoadBalancingConnectionProxy.badValueForLoadBalanceBlacklistTimeout",
					new Object[] { retriesAllDownAsString }),
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
		}
		
		if ("random".equals(strategy)) {
			this.balancer = (BalanceStrategy) Util.loadExtensions(null, props,
					"com.mysql.jdbc.RandomBalanceStrategy",
					"InvalidLoadBalanceStrategy", null).get(0);
		} else if ("bestResponseTime".equals(strategy)) {
			this.balancer = (BalanceStrategy) Util.loadExtensions(null, props,
					"com.mysql.jdbc.BestResponseTimeBalanceStrategy",
			"InvalidLoadBalanceStrategy", null).get(0);
		} else {
			this.balancer = (BalanceStrategy) Util.loadExtensions(null, props,
					strategy, "InvalidLoadBalanceStrategy", null).get(0);
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
			// add host to the global blacklist, if enabled
			if (this.isGlobalBlacklistEnabled()) {
				this.addToGlobalBlacklist((String) this.connectionsToHostsMap.get(this.currentConn)); 
				
			}
			// remove from liveConnections
			this.liveConnections.remove(this.connectionsToHostsMap
					.get(this.currentConn));
			Object mappedHost = this.connectionsToHostsMap.remove(this.currentConn);
			if(mappedHost != null && this.hostsToListIndexMap.containsKey(mappedHost)){
				int hostIndex = ((Integer) this.hostsToListIndexMap.get(mappedHost)).intValue();
				// reset the statistics for the host
				synchronized (this.responseTimes) {
					this.responseTimes[hostIndex] = 0;
				}
			}
		}
	}
	
	private void closeAllConnections(){
		synchronized (this) {
			// close all underlying connections
			Iterator allConnections = this.liveConnections.values()
					.iterator();

			while (allConnections.hasNext()) {
				try{
					((Connection) allConnections.next()).close();
				} catch(SQLException e){}
			}

			if (!this.isClosed) {
				this.balancer.destroy();
			}
			
			this.liveConnections.clear();
			this.connectionsToHostsMap.clear();
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

		if ("equals".equals(methodName) && args.length == 1) {
			if (args[0] instanceof Proxy) {
				return Boolean.valueOf((((Proxy)args[0]).equals(this)));
			}
			return Boolean.valueOf(this.equals(args[0]));
		}
		
		if ("hashCode".equals(methodName)) {
			return new Integer(this.hashCode());
		}
		
		if ("close".equals(methodName)) {
			closeAllConnections();

			return null;
		}

		if ("isClosed".equals(methodName)) {
			return Boolean.valueOf(this.isClosed);
		}

		if (this.isClosed) {
			throw SQLError.createSQLException(
					"No operations allowed after connection closed.",
					SQLError.SQL_STATE_CONNECTION_NOT_OPEN, null /* no access to a interceptor here... */);
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
				String host = (String) this.connectionsToHostsMap.get(this.currentConn);
				// avoid NPE if the connection has already been removed from connectionsToHostsMap
				// in invalidateCurrenctConnection()
				if ( host != null ) {
					int hostIndex = ((Integer) this.hostsToListIndexMap
							.get( host ))
							.intValue();
	
					
					synchronized (this.responseTimes) {
						this.responseTimes[hostIndex] = getLocalTimeBestResolution()
								- this.transactionStartTime;
					}
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
					|| "javax.sql".equals(packageName) 
					|| "com.mysql.jdbc".equals(packageName)) {
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
		SQLException se = null;
		boolean foundHost = false;

		synchronized(this){
			for(Iterator i = this.hostList.iterator(); i.hasNext(); ){
				String host = (String) i.next();
				Connection conn = (Connection) this.liveConnections.get(host);
				if(conn == null){
					continue;
				}
				try{
					conn.ping();
					foundHost = true;
				} catch (SQLException e){
					// give up if it is the current connection, otherwise NPE faking resultset later.
					if(host.equals(this.connectionsToHostsMap.get(this.currentConn))){
						// clean up underlying connections, since connection pool won't do it
						closeAllConnections();
						this.isClosed = true;
						throw e;
					}
					
					// if the Exception is caused by ping connection lifetime checks, don't add to blacklist
					if(e.getMessage().equals(Messages.getString("Connection.exceededConnectionLifetime"))){
						// only set the return Exception if it's null
						if(se == null){
							se = e;
						}
					} else {
						// overwrite the return Exception no matter what
						se = e;
						if(this.isGlobalBlacklistEnabled()){
							this.addToGlobalBlacklist(host);
						}
					}
					// take the connection out of the liveConnections Map
					this.liveConnections.remove(this.connectionsToHostsMap
							.get(conn));
				}
			}
		}
		// if there were no successful pings
		if(!foundHost){
			closeAllConnections();
			this.isClosed = true;
			// throw the stored Exception, if exists
			if(se != null){
				throw se;
			}
			// or create a new SQLException and throw it, must be no liveConnections
			((ConnectionImpl)this.currentConn).throwConnectionClosedException();
		}
		
	}
	
	public void addToGlobalBlacklist(String host) {
		if(this.isGlobalBlacklistEnabled()) {
			synchronized(globalBlacklist){
				globalBlacklist.put(host, new Long(System.currentTimeMillis() 
					+ this.globalBlacklistTimeout));
			}
		}
	}
	
	public boolean isGlobalBlacklistEnabled() {
		return (this.globalBlacklistTimeout > 0);
	}
	
	public Map getGlobalBlacklist() {
		if(!this.isGlobalBlacklistEnabled()) {
			return new HashMap(1);
		}
		
		// Make a local copy of the blacklist
		Map blacklistClone = new HashMap(globalBlacklist.size());
		// Copy everything from synchronized global blacklist to local copy for manipulation
		synchronized (globalBlacklist) {
			blacklistClone.putAll(globalBlacklist);
		}
		Set keys = blacklistClone.keySet();
		
		// we're only interested in blacklisted hosts that are in the hostList
		keys.retainAll(this.hostList);
		if(keys.size() == this.hostList.size()){
			// return an empty blacklist, let the BalanceStrategy implementations try to connect to everything
			// since it appears that all hosts are unavailable - we don't want to wait for 
			// loadBalanceBlacklistTimeout to expire.
			return new HashMap(1); 			
		}
		
		// Don't need to synchronize here as we using a local copy
		for(Iterator i = keys.iterator(); i.hasNext(); ) {
			String host = (String) i.next();
			// OK if null is returned because another thread already purged Map entry.
			Long timeout = (Long) globalBlacklist.get(host);
			if(timeout != null && timeout.longValue() < System.currentTimeMillis()){
				// Timeout has expired, remove from blacklist
				synchronized(globalBlacklist){
					globalBlacklist.remove(host);
				}
				i.remove();
			}
				
		}
		
		return blacklistClone;
	}
}