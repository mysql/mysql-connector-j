/*
  Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * An abstract class that processes generic multi-host configurations. This class has to be sub-classed by specific multi-host implementations, such as
 * load-balancing and failover.
 */
public abstract class MultiHostConnectionProxy implements InvocationHandler {
    private static final String METHOD_GET_MULTI_HOST_SAFE_PROXY = "getMultiHostSafeProxy";
    private static final String METHOD_EQUALS = "equals";
    private static final String METHOD_HASH_CODE = "hashCode";
    private static final String METHOD_CLOSE = "close";
    private static final String METHOD_ABORT_INTERNAL = "abortInternal";
    private static final String METHOD_ABORT = "abort";
    private static final String METHOD_IS_CLOSED = "isClosed";
    private static final String METHOD_GET_AUTO_COMMIT = "getAutoCommit";
    private static final String METHOD_GET_CATALOG = "getCatalog";
    private static final String METHOD_GET_TRANSACTION_ISOLATION = "getTransactionIsolation";
    private static final String METHOD_GET_SESSION_MAX_ROWS = "getSessionMaxRows";

    List<String> hostList;
    Properties localProps;

    boolean autoReconnect = false;

    MySQLConnection thisAsConnection = null;
    MySQLConnection proxyConnection = null;

    MySQLConnection currentConnection = null;

    boolean isClosed = false;
    boolean closedExplicitly = false;
    String closedReason = null;

    // Keep track of the last exception processed in 'dealWithInvocationException()' in order to avoid creating connections repeatedly from each time the same
    // exception is caught in every proxy instance belonging to the same call stack.
    protected Throwable lastExceptionDealtWith = null;

    private static Constructor<?> JDBC_4_MS_CONNECTION_CTOR;

    static {
        if (Util.isJdbc4()) {
            try {
                JDBC_4_MS_CONNECTION_CTOR = Class.forName("com.mysql.jdbc.JDBC4MultiHostMySQLConnection")
                        .getConstructor(new Class[] { MultiHostConnectionProxy.class });
            } catch (SecurityException e) {
                throw new RuntimeException(e);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Proxy class to intercept and deal with errors that may occur in any object bound to the current connection.
     */
    class JdbcInterfaceProxy implements InvocationHandler {
        Object invokeOn = null;

        JdbcInterfaceProxy(Object toInvokeOn) {
            this.invokeOn = toInvokeOn;
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (METHOD_EQUALS.equals(method.getName())) {
                // Let args[0] "unwrap" to its InvocationHandler if it is a proxy.
                return args[0].equals(this);
            }

            synchronized (MultiHostConnectionProxy.this) {
                Object result = null;

                try {
                    result = method.invoke(this.invokeOn, args);
                    result = proxyIfReturnTypeIsJdbcInterface(method.getReturnType(), result);
                } catch (InvocationTargetException e) {
                    dealWithInvocationException(e);
                }

                return result;
            }
        }
    }

    /**
     * Initializes a connection wrapper for this MultiHostConnectionProxy instance.
     * 
     * @param props
     *            The properties to be used in new internal connections.
     */
    MultiHostConnectionProxy() throws SQLException {
        this.thisAsConnection = getNewWrapperForThisAsConnection();
    }

    /**
     * Constructs a MultiHostConnectionProxy instance for the given list of hosts and connection properties.
     * 
     * @param hosts
     *            The lists of hosts available to switch on.
     * @param props
     *            The properties to be used in new internal connections.
     */
    MultiHostConnectionProxy(List<String> hosts, Properties props) throws SQLException {
        this();
        initializeHostsSpecs(hosts, props);
    }

    /**
     * Initializes the hosts lists and makes a "clean" local copy of the given connection properties so that it can be later used to create standard
     * connections.
     * 
     * @param hosts
     *            The list of hosts for this multi-host connection.
     * @param props
     *            Connection properties from where to get initial settings and to be used in new connections.
     * @return
     *         The number of hosts found in the hosts list.
     */
    int initializeHostsSpecs(List<String> hosts, Properties props) {
        this.autoReconnect = "true".equalsIgnoreCase(props.getProperty("autoReconnect")) || "true".equalsIgnoreCase(props.getProperty("autoReconnectForPools"));

        this.hostList = hosts;
        int numHosts = this.hostList.size();

        this.localProps = (Properties) props.clone();
        this.localProps.remove(NonRegisteringDriver.HOST_PROPERTY_KEY);
        this.localProps.remove(NonRegisteringDriver.PORT_PROPERTY_KEY);

        for (int i = 0; i < numHosts; i++) {
            this.localProps.remove(NonRegisteringDriver.HOST_PROPERTY_KEY + "." + (i + 1));
            this.localProps.remove(NonRegisteringDriver.PORT_PROPERTY_KEY + "." + (i + 1));
        }

        this.localProps.remove(NonRegisteringDriver.NUM_HOSTS_PROPERTY_KEY);
        this.localProps.setProperty("useLocalSessionState", "true");

        return numHosts;
    }

    /**
     * Wraps this object with a new multi-host Connection instance.
     * 
     * @return
     *         The connection object instance that wraps 'this'.
     */
    MySQLConnection getNewWrapperForThisAsConnection() throws SQLException {
        if (Util.isJdbc4() || JDBC_4_MS_CONNECTION_CTOR != null) {
            return (MySQLConnection) Util.handleNewInstance(JDBC_4_MS_CONNECTION_CTOR, new Object[] { this }, null);
        }

        return new MultiHostMySQLConnection(this);
    }

    /**
     * Get this connection's proxy.
     * A multi-host connection may not be at top level in the multi-host connections chain. In such case the first connection in the chain is available as a
     * proxy.
     * 
     * @return
     *         Returns this connection's proxy if there is one or itself if this is the first one.
     */
    protected MySQLConnection getProxy() {
        return this.proxyConnection != null ? this.proxyConnection : this.thisAsConnection;
    }

    /**
     * Sets this connection's proxy. This proxy should be the first connection in the multi-host connections chain.
     * After setting the connection proxy locally, propagates it through the dependant connections.
     * 
     * @param proxyConn
     *            The top level connection in the multi-host connections chain.
     */
    protected final void setProxy(MySQLConnection proxyConn) {
        this.proxyConnection = proxyConn;
        propagateProxyDown(proxyConn);
    }

    /**
     * Propagates the connection proxy down through the multi-host connections chain.
     * This method is intended to be overridden in subclasses that manage more than one active connection at same time.
     * 
     * @param proxyConn
     *            The top level connection in the multi-host connections chain.
     */
    protected void propagateProxyDown(MySQLConnection proxyConn) {
        this.currentConnection.setProxy(proxyConn);
    }

    /**
     * If the given return type is or implements a JDBC interface, proxies the given object so that we can catch SQL errors and fire a connection switch.
     * 
     * @param returnType
     *            The type the object instance to proxy is supposed to be.
     * @param toProxy
     *            The object instance to proxy.
     * @return
     *         The proxied object or the original one if it does not implement a JDBC interface.
     */
    Object proxyIfReturnTypeIsJdbcInterface(Class<?> returnType, Object toProxy) {
        if (toProxy != null) {
            if (Util.isJdbcInterface(returnType)) {
                Class<?> toProxyClass = toProxy.getClass();
                return Proxy.newProxyInstance(toProxyClass.getClassLoader(), Util.getImplementedInterfaces(toProxyClass), getNewJdbcInterfaceProxy(toProxy));
            }
        }
        return toProxy;
    }

    /**
     * Instantiates a new JdbcInterfaceProxy for the given object. Subclasses can override this to return instances of JdbcInterfaceProxy subclasses.
     * 
     * @param toProxy
     *            The object instance to be proxied.
     * @return
     *         The new InvocationHandler instance.
     */
    InvocationHandler getNewJdbcInterfaceProxy(Object toProxy) {
        return new JdbcInterfaceProxy(toProxy);
    }

    /**
     * Deals with InvocationException from proxied objects.
     * 
     * @param e
     *            The Exception instance to check.
     */
    void dealWithInvocationException(InvocationTargetException e) throws SQLException, Throwable, InvocationTargetException {
        Throwable t = e.getTargetException();

        if (t != null) {
            if (this.lastExceptionDealtWith != t && shouldExceptionTriggerConnectionSwitch(t)) {
                invalidateCurrentConnection();
                pickNewConnection();
                this.lastExceptionDealtWith = t;
            }
            throw t;
        }
        throw e;
    }

    /**
     * Checks if the given throwable should trigger a connection switch.
     * 
     * @param t
     *            The Throwable instance to analyze.
     */
    abstract boolean shouldExceptionTriggerConnectionSwitch(Throwable t);

    /**
     * Checks if current connection is to a master host.
     */
    abstract boolean isMasterConnection();

    /**
     * Invalidates the current connection.
     */
    synchronized void invalidateCurrentConnection() throws SQLException {
        invalidateConnection(this.currentConnection);
    }

    /**
     * Invalidates the specified connection by closing it.
     * 
     * @param conn
     *            The connection instance to invalidate.
     */
    synchronized void invalidateConnection(MySQLConnection conn) throws SQLException {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.realClose(true, !conn.getAutoCommit(), true, null);
            }
        } catch (SQLException e) {
            // swallow this exception, current connection should be useless anyway.
        }
    }

    /**
     * Picks the "best" connection to use from now on. Each subclass needs to implement its connection switch strategy on it.
     */
    abstract void pickNewConnection() throws SQLException;

    /**
     * Creates a new physical connection for the given host:port.
     * 
     * @param hostPortSpec
     *            The host:port specification.
     * @return
     *         The new Connection instance.
     */
    synchronized ConnectionImpl createConnectionForHost(String hostPortSpec) throws SQLException {
        Properties connProps = (Properties) this.localProps.clone();

        String[] hostPortPair = NonRegisteringDriver.parseHostPortPair(hostPortSpec);
        String hostName = hostPortPair[NonRegisteringDriver.HOST_NAME_INDEX];
        String portNumber = hostPortPair[NonRegisteringDriver.PORT_NUMBER_INDEX];
        String dbName = connProps.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);

        if (hostName == null) {
            throw new SQLException("Could not find a hostname to start a connection to");
        }
        if (portNumber == null) {
            portNumber = "3306"; // use default
        }

        connProps.setProperty(NonRegisteringDriver.HOST_PROPERTY_KEY, hostName);
        connProps.setProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, portNumber);
        connProps.setProperty(NonRegisteringDriver.HOST_PROPERTY_KEY + ".1", hostName);
        connProps.setProperty(NonRegisteringDriver.PORT_PROPERTY_KEY + ".1", portNumber);
        connProps.setProperty(NonRegisteringDriver.NUM_HOSTS_PROPERTY_KEY, "1");
        connProps.setProperty("roundRobinLoadBalance", "false"); // make sure we don't pickup the default value

        ConnectionImpl conn = (ConnectionImpl) ConnectionImpl.getInstance(hostName, Integer.parseInt(portNumber), connProps, dbName,
                "jdbc:mysql://" + hostName + ":" + portNumber + "/");

        conn.setProxy(getProxy());

        return conn;
    }

    /**
     * Synchronizes session state between two connections.
     * 
     * @param source
     *            The connection where to get state from.
     * @param target
     *            The connection where to set state.
     */
    static void syncSessionState(Connection source, Connection target) throws SQLException {
        if (source == null || target == null) {
            return;
        }
        syncSessionState(source, target, source.isReadOnly());
    }

    /**
     * Synchronizes session state between two connections, allowing to override the read-only status.
     * 
     * @param source
     *            The connection where to get state from.
     * @param target
     *            The connection where to set state.
     * @param readOnly
     *            The new read-only status.
     */
    static void syncSessionState(Connection source, Connection target, boolean readOnly) throws SQLException {
        if (target != null) {
            target.setReadOnly(readOnly);
        }

        if (source == null || target == null) {
            return;
        }
        target.setAutoCommit(source.getAutoCommit());
        target.setCatalog(source.getCatalog());
        target.setTransactionIsolation(source.getTransactionIsolation());
        target.setSessionMaxRows(source.getSessionMaxRows());
    }

    /**
     * Executes a close() invocation;
     */
    abstract void doClose() throws SQLException;

    /**
     * Executes a abortInternal() invocation;
     */
    abstract void doAbortInternal() throws SQLException;

    /**
     * Executes a abort() invocation;
     */
    abstract void doAbort(Executor executor) throws SQLException;

    /**
     * Proxies method invocation on the java.sql.Connection interface, trapping multi-host specific methods and generic methods.
     * Subclasses have to override this to complete the method invocation process, deal with exceptions and decide when to switch connection.
     * To avoid unnecessary additional exception handling overriders should consult #canDealWith(Method) before chaining here.
     */
    public synchronized Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (METHOD_GET_MULTI_HOST_SAFE_PROXY.equals(methodName)) {
            return this.thisAsConnection;
        }

        if (METHOD_EQUALS.equals(methodName)) {
            // Let args[0] "unwrap" to its InvocationHandler if it is a proxy.
            return args[0].equals(this);
        }

        if (METHOD_HASH_CODE.equals(methodName)) {
            return this.hashCode();
        }

        if (METHOD_CLOSE.equals(methodName)) {
            doClose();
            this.isClosed = true;
            this.closedReason = "Connection explicitly closed.";
            this.closedExplicitly = true;
            return null;
        }

        if (METHOD_ABORT_INTERNAL.equals(methodName)) {
            doAbortInternal();
            this.currentConnection.abortInternal();
            this.isClosed = true;
            this.closedReason = "Connection explicitly closed.";
            return null;
        }

        if (METHOD_ABORT.equals(methodName) && args.length == 1) {
            doAbort((Executor) args[0]);
            this.isClosed = true;
            this.closedReason = "Connection explicitly closed.";
            return null;
        }

        if (METHOD_IS_CLOSED.equals(methodName)) {
            return this.isClosed;
        }

        try {
            return invokeMore(proxy, method, args);
        } catch (InvocationTargetException e) {
            throw e.getCause() != null ? e.getCause() : e;
        } catch (Exception e) {
            // Check if the captured exception must be wrapped by an unchecked exception.
            Class<?>[] declaredException = method.getExceptionTypes();
            for (Class<?> declEx : declaredException) {
                if (declEx.isAssignableFrom(e.getClass())) {
                    throw e;
                }
            }
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * Continuation of the method invocation process, to be implemented within each subclass.
     */
    abstract Object invokeMore(Object proxy, Method method, Object[] args) throws Throwable;

    /**
     * Checks if the given method is allowed on closed connections.
     */
    protected boolean allowedOnClosedConnection(Method method) {
        String methodName = method.getName();

        return methodName.equals(METHOD_GET_AUTO_COMMIT) || methodName.equals(METHOD_GET_CATALOG) || methodName.equals(METHOD_GET_TRANSACTION_ISOLATION)
                || methodName.equals(METHOD_GET_SESSION_MAX_ROWS);
    }
}
