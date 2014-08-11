/*
  Copyright (c) 2010, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class FailoverConnectionProxy extends LoadBalancingConnectionProxy {
    class FailoverInvocationHandler extends ConnectionErrorFiringInvocationHandler {

        public FailoverInvocationHandler(Object toInvokeOn) {
            super(toInvokeOn);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();

            if (FailoverConnectionProxy.this.failedOver && methodName.indexOf("execute") != -1) {
                FailoverConnectionProxy.this.queriesIssuedFailedOver++;
            }

            return super.invoke(proxy, method, args);
        }

    }

    boolean failedOver;
    boolean hasTriedMaster;
    private long masterFailTimeMillis;
    boolean preferSlaveDuringFailover;
    private String primaryHostPortSpec;
    private long queriesBeforeRetryMaster;
    long queriesIssuedFailedOver;
    private int secondsBeforeRetryMaster;

    FailoverConnectionProxy(List<String> hosts, Properties props) throws SQLException {
        super(hosts, props);
        ConnectionPropertiesImpl connectionProps = new ConnectionPropertiesImpl();
        connectionProps.initializeProperties(props);

        this.queriesBeforeRetryMaster = connectionProps.getQueriesBeforeRetryMaster();
        this.secondsBeforeRetryMaster = connectionProps.getSecondsBeforeRetryMaster();
        this.preferSlaveDuringFailover = false;
    }

    @Override
    protected ConnectionErrorFiringInvocationHandler createConnectionProxy(Object toProxy) {
        return new FailoverInvocationHandler(toProxy);
    }

    // slightly different behavior than load balancing, we only pick a new connection if we've issued enough queries or enough time has passed since we failed
    // over, and that's all handled in pickNewConnection().
    @Override
    synchronized void dealWithInvocationException(InvocationTargetException e) throws SQLException, Throwable, InvocationTargetException {
        Throwable t = e.getTargetException();

        if (t != null) {
            if (this.failedOver) { // try and fall back
                createPrimaryConnection();

                if (this.currentConn != null) {
                    throw t;
                }
            }

            failOver();

            throw t;
        }

        throw e;
    }

    @Override
    public synchronized Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if ("setPreferSlaveDuringFailover".equals(methodName)) {
            this.preferSlaveDuringFailover = ((Boolean) args[0]).booleanValue();
        } else if ("clearHasTriedMaster".equals(methodName)) {
            this.hasTriedMaster = false;
        } else if ("hasTriedMaster".equals(methodName)) {
            return Boolean.valueOf(this.hasTriedMaster);
        } else if ("isMasterConnection".equals(methodName)) {
            return Boolean.valueOf(!this.failedOver);
        } else if ("isSlaveConnection".equals(methodName)) {
            return Boolean.valueOf(this.failedOver);
        } else if ("setReadOnly".equals(methodName)) {
            if (this.failedOver) {
                return null; // no-op when failed over
            }
        } else if ("setAutoCommit".equals(methodName) && this.failedOver && shouldFallBack() && Boolean.TRUE.equals(args[0]) && this.failedOver) {
            createPrimaryConnection();

            return super.invoke(proxy, method, args, this.failedOver);
        } else if ("hashCode".equals(methodName)) {
            return Integer.valueOf(this.hashCode());
        } else if ("equals".equals(methodName)) {
            if (args[0] instanceof Proxy) {
                return Boolean.valueOf((((Proxy) args[0]).equals(this)));
            }
            return Boolean.valueOf(this.equals(args[0]));
        }
        return super.invoke(proxy, method, args, this.failedOver);
    }

    private synchronized void createPrimaryConnection() throws SQLException {
        try {
            this.currentConn = createConnectionForHost(this.primaryHostPortSpec);
            this.failedOver = false;
            this.hasTriedMaster = true;

            // reset failed-over state
            this.queriesIssuedFailedOver = 0;
        } catch (SQLException sqlEx) {
            this.failedOver = true;

            if (this.currentConn != null) {
                this.currentConn.getLog().logWarn("Connection to primary host failed", sqlEx);
            }
        }
    }

    @Override
    synchronized void invalidateCurrentConnection() throws SQLException {
        if (!this.failedOver) {
            this.failedOver = true;
            this.queriesIssuedFailedOver = 0;
            this.masterFailTimeMillis = System.currentTimeMillis();
        }
        super.invalidateCurrentConnection();
    }

    @Override
    protected synchronized void pickNewConnection() throws SQLException {
        if (this.isClosed && this.closedExplicitly) {
            return;
        }

        if (this.primaryHostPortSpec == null) {
            this.primaryHostPortSpec = this.hostList.remove(0); // first connect
        }

        if (this.currentConn == null || (this.failedOver && shouldFallBack())) {
            createPrimaryConnection();

            if (this.currentConn != null) {
                return;
            }
        }

        failOver();
    }

    private synchronized void failOver() throws SQLException {
        if (this.failedOver) {
            Iterator<Map.Entry<String, ConnectionImpl>> iter = this.liveConnections.entrySet().iterator();

            while (iter.hasNext()) {
                Map.Entry<String, ConnectionImpl> entry = iter.next();
                entry.getValue().close();
            }

            this.liveConnections.clear();
        }

        super.pickNewConnection();

        if (this.currentConn.getFailOverReadOnly()) {
            this.currentConn.setReadOnly(true);
        } else {
            this.currentConn.setReadOnly(false);
        }

        this.failedOver = true;
    }

    /**
     * Should we try to connect back to the master? We try when we've been
     * failed over >= this.secondsBeforeRetryMaster _or_ we've issued >
     * this.queriesIssuedFailedOver
     */
    private boolean shouldFallBack() {
        long secondsSinceFailedOver = (System.currentTimeMillis() - this.masterFailTimeMillis) / 1000;

        if (secondsSinceFailedOver >= this.secondsBeforeRetryMaster) {
            // reset the timer
            this.masterFailTimeMillis = System.currentTimeMillis();

            return true;
        } else if (this.queriesBeforeRetryMaster != 0 && this.queriesIssuedFailedOver >= this.queriesBeforeRetryMaster) {
            return true;
        }

        return false;
    }
}
