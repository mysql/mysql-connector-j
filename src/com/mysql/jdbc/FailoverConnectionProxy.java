/*
 Copyright 2010 Sun Microsystems
 All rights reserved. Use is subject to license terms.

  The MySQL Connector/J is licensed under the terms of the GPL,
  like most MySQL Connectors. There are special exceptions to the
  terms and conditions of the GPL as it is applied to this software,
  see the FLOSS License Exception available on mysql.com.

  This program is free software; you can redistribute it and/or
  modify it under the terms of the GNU General Public License as
  published by the Free Software Foundation; version 2 of the
  License.

  This program is distributed in the hope that it will be useful,  
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  02110-1301 USA
 
 */

package com.mysql.jdbc;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class FailoverConnectionProxy extends LoadBalancingConnectionProxy {
	boolean failedOver;
	boolean preferSlaveDuringFailover;
	long queriesIssuedFailedOver;
	private long queriesBeforeRetryMaster;
	private int secondsBeforeRetryMaster;
	private long masterFailTimeMillis;
	private String primaryHostPortSpec;
	boolean hasTriedMaster;
	
	class FailoverInvocationHandler extends ConnectionErrorFiringInvocationHandler {

		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			String methodName = method.getName();
			
			if (failedOver && methodName.indexOf("execute") != -1) {
				queriesIssuedFailedOver++;
			}
			
			if ("setPreferSlaveDuringFailover".equals(methodName)) {
				preferSlaveDuringFailover = ((Boolean)args[0]).booleanValue();
			} else if ("clearHasTriedMaster".equals(methodName)) {
				hasTriedMaster = false;
			} else if ("hasTriedMaster".equals(methodName)) {
				return Boolean.valueOf(hasTriedMaster);
			}
			
			return super.invoke(proxy, method, args);
		}

		public FailoverInvocationHandler(Object toInvokeOn) {
			super(toInvokeOn);
		}
		
	}
	
	FailoverConnectionProxy(List hosts, Properties props) throws SQLException {
		super(hosts, props);
		ConnectionPropertiesImpl connectionProps = new ConnectionPropertiesImpl();
		connectionProps.initializeProperties(props);
		
		this.queriesBeforeRetryMaster = connectionProps.getQueriesBeforeRetryMaster();
		this.secondsBeforeRetryMaster = connectionProps.getSecondsBeforeRetryMaster();
		this.preferSlaveDuringFailover = false;
	}
	
	

	synchronized void invalidateCurrentConnection() throws SQLException {
		if (!this.failedOver) {
			this.failedOver = true;
			this.queriesIssuedFailedOver = 0;
			this.masterFailTimeMillis = System.currentTimeMillis();
		}
		super.invalidateCurrentConnection();
	}

	protected synchronized void pickNewConnection() throws SQLException {
		if (this.primaryHostPortSpec == null) {
			this.primaryHostPortSpec = (String)this.hostList.remove(0); // first connect
		}

		if (this.currentConn == null || (this.failedOver && shouldFallBack())) {
			createPrimaryConnection();
			
			if (this.currentConn != null) {
				return;
			}
		}
		
		super.pickNewConnection();
		
		this.failedOver = true;
	}

	private void createPrimaryConnection() throws SQLException {
		try {
			this.currentConn = createConnectionForHost(this.primaryHostPortSpec);
			this.failedOver = false;
			this.hasTriedMaster = true;
		} catch (SQLException sqlEx) {
			this.failedOver = true;
			
			if (this.currentConn != null) {
				this.currentConn.getLog().logWarn("Connection to primary host failed", sqlEx);
			}
		}
	}

	/**
	 * Should we try to connect back to the master? We try when we've been
	 * failed over >= this.secondsBeforeRetryMaster _or_ we've issued >
	 * this.queriesIssuedFailedOver
	 * 
	 * @return DOCUMENT ME!
	 */
	private boolean shouldFallBack() {
		long secondsSinceFailedOver = (System.currentTimeMillis() - this.masterFailTimeMillis) / 1000;

		// Done this way so we can set a condition in the debugger
		boolean tryFallback = ((secondsSinceFailedOver >= this.secondsBeforeRetryMaster) || (this.queriesIssuedFailedOver >= this.queriesBeforeRetryMaster));

		return tryFallback;
	}
}
