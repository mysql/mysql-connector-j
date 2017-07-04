/*
  Copyright (c) 2002, 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc.jdbc2.optional;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.Map;

import com.mysql.jdbc.ExceptionInterceptor;
import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.Util;

/**
 * Base class for all wrapped instances created by LogicalHandle
 */
abstract class WrapperBase {
    protected MysqlPooledConnection pooledConnection;

    /**
     * Fires connection error event if required, before re-throwing exception
     * 
     * @param sqlEx
     *            the SQLException that has occurred
     * @throws SQLException
     *             (rethrown)
     */
    protected void checkAndFireConnectionError(SQLException sqlEx) throws SQLException {
        if (this.pooledConnection != null) {
            if (SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE.equals(sqlEx.getSQLState())) {
                this.pooledConnection.callConnectionEventListeners(MysqlPooledConnection.CONNECTION_ERROR_EVENT, sqlEx);
            }
        }

        throw sqlEx;
    }

    protected Map<Class<?>, Object> unwrappedInterfaces = null;
    protected ExceptionInterceptor exceptionInterceptor;

    protected WrapperBase(MysqlPooledConnection pooledConnection) {
        this.pooledConnection = pooledConnection;
        this.exceptionInterceptor = this.pooledConnection.getExceptionInterceptor();
    }

    protected class ConnectionErrorFiringInvocationHandler implements InvocationHandler {
        Object invokeOn = null;

        public ConnectionErrorFiringInvocationHandler(Object toInvokeOn) {
            this.invokeOn = toInvokeOn;
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if ("equals".equals(method.getName())) {
                // Let args[0] "unwrap" to its InvocationHandler if it is a proxy.
                return args[0].equals(this);
            }

            Object result = null;

            try {
                result = method.invoke(this.invokeOn, args);

                if (result != null) {
                    result = proxyIfInterfaceIsJdbc(result, result.getClass());
                }
            } catch (InvocationTargetException e) {
                if (e.getTargetException() instanceof SQLException) {
                    checkAndFireConnectionError((SQLException) e.getTargetException());
                } else {
                    throw e;
                }
            }

            return result;
        }

        /**
         * Recursively checks for interfaces on the given object to determine
         * if it implements a java.sql interface, and if so, proxies the
         * instance so that we can catch and fire SQL errors.
         * 
         * @param toProxy
         * @param clazz
         */
        private Object proxyIfInterfaceIsJdbc(Object toProxy, Class<?> clazz) {
            Class<?>[] interfaces = clazz.getInterfaces();

            for (Class<?> iclass : interfaces) {
                String packageName = Util.getPackageName(iclass);

                if ("java.sql".equals(packageName) || "javax.sql".equals(packageName)) {
                    return Proxy.newProxyInstance(toProxy.getClass().getClassLoader(), interfaces, new ConnectionErrorFiringInvocationHandler(toProxy));
                }

                return proxyIfInterfaceIsJdbc(toProxy, iclass);
            }

            return toProxy;
        }
    }
}