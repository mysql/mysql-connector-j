/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.Map;

import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.util.Util;

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
            if (MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE.equals(sqlEx.getSQLState())) {
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
         *            object to be proxied
         * @param clazz
         *            desired class
         * @return proxy object
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
