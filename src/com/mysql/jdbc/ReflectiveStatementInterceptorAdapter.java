/*
  Copyright (c) 2009, 2016, Oracle and/or its affiliates. All rights reserved.

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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Properties;

public class ReflectiveStatementInterceptorAdapter implements StatementInterceptorV2 {

    private final StatementInterceptor toProxy;

    final Method v2PostProcessMethod;

    public ReflectiveStatementInterceptorAdapter(StatementInterceptor toProxy) {
        this.toProxy = toProxy;
        this.v2PostProcessMethod = getV2PostProcessMethod(toProxy.getClass());
    }

    public void destroy() {
        this.toProxy.destroy();
    }

    public boolean executeTopLevelOnly() {
        return this.toProxy.executeTopLevelOnly();
    }

    public void init(Connection conn, Properties props) throws SQLException {
        this.toProxy.init(conn, props);
    }

    public ResultSetInternalMethods postProcess(String sql, Statement interceptedStatement, ResultSetInternalMethods originalResultSet, Connection connection,
            int warningCount, boolean noIndexUsed, boolean noGoodIndexUsed, SQLException statementException) throws SQLException {
        try {
            return (ResultSetInternalMethods) this.v2PostProcessMethod.invoke(this.toProxy,
                    new Object[] { sql, interceptedStatement, originalResultSet, connection, Integer.valueOf(warningCount),
                            noIndexUsed ? Boolean.TRUE : Boolean.FALSE, noGoodIndexUsed ? Boolean.TRUE : Boolean.FALSE, statementException });
        } catch (IllegalArgumentException e) {
            SQLException sqlEx = new SQLException("Unable to reflectively invoke interceptor");
            sqlEx.initCause(e);

            throw sqlEx;
        } catch (IllegalAccessException e) {
            SQLException sqlEx = new SQLException("Unable to reflectively invoke interceptor");
            sqlEx.initCause(e);

            throw sqlEx;
        } catch (InvocationTargetException e) {
            SQLException sqlEx = new SQLException("Unable to reflectively invoke interceptor");
            sqlEx.initCause(e);

            throw sqlEx;
        }
    }

    public ResultSetInternalMethods preProcess(String sql, Statement interceptedStatement, Connection connection) throws SQLException {
        return this.toProxy.preProcess(sql, interceptedStatement, connection);
    }

    public static final Method getV2PostProcessMethod(Class<?> toProxyClass) {
        try {
            Method postProcessMethod = toProxyClass.getMethod("postProcess", new Class[] { String.class, Statement.class, ResultSetInternalMethods.class,
                    Connection.class, Integer.TYPE, Boolean.TYPE, Boolean.TYPE, SQLException.class });

            return postProcessMethod;
        } catch (SecurityException e) {
            return null;
        } catch (NoSuchMethodException e) {
            return null;
        }
    }
}
