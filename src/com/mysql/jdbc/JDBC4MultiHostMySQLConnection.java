/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.NClob;
import java.sql.Struct;
import java.util.Properties;
import java.util.TimerTask;

import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.Messages;
import com.mysql.jdbc.MultiHostConnectionProxy;
import com.mysql.jdbc.MultiHostMySQLConnection;
import com.mysql.jdbc.SQLError;

public class JDBC4MultiHostMySQLConnection extends MultiHostMySQLConnection implements JDBC4MySQLConnection {

    public JDBC4MultiHostMySQLConnection(MultiHostConnectionProxy proxy) throws SQLException {
        super(proxy);
    }

    private JDBC4Connection getJDBC4Connection() {
        return (JDBC4Connection) this.proxy.currentConnection;
    }

    public SQLXML createSQLXML() throws SQLException {
        return this.getJDBC4Connection().createSQLXML();
    }

    public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return this.getJDBC4Connection().createArrayOf(typeName, elements);
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return this.getJDBC4Connection().createStruct(typeName, attributes);
    }

    public Properties getClientInfo() throws SQLException {
        return this.getJDBC4Connection().getClientInfo();
    }

    public String getClientInfo(String name) throws SQLException {
        return this.getJDBC4Connection().getClientInfo(name);
    }

    public boolean isValid(int timeout) throws SQLException {
        synchronized (proxy) {
            return this.getJDBC4Connection().isValid(timeout);
        }
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        this.getJDBC4Connection().setClientInfo(properties);
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        this.getJDBC4Connection().setClientInfo(name, value);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        checkClosed();

        // This works for classes that aren't actually wrapping anything
        return iface.isInstance(this);
    }

    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
        try {
            // This works for classes that aren't actually wrapping anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException("Unable to unwrap to " + iface.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }
    }

    /**
     * @see java.sql.Connection#createBlob()
     */
    public Blob createBlob() {
        return this.getJDBC4Connection().createBlob();
    }

    /**
     * @see java.sql.Connection#createClob()
     */
    public Clob createClob() {
        return this.getJDBC4Connection().createClob();
    }

    /**
     * @see java.sql.Connection#createNClob()
     */
    public NClob createNClob() {
        return this.getJDBC4Connection().createNClob();
    }

    protected JDBC4ClientInfoProvider getClientInfoProviderImpl() throws SQLException {
        synchronized (proxy) {
            return this.getJDBC4Connection().getClientInfoProviderImpl();
        }
    }
}
