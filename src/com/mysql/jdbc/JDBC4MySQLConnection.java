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
import com.mysql.jdbc.SQLError;

public interface JDBC4MySQLConnection extends MySQLConnection {

    public SQLXML createSQLXML() throws SQLException;

    public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException;

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException;

    public Properties getClientInfo() throws SQLException;

    public String getClientInfo(String name) throws SQLException;

    public boolean isValid(int timeout) throws SQLException;

    public void setClientInfo(Properties properties) throws SQLClientInfoException;

    public void setClientInfo(String name, String value) throws SQLClientInfoException;

    public boolean isWrapperFor(Class<?> iface) throws SQLException;

    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException;

    public Blob createBlob();

    public Clob createClob();

    public NClob createNClob();

}
