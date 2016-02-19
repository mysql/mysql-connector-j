/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

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

import java.io.Reader;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLXML;
import java.sql.SQLException;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.MysqlDefs;
import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.ServerPreparedStatement;
import com.mysql.jdbc.ServerPreparedStatement.BindValue;

public class JDBC4ServerPreparedStatement extends ServerPreparedStatement {

    public JDBC4ServerPreparedStatement(MySQLConnection conn, String sql, String catalog, int resultSetType, int resultSetConcurrency) throws SQLException {
        super(conn, sql, catalog, resultSetType, resultSetConcurrency);
    }

    /**
     * @see java.sql.PreparedStatement#setNCharacterStream(int, java.io.Reader, long)
     */
    public void setNCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        // can't take if characterEncoding isn't utf8
        if (!this.charEncoding.equalsIgnoreCase("UTF-8") && !this.charEncoding.equalsIgnoreCase("utf8")) {
            throw SQLError.createSQLException("Can not call setNCharacterStream() when connection character set isn't UTF-8", getExceptionInterceptor());
        }

        checkClosed();

        if (reader == null) {
            setNull(parameterIndex, java.sql.Types.BINARY);
        } else {
            BindValue binding = getBinding(parameterIndex, true);
            resetToType(binding, MysqlDefs.FIELD_TYPE_BLOB);

            binding.value = reader;
            binding.isLongData = true;

            if (this.connection.getUseStreamLengthsInPrepStmts()) {
                binding.bindLength = length;
            } else {
                binding.bindLength = -1;
            }
        }
    }

    /**
     * @see java.sql.PreparedStatement#setNClob(int, java.sql.NClob)
     */
    public void setNClob(int parameterIndex, NClob x) throws SQLException {
        setNClob(parameterIndex, x.getCharacterStream(), this.connection.getUseStreamLengthsInPrepStmts() ? x.length() : -1);
    }

    /**
     * JDBC 4.0 Set a NCLOB parameter.
     * 
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param reader
     *            the java reader which contains the UNICODE data
     * @param length
     *            the number of characters in the stream
     * 
     * @throws SQLException
     *             if a database error occurs
     */
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        // can't take if characterEncoding isn't utf8
        if (!this.charEncoding.equalsIgnoreCase("UTF-8") && !this.charEncoding.equalsIgnoreCase("utf8")) {
            throw SQLError.createSQLException("Can not call setNClob() when connection character set isn't UTF-8", getExceptionInterceptor());
        }

        checkClosed();

        if (reader == null) {
            setNull(parameterIndex, java.sql.Types.NCLOB);
        } else {
            BindValue binding = getBinding(parameterIndex, true);
            resetToType(binding, MysqlDefs.FIELD_TYPE_BLOB);

            binding.value = reader;
            binding.isLongData = true;

            if (this.connection.getUseStreamLengthsInPrepStmts()) {
                binding.bindLength = length;
            } else {
                binding.bindLength = -1;
            }
        }
    }

    /**
     * @see java.sql.PreparedStatement#setNString(int, java.lang.String)
     */
    public void setNString(int parameterIndex, String x) throws SQLException {
        if (this.charEncoding.equalsIgnoreCase("UTF-8") || this.charEncoding.equalsIgnoreCase("utf8")) {
            setString(parameterIndex, x);
        } else {
            throw SQLError.createSQLException("Can not call setNString() when connection character set isn't UTF-8", getExceptionInterceptor());
        }
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        JDBC4PreparedStatementHelper.setRowId(this, parameterIndex, x);
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        JDBC4PreparedStatementHelper.setSQLXML(this, parameterIndex, xmlObject);
    }
}
