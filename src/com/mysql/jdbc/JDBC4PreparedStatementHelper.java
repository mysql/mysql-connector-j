/*
  Copyright (c) 2007, 2015, Oracle and/or its affiliates. All rights reserved.

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
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;

import com.mysql.jdbc.PreparedStatement;

public class JDBC4PreparedStatementHelper {
    private JDBC4PreparedStatementHelper() {

    }

    static void setRowId(PreparedStatement pstmt, int parameterIndex, RowId x) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    /**
     * JDBC 4.0 Set a NCLOB parameter.
     * 
     * @param i
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            an object representing a NCLOB
     * 
     * @throws SQLException
     *             if a database error occurs
     */
    static void setNClob(PreparedStatement pstmt, int parameterIndex, NClob value) throws SQLException {
        if (value == null) {
            pstmt.setNull(parameterIndex, java.sql.Types.NCLOB);
        } else {
            pstmt.setNCharacterStream(parameterIndex, value.getCharacterStream(), value.length());
        }
    }

    static void setNClob(PreparedStatement pstmt, int parameterIndex, Reader reader) throws SQLException {
        pstmt.setNCharacterStream(parameterIndex, reader);
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
    static void setNClob(PreparedStatement pstmt, int parameterIndex, Reader reader, long length) throws SQLException {
        if (reader == null) {
            pstmt.setNull(parameterIndex, java.sql.Types.NCLOB);
        } else {
            pstmt.setNCharacterStream(parameterIndex, reader, length);
        }
    }

    static void setSQLXML(PreparedStatement pstmt, int parameterIndex, SQLXML xmlObject) throws SQLException {
        if (xmlObject == null) {
            pstmt.setNull(parameterIndex, Types.SQLXML);
        } else {
            // FIXME: Won't work for Non-MYSQL SQLXMLs
            pstmt.setCharacterStream(parameterIndex, ((JDBC4MysqlSQLXML) xmlObject).serializeAsCharacterStream());
        }
    }
}
