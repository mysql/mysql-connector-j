/*
  Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.jdbc.io;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.mysqla.io.StructureFactory;
import com.mysql.cj.api.mysqla.result.ResultsetRows;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.jdbc.result.ResultSetImpl;
import com.mysql.cj.jdbc.result.UpdatableResultSet;
import com.mysql.cj.mysqla.result.OkPacket;
import com.mysql.cj.mysqla.result.ResultsetRowsCursor;

public class ResultSetFactory implements StructureFactory<ResultSetImpl> {

    private JdbcConnection conn;
    private StatementImpl stmt;

    public ResultSetFactory(JdbcConnection connection, StatementImpl creatorStmt) {
        this.conn = connection;
        this.stmt = creatorStmt;
    }

    public int getResultSetType() throws SQLException {
        if (this.stmt != null) {
            return this.stmt.getResultSetType();
        }
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public int getResultSetConcurrency() throws SQLException {
        if (this.stmt != null) {
            return this.stmt.getResultSetConcurrency();
        }
        return ResultSet.CONCUR_READ_ONLY;
    }

    public ResultSetImpl getInstance(OkPacket ok) throws SQLException {
        return new ResultSetImpl(ok, this.conn, this.stmt);
    }

    /**
     * Build ResultSet from ResultsetRows
     * 
     * @param resultSetType
     *            scrollability (TYPE_FORWARD_ONLY, TYPE_SCROLL_????)
     * @param resultSetConcurrency
     *            the type of result set (CONCUR_UPDATABLE or READ_ONLY)
     */
    public ResultSetImpl getInstance(int resultSetConcurrency, int resultSetType, ResultsetRows rows) throws SQLException {

        ResultSetImpl rs;

        switch (resultSetConcurrency) {
            case java.sql.ResultSet.CONCUR_UPDATABLE:
                rs = new UpdatableResultSet(rows, this.conn, this.stmt);
                break;

            default:
                // CONCUR_READ_ONLY
                rs = new ResultSetImpl(rows, this.conn, this.stmt);
                break;
        }

        rs.setResultSetType(resultSetType);
        rs.setResultSetConcurrency(resultSetConcurrency);

        if (rows instanceof ResultsetRowsCursor) {
            rs.setFetchSize(this.stmt.getFetchSize());
        }
        return rs;
    }

}
