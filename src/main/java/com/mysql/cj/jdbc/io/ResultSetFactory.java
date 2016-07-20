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

import java.sql.SQLException;

import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.mysqla.io.ProtocolEntityFactory;
import com.mysql.cj.api.mysqla.result.ProtocolEntity;
import com.mysql.cj.api.mysqla.result.Resultset;
import com.mysql.cj.api.mysqla.result.Resultset.Concurrency;
import com.mysql.cj.api.mysqla.result.Resultset.Type;
import com.mysql.cj.api.mysqla.result.ResultsetRows;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.jdbc.result.ResultSetImpl;
import com.mysql.cj.jdbc.result.UpdatableResultSet;
import com.mysql.cj.mysqla.result.OkPacket;
import com.mysql.cj.mysqla.result.ResultsetRowsCursor;

public class ResultSetFactory implements ProtocolEntityFactory<ResultSetImpl> {

    private JdbcConnection conn;
    private StatementImpl stmt;

    private Type type = Type.FORWARD_ONLY;
    private Concurrency concurrency = Concurrency.READ_ONLY;

    public ResultSetFactory(JdbcConnection connection, StatementImpl creatorStmt) throws SQLException {
        this.conn = connection;
        this.stmt = creatorStmt;

        if (creatorStmt != null) {
            this.type = Type.fromValue(creatorStmt.getResultSetType(), Type.FORWARD_ONLY);
            this.concurrency = Concurrency.fromValue(creatorStmt.getResultSetConcurrency(), Concurrency.READ_ONLY);
        }
    }

    public Resultset.Type getResultSetType() {
        return this.type;
    }

    public Resultset.Concurrency getResultSetConcurrency() {
        return this.concurrency;
    }

    @Override
    public ResultSetImpl createFromProtocolEntity(ProtocolEntity protocolEntity) {
        try {
            if (protocolEntity instanceof OkPacket) {
                return new ResultSetImpl((OkPacket) protocolEntity, this.conn, this.stmt);

            } else if (protocolEntity instanceof ResultsetRows) {
                int resultSetConcurrency = getResultSetConcurrency().getIntValue();
                int resultSetType = getResultSetType().getIntValue();

                return createFromResultsetRows(resultSetConcurrency, resultSetType, (ResultsetRows) protocolEntity);

            }
            throw ExceptionFactory.createException(WrongArgumentException.class, "Unknown ProtocolEntity class " + protocolEntity);

        } catch (SQLException ex) {
            throw ExceptionFactory.createException(ex.getMessage(), ex);
        }
    }

    /**
     * Build ResultSet from ResultsetRows
     * 
     * @param resultSetType
     *            scrollability (TYPE_FORWARD_ONLY, TYPE_SCROLL_????)
     * @param resultSetConcurrency
     *            the type of result set (CONCUR_UPDATABLE or READ_ONLY)
     */
    public ResultSetImpl createFromResultsetRows(int resultSetConcurrency, int resultSetType, ResultsetRows rows) throws SQLException {

        ResultSetImpl rs;

        StatementImpl st = this.stmt;
        switch (resultSetConcurrency) {
            case java.sql.ResultSet.CONCUR_UPDATABLE:
                rs = new UpdatableResultSet(rows, this.conn, st);
                break;

            default:
                // CONCUR_READ_ONLY
                rs = new ResultSetImpl(rows, this.conn, st);
                break;
        }

        rs.setResultSetType(resultSetType);
        rs.setResultSetConcurrency(resultSetConcurrency);

        if (rows instanceof ResultsetRowsCursor && st != null) {
            rs.setFetchSize(st.getFetchSize());
        }
        return rs;
    }

}
