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

import java.lang.ref.SoftReference;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.mysqla.io.StructureFactory;
import com.mysql.cj.api.mysqla.result.ProtocolStructure;
import com.mysql.cj.api.mysqla.result.Resultset;
import com.mysql.cj.api.mysqla.result.Resultset.Concurrency;
import com.mysql.cj.api.mysqla.result.Resultset.Type;
import com.mysql.cj.api.mysqla.result.ResultsetRows;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.jdbc.result.ResultSetImpl;
import com.mysql.cj.jdbc.result.UpdatableResultSet;
import com.mysql.cj.mysqla.io.ResultsetFactory;
import com.mysql.cj.mysqla.result.OkPacket;
import com.mysql.cj.mysqla.result.ResultsetRowsCursor;

public class ResultSetFactory implements StructureFactory<ResultSetImpl> {

    private SoftReference<JdbcConnection> conn;
    private SoftReference<StatementImpl> stmt;

    private ResultsetFactory protoRsFactory;

    public ResultSetFactory(JdbcConnection connection, StatementImpl creatorStmt) throws SQLException {
        this.conn = new SoftReference<JdbcConnection>(connection);
        this.stmt = new SoftReference<StatementImpl>(creatorStmt);

        Type type = Type.FORWARD_ONLY;
        Concurrency concurrency = Concurrency.READ_ONLY;

        if (creatorStmt != null) {
            switch (creatorStmt.getResultSetType()) {
                case ResultSet.TYPE_SCROLL_INSENSITIVE:
                    type = Type.SCROLL_INSENSITIVE;
                    break;
                case ResultSet.TYPE_SCROLL_SENSITIVE:
                    type = Type.SCROLL_SENSITIVE;
                    break;
                default:
                    type = Type.FORWARD_ONLY;
                    break;
            }
            switch (creatorStmt.getResultSetConcurrency()) {
                case ResultSet.CONCUR_UPDATABLE:
                    concurrency = Concurrency.UPDATABLE;
                    break;
                default:
                    concurrency = Concurrency.READ_ONLY;
                    break;
            }
        }

        this.protoRsFactory = new ResultsetFactory(type, concurrency);
    }

    public Resultset.Type getResultSetType() {
        return this.protoRsFactory.getResultSetType();
    }

    public Resultset.Concurrency getResultSetConcurrency() {
        return this.protoRsFactory.getResultSetConcurrency();
    }

    @Override
    public ResultSetImpl createFromProtocolStructure(ProtocolStructure protocolStructure) {
        try {
            if (protocolStructure instanceof OkPacket) {
                return new ResultSetImpl((OkPacket) protocolStructure, this.conn.get(), this.stmt.get());

            } else if (protocolStructure instanceof ResultsetRows) {
                int resultSetConcurrency = getResultSetConcurrency() == Concurrency.READ_ONLY ? ResultSet.CONCUR_READ_ONLY : ResultSet.CONCUR_UPDATABLE;
                int resultSetType = getResultSetType() == Type.FORWARD_ONLY ? ResultSet.TYPE_FORWARD_ONLY
                        : (getResultSetType() == Type.SCROLL_INSENSITIVE ? ResultSet.TYPE_SCROLL_INSENSITIVE : ResultSet.TYPE_SCROLL_SENSITIVE);

                return createJdbcResultSet(resultSetConcurrency, resultSetType, (ResultsetRows) protocolStructure);

            }
            throw ExceptionFactory.createException(WrongArgumentException.class, "Unknown ProtocolStructure class " + protocolStructure);

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
    public ResultSetImpl createJdbcResultSet(int resultSetConcurrency, int resultSetType, ResultsetRows rows) throws SQLException {

        ResultSetImpl rs;

        StatementImpl st = this.stmt.get();
        switch (resultSetConcurrency) {
            case java.sql.ResultSet.CONCUR_UPDATABLE:
                rs = new UpdatableResultSet(rows, this.conn.get(), st);
                break;

            default:
                // CONCUR_READ_ONLY
                rs = new ResultSetImpl(rows, this.conn.get(), st);
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
