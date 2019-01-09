/*
 * Copyright (c) 2019, Oracle and/or its affiliates. All rights reserved.
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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import com.mysql.cj.BindValue;
import com.mysql.cj.CharsetMapping;
import com.mysql.cj.PreparedQuery;
import com.mysql.cj.QueryBindings;
import com.mysql.cj.Session;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.result.ResultSetFactory;
import com.mysql.cj.jdbc.result.ResultSetImpl;
import com.mysql.cj.protocol.a.result.ByteArrayRow;
import com.mysql.cj.protocol.a.result.ResultsetRowsStatic;
import com.mysql.cj.result.DefaultColumnDefinition;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.Row;
import com.mysql.cj.util.StringUtils;

public class ParameterBindingsImpl implements ParameterBindings {

    private QueryBindings<?> queryBindings;
    private List<Object> batchedArgs;
    private PropertySet propertySet;
    private ExceptionInterceptor exceptionInterceptor;

    private ResultSetImpl bindingsAsRs;
    private BindValue[] bindValues;

    ParameterBindingsImpl(PreparedQuery<?> query, Session session, ResultSetFactory resultSetFactory) throws SQLException {
        this.queryBindings = query.getQueryBindings();
        this.batchedArgs = query.getBatchedArgs();
        this.propertySet = session.getPropertySet();
        this.exceptionInterceptor = session.getExceptionInterceptor();

        List<Row> rows = new ArrayList<>();
        int paramCount = query.getParameterCount();
        this.bindValues = new BindValue[paramCount];
        for (int i = 0; i < paramCount; i++) {
            this.bindValues[i] = this.queryBindings.getBindValues()[i].clone();
        }
        byte[][] rowData = new byte[paramCount][];
        Field[] typeMetadata = new Field[paramCount];

        for (int i = 0; i < paramCount; i++) {
            int batchCommandIndex = query.getBatchCommandIndex();
            rowData[i] = batchCommandIndex == -1 ? getBytesRepresentation(i) : getBytesRepresentationForBatch(i, batchCommandIndex);

            int charsetIndex = 0;

            switch (this.queryBindings.getBindValues()[i].getMysqlType()) {
                case BINARY:
                case BLOB:
                case GEOMETRY:
                case LONGBLOB:
                case MEDIUMBLOB:
                case TINYBLOB:
                case UNKNOWN:
                case VARBINARY:
                    charsetIndex = CharsetMapping.MYSQL_COLLATION_INDEX_binary;
                    break;
                default:
                    try {
                        charsetIndex = CharsetMapping.getCollationIndexForJavaEncoding(
                                this.propertySet.getStringProperty(PropertyKey.characterEncoding).getValue(), session.getServerSession().getServerVersion());
                    } catch (RuntimeException ex) {
                        throw SQLError.createSQLException(ex.toString(), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, ex, null);
                    }
                    break;
            }

            Field parameterMetadata = new Field(null, "parameter_" + (i + 1), charsetIndex,
                    this.propertySet.getStringProperty(PropertyKey.characterEncoding).getValue(), this.queryBindings.getBindValues()[i].getMysqlType(),
                    rowData[i].length);
            typeMetadata[i] = parameterMetadata;
        }

        rows.add(new ByteArrayRow(rowData, this.exceptionInterceptor));

        this.bindingsAsRs = resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(rows, new DefaultColumnDefinition(typeMetadata)));
        this.bindingsAsRs.next();
    }

    /**
     * @param parameterIndex
     *            parameter index
     * @return bytes
     */
    private byte[] getBytesRepresentation(int parameterIndex) {
        return this.queryBindings.getBytesRepresentation(parameterIndex);
    }

    /**
     * Get bytes representation for a parameter in a statement batch.
     * 
     * @param parameterIndex
     *            parameter index
     * @param commandIndex
     *            command index
     * @return bytes
     */
    private byte[] getBytesRepresentationForBatch(int parameterIndex, int commandIndex) {
        Object batchedArg = this.batchedArgs.get(commandIndex);
        if (batchedArg instanceof String) {
            return StringUtils.getBytes((String) batchedArg, this.propertySet.getStringProperty(PropertyKey.characterEncoding).getValue());
        }

        return ((QueryBindings<?>) batchedArg).getBytesRepresentation(parameterIndex);
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getArray(parameterIndex);
    }

    @Override
    public InputStream getAsciiStream(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getAsciiStream(parameterIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getBigDecimal(parameterIndex);
    }

    @Override
    public InputStream getBinaryStream(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getBinaryStream(parameterIndex);
    }

    @Override
    public java.sql.Blob getBlob(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getBlob(parameterIndex);
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getBoolean(parameterIndex);
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getByte(parameterIndex);
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getBytes(parameterIndex);
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getCharacterStream(parameterIndex);
    }

    @Override
    public java.sql.Clob getClob(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getClob(parameterIndex);
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getDate(parameterIndex);
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getDouble(parameterIndex);
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getFloat(parameterIndex);
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getInt(parameterIndex);
    }

    @Override
    public BigInteger getBigInteger(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getBigInteger(parameterIndex);
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getLong(parameterIndex);
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getCharacterStream(parameterIndex);
    }

    @Override
    public Reader getNClob(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getCharacterStream(parameterIndex);
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        //        checkBounds(parameterIndex, 0);

        if (this.bindValues[parameterIndex - 1].isNull()) {
            return null;
        }

        // we can't rely on the default mapping for JDBC's ResultSet.getObject() for numerics, they're not one-to-one with PreparedStatement.setObject

        switch (this.queryBindings.getBindValues()[parameterIndex - 1].getMysqlType()) {
            case TINYINT:
            case TINYINT_UNSIGNED:
                return Byte.valueOf(getByte(parameterIndex));
            case SMALLINT:
            case SMALLINT_UNSIGNED:
                return Short.valueOf(getShort(parameterIndex));
            case INT:
            case INT_UNSIGNED:
                return Integer.valueOf(getInt(parameterIndex));
            case BIGINT:
                return Long.valueOf(getLong(parameterIndex));
            case BIGINT_UNSIGNED:
                return getBigInteger(parameterIndex);
            case FLOAT:
            case FLOAT_UNSIGNED:
                return Float.valueOf(getFloat(parameterIndex));
            case DOUBLE:
            case DOUBLE_UNSIGNED:
                return Double.valueOf(getDouble(parameterIndex));
            default:
                return this.bindingsAsRs.getObject(parameterIndex);
        }
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getRef(parameterIndex);
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getShort(parameterIndex);
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getString(parameterIndex);
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getTime(parameterIndex);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getTimestamp(parameterIndex);
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        return this.bindingsAsRs.getURL(parameterIndex);
    }

    @Override
    public boolean isNull(int parameterIndex) throws SQLException {
        //        checkBounds(parameterIndex, 0);
        //return this.bindValues[parameterIndex - 1].isNull();
        return this.queryBindings.isNull(parameterIndex - 1);
    }

}
