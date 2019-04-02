/*
 * Copyright (c) 2002, 2019, Oracle and/or its affiliates. All rights reserved.
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
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Wrapper;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.PreparedQuery;
import com.mysql.cj.conf.PropertyDefinitions.DatabaseTerm;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.result.ResultSetImpl;
import com.mysql.cj.jdbc.result.ResultSetInternalMethods;
import com.mysql.cj.protocol.a.result.ByteArrayRow;
import com.mysql.cj.protocol.a.result.ResultsetRowsStatic;
import com.mysql.cj.result.DefaultColumnDefinition;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.Row;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;

/**
 * Representation of stored procedures for JDBC
 */
public class CallableStatement extends ClientPreparedStatement implements java.sql.CallableStatement {

    protected static class CallableStatementParam {

        int index;

        int inOutModifier;

        boolean isIn;

        boolean isOut;

        int jdbcType;

        short nullability;

        String paramName;

        int precision;

        int scale;

        String typeName;

        MysqlType desiredMysqlType = MysqlType.UNKNOWN;

        CallableStatementParam(String name, int idx, boolean in, boolean out, int jdbcType, String typeName, int precision, int scale, short nullability,
                int inOutModifier) {
            this.paramName = name;
            this.isIn = in;
            this.isOut = out;
            this.index = idx;

            this.jdbcType = jdbcType;
            this.typeName = typeName;
            this.precision = precision;
            this.scale = scale;
            this.nullability = nullability;
            this.inOutModifier = inOutModifier;
        }

        @Override
        protected Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    public class CallableStatementParamInfo implements ParameterMetaData {
        String dbInUse;

        boolean isFunctionCall;

        String nativeSql;

        int numParameters;

        List<CallableStatementParam> parameterList;

        Map<String, CallableStatementParam> parameterMap;

        /**
         * synchronized externally in checkReadOnlyProcedure()
         */
        boolean isReadOnlySafeProcedure = false;

        /**
         * synchronized externally in checkReadOnlyProcedure()
         */
        boolean isReadOnlySafeChecked = false;

        /**
         * Constructor that converts a full list of parameter metadata into one
         * that only represents the placeholders present in the {CALL ()}.
         * 
         * @param fullParamInfo
         *            the metadata for all parameters for this stored
         *            procedure or function.
         */
        CallableStatementParamInfo(CallableStatementParamInfo fullParamInfo) {
            this.nativeSql = ((PreparedQuery<?>) CallableStatement.this.query).getOriginalSql();
            this.dbInUse = CallableStatement.this.getCurrentDatabase();
            this.isFunctionCall = fullParamInfo.isFunctionCall;
            @SuppressWarnings("synthetic-access")
            int[] localParameterMap = CallableStatement.this.placeholderToParameterIndexMap;
            int parameterMapLength = localParameterMap.length;

            this.isReadOnlySafeProcedure = fullParamInfo.isReadOnlySafeProcedure;
            this.isReadOnlySafeChecked = fullParamInfo.isReadOnlySafeChecked;
            this.parameterList = new ArrayList<>(fullParamInfo.numParameters);
            this.parameterMap = new HashMap<>(fullParamInfo.numParameters);

            if (this.isFunctionCall) {
                // Take the return value
                this.parameterList.add(fullParamInfo.parameterList.get(0));
            }

            int offset = this.isFunctionCall ? 1 : 0;

            for (int i = 0; i < parameterMapLength; i++) {
                if (localParameterMap[i] != 0) {
                    CallableStatementParam param = fullParamInfo.parameterList.get(localParameterMap[i] + offset);

                    this.parameterList.add(param);
                    this.parameterMap.put(param.paramName, param);
                }
            }

            this.numParameters = this.parameterList.size();
        }

        @SuppressWarnings("synthetic-access")
        CallableStatementParamInfo(java.sql.ResultSet paramTypesRs) throws SQLException {
            boolean hadRows = paramTypesRs.last();

            this.nativeSql = ((PreparedQuery<?>) CallableStatement.this.query).getOriginalSql();
            this.dbInUse = CallableStatement.this.getCurrentDatabase();
            this.isFunctionCall = CallableStatement.this.callingStoredFunction;

            if (hadRows) {
                this.numParameters = paramTypesRs.getRow();

                this.parameterList = new ArrayList<>(this.numParameters);
                this.parameterMap = new HashMap<>(this.numParameters);

                paramTypesRs.beforeFirst();

                addParametersFromDBMD(paramTypesRs);
            } else {
                this.numParameters = 0;
            }

            if (this.isFunctionCall) {
                this.numParameters += 1;
            }
        }

        private void addParametersFromDBMD(java.sql.ResultSet paramTypesRs) throws SQLException {
            int i = 0;

            while (paramTypesRs.next()) {
                String paramName = paramTypesRs.getString(4);
                int inOutModifier;
                switch (paramTypesRs.getInt(5)) {
                    case DatabaseMetaData.procedureColumnIn:
                        inOutModifier = ParameterMetaData.parameterModeIn;
                        break;
                    case DatabaseMetaData.procedureColumnInOut:
                        inOutModifier = ParameterMetaData.parameterModeInOut;
                        break;
                    case DatabaseMetaData.procedureColumnOut:
                    case DatabaseMetaData.procedureColumnReturn:
                        inOutModifier = ParameterMetaData.parameterModeOut;
                        break;
                    default:
                        inOutModifier = ParameterMetaData.parameterModeUnknown;
                }

                boolean isOutParameter = false;
                boolean isInParameter = false;

                if (i == 0 && this.isFunctionCall) {
                    isOutParameter = true;
                    isInParameter = false;
                } else if (inOutModifier == java.sql.DatabaseMetaData.procedureColumnInOut) {
                    isOutParameter = true;
                    isInParameter = true;
                } else if (inOutModifier == java.sql.DatabaseMetaData.procedureColumnIn) {
                    isOutParameter = false;
                    isInParameter = true;
                } else if (inOutModifier == java.sql.DatabaseMetaData.procedureColumnOut) {
                    isOutParameter = true;
                    isInParameter = false;
                }

                int jdbcType = paramTypesRs.getInt(6);
                String typeName = paramTypesRs.getString(7);
                int precision = paramTypesRs.getInt(8);
                int scale = paramTypesRs.getInt(10);
                short nullability = paramTypesRs.getShort(12);

                CallableStatementParam paramInfoToAdd = new CallableStatementParam(paramName, i++, isInParameter, isOutParameter, jdbcType, typeName, precision,
                        scale, nullability, inOutModifier);

                this.parameterList.add(paramInfoToAdd);
                this.parameterMap.put(paramName, paramInfoToAdd);
            }
        }

        protected void checkBounds(int paramIndex) throws SQLException {
            int localParamIndex = paramIndex - 1;

            if ((paramIndex < 0) || (localParamIndex >= this.numParameters)) {
                throw SQLError.createSQLException(Messages.getString("CallableStatement.11", new Object[] { paramIndex, this.numParameters }),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }

        @Override
        protected Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

        CallableStatementParam getParameter(int index) {
            return this.parameterList.get(index);
        }

        CallableStatementParam getParameter(String name) {
            return this.parameterMap.get(name);
        }

        @Override
        public String getParameterClassName(int arg0) throws SQLException {
            String mysqlTypeName = getParameterTypeName(arg0);

            MysqlType mysqlType = MysqlType.getByName(mysqlTypeName);
            switch (mysqlType) {
                case YEAR:
                    if (!CallableStatement.this.session.getPropertySet().getBooleanProperty(PropertyKey.yearIsDateType).getValue()) {
                        return Short.class.getName();
                    }
                    // TODO Adjust for pseudo-boolean ?
                    //if (length == 1) {
                    //    if (propertySet.getBooleanReadableProperty(PropertyKey.transformedBitIsBoolean).getValue()) {
                    //        return MysqlType.BOOLEAN;
                    //    } else if (propertySet.getBooleanReadableProperty(PropertyKey.tinyInt1isBit).getValue()) {
                    //        return MysqlType.BIT;
                    //    }
                    //}
                    return mysqlType.getClassName();

                default:
                    return mysqlType.getClassName();
            }

        }

        @Override
        public int getParameterCount() throws SQLException {
            if (this.parameterList == null) {
                return 0;
            }

            return this.parameterList.size();
        }

        @Override
        public int getParameterMode(int arg0) throws SQLException {
            checkBounds(arg0);

            return getParameter(arg0 - 1).inOutModifier;
        }

        @Override
        public int getParameterType(int arg0) throws SQLException {
            checkBounds(arg0);

            return getParameter(arg0 - 1).jdbcType;
        }

        @Override
        public String getParameterTypeName(int arg0) throws SQLException {
            checkBounds(arg0);

            return getParameter(arg0 - 1).typeName;
        }

        @Override
        public int getPrecision(int arg0) throws SQLException {
            checkBounds(arg0);

            return getParameter(arg0 - 1).precision;
        }

        @Override
        public int getScale(int arg0) throws SQLException {
            checkBounds(arg0);

            return getParameter(arg0 - 1).scale;
        }

        @Override
        public int isNullable(int arg0) throws SQLException {
            checkBounds(arg0);

            return getParameter(arg0 - 1).nullability;
        }

        @Override
        public boolean isSigned(int arg0) throws SQLException {
            checkBounds(arg0);

            return false;
        }

        Iterator<CallableStatementParam> iterator() {
            return this.parameterList.iterator();
        }

        int numberOfParameters() {
            return this.numParameters;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            checkClosed();

            // This works for classes that aren't actually wrapping anything
            return iface.isInstance(this);
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws java.sql.SQLException {
            try {
                // This works for classes that aren't actually wrapping anything
                return iface.cast(this);
            } catch (ClassCastException cce) {
                throw SQLError.createSQLException(Messages.getString("Common.UnableToUnwrap", new Object[] { iface.toString() }),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }
    }

    private final static int NOT_OUTPUT_PARAMETER_INDICATOR = Integer.MIN_VALUE;

    private final static String PARAMETER_NAMESPACE_PREFIX = "@com_mysql_jdbc_outparam_";

    private static String mangleParameterName(String origParameterName) {
        if (origParameterName == null) {
            return null;
        }

        int offset = 0;

        if (origParameterName.length() > 0 && origParameterName.charAt(0) == '@') {
            offset = 1;
        }

        StringBuilder paramNameBuf = new StringBuilder(PARAMETER_NAMESPACE_PREFIX.length() + origParameterName.length());
        paramNameBuf.append(PARAMETER_NAMESPACE_PREFIX);
        paramNameBuf.append(origParameterName.substring(offset));

        return paramNameBuf.toString();
    }

    private boolean callingStoredFunction = false;

    private ResultSetInternalMethods functionReturnValueResults;

    private boolean hasOutputParams = false;

    private ResultSetInternalMethods outputParameterResults;

    protected boolean outputParamWasNull = false;

    private int[] parameterIndexToRsIndex;

    protected CallableStatementParamInfo paramInfo;

    private CallableStatementParam returnValueParam;

    private boolean noAccessToProcedureBodies;

    /**
     * Creates a new CallableStatement
     * 
     * @param conn
     *            the connection creating this statement
     * @param paramInfo
     *            the SQL to prepare
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public CallableStatement(JdbcConnection conn, CallableStatementParamInfo paramInfo) throws SQLException {
        super(conn, paramInfo.nativeSql, paramInfo.dbInUse);

        this.paramInfo = paramInfo;
        this.callingStoredFunction = this.paramInfo.isFunctionCall;

        if (this.callingStoredFunction) {
            ((PreparedQuery<?>) this.query).setParameterCount(((PreparedQuery<?>) this.query).getParameterCount() + 1);
        }

        this.retrieveGeneratedKeys = true; // not provided for in the JDBC spec

        this.noAccessToProcedureBodies = conn.getPropertySet().getBooleanProperty(PropertyKey.noAccessToProcedureBodies).getValue();
    }

    /**
     * Creates a callable statement instance
     * 
     * @param conn
     *            the connection creating this statement
     * @param sql
     *            the SQL to prepare
     * @param db
     *            the current database
     * @param isFunctionCall
     *            is it a function call or a procedure call?
     * @return CallableStatement
     * @throws SQLException
     *             if an error occurs
     */

    protected static CallableStatement getInstance(JdbcConnection conn, String sql, String db, boolean isFunctionCall) throws SQLException {
        return new CallableStatement(conn, sql, db, isFunctionCall);
    }

    /**
     * Creates a callable statement instance
     * 
     * @param conn
     *            the connection creating this statement
     * @param paramInfo
     *            the SQL to prepare
     * @return CallableStatement
     * @throws SQLException
     *             if an error occurs
     */

    protected static CallableStatement getInstance(JdbcConnection conn, CallableStatementParamInfo paramInfo) throws SQLException {
        return new CallableStatement(conn, paramInfo);
    }

    private int[] placeholderToParameterIndexMap;

    private void generateParameterMap() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.paramInfo == null) {
                return;
            }

            // if the user specified some parameters as literals, we need to provide a map from the specified placeholders to the actual parameter numbers

            int parameterCountFromMetaData = this.paramInfo.getParameterCount();

            // Ignore the first ? if this is a stored function, it doesn't count

            if (this.callingStoredFunction) {
                parameterCountFromMetaData--;
            }

            PreparedQuery<?> q = ((PreparedQuery<?>) this.query);
            if (this.paramInfo != null && q.getParameterCount() != parameterCountFromMetaData) {
                this.placeholderToParameterIndexMap = new int[q.getParameterCount()];

                int startPos = this.callingStoredFunction ? StringUtils.indexOfIgnoreCase(q.getOriginalSql(), "SELECT")
                        : StringUtils.indexOfIgnoreCase(q.getOriginalSql(), "CALL");

                if (startPos != -1) {
                    int parenOpenPos = q.getOriginalSql().indexOf('(', startPos + 4);

                    if (parenOpenPos != -1) {
                        int parenClosePos = StringUtils.indexOfIgnoreCase(parenOpenPos, q.getOriginalSql(), ")", "'", "'", StringUtils.SEARCH_MODE__ALL);

                        if (parenClosePos != -1) {
                            List<?> parsedParameters = StringUtils.split(q.getOriginalSql().substring(parenOpenPos + 1, parenClosePos), ",", "'\"", "'\"",
                                    true);

                            int numParsedParameters = parsedParameters.size();

                            // sanity check

                            if (numParsedParameters != q.getParameterCount()) {
                                // bail?
                            }

                            int placeholderCount = 0;

                            for (int i = 0; i < numParsedParameters; i++) {
                                if (((String) parsedParameters.get(i)).equals("?")) {
                                    this.placeholderToParameterIndexMap[placeholderCount++] = i;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Creates a new CallableStatement
     * 
     * @param conn
     *            the connection creating this statement
     * @param sql
     *            the SQL to prepare
     * @param db
     *            the current database
     * @param isFunctionCall
     *            is it a function call or a procedure call?
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public CallableStatement(JdbcConnection conn, String sql, String db, boolean isFunctionCall) throws SQLException {
        super(conn, sql, db);

        this.callingStoredFunction = isFunctionCall;

        if (!this.callingStoredFunction) {
            if (!StringUtils.startsWithIgnoreCaseAndWs(sql, "CALL")) {
                // not really a stored procedure call
                fakeParameterTypes(false);
            } else {
                determineParameterTypes();
            }

            generateParameterMap();
        } else {
            determineParameterTypes();
            generateParameterMap();

            ((PreparedQuery<?>) this.query).setParameterCount(((PreparedQuery<?>) this.query).getParameterCount() + 1);
        }

        this.retrieveGeneratedKeys = true; // not provided for in the JDBC spec
        this.noAccessToProcedureBodies = conn.getPropertySet().getBooleanProperty(PropertyKey.noAccessToProcedureBodies).getValue();
    }

    @Override
    public void addBatch() throws SQLException {
        setOutParams();

        super.addBatch();
    }

    private CallableStatementParam checkIsOutputParam(int paramIndex) throws SQLException {

        synchronized (checkClosed().getConnectionMutex()) {
            if (this.callingStoredFunction) {
                if (paramIndex == 1) {

                    if (this.returnValueParam == null) {
                        this.returnValueParam = new CallableStatementParam("", 0, false, true, MysqlType.VARCHAR.getJdbcType(), "VARCHAR", 0, 0,
                                java.sql.DatabaseMetaData.attributeNullableUnknown, java.sql.DatabaseMetaData.procedureColumnReturn);
                    }

                    return this.returnValueParam;
                }

                // Move to position in output result set
                paramIndex--;
            }

            checkParameterIndexBounds(paramIndex);

            int localParamIndex = paramIndex - 1;

            if (this.placeholderToParameterIndexMap != null) {
                localParamIndex = this.placeholderToParameterIndexMap[localParamIndex];
            }

            CallableStatementParam paramDescriptor = this.paramInfo.getParameter(localParamIndex);

            // We don't have reliable metadata in this case, trust the caller

            if (this.noAccessToProcedureBodies) {
                paramDescriptor.isOut = true;
                paramDescriptor.isIn = true;
                paramDescriptor.inOutModifier = java.sql.DatabaseMetaData.procedureColumnInOut;
            } else if (!paramDescriptor.isOut) {
                throw SQLError.createSQLException(Messages.getString("CallableStatement.9", new Object[] { paramIndex }),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            this.hasOutputParams = true;

            return paramDescriptor;
        }
    }

    /**
     * @param paramIndex
     *            parameter index
     * 
     * @throws SQLException
     *             if a database access error occurs or this method is called on a closed PreparedStatement
     */
    private void checkParameterIndexBounds(int paramIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            this.paramInfo.checkBounds(paramIndex);
        }
    }

    /**
     * Checks whether or not this statement is supposed to be providing
     * streamable result sets...If output parameters are registered, the driver
     * can not stream the results.
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    private void checkStreamability() throws SQLException {
        if (this.hasOutputParams && createStreamingResultSet()) {
            throw SQLError.createSQLException(Messages.getString("CallableStatement.14"), MysqlErrorNumbers.SQL_STATE_DRIVER_NOT_CAPABLE,
                    getExceptionInterceptor());
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            super.clearParameters();

            try {
                if (this.outputParameterResults != null) {
                    this.outputParameterResults.close();
                }
            } finally {
                this.outputParameterResults = null;
            }
        }
    }

    /**
     * Used to fake up some metadata when we don't have access to
     * SHOW CREATE PROCEDURE or mysql.proc.
     * 
     * @param isReallyProcedure
     *            is it a procedure or function
     * 
     * @throws SQLException
     *             if we can't build the metadata.
     */
    private void fakeParameterTypes(boolean isReallyProcedure) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            String encoding = this.connection.getSession().getServerSession().getCharacterSetMetadata();
            int collationIndex = this.connection.getSession().getServerSession().getMetadataCollationIndex();
            Field[] fields = new Field[13];

            fields[0] = new Field("", "PROCEDURE_CAT", collationIndex, encoding, MysqlType.CHAR, 0);
            fields[1] = new Field("", "PROCEDURE_SCHEM", collationIndex, encoding, MysqlType.CHAR, 0);
            fields[2] = new Field("", "PROCEDURE_NAME", collationIndex, encoding, MysqlType.CHAR, 0);
            fields[3] = new Field("", "COLUMN_NAME", collationIndex, encoding, MysqlType.CHAR, 0);
            fields[4] = new Field("", "COLUMN_TYPE", collationIndex, encoding, MysqlType.CHAR, 0);
            fields[5] = new Field("", "DATA_TYPE", collationIndex, encoding, MysqlType.SMALLINT, 0);
            fields[6] = new Field("", "TYPE_NAME", collationIndex, encoding, MysqlType.CHAR, 0);
            fields[7] = new Field("", "PRECISION", collationIndex, encoding, MysqlType.INT, 0);
            fields[8] = new Field("", "LENGTH", collationIndex, encoding, MysqlType.INT, 0);
            fields[9] = new Field("", "SCALE", collationIndex, encoding, MysqlType.SMALLINT, 0);
            fields[10] = new Field("", "RADIX", collationIndex, encoding, MysqlType.SMALLINT, 0);
            fields[11] = new Field("", "NULLABLE", collationIndex, encoding, MysqlType.SMALLINT, 0);
            fields[12] = new Field("", "REMARKS", collationIndex, encoding, MysqlType.CHAR, 0);

            String procName = isReallyProcedure ? extractProcedureName() : null;

            byte[] procNameAsBytes = null;

            procNameAsBytes = procName == null ? null : StringUtils.getBytes(procName, "UTF-8");

            ArrayList<Row> resultRows = new ArrayList<>();

            for (int i = 0; i < ((PreparedQuery<?>) this.query).getParameterCount(); i++) {
                byte[][] row = new byte[13][];
                row[0] = null; // PROCEDURE_CAT
                row[1] = null; // PROCEDURE_SCHEM
                row[2] = procNameAsBytes; // PROCEDURE/NAME
                row[3] = s2b(String.valueOf(i)); // COLUMN_NAME

                row[4] = s2b(String.valueOf(java.sql.DatabaseMetaData.procedureColumnIn));

                row[5] = s2b(String.valueOf(MysqlType.VARCHAR.getJdbcType())); // DATA_TYPE
                row[6] = s2b(MysqlType.VARCHAR.getName()); // TYPE_NAME
                row[7] = s2b(Integer.toString(65535)); // PRECISION
                row[8] = s2b(Integer.toString(65535)); // LENGTH
                row[9] = s2b(Integer.toString(0)); // SCALE
                row[10] = s2b(Integer.toString(10)); // RADIX

                row[11] = s2b(Integer.toString(java.sql.DatabaseMetaData.procedureNullableUnknown)); // nullable

                row[12] = null;

                resultRows.add(new ByteArrayRow(row, getExceptionInterceptor()));
            }

            java.sql.ResultSet paramTypesRs = this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    new ResultsetRowsStatic(resultRows, new DefaultColumnDefinition(fields)));

            convertGetProcedureColumnsToInternalDescriptors(paramTypesRs);
        }
    }

    private void determineParameterTypes() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            java.sql.ResultSet paramTypesRs = null;

            try {
                //Bug#57022, we need to check for db.SPname notation first and pass on only SPname
                String procName = extractProcedureName();
                String quotedId = this.session.getIdentifierQuoteString();

                List<?> parseList = StringUtils.splitDBdotName(procName, "", quotedId, this.session.getServerSession().isNoBackslashEscapesSet());
                String tmpDb = "";
                //There *should* be 2 rows, if any.
                if (parseList.size() == 2) {
                    tmpDb = (String) parseList.get(0);
                    procName = (String) parseList.get(1);
                } else {
                    //keep values as they are
                }

                java.sql.DatabaseMetaData dbmd = this.connection.getMetaData();

                boolean useDb = false;

                if (tmpDb.length() <= 0) {
                    useDb = true;
                }

                paramTypesRs = this.session.getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA
                        ? dbmd.getProcedureColumns(null, useDb ? this.getCurrentDatabase() : tmpDb/* null */, procName, "%")
                        : dbmd.getProcedureColumns(useDb ? this.getCurrentDatabase() : tmpDb/* null */, null, procName, "%");

                boolean hasResults = false;
                try {
                    if (paramTypesRs.next()) {
                        paramTypesRs.previous();
                        hasResults = true;
                    }
                } catch (Exception e) {
                    // paramTypesRs is empty, proceed with fake params. swallow, was expected 
                }
                if (hasResults) {
                    convertGetProcedureColumnsToInternalDescriptors(paramTypesRs);
                } else {
                    fakeParameterTypes(true);
                }
            } finally {
                SQLException sqlExRethrow = null;

                if (paramTypesRs != null) {
                    try {
                        paramTypesRs.close();
                    } catch (SQLException sqlEx) {
                        sqlExRethrow = sqlEx;
                    }

                    paramTypesRs = null;
                }

                if (sqlExRethrow != null) {
                    throw sqlExRethrow;
                }
            }
        }
    }

    private void convertGetProcedureColumnsToInternalDescriptors(java.sql.ResultSet paramTypesRs) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            this.paramInfo = new CallableStatementParamInfo(paramTypesRs);
        }
    }

    @Override
    public boolean execute() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            boolean returnVal = false;

            checkStreamability();

            setInOutParamsOnServer();
            setOutParams();

            returnVal = super.execute();

            if (this.callingStoredFunction) {
                this.functionReturnValueResults = this.results;
                this.functionReturnValueResults.next();
                this.results = null;
            }

            // TODO There is something strange here:
            // From ResultSetRegressionTest.testBug14562():
            //
            // $ CREATE  TABLE testBug14562 (row_order INT, signed_field MEDIUMINT, unsigned_field MEDIUMINT UNSIGNED)
            // $ INSERT INTO testBug14562 VALUES (1, -8388608, 0), (2, 8388607, 16777215)
            // $ CREATE  PROCEDURE sp_testBug14562_1 (OUT param_1 MEDIUMINT, OUT param_2 MEDIUMINT UNSIGNED)
            //    BEGIN
            //     SELECT signed_field, unsigned_field INTO param_1, param_2 FROM testBug14562 WHERE row_order=1;
            //    END
            // $ CALL sp_testBug14562_1(@com_mysql_jdbc_outparam_param_1, @com_mysql_jdbc_outparam_param_2)
            // $ SELECT @com_mysql_jdbc_outparam_param_1,@com_mysql_jdbc_outparam_param_2
            //
            // ResultSet metadata returns BIGINT for @com_mysql_jdbc_outparam_param_1 and @com_mysql_jdbc_outparam_param_2
            // instead of expected MEDIUMINT. I wonder what happens to other types...
            retrieveOutParams();

            if (!this.callingStoredFunction) {
                return returnVal;
            }

            // Functions can't return results
            return false;
        }
    }

    @Override
    public java.sql.ResultSet executeQuery() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            checkStreamability();

            java.sql.ResultSet execResults = null;

            setInOutParamsOnServer();
            setOutParams();

            execResults = super.executeQuery();

            retrieveOutParams();

            return execResults;
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        return Util.truncateAndConvertToInt(executeLargeUpdate());
    }

    private String extractProcedureName() throws SQLException {
        String sanitizedSql = StringUtils.stripComments(((PreparedQuery<?>) this.query).getOriginalSql(), "`\"'", "`\"'", true, false, true, true);

        // TODO: Do this with less memory allocation
        int endCallIndex = StringUtils.indexOfIgnoreCase(sanitizedSql, "CALL ");
        int offset = 5;

        if (endCallIndex == -1) {
            endCallIndex = StringUtils.indexOfIgnoreCase(sanitizedSql, "SELECT ");
            offset = 7;
        }

        if (endCallIndex != -1) {
            StringBuilder nameBuf = new StringBuilder();

            String trimmedStatement = sanitizedSql.substring(endCallIndex + offset).trim();

            int statementLength = trimmedStatement.length();

            for (int i = 0; i < statementLength; i++) {
                char c = trimmedStatement.charAt(i);

                if (Character.isWhitespace(c) || (c == '(') || (c == '?')) {
                    break;
                }
                nameBuf.append(c);

            }

            return nameBuf.toString();
        }

        throw SQLError.createSQLException(Messages.getString("CallableStatement.1"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
    }

    /**
     * Adds 'at' symbol to beginning of parameter names if needed.
     * 
     * @param paramNameIn
     *            the parameter name to 'fix'
     * 
     * @return the parameter name with an 'a' prepended, if needed
     * 
     * @throws SQLException
     *             if the parameter name is null or empty.
     */
    protected String fixParameterName(String paramNameIn) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (paramNameIn == null) {
                paramNameIn = "nullpn";
            }

            if (this.noAccessToProcedureBodies) {
                throw SQLError.createSQLException(Messages.getString("CallableStatement.23"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }

            return mangleParameterName(paramNameIn);
        }
    }

    @Override
    public Array getArray(int i) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(i);

            Array retValue = rs.getArray(mapOutputParameterIndexToRsIndex(i));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Array retValue = rs.getArray(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            BigDecimal retValue = rs.getBigDecimal(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            BigDecimal retValue = rs.getBigDecimal(mapOutputParameterIndexToRsIndex(parameterIndex), scale);

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            BigDecimal retValue = rs.getBigDecimal(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Blob retValue = rs.getBlob(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Blob retValue = rs.getBlob(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            boolean retValue = rs.getBoolean(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            boolean retValue = rs.getBoolean(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            byte retValue = rs.getByte(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            byte retValue = rs.getByte(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            byte[] retValue = rs.getBytes(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            byte[] retValue = rs.getBytes(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Clob retValue = rs.getClob(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Clob retValue = rs.getClob(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Date retValue = rs.getDate(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Date retValue = rs.getDate(mapOutputParameterIndexToRsIndex(parameterIndex), cal);

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Date retValue = rs.getDate(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Date retValue = rs.getDate(fixParameterName(parameterName), cal);

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            double retValue = rs.getDouble(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            double retValue = rs.getDouble(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            float retValue = rs.getFloat(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            float retValue = rs.getFloat(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            int retValue = rs.getInt(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            int retValue = rs.getInt(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            long retValue = rs.getLong(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            long retValue = rs.getLong(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    protected int getNamedParamIndex(String paramName, boolean forOut) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.noAccessToProcedureBodies) {
                throw SQLError.createSQLException("No access to parameters by name when connection has been configured not to access procedure bodies",
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            if ((paramName == null) || (paramName.length() == 0)) {
                throw SQLError.createSQLException(Messages.getString("CallableStatement.2"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }

            CallableStatementParam namedParamInfo;
            if (this.paramInfo == null || (namedParamInfo = this.paramInfo.getParameter(paramName)) == null) {
                throw SQLError.createSQLException(Messages.getString("CallableStatement.3", new Object[] { paramName }),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            if (forOut && !namedParamInfo.isOut) {
                throw SQLError.createSQLException(Messages.getString("CallableStatement.5", new Object[] { paramName }),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            if (this.placeholderToParameterIndexMap == null) {
                return namedParamInfo.index + 1; // JDBC indices are 1-based
            }

            for (int i = 0; i < this.placeholderToParameterIndexMap.length; i++) {
                if (this.placeholderToParameterIndexMap[i] == namedParamInfo.index) {
                    return i + 1;
                }
            }

            throw SQLError.createSQLException(Messages.getString("CallableStatement.6", new Object[] { paramName }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            CallableStatementParam paramDescriptor = checkIsOutputParam(parameterIndex);

            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Object retVal = rs.getObjectStoredProc(mapOutputParameterIndexToRsIndex(parameterIndex), paramDescriptor.desiredMysqlType.getJdbcType());

            this.outputParamWasNull = rs.wasNull();

            return retVal;
        }
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Object retVal = rs.getObject(mapOutputParameterIndexToRsIndex(parameterIndex), map);

            this.outputParamWasNull = rs.wasNull();

            return retVal;
        }
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Object retValue = rs.getObject(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Object retValue = rs.getObject(fixParameterName(parameterName), map);

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            // remove cast once 1.5, 1.6 EOL'd
            T retVal = ((ResultSetImpl) rs).getObject(mapOutputParameterIndexToRsIndex(parameterIndex), type);

            this.outputParamWasNull = rs.wasNull();

            return retVal;
        }
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            T retValue = ((ResultSetImpl) rs).getObject(fixParameterName(parameterName), type);

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * Returns the ResultSet that holds the output parameters, or throws an
     * appropriate exception if none exist, or they weren't returned.
     * 
     * @param paramIndex
     *            parameter index
     * 
     * @return the ResultSet that holds the output parameters
     * 
     * @throws SQLException
     *             if no output parameters were defined, or if no output
     *             parameters were returned.
     */
    protected ResultSetInternalMethods getOutputParameters(int paramIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            this.outputParamWasNull = false;

            if (paramIndex == 1 && this.callingStoredFunction && this.returnValueParam != null) {
                return this.functionReturnValueResults;
            }

            if (this.outputParameterResults == null) {
                if (this.paramInfo.numberOfParameters() == 0) {
                    throw SQLError.createSQLException(Messages.getString("CallableStatement.7"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                            getExceptionInterceptor());
                }
                throw SQLError.createSQLException(Messages.getString("CallableStatement.8"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        getExceptionInterceptor());
            }

            return this.outputParameterResults;
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.placeholderToParameterIndexMap == null) {
                return this.paramInfo;
            }

            return new CallableStatementParamInfo(this.paramInfo);
        }
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Ref retValue = rs.getRef(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Ref retValue = rs.getRef(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            short retValue = rs.getShort(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            short retValue = rs.getShort(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            String retValue = rs.getString(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            String retValue = rs.getString(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Time retValue = rs.getTime(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Time retValue = rs.getTime(mapOutputParameterIndexToRsIndex(parameterIndex), cal);

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Time retValue = rs.getTime(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Time retValue = rs.getTime(fixParameterName(parameterName), cal);

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Timestamp retValue = rs.getTimestamp(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Timestamp retValue = rs.getTimestamp(mapOutputParameterIndexToRsIndex(parameterIndex), cal);

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Timestamp retValue = rs.getTimestamp(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Timestamp retValue = rs.getTimestamp(fixParameterName(parameterName), cal);

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            URL retValue = rs.getURL(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            URL retValue = rs.getURL(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    protected int mapOutputParameterIndexToRsIndex(int paramIndex) throws SQLException {

        synchronized (checkClosed().getConnectionMutex()) {
            if (this.returnValueParam != null && paramIndex == 1) {
                return 1;
            }

            checkParameterIndexBounds(paramIndex);

            int localParamIndex = paramIndex - 1;

            if (this.placeholderToParameterIndexMap != null) {
                localParamIndex = this.placeholderToParameterIndexMap[localParamIndex];
            }

            int rsIndex = this.parameterIndexToRsIndex[localParamIndex];

            if (rsIndex == NOT_OUTPUT_PARAMETER_INDICATOR) {
                throw SQLError.createSQLException(Messages.getString("CallableStatement.21", new Object[] { paramIndex }),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            return rsIndex + 1;
        }
    }

    protected void registerOutParameter(int parameterIndex, MysqlType mysqlType) throws SQLException {
        CallableStatementParam paramDescriptor = checkIsOutputParam(parameterIndex);
        paramDescriptor.desiredMysqlType = mysqlType;
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        try {
            MysqlType mt = MysqlType.getByJdbcType(sqlType);
            registerOutParameter(parameterIndex, mt);
        } catch (FeatureNotAvailableException nae) {
            throw SQLError.createSQLFeatureNotSupportedException(Messages.getString("Statement.UnsupportedSQLType") + JDBCType.valueOf(sqlType),
                    MysqlErrorNumbers.SQL_STATE_DRIVER_NOT_CAPABLE, getExceptionInterceptor());
        }
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType) throws SQLException {
        if (sqlType instanceof MysqlType) {
            registerOutParameter(parameterIndex, (MysqlType) sqlType);
        } else {
            registerOutParameter(parameterIndex, sqlType.getVendorTypeNumber());
        }
    }

    protected void registerOutParameter(int parameterIndex, MysqlType mysqlType, @SuppressWarnings("unused") int scale) throws SQLException {
        registerOutParameter(parameterIndex, mysqlType); // TODO is that correct that we ignore scale?
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        registerOutParameter(parameterIndex, sqlType); // TODO is that correct that we ignore scale?
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType, int scale) throws SQLException {
        if (sqlType instanceof MysqlType) {
            registerOutParameter(parameterIndex, (MysqlType) sqlType, scale);
        } else {
            registerOutParameter(parameterIndex, sqlType.getVendorTypeNumber(), scale);
        }
    }

    protected void registerOutParameter(int parameterIndex, MysqlType mysqlType, @SuppressWarnings("unused") String typeName) throws SQLException {
        registerOutParameter(parameterIndex, mysqlType); // TODO is that correct that we ignore typeName?
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        try {
            MysqlType mt = MysqlType.getByJdbcType(sqlType);
            registerOutParameter(parameterIndex, mt, typeName);
        } catch (FeatureNotAvailableException nae) {
            throw SQLError.createSQLFeatureNotSupportedException(Messages.getString("Statement.UnsupportedSQLType") + JDBCType.valueOf(sqlType),
                    MysqlErrorNumbers.SQL_STATE_DRIVER_NOT_CAPABLE, getExceptionInterceptor());
        }
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType, String typeName) throws SQLException {
        if (sqlType instanceof MysqlType) {
            registerOutParameter(parameterIndex, (MysqlType) sqlType, typeName);
        } else {
            registerOutParameter(parameterIndex, sqlType.getVendorTypeNumber(), typeName);
        }
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            registerOutParameter(getNamedParamIndex(parameterName, true), sqlType);
        }
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType) throws SQLException {
        if (sqlType instanceof MysqlType) {
            registerOutParameter(getNamedParamIndex(parameterName, true), (MysqlType) sqlType);
        } else {
            registerOutParameter(getNamedParamIndex(parameterName, true), sqlType.getVendorTypeNumber());
        }
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        registerOutParameter(getNamedParamIndex(parameterName, true), sqlType, scale);
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType, int scale) throws SQLException {
        if (sqlType instanceof MysqlType) {
            registerOutParameter(getNamedParamIndex(parameterName, true), (MysqlType) sqlType, scale);
        } else {
            registerOutParameter(getNamedParamIndex(parameterName, true), sqlType.getVendorTypeNumber(), scale);
        }
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        registerOutParameter(getNamedParamIndex(parameterName, true), sqlType, typeName);
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType, String typeName) throws SQLException {
        if (sqlType instanceof MysqlType) {
            registerOutParameter(getNamedParamIndex(parameterName, true), (MysqlType) sqlType, typeName);
        } else {
            registerOutParameter(parameterName, sqlType.getVendorTypeNumber(), typeName);
        }
    }

    /**
     * Issues a second query to retrieve all output parameters.
     * 
     * @throws SQLException
     *             if an error occurs.
     */
    private void retrieveOutParams() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            int numParameters = this.paramInfo.numberOfParameters();

            this.parameterIndexToRsIndex = new int[numParameters];

            for (int i = 0; i < numParameters; i++) {
                this.parameterIndexToRsIndex[i] = NOT_OUTPUT_PARAMETER_INDICATOR;
            }

            int localParamIndex = 0;

            if (numParameters > 0) {
                StringBuilder outParameterQuery = new StringBuilder("SELECT ");

                boolean firstParam = true;
                boolean hadOutputParams = false;

                for (Iterator<CallableStatementParam> paramIter = this.paramInfo.iterator(); paramIter.hasNext();) {
                    CallableStatementParam retrParamInfo = paramIter.next();

                    if (retrParamInfo.isOut) {
                        hadOutputParams = true;

                        this.parameterIndexToRsIndex[retrParamInfo.index] = localParamIndex++;

                        if (retrParamInfo.paramName == null) {
                            retrParamInfo.paramName = "nullnp" + retrParamInfo.index;
                        }

                        String outParameterName = mangleParameterName(retrParamInfo.paramName);

                        if (!firstParam) {
                            outParameterQuery.append(",");
                        } else {
                            firstParam = false;
                        }

                        if (!outParameterName.startsWith("@")) {
                            outParameterQuery.append('@');
                        }

                        outParameterQuery.append(outParameterName);
                    }
                }

                if (hadOutputParams) {
                    // We can't use 'ourself' to execute this query, or any pending result sets would be overwritten
                    java.sql.Statement outParameterStmt = null;
                    java.sql.ResultSet outParamRs = null;

                    try {
                        outParameterStmt = this.connection.createStatement();
                        outParamRs = outParameterStmt.executeQuery(outParameterQuery.toString());
                        this.outputParameterResults = this.resultSetFactory.createFromResultsetRows(outParamRs.getConcurrency(), outParamRs.getType(),
                                ((com.mysql.cj.jdbc.result.ResultSetInternalMethods) outParamRs).getRows()); // note, doesn't work for updatable result sets

                        if (!this.outputParameterResults.next()) {
                            this.outputParameterResults.close();
                            this.outputParameterResults = null;
                        }
                    } finally {
                        if (outParameterStmt != null) {
                            outParameterStmt.close();
                        }
                    }
                } else {
                    this.outputParameterResults = null;
                }
            } else {
                this.outputParameterResults = null;
            }
        }
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        setAsciiStream(getNamedParamIndex(parameterName, false), x, length);
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        setBigDecimal(getNamedParamIndex(parameterName, false), x);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        setBinaryStream(getNamedParamIndex(parameterName, false), x, length);
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        setBoolean(getNamedParamIndex(parameterName, false), x);
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        setByte(getNamedParamIndex(parameterName, false), x);
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        setBytes(getNamedParamIndex(parameterName, false), x);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        setCharacterStream(getNamedParamIndex(parameterName, false), reader, length);
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        setDate(getNamedParamIndex(parameterName, false), x);
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        setDate(getNamedParamIndex(parameterName, false), x, cal);
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        setDouble(getNamedParamIndex(parameterName, false), x);
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        setFloat(getNamedParamIndex(parameterName, false), x);
    }

    private void setInOutParamsOnServer() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.paramInfo.numParameters > 0) {
                for (Iterator<CallableStatementParam> paramIter = this.paramInfo.iterator(); paramIter.hasNext();) {

                    CallableStatementParam inParamInfo = paramIter.next();

                    if (inParamInfo.isOut && inParamInfo.isIn) {
                        if (inParamInfo.paramName == null) {
                            inParamInfo.paramName = "nullnp" + inParamInfo.index;
                        }

                        String inOutParameterName = mangleParameterName(inParamInfo.paramName);
                        StringBuilder queryBuf = new StringBuilder(4 + inOutParameterName.length() + 1 + 1);
                        queryBuf.append("SET ");
                        queryBuf.append(inOutParameterName);
                        queryBuf.append("=?");

                        ClientPreparedStatement setPstmt = null;

                        try {
                            setPstmt = ((Wrapper) this.connection.clientPrepareStatement(queryBuf.toString())).unwrap(ClientPreparedStatement.class);

                            if (((PreparedQuery<?>) this.query).getQueryBindings().getBindValues()[inParamInfo.index].isNull()) {
                                setPstmt.setBytesNoEscapeNoQuotes(1, "NULL".getBytes());

                            } else {
                                byte[] parameterAsBytes = getBytesRepresentation(inParamInfo.index + 1);

                                if (parameterAsBytes != null) {
                                    if (parameterAsBytes.length > 8 && parameterAsBytes[0] == '_' && parameterAsBytes[1] == 'b' && parameterAsBytes[2] == 'i'
                                            && parameterAsBytes[3] == 'n' && parameterAsBytes[4] == 'a' && parameterAsBytes[5] == 'r'
                                            && parameterAsBytes[6] == 'y' && parameterAsBytes[7] == '\'') {
                                        setPstmt.setBytesNoEscapeNoQuotes(1, parameterAsBytes);
                                    } else {
                                        switch (inParamInfo.desiredMysqlType) {
                                            case BIT:
                                            case BINARY:
                                            case GEOMETRY:
                                            case TINYBLOB:
                                            case BLOB:
                                            case MEDIUMBLOB:
                                            case LONGBLOB:
                                            case VARBINARY:
                                                setPstmt.setBytes(1, parameterAsBytes);
                                                break;
                                            default:
                                                // the inherited PreparedStatement methods have already escaped and quoted these parameters
                                                setPstmt.setBytesNoEscape(1, parameterAsBytes);
                                        }
                                    }
                                } else {
                                    setPstmt.setNull(1, MysqlType.NULL);
                                }
                            }

                            setPstmt.executeUpdate();
                        } finally {
                            if (setPstmt != null) {
                                setPstmt.close();
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        setInt(getNamedParamIndex(parameterName, false), x);
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        setLong(getNamedParamIndex(parameterName, false), x);
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        setNull(getNamedParamIndex(parameterName, false), sqlType);
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        setNull(getNamedParamIndex(parameterName, false), sqlType, typeName);
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        setObject(getNamedParamIndex(parameterName, false), x);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        setObject(getNamedParamIndex(parameterName, false), x, targetSqlType);
    }

    @Override
    public void setObject(String parameterName, Object x, SQLType targetSqlType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setObject(getNamedParamIndex(parameterName, false), x, targetSqlType);
        }
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        setObject(getNamedParamIndex(parameterName, false), x, targetSqlType, scale);
    }

    @Override
    public void setObject(String parameterName, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setObject(getNamedParamIndex(parameterName, false), x, targetSqlType, scaleOrLength);
        }
    }

    private void setOutParams() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.paramInfo.numParameters > 0) {
                for (Iterator<CallableStatementParam> paramIter = this.paramInfo.iterator(); paramIter.hasNext();) {
                    CallableStatementParam outParamInfo = paramIter.next();

                    if (!this.callingStoredFunction && outParamInfo.isOut) {

                        if (outParamInfo.paramName == null) {
                            outParamInfo.paramName = "nullnp" + outParamInfo.index;
                        }

                        String outParameterName = mangleParameterName(outParamInfo.paramName);

                        int outParamIndex = 0;

                        if (this.placeholderToParameterIndexMap == null) {
                            outParamIndex = outParamInfo.index + 1;
                        } else {
                            // Find it, todo: remove this linear search
                            boolean found = false;

                            for (int i = 0; i < this.placeholderToParameterIndexMap.length; i++) {
                                if (this.placeholderToParameterIndexMap[i] == outParamInfo.index) {
                                    outParamIndex = i + 1; /* JDBC is 1-based */
                                    found = true;
                                    break;
                                }
                            }

                            if (!found) {
                                throw SQLError.createSQLException(Messages.getString("CallableStatement.21", new Object[] { outParamInfo.paramName }),
                                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
                            }
                        }

                        this.setBytesNoEscapeNoQuotes(outParamIndex, StringUtils.getBytes(outParameterName, this.charEncoding));
                    }
                }
            }
        }
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        setShort(getNamedParamIndex(parameterName, false), x);
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        setString(getNamedParamIndex(parameterName, false), x);
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        setTime(getNamedParamIndex(parameterName, false), x);
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        setTime(getNamedParamIndex(parameterName, false), x, cal);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        setTimestamp(getNamedParamIndex(parameterName, false), x);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(getNamedParamIndex(parameterName, false), x, cal);
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        setURL(getNamedParamIndex(parameterName, false), val);
    }

    @Override
    public boolean wasNull() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return this.outputParamWasNull;
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return Util.truncateAndConvertToInt(executeLargeBatch());

    }

    @Override
    protected int getParameterIndexOffset() {
        if (this.callingStoredFunction) {
            return -1;
        }

        return super.getParameterIndexOffset();
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        setAsciiStream(getNamedParamIndex(parameterName, false), x);

    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        setAsciiStream(getNamedParamIndex(parameterName, false), x, length);

    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        setBinaryStream(getNamedParamIndex(parameterName, false), x);

    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        setBinaryStream(getNamedParamIndex(parameterName, false), x, length);

    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        setBlob(getNamedParamIndex(parameterName, false), x);

    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        setBlob(getNamedParamIndex(parameterName, false), inputStream);

    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        setBlob(getNamedParamIndex(parameterName, false), inputStream, length);

    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        setCharacterStream(getNamedParamIndex(parameterName, false), reader);

    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        setCharacterStream(getNamedParamIndex(parameterName, false), reader, length);

    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        setClob(getNamedParamIndex(parameterName, false), x);

    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        setClob(getNamedParamIndex(parameterName, false), reader);

    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        setClob(getNamedParamIndex(parameterName, false), reader, length);

    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        setNCharacterStream(getNamedParamIndex(parameterName, false), value);

    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        setNCharacterStream(getNamedParamIndex(parameterName, false), value, length);

    }

    /**
     * Check whether the stored procedure alters any data or is safe for read-only usage.
     * 
     * @return true if procedure does not alter data
     * @throws SQLException
     *             if a database access error occurs or this method is called on a closed PreparedStatement
     */
    private boolean checkReadOnlyProcedure() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.noAccessToProcedureBodies) {
                return false;
            }

            if (this.paramInfo.isReadOnlySafeChecked) {
                return this.paramInfo.isReadOnlySafeProcedure;
            }

            ResultSet rs = null;
            java.sql.PreparedStatement ps = null;

            try {
                String procName = extractProcedureName();

                String db = this.getCurrentDatabase();

                if (procName.indexOf(".") != -1) {
                    db = procName.substring(0, procName.indexOf("."));

                    if (StringUtils.startsWithIgnoreCaseAndWs(db, "`") && db.trim().endsWith("`")) {
                        db = db.substring(1, db.length() - 1);
                    }

                    procName = procName.substring(procName.indexOf(".") + 1);
                    procName = StringUtils.toString(StringUtils.stripEnclosure(StringUtils.getBytes(procName), "`", "`"));
                }
                ps = this.connection.prepareStatement("SELECT SQL_DATA_ACCESS FROM information_schema.routines WHERE routine_schema = ? AND routine_name = ?");
                ps.setMaxRows(0);
                ps.setFetchSize(0);

                ps.setString(1, db);
                ps.setString(2, procName);
                rs = ps.executeQuery();
                if (rs.next()) {
                    String sqlDataAccess = rs.getString(1);
                    if ("READS SQL DATA".equalsIgnoreCase(sqlDataAccess) || "NO SQL".equalsIgnoreCase(sqlDataAccess)) {
                        synchronized (this.paramInfo) {
                            this.paramInfo.isReadOnlySafeChecked = true;
                            this.paramInfo.isReadOnlySafeProcedure = true;
                        }
                        return true;
                    }
                }
            } catch (SQLException e) {
                // swallow the Exception
            } finally {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }

            }
            this.paramInfo.isReadOnlySafeChecked = false;
            this.paramInfo.isReadOnlySafeProcedure = false;
        }
        return false;

    }

    @Override
    protected boolean checkReadOnlySafeStatement() throws SQLException {
        return (super.checkReadOnlySafeStatement() || this.checkReadOnlyProcedure());
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

        RowId retValue = rs.getRowId(mapOutputParameterIndexToRsIndex(parameterIndex));

        this.outputParamWasNull = rs.wasNull();

        return retValue;
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

        RowId retValue = rs.getRowId(fixParameterName(parameterName));

        this.outputParamWasNull = rs.wasNull();

        return retValue;
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        setRowId(getNamedParamIndex(parameterName, false), x);
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        setNString(getNamedParamIndex(parameterName, false), value);
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        setNClob(getNamedParamIndex(parameterName, false), value);

    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        setNClob(getNamedParamIndex(parameterName, false), reader);

    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        setNClob(getNamedParamIndex(parameterName, false), reader, length);

    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        setSQLXML(getNamedParamIndex(parameterName, false), xmlObject);

    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

        SQLXML retValue = rs.getSQLXML(mapOutputParameterIndexToRsIndex(parameterIndex));

        this.outputParamWasNull = rs.wasNull();

        return retValue;

    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

        SQLXML retValue = rs.getSQLXML(fixParameterName(parameterName));

        this.outputParamWasNull = rs.wasNull();

        return retValue;
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

        String retValue = rs.getNString(mapOutputParameterIndexToRsIndex(parameterIndex));

        this.outputParamWasNull = rs.wasNull();

        return retValue;
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=
        String retValue = rs.getNString(fixParameterName(parameterName));

        this.outputParamWasNull = rs.wasNull();

        return retValue;
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

        Reader retValue = rs.getNCharacterStream(mapOutputParameterIndexToRsIndex(parameterIndex));

        this.outputParamWasNull = rs.wasNull();

        return retValue;
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=
        Reader retValue = rs.getNCharacterStream(fixParameterName(parameterName));

        this.outputParamWasNull = rs.wasNull();

        return retValue;
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

        Reader retValue = rs.getCharacterStream(mapOutputParameterIndexToRsIndex(parameterIndex));

        this.outputParamWasNull = rs.wasNull();

        return retValue;
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

        Reader retValue = rs.getCharacterStream(fixParameterName(parameterName));

        this.outputParamWasNull = rs.wasNull();

        return retValue;
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

        NClob retValue = rs.getNClob(mapOutputParameterIndexToRsIndex(parameterIndex));

        this.outputParamWasNull = rs.wasNull();

        return retValue;
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

        NClob retValue = rs.getNClob(fixParameterName(parameterName));

        this.outputParamWasNull = rs.wasNull();

        return retValue;
    }

    /**
     * Converts the given string to bytes, using the connection's character
     * encoding.
     *
     * @param s
     *            string
     * @return bytes
     */
    protected byte[] s2b(String s) {
        return s == null ? null : StringUtils.getBytes(s, this.charEncoding);
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            long returnVal = -1;

            checkStreamability();

            if (this.callingStoredFunction) {
                execute();

                return -1;
            }

            setInOutParamsOnServer();
            setOutParams();

            returnVal = super.executeLargeUpdate();

            retrieveOutParams();

            return returnVal;
        }
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        if (this.hasOutputParams) {
            throw SQLError.createSQLException("Can't call executeBatch() on CallableStatement with OUTPUT parameters",
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        return super.executeLargeBatch();
    }
}
