/*
  Copyright (c) 2002, 2017, Oracle and/or its affiliates. All rights reserved.

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

import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Representation of stored procedures for JDBC
 */
public class CallableStatement extends PreparedStatement implements java.sql.CallableStatement {
    protected final static Constructor<?> JDBC_4_CSTMT_2_ARGS_CTOR;

    protected final static Constructor<?> JDBC_4_CSTMT_4_ARGS_CTOR;

    static {
        if (Util.isJdbc4()) {
            try {
                String jdbc4ClassName = Util.isJdbc42() ? "com.mysql.jdbc.JDBC42CallableStatement" : "com.mysql.jdbc.JDBC4CallableStatement";
                JDBC_4_CSTMT_2_ARGS_CTOR = Class.forName(jdbc4ClassName)
                        .getConstructor(new Class[] { MySQLConnection.class, CallableStatementParamInfo.class });
                JDBC_4_CSTMT_4_ARGS_CTOR = Class.forName(jdbc4ClassName)
                        .getConstructor(new Class[] { MySQLConnection.class, String.class, String.class, Boolean.TYPE });
            } catch (SecurityException e) {
                throw new RuntimeException(e);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else {
            JDBC_4_CSTMT_4_ARGS_CTOR = null;
            JDBC_4_CSTMT_2_ARGS_CTOR = null;
        }
    }

    protected static class CallableStatementParam {
        int desiredJdbcType;

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

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#clone()
         */
        @Override
        protected Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    protected class CallableStatementParamInfo implements ParameterMetaData {
        String catalogInUse;

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
            this.nativeSql = CallableStatement.this.originalSql;
            this.catalogInUse = CallableStatement.this.currentCatalog;
            this.isFunctionCall = fullParamInfo.isFunctionCall;
            @SuppressWarnings("synthetic-access")
            int[] localParameterMap = CallableStatement.this.placeholderToParameterIndexMap;
            int parameterMapLength = localParameterMap.length;

            this.isReadOnlySafeProcedure = fullParamInfo.isReadOnlySafeProcedure;
            this.isReadOnlySafeChecked = fullParamInfo.isReadOnlySafeChecked;
            this.parameterList = new ArrayList<CallableStatementParam>(fullParamInfo.numParameters);
            this.parameterMap = new HashMap<String, CallableStatementParam>(fullParamInfo.numParameters);

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

            this.nativeSql = CallableStatement.this.originalSql;
            this.catalogInUse = CallableStatement.this.currentCatalog;
            this.isFunctionCall = CallableStatement.this.callingStoredFunction;

            if (hadRows) {
                this.numParameters = paramTypesRs.getRow();

                this.parameterList = new ArrayList<CallableStatementParam>(this.numParameters);
                this.parameterMap = new HashMap<String, CallableStatementParam>(this.numParameters);

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
                throw SQLError.createSQLException(Messages.getString("CallableStatement.11") + paramIndex + Messages.getString("CallableStatement.12")
                        + this.numParameters + Messages.getString("CallableStatement.13"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#clone()
         */
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

        public String getParameterClassName(int arg0) throws SQLException {
            String mysqlTypeName = getParameterTypeName(arg0);

            boolean isBinaryOrBlob = StringUtils.indexOfIgnoreCase(mysqlTypeName, "BLOB") != -1 || StringUtils.indexOfIgnoreCase(mysqlTypeName, "BINARY") != -1;

            boolean isUnsigned = StringUtils.indexOfIgnoreCase(mysqlTypeName, "UNSIGNED") != -1;

            int mysqlTypeIfKnown = 0;

            if (StringUtils.startsWithIgnoreCase(mysqlTypeName, "MEDIUMINT")) {
                mysqlTypeIfKnown = MysqlDefs.FIELD_TYPE_INT24;
            }

            return ResultSetMetaData.getClassNameForJavaType(getParameterType(arg0), isUnsigned, mysqlTypeIfKnown, isBinaryOrBlob, false,
                    CallableStatement.this.connection.getYearIsDateType());
        }

        public int getParameterCount() throws SQLException {
            if (this.parameterList == null) {
                return 0;
            }

            return this.parameterList.size();
        }

        public int getParameterMode(int arg0) throws SQLException {
            checkBounds(arg0);

            return getParameter(arg0 - 1).inOutModifier;
        }

        public int getParameterType(int arg0) throws SQLException {
            checkBounds(arg0);

            return getParameter(arg0 - 1).jdbcType;
        }

        public String getParameterTypeName(int arg0) throws SQLException {
            checkBounds(arg0);

            return getParameter(arg0 - 1).typeName;
        }

        public int getPrecision(int arg0) throws SQLException {
            checkBounds(arg0);

            return getParameter(arg0 - 1).precision;
        }

        public int getScale(int arg0) throws SQLException {
            checkBounds(arg0);

            return getParameter(arg0 - 1).scale;
        }

        public int isNullable(int arg0) throws SQLException {
            checkBounds(arg0);

            return getParameter(arg0 - 1).nullability;
        }

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

        /**
         * @see java.sql.Wrapper#isWrapperFor(Class)
         */
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            checkClosed();

            // This works for classes that aren't actually wrapping anything
            return iface.isInstance(this);
        }

        /**
         * @see java.sql.Wrapper#unwrap(Class)
         */
        public <T> T unwrap(Class<T> iface) throws java.sql.SQLException {
            try {
                // This works for classes that aren't actually wrapping anything
                return iface.cast(this);
            } catch (ClassCastException cce) {
                throw SQLError.createSQLException("Unable to unwrap to " + iface.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }
    }

    private final static int NOT_OUTPUT_PARAMETER_INDICATOR = Integer.MIN_VALUE;

    private final static String PARAMETER_NAMESPACE_PREFIX = "@com_mysql_jdbc_outparam_";

    private static String mangleParameterName(String origParameterName) {
        //Fixed for 5.5+ in callers
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

    // private List parameterList;
    // private Map parameterMap;
    private ResultSetInternalMethods outputParameterResults;

    protected boolean outputParamWasNull = false;

    private int[] parameterIndexToRsIndex;

    protected CallableStatementParamInfo paramInfo;

    private CallableStatementParam returnValueParam;

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
    public CallableStatement(MySQLConnection conn, CallableStatementParamInfo paramInfo) throws SQLException {
        super(conn, paramInfo.nativeSql, paramInfo.catalogInUse);

        this.paramInfo = paramInfo;
        this.callingStoredFunction = this.paramInfo.isFunctionCall;

        if (this.callingStoredFunction) {
            this.parameterCount += 1;
        }

        this.retrieveGeneratedKeys = true; // not provided for in the JDBC spec
    }

    /**
     * Creates a callable statement instance -- We need to provide factory-style methods
     * so we can support both JDBC3 (and older) and JDBC4 runtimes, otherwise
     * the class verifier complains when it tries to load JDBC4-only interface
     * classes that are present in JDBC4 method signatures.
     */

    protected static CallableStatement getInstance(MySQLConnection conn, String sql, String catalog, boolean isFunctionCall) throws SQLException {
        if (!Util.isJdbc4()) {
            return new CallableStatement(conn, sql, catalog, isFunctionCall);
        }

        return (CallableStatement) Util.handleNewInstance(JDBC_4_CSTMT_4_ARGS_CTOR, new Object[] { conn, sql, catalog, Boolean.valueOf(isFunctionCall) },
                conn.getExceptionInterceptor());
    }

    /**
     * Creates a callable statement instance -- We need to provide factory-style methods
     * so we can support both JDBC3 (and older) and JDBC4 runtimes, otherwise
     * the class verifier complains when it tries to load JDBC4-only interface
     * classes that are present in JDBC4 method signatures.
     */

    protected static CallableStatement getInstance(MySQLConnection conn, CallableStatementParamInfo paramInfo) throws SQLException {
        if (!Util.isJdbc4()) {
            return new CallableStatement(conn, paramInfo);
        }

        return (CallableStatement) Util.handleNewInstance(JDBC_4_CSTMT_2_ARGS_CTOR, new Object[] { conn, paramInfo }, conn.getExceptionInterceptor());

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

            if (this.paramInfo != null && this.parameterCount != parameterCountFromMetaData) {
                this.placeholderToParameterIndexMap = new int[this.parameterCount];

                int startPos = this.callingStoredFunction ? StringUtils.indexOfIgnoreCase(this.originalSql, "SELECT")
                        : StringUtils.indexOfIgnoreCase(this.originalSql, "CALL");

                if (startPos != -1) {
                    int parenOpenPos = this.originalSql.indexOf('(', startPos + 4);

                    if (parenOpenPos != -1) {
                        int parenClosePos = StringUtils.indexOfIgnoreCase(parenOpenPos, this.originalSql, ")", "'", "'", StringUtils.SEARCH_MODE__ALL);

                        if (parenClosePos != -1) {
                            List<?> parsedParameters = StringUtils.split(this.originalSql.substring(parenOpenPos + 1, parenClosePos), ",", "'\"", "'\"", true);

                            int numParsedParameters = parsedParameters.size();

                            // sanity check

                            if (numParsedParameters != this.parameterCount) {
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
     * @param catalog
     *            the current catalog
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public CallableStatement(MySQLConnection conn, String sql, String catalog, boolean isFunctionCall) throws SQLException {
        super(conn, sql, catalog);

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

            this.parameterCount += 1;
        }

        this.retrieveGeneratedKeys = true; // not provided for in the JDBC spec
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.PreparedStatement#addBatch()
     */
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
                        this.returnValueParam = new CallableStatementParam("", 0, false, true, Types.VARCHAR, "VARCHAR", 0, 0,
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

            if (this.connection.getNoAccessToProcedureBodies()) {
                paramDescriptor.isOut = true;
                paramDescriptor.isIn = true;
                paramDescriptor.inOutModifier = java.sql.DatabaseMetaData.procedureColumnInOut;
            } else if (!paramDescriptor.isOut) {
                throw SQLError.createSQLException(Messages.getString("CallableStatement.9") + paramIndex + Messages.getString("CallableStatement.10"),
                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            this.hasOutputParams = true;

            return paramDescriptor;
        }
    }

    /**
     * @param paramIndex
     * 
     * @throws SQLException
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
     */
    private void checkStreamability() throws SQLException {
        if (this.hasOutputParams && createStreamingResultSet()) {
            throw SQLError.createSQLException(Messages.getString("CallableStatement.14"), SQLError.SQL_STATE_DRIVER_NOT_CAPABLE, getExceptionInterceptor());
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
     * @throws SQLException
     *             if we can't build the metadata.
     */
    private void fakeParameterTypes(boolean isReallyProcedure) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            Field[] fields = new Field[13];

            fields[0] = new Field("", "PROCEDURE_CAT", Types.CHAR, 0);
            fields[1] = new Field("", "PROCEDURE_SCHEM", Types.CHAR, 0);
            fields[2] = new Field("", "PROCEDURE_NAME", Types.CHAR, 0);
            fields[3] = new Field("", "COLUMN_NAME", Types.CHAR, 0);
            fields[4] = new Field("", "COLUMN_TYPE", Types.CHAR, 0);
            fields[5] = new Field("", "DATA_TYPE", Types.SMALLINT, 0);
            fields[6] = new Field("", "TYPE_NAME", Types.CHAR, 0);
            fields[7] = new Field("", "PRECISION", Types.INTEGER, 0);
            fields[8] = new Field("", "LENGTH", Types.INTEGER, 0);
            fields[9] = new Field("", "SCALE", Types.SMALLINT, 0);
            fields[10] = new Field("", "RADIX", Types.SMALLINT, 0);
            fields[11] = new Field("", "NULLABLE", Types.SMALLINT, 0);
            fields[12] = new Field("", "REMARKS", Types.CHAR, 0);

            String procName = isReallyProcedure ? extractProcedureName() : null;

            byte[] procNameAsBytes = null;

            try {
                procNameAsBytes = procName == null ? null : StringUtils.getBytes(procName, "UTF-8");
            } catch (UnsupportedEncodingException ueEx) {
                procNameAsBytes = StringUtils.s2b(procName, this.connection);
            }

            ArrayList<ResultSetRow> resultRows = new ArrayList<ResultSetRow>();

            for (int i = 0; i < this.parameterCount; i++) {
                byte[][] row = new byte[13][];
                row[0] = null; // PROCEDURE_CAT
                row[1] = null; // PROCEDURE_SCHEM
                row[2] = procNameAsBytes; // PROCEDURE/NAME
                row[3] = StringUtils.s2b(String.valueOf(i), this.connection); // COLUMN_NAME

                row[4] = StringUtils.s2b(String.valueOf(java.sql.DatabaseMetaData.procedureColumnIn), this.connection);

                row[5] = StringUtils.s2b(String.valueOf(Types.VARCHAR), this.connection); // DATA_TYPE
                row[6] = StringUtils.s2b("VARCHAR", this.connection); // TYPE_NAME
                row[7] = StringUtils.s2b(Integer.toString(65535), this.connection); // PRECISION
                row[8] = StringUtils.s2b(Integer.toString(65535), this.connection); // LENGTH
                row[9] = StringUtils.s2b(Integer.toString(0), this.connection); // SCALE
                row[10] = StringUtils.s2b(Integer.toString(10), this.connection); // RADIX

                row[11] = StringUtils.s2b(Integer.toString(java.sql.DatabaseMetaData.procedureNullableUnknown), this.connection); // nullable

                row[12] = null;

                resultRows.add(new ByteArrayRow(row, getExceptionInterceptor()));
            }

            java.sql.ResultSet paramTypesRs = DatabaseMetaData.buildResultSet(fields, resultRows, this.connection);

            convertGetProcedureColumnsToInternalDescriptors(paramTypesRs);
        }
    }

    private void determineParameterTypes() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            java.sql.ResultSet paramTypesRs = null;

            try {
                //Bug#57022, we need to check for db.SPname notation first and pass on only SPname
                String procName = extractProcedureName();
                String quotedId = "";
                try {
                    quotedId = this.connection.supportsQuotedIdentifiers() ? this.connection.getMetaData().getIdentifierQuoteString() : "";
                } catch (SQLException sqlEx) {
                    // Forced by API, never thrown from getIdentifierQuoteString() in
                    // this implementation.
                    AssertionFailedException.shouldNotHappen(sqlEx);
                }

                List<?> parseList = StringUtils.splitDBdotName(procName, "", quotedId, this.connection.isNoBackslashEscapesSet());
                String tmpCatalog = "";
                //There *should* be 2 rows, if any.
                if (parseList.size() == 2) {
                    tmpCatalog = (String) parseList.get(0);
                    procName = (String) parseList.get(1);
                } else {
                    //keep values as they are
                }

                java.sql.DatabaseMetaData dbmd = this.connection.getMetaData();

                boolean useCatalog = false;

                if (tmpCatalog.length() <= 0) {
                    useCatalog = true;
                }

                paramTypesRs = dbmd.getProcedureColumns(this.connection.versionMeetsMinimum(5, 0, 2) && useCatalog ? this.currentCatalog : tmpCatalog/* null */,
                        null, procName, "%");

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

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.PreparedStatement#execute()
     */
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

            retrieveOutParams();

            if (!this.callingStoredFunction) {
                return returnVal;
            }

            // Functions can't return results
            return false;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.PreparedStatement#executeQuery()
     */
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

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.PreparedStatement#executeUpdate()
     */
    @Override
    public int executeUpdate() throws SQLException {
        return Util.truncateAndConvertToInt(executeLargeUpdate());
    }

    private String extractProcedureName() throws SQLException {
        String sanitizedSql = StringUtils.stripComments(this.originalSql, "`\"'", "`\"'", true, false, true, true);

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

        throw SQLError.createSQLException(Messages.getString("CallableStatement.1"), SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
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
            //Fixed for 5.5+
            if (((paramNameIn == null) || (paramNameIn.length() == 0)) && (!hasParametersView())) {
                throw SQLError.createSQLException(((Messages.getString("CallableStatement.0") + paramNameIn) == null)
                        ? Messages.getString("CallableStatement.15") : Messages.getString("CallableStatement.16"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }

            if ((paramNameIn == null) && (hasParametersView())) {
                paramNameIn = "nullpn";
            }

            if (this.connection.getNoAccessToProcedureBodies()) {
                throw SQLError.createSQLException("No access to parameters by name when connection has been configured not to access procedure bodies",
                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            return mangleParameterName(paramNameIn);
        }
    }

    /**
     * @see java.sql.CallableStatement#getArray(int)
     */
    public Array getArray(int i) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(i);

            Array retValue = rs.getArray(mapOutputParameterIndexToRsIndex(i));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getArray(java.lang.String)
     */
    public Array getArray(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Array retValue = rs.getArray(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getBigDecimal(int)
     */
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            BigDecimal retValue = rs.getBigDecimal(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @param parameterIndex
     * @param scale
     * 
     * @throws SQLException
     * 
     * @see java.sql.CallableStatement#getBigDecimal(int, int)
     * @deprecated
     */
    @Deprecated
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            BigDecimal retValue = rs.getBigDecimal(mapOutputParameterIndexToRsIndex(parameterIndex), scale);

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getBigDecimal(java.lang.String)
     */
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            BigDecimal retValue = rs.getBigDecimal(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getBlob(int)
     */
    public Blob getBlob(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Blob retValue = rs.getBlob(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getBlob(java.lang.String)
     */
    public Blob getBlob(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Blob retValue = rs.getBlob(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getBoolean(int)
     */
    public boolean getBoolean(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            boolean retValue = rs.getBoolean(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getBoolean(java.lang.String)
     */
    public boolean getBoolean(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            boolean retValue = rs.getBoolean(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getByte(int)
     */
    public byte getByte(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            byte retValue = rs.getByte(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getByte(java.lang.String)
     */
    public byte getByte(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            byte retValue = rs.getByte(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getBytes(int)
     */
    public byte[] getBytes(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            byte[] retValue = rs.getBytes(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getBytes(java.lang.String)
     */
    public byte[] getBytes(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            byte[] retValue = rs.getBytes(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getClob(int)
     */
    public Clob getClob(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Clob retValue = rs.getClob(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getClob(java.lang.String)
     */
    public Clob getClob(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Clob retValue = rs.getClob(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getDate(int)
     */
    public Date getDate(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Date retValue = rs.getDate(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getDate(int, java.util.Calendar)
     */
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Date retValue = rs.getDate(mapOutputParameterIndexToRsIndex(parameterIndex), cal);

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getDate(java.lang.String)
     */
    public Date getDate(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Date retValue = rs.getDate(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getDate(java.lang.String, java.util.Calendar)
     */
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Date retValue = rs.getDate(fixParameterName(parameterName), cal);

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getDouble(int)
     */
    public double getDouble(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            double retValue = rs.getDouble(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getDouble(java.lang.String)
     */
    public double getDouble(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            double retValue = rs.getDouble(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getFloat(int)
     */
    public float getFloat(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            float retValue = rs.getFloat(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getFloat(java.lang.String)
     */
    public float getFloat(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            float retValue = rs.getFloat(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getInt(int)
     */
    public int getInt(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            int retValue = rs.getInt(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getInt(java.lang.String)
     */
    public int getInt(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            int retValue = rs.getInt(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getLong(int)
     */
    public long getLong(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            long retValue = rs.getLong(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getLong(java.lang.String)
     */
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
            if (this.connection.getNoAccessToProcedureBodies()) {
                throw SQLError.createSQLException("No access to parameters by name when connection has been configured not to access procedure bodies",
                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            //Fixed for 5.5+ in callers
            if ((paramName == null) || (paramName.length() == 0)) {
                throw SQLError.createSQLException(Messages.getString("CallableStatement.2"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            CallableStatementParam namedParamInfo;
            if (this.paramInfo == null || (namedParamInfo = this.paramInfo.getParameter(paramName)) == null) {
                throw SQLError.createSQLException(Messages.getString("CallableStatement.3") + paramName + Messages.getString("CallableStatement.4"),
                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            if (forOut && !namedParamInfo.isOut) {
                throw SQLError.createSQLException(Messages.getString("CallableStatement.5") + paramName + Messages.getString("CallableStatement.6"),
                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            if (this.placeholderToParameterIndexMap == null) {
                return namedParamInfo.index + 1; // JDBC indices are 1-based
            }

            for (int i = 0; i < this.placeholderToParameterIndexMap.length; i++) {
                if (this.placeholderToParameterIndexMap[i] == namedParamInfo.index) {
                    return i + 1;
                }
            }

            throw SQLError.createSQLException("Can't find local placeholder mapping for parameter named \"" + paramName + "\".",
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }
    }

    /**
     * @see java.sql.CallableStatement#getObject(int)
     */
    public Object getObject(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            CallableStatementParam paramDescriptor = checkIsOutputParam(parameterIndex);

            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Object retVal = rs.getObjectStoredProc(mapOutputParameterIndexToRsIndex(parameterIndex), paramDescriptor.desiredJdbcType);

            this.outputParamWasNull = rs.wasNull();

            return retVal;
        }
    }

    /**
     * @see java.sql.CallableStatement#getObject(int, java.util.Map)
     */
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Object retVal = rs.getObject(mapOutputParameterIndexToRsIndex(parameterIndex), map);

            this.outputParamWasNull = rs.wasNull();

            return retVal;
        }
    }

    /**
     * @see java.sql.CallableStatement#getObject(java.lang.String)
     */
    public Object getObject(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Object retValue = rs.getObject(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getObject(java.lang.String, java.util.Map)
     */
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Object retValue = rs.getObject(fixParameterName(parameterName), map);

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    // JDBC-4.1
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            // remove cast once 1.5, 1.6 EOL'd
            T retVal = ((ResultSetImpl) rs).getObject(mapOutputParameterIndexToRsIndex(parameterIndex), type);

            this.outputParamWasNull = rs.wasNull();

            return retVal;
        }
    }

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
                    throw SQLError.createSQLException(Messages.getString("CallableStatement.7"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                            getExceptionInterceptor());
                }
                throw SQLError.createSQLException(Messages.getString("CallableStatement.8"), SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
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

    /**
     * @see java.sql.CallableStatement#getRef(int)
     */
    public Ref getRef(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Ref retValue = rs.getRef(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getRef(java.lang.String)
     */
    public Ref getRef(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Ref retValue = rs.getRef(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getShort(int)
     */
    public short getShort(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            short retValue = rs.getShort(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getShort(java.lang.String)
     */
    public short getShort(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            short retValue = rs.getShort(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getString(int)
     */
    public String getString(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            String retValue = rs.getString(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getString(java.lang.String)
     */
    public String getString(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            String retValue = rs.getString(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getTime(int)
     */
    public Time getTime(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Time retValue = rs.getTime(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getTime(int, java.util.Calendar)
     */
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Time retValue = rs.getTime(mapOutputParameterIndexToRsIndex(parameterIndex), cal);

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getTime(java.lang.String)
     */
    public Time getTime(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Time retValue = rs.getTime(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getTime(java.lang.String, java.util.Calendar)
     */
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Time retValue = rs.getTime(fixParameterName(parameterName), cal);

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getTimestamp(int)
     */
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Timestamp retValue = rs.getTimestamp(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getTimestamp(int, java.util.Calendar)
     */
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            Timestamp retValue = rs.getTimestamp(mapOutputParameterIndexToRsIndex(parameterIndex), cal);

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getTimestamp(java.lang.String)
     */
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Timestamp retValue = rs.getTimestamp(fixParameterName(parameterName));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getTimestamp(java.lang.String, java.util.Calendar)
     */
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(0); // definitely not going to be from ?=

            Timestamp retValue = rs.getTimestamp(fixParameterName(parameterName), cal);

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getURL(int)
     */
    public URL getURL(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

            URL retValue = rs.getURL(mapOutputParameterIndexToRsIndex(parameterIndex));

            this.outputParamWasNull = rs.wasNull();

            return retValue;
        }
    }

    /**
     * @see java.sql.CallableStatement#getURL(java.lang.String)
     */
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
                throw SQLError.createSQLException(Messages.getString("CallableStatement.21") + paramIndex + Messages.getString("CallableStatement.22"),
                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            return rsIndex + 1;
        }
    }

    /**
     * @see java.sql.CallableStatement#registerOutParameter(int, int)
     */
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        CallableStatementParam paramDescriptor = checkIsOutputParam(parameterIndex);
        paramDescriptor.desiredJdbcType = sqlType;
    }

    /**
     * @see java.sql.CallableStatement#registerOutParameter(int, int, int)
     */
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        registerOutParameter(parameterIndex, sqlType);
    }

    /**
     * @see java.sql.CallableStatement#registerOutParameter(int, int, java.lang.String)
     */
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        checkIsOutputParam(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int)
     */
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            registerOutParameter(getNamedParamIndex(parameterName, true), sqlType);
        }
    }

    /**
     * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int, int)
     */
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        registerOutParameter(getNamedParamIndex(parameterName, true), sqlType);
    }

    /**
     * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int, java.lang.String)
     */
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        registerOutParameter(getNamedParamIndex(parameterName, true), sqlType, typeName);
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

                        if ((retrParamInfo.paramName == null) && (hasParametersView())) {
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
                        this.outputParameterResults = ((com.mysql.jdbc.ResultSetInternalMethods) outParamRs).copy();

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

    /**
     * @see java.sql.CallableStatement#setAsciiStream(java.lang.String, java.io.InputStream, int)
     */
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        setAsciiStream(getNamedParamIndex(parameterName, false), x, length);
    }

    /**
     * @see java.sql.CallableStatement#setBigDecimal(java.lang.String, java.math.BigDecimal)
     */
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        setBigDecimal(getNamedParamIndex(parameterName, false), x);
    }

    /**
     * @see java.sql.CallableStatement#setBinaryStream(java.lang.String, java.io.InputStream, int)
     */
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        setBinaryStream(getNamedParamIndex(parameterName, false), x, length);
    }

    /**
     * @see java.sql.CallableStatement#setBoolean(java.lang.String, boolean)
     */
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        setBoolean(getNamedParamIndex(parameterName, false), x);
    }

    /**
     * @see java.sql.CallableStatement#setByte(java.lang.String, byte)
     */
    public void setByte(String parameterName, byte x) throws SQLException {
        setByte(getNamedParamIndex(parameterName, false), x);
    }

    /**
     * @see java.sql.CallableStatement#setBytes(java.lang.String, byte[])
     */
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        setBytes(getNamedParamIndex(parameterName, false), x);
    }

    /**
     * @see java.sql.CallableStatement#setCharacterStream(java.lang.String, java.io.Reader, int)
     */
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        setCharacterStream(getNamedParamIndex(parameterName, false), reader, length);
    }

    /**
     * @see java.sql.CallableStatement#setDate(java.lang.String, java.sql.Date)
     */
    public void setDate(String parameterName, Date x) throws SQLException {
        setDate(getNamedParamIndex(parameterName, false), x);
    }

    /**
     * @see java.sql.CallableStatement#setDate(java.lang.String, java.sql.Date, java.util.Calendar)
     */
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        setDate(getNamedParamIndex(parameterName, false), x, cal);
    }

    /**
     * @see java.sql.CallableStatement#setDouble(java.lang.String, double)
     */
    public void setDouble(String parameterName, double x) throws SQLException {
        setDouble(getNamedParamIndex(parameterName, false), x);
    }

    /**
     * @see java.sql.CallableStatement#setFloat(java.lang.String, float)
     */
    public void setFloat(String parameterName, float x) throws SQLException {
        setFloat(getNamedParamIndex(parameterName, false), x);
    }

    private void setInOutParamsOnServer() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.paramInfo.numParameters > 0) {
                for (Iterator<CallableStatementParam> paramIter = this.paramInfo.iterator(); paramIter.hasNext();) {

                    CallableStatementParam inParamInfo = paramIter.next();

                    //Fix for 5.5+
                    if (inParamInfo.isOut && inParamInfo.isIn) {
                        if ((inParamInfo.paramName == null) && (hasParametersView())) {
                            inParamInfo.paramName = "nullnp" + inParamInfo.index;
                        }

                        String inOutParameterName = mangleParameterName(inParamInfo.paramName);
                        StringBuilder queryBuf = new StringBuilder(4 + inOutParameterName.length() + 1 + 1);
                        queryBuf.append("SET ");
                        queryBuf.append(inOutParameterName);
                        queryBuf.append("=?");

                        PreparedStatement setPstmt = null;

                        try {
                            setPstmt = ((Wrapper) this.connection.clientPrepareStatement(queryBuf.toString())).unwrap(PreparedStatement.class);

                            if (this.isNull[inParamInfo.index]) {
                                setPstmt.setBytesNoEscapeNoQuotes(1, "NULL".getBytes());

                            } else {
                                byte[] parameterAsBytes = getBytesRepresentation(inParamInfo.index);

                                if (parameterAsBytes != null) {
                                    if (parameterAsBytes.length > 8 && parameterAsBytes[0] == '_' && parameterAsBytes[1] == 'b' && parameterAsBytes[2] == 'i'
                                            && parameterAsBytes[3] == 'n' && parameterAsBytes[4] == 'a' && parameterAsBytes[5] == 'r'
                                            && parameterAsBytes[6] == 'y' && parameterAsBytes[7] == '\'') {
                                        setPstmt.setBytesNoEscapeNoQuotes(1, parameterAsBytes);
                                    } else {
                                        int sqlType = inParamInfo.desiredJdbcType;

                                        switch (sqlType) {
                                            case Types.BIT:
                                            case Types.BINARY:
                                            case Types.BLOB:
                                            case Types.JAVA_OBJECT:
                                            case Types.LONGVARBINARY:
                                            case Types.VARBINARY:
                                                setPstmt.setBytes(1, parameterAsBytes);
                                                break;
                                            default:
                                                // the inherited PreparedStatement methods have already escaped and quoted these parameters
                                                setPstmt.setBytesNoEscape(1, parameterAsBytes);
                                        }
                                    }
                                } else {
                                    setPstmt.setNull(1, Types.NULL);
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

    /**
     * @see java.sql.CallableStatement#setInt(java.lang.String, int)
     */
    public void setInt(String parameterName, int x) throws SQLException {
        setInt(getNamedParamIndex(parameterName, false), x);
    }

    /**
     * @see java.sql.CallableStatement#setLong(java.lang.String, long)
     */
    public void setLong(String parameterName, long x) throws SQLException {
        setLong(getNamedParamIndex(parameterName, false), x);
    }

    /**
     * @see java.sql.CallableStatement#setNull(java.lang.String, int)
     */
    public void setNull(String parameterName, int sqlType) throws SQLException {
        setNull(getNamedParamIndex(parameterName, false), sqlType);
    }

    /**
     * @see java.sql.CallableStatement#setNull(java.lang.String, int, java.lang.String)
     */
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        setNull(getNamedParamIndex(parameterName, false), sqlType, typeName);
    }

    /**
     * @see java.sql.CallableStatement#setObject(java.lang.String, java.lang.Object)
     */
    public void setObject(String parameterName, Object x) throws SQLException {
        setObject(getNamedParamIndex(parameterName, false), x);
    }

    /**
     * @see java.sql.CallableStatement#setObject(java.lang.String, java.lang.Object, int)
     */
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        setObject(getNamedParamIndex(parameterName, false), x, targetSqlType);
    }

    /**
     * @see java.sql.CallableStatement#setObject(java.lang.String, java.lang.Object, int, int)
     */
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
    }

    private void setOutParams() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.paramInfo.numParameters > 0) {
                for (Iterator<CallableStatementParam> paramIter = this.paramInfo.iterator(); paramIter.hasNext();) {
                    CallableStatementParam outParamInfo = paramIter.next();

                    if (!this.callingStoredFunction && outParamInfo.isOut) {

                        if ((outParamInfo.paramName == null) && (hasParametersView())) {
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
                                throw SQLError.createSQLException(
                                        Messages.getString("CallableStatement.21") + outParamInfo.paramName + Messages.getString("CallableStatement.22"),
                                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
                            }
                        }

                        this.setBytesNoEscapeNoQuotes(outParamIndex, StringUtils.getBytes(outParameterName, this.charConverter, this.charEncoding,
                                this.connection.getServerCharset(), this.connection.parserKnowsUnicode(), getExceptionInterceptor()));
                    }
                }
            }
        }
    }

    /**
     * @see java.sql.CallableStatement#setShort(java.lang.String, short)
     */
    public void setShort(String parameterName, short x) throws SQLException {
        setShort(getNamedParamIndex(parameterName, false), x);
    }

    /**
     * @see java.sql.CallableStatement#setString(java.lang.String, java.lang.String)
     */
    public void setString(String parameterName, String x) throws SQLException {
        setString(getNamedParamIndex(parameterName, false), x);
    }

    /**
     * @see java.sql.CallableStatement#setTime(java.lang.String, java.sql.Time)
     */
    public void setTime(String parameterName, Time x) throws SQLException {
        setTime(getNamedParamIndex(parameterName, false), x);
    }

    /**
     * @see java.sql.CallableStatement#setTime(java.lang.String, java.sql.Time, java.util.Calendar)
     */
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        setTime(getNamedParamIndex(parameterName, false), x, cal);
    }

    /**
     * @see java.sql.CallableStatement#setTimestamp(java.lang.String, java.sql.Timestamp)
     */
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        setTimestamp(getNamedParamIndex(parameterName, false), x);
    }

    /**
     * @see java.sql.CallableStatement#setTimestamp(java.lang.String, java.sql.Timestamp, java.util.Calendar)
     */
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(getNamedParamIndex(parameterName, false), x, cal);
    }

    /**
     * @see java.sql.CallableStatement#setURL(java.lang.String, java.net.URL)
     */
    public void setURL(String parameterName, URL val) throws SQLException {
        setURL(getNamedParamIndex(parameterName, false), val);
    }

    /**
     * @see java.sql.CallableStatement#wasNull()
     */
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

    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        setAsciiStream(getNamedParamIndex(parameterName, false), x);

    }

    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        setAsciiStream(getNamedParamIndex(parameterName, false), x, length);

    }

    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        setBinaryStream(getNamedParamIndex(parameterName, false), x);

    }

    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        setBinaryStream(getNamedParamIndex(parameterName, false), x, length);

    }

    public void setBlob(String parameterName, Blob x) throws SQLException {
        setBlob(getNamedParamIndex(parameterName, false), x);

    }

    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        setBlob(getNamedParamIndex(parameterName, false), inputStream);

    }

    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        setBlob(getNamedParamIndex(parameterName, false), inputStream, length);

    }

    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        setCharacterStream(getNamedParamIndex(parameterName, false), reader);

    }

    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        setCharacterStream(getNamedParamIndex(parameterName, false), reader, length);

    }

    public void setClob(String parameterName, Clob x) throws SQLException {
        setClob(getNamedParamIndex(parameterName, false), x);

    }

    public void setClob(String parameterName, Reader reader) throws SQLException {
        setClob(getNamedParamIndex(parameterName, false), reader);

    }

    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        setClob(getNamedParamIndex(parameterName, false), reader, length);

    }

    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        setNCharacterStream(getNamedParamIndex(parameterName, false), value);

    }

    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        setNCharacterStream(getNamedParamIndex(parameterName, false), value, length);

    }

    /**
     * Check whether the stored procedure alters any data or is safe for read-only usage.
     * 
     * @return true if procedure does not alter data
     * @throws SQLException
     */
    private boolean checkReadOnlyProcedure() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.connection.getNoAccessToProcedureBodies()) {
                return false;
            }

            if (this.paramInfo.isReadOnlySafeChecked) {
                return this.paramInfo.isReadOnlySafeProcedure;
            }

            ResultSet rs = null;
            java.sql.PreparedStatement ps = null;

            try {
                String procName = extractProcedureName();

                String catalog = this.currentCatalog;

                if (procName.indexOf(".") != -1) {
                    catalog = procName.substring(0, procName.indexOf("."));

                    if (StringUtils.startsWithIgnoreCaseAndWs(catalog, "`") && catalog.trim().endsWith("`")) {
                        catalog = catalog.substring(1, catalog.length() - 1);
                    }

                    procName = procName.substring(procName.indexOf(".") + 1);
                    procName = StringUtils.toString(StringUtils.stripEnclosure(StringUtils.getBytes(procName), "`", "`"));
                }
                ps = this.connection.prepareStatement("SELECT SQL_DATA_ACCESS FROM information_schema.routines WHERE routine_schema = ? AND routine_name = ?");
                ps.setMaxRows(0);
                ps.setFetchSize(0);

                ps.setString(1, catalog);
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

    private boolean hasParametersView() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            try {
                if (this.connection.versionMeetsMinimum(5, 5, 0)) {
                    java.sql.DatabaseMetaData dbmd1 = new DatabaseMetaDataUsingInfoSchema(this.connection, this.connection.getCatalog());
                    return ((DatabaseMetaDataUsingInfoSchema) dbmd1).gethasParametersView();
                }

                return false;
            } catch (SQLException e) {
                return false;
            }
        }
    }

    /**
     * JDBC 4.2
     */
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
            throw SQLError.createSQLException("Can't call executeBatch() on CallableStatement with OUTPUT parameters", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }

        return super.executeLargeBatch();
    }
}
