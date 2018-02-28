/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.jdbc.result;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;

import com.mysql.cj.Constants;
import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.NativeSession;
import com.mysql.cj.Session;
import com.mysql.cj.WarningListener;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.ReadableProperty;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.Blob;
import com.mysql.cj.jdbc.BlobFromLocator;
import com.mysql.cj.jdbc.Clob;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.JdbcPreparedStatement;
import com.mysql.cj.jdbc.JdbcStatement;
import com.mysql.cj.jdbc.MysqlSQLXML;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.log.ProfilerEvent;
import com.mysql.cj.log.ProfilerEventHandler;
import com.mysql.cj.log.ProfilerEventHandlerFactory;
import com.mysql.cj.log.ProfilerEventImpl;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ResultsetRows;
import com.mysql.cj.protocol.a.result.NativeResultset;
import com.mysql.cj.protocol.a.result.OkPacket;
import com.mysql.cj.protocol.a.result.ResultsetRowsStatic;
import com.mysql.cj.result.BigDecimalValueFactory;
import com.mysql.cj.result.BinaryStreamValueFactory;
import com.mysql.cj.result.BooleanValueFactory;
import com.mysql.cj.result.ByteValueFactory;
import com.mysql.cj.result.DoubleValueFactory;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.FloatValueFactory;
import com.mysql.cj.result.FloatingPointBoundsEnforcer;
import com.mysql.cj.result.IntegerBoundsEnforcer;
import com.mysql.cj.result.IntegerValueFactory;
import com.mysql.cj.result.LocalDateTimeValueFactory;
import com.mysql.cj.result.LocalDateValueFactory;
import com.mysql.cj.result.LocalTimeValueFactory;
import com.mysql.cj.result.LongValueFactory;
import com.mysql.cj.result.ShortValueFactory;
import com.mysql.cj.result.SqlDateValueFactory;
import com.mysql.cj.result.SqlTimeValueFactory;
import com.mysql.cj.result.SqlTimestampValueFactory;
import com.mysql.cj.result.StringConverter;
import com.mysql.cj.result.StringValueFactory;
import com.mysql.cj.result.ValueFactory;
import com.mysql.cj.result.YearToDateValueFactory;
import com.mysql.cj.result.ZeroDateTimeToDefaultValueFactory;
import com.mysql.cj.result.ZeroDateTimeToNullValueFactory;
import com.mysql.cj.util.LogUtils;
import com.mysql.cj.util.StringUtils;

public class ResultSetImpl extends NativeResultset implements ResultSetInternalMethods, WarningListener {

    /** Counter used to generate IDs for profiling. */
    static int resultCounter = 1;

    /** The catalog that was in use when we were created */
    protected String catalog = null;

    /** Keep track of columns accessed */
    protected boolean[] columnUsed = null;

    /** The Connection instance that created us */
    protected volatile JdbcConnection connection;

    protected NativeSession session = null;

    private long connectionId = 0;

    /** The current row #, -1 == before start of result set */
    protected int currentRow = -1; // Cursor to current row;

    protected ProfilerEventHandler eventSink = null;

    Calendar fastDefaultCal = null;
    Calendar fastClientCal = null;

    /** The direction to fetch rows (always FETCH_FORWARD) */
    protected int fetchDirection = FETCH_FORWARD;

    /** The number of rows to fetch in one go... */
    protected int fetchSize = 0;

    /**
     * First character of the query that created this result set...Used to determine whether or not to parse server info messages in certain
     * circumstances.
     */
    protected char firstCharOfQuery;

    /** Has this result set been closed? */
    protected boolean isClosed = false;

    /** The statement that created us */
    private com.mysql.cj.jdbc.StatementImpl owningStatement;

    /**
     * StackTrace generated where ResultSet was created... used when profiling
     */
    private String pointOfOrigin;

    /** Are we tracking items for profileSQL? */
    protected boolean profileSQL = false;

    /** Are we read-only or updatable? */
    protected int resultSetConcurrency = 0;

    /** Are we scroll-sensitive/insensitive? */
    protected int resultSetType = 0;

    JdbcPreparedStatement statementUsedForFetchingRows;

    protected boolean useUsageAdvisor = false;

    /** The warning chain */
    protected java.sql.SQLWarning warningChain = null;

    protected java.sql.Statement wrapperStatement;

    private boolean padCharsWithSpace = false;

    private boolean useColumnNamesInFindColumn;

    private ExceptionInterceptor exceptionInterceptor;

    private ValueFactory<Boolean> booleanValueFactory;
    private ValueFactory<Byte> byteValueFactory;
    private ValueFactory<Short> shortValueFactory;
    private ValueFactory<Integer> integerValueFactory;
    private ValueFactory<Long> longValueFactory;
    private ValueFactory<Float> floatValueFactory;
    private ValueFactory<Double> doubleValueFactory;
    private ValueFactory<BigDecimal> bigDecimalValueFactory;
    private ValueFactory<InputStream> binaryStreamValueFactory;
    // temporal values include the default conn TZ, can be overridden with cal param, e.g. getDate(1, calWithOtherTZ)
    private ValueFactory<Date> defaultDateValueFactory;
    private ValueFactory<Time> defaultTimeValueFactory;
    private ValueFactory<Timestamp> defaultTimestampValueFactory;

    private ValueFactory<LocalDate> defaultLocalDateValueFactory;
    private ValueFactory<LocalDateTime> defaultLocalDateTimeValueFactory;
    private ValueFactory<LocalTime> defaultLocalTimeValueFactory;

    protected ReadableProperty<Boolean> emptyStringsConvertToZero;
    protected ReadableProperty<Boolean> emulateLocators;
    protected boolean yearIsDateType = true;
    protected PropertyDefinitions.ZeroDatetimeBehavior zeroDateTimeBehavior;

    /**
     * Create a result set for an executeUpdate statement.
     * 
     * @param ok
     * @param conn
     * @param creatorStmt
     */
    public ResultSetImpl(OkPacket ok, JdbcConnection conn, StatementImpl creatorStmt) {
        super(ok);

        this.connection = conn;
        this.owningStatement = creatorStmt;

        if (this.connection != null) {
            this.exceptionInterceptor = this.connection.getExceptionInterceptor();

            this.connectionId = this.connection.getSession().getThreadId();
            this.padCharsWithSpace = this.connection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_padCharsWithSpace).getValue();
        }
    }

    /**
     * Creates a new ResultSet object.
     * 
     * @param tuples
     *            actual row data
     * @param conn
     *            the Connection that created us.
     * @param creatorStmt
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public ResultSetImpl(ResultsetRows tuples, JdbcConnection conn, StatementImpl creatorStmt) throws SQLException {
        this.connection = conn;
        this.session = (NativeSession) conn.getSession();
        // TODO which catalog to use, from connection or from statement?
        this.catalog = creatorStmt != null ? creatorStmt.getCurrentCatalog() : conn.getCatalog();
        this.owningStatement = creatorStmt;

        if (this.connection != null) {
            this.exceptionInterceptor = this.connection.getExceptionInterceptor();
            this.connectionId = this.session.getThreadId();
            this.profileSQL = this.connection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_profileSQL).getValue();
            this.emptyStringsConvertToZero = this.connection.getPropertySet().getReadableProperty(PropertyDefinitions.PNAME_emptyStringsConvertToZero);
            this.emulateLocators = this.connection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_emulateLocators);
            this.padCharsWithSpace = this.connection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_padCharsWithSpace).getValue();
            this.yearIsDateType = this.connection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_yearIsDateType).getValue();
        }

        this.booleanValueFactory = new BooleanValueFactory();
        this.byteValueFactory = new ByteValueFactory();
        this.shortValueFactory = new ShortValueFactory();
        this.integerValueFactory = new IntegerValueFactory();
        this.longValueFactory = new LongValueFactory();
        this.floatValueFactory = new FloatValueFactory();
        this.doubleValueFactory = new DoubleValueFactory();
        this.bigDecimalValueFactory = new BigDecimalValueFactory();
        this.binaryStreamValueFactory = new BinaryStreamValueFactory();

        this.zeroDateTimeBehavior = this.connection
                .getPropertySet().<PropertyDefinitions.ZeroDatetimeBehavior> getEnumReadableProperty(PropertyDefinitions.PNAME_zeroDateTimeBehavior).getValue();
        this.defaultDateValueFactory = decorateDateTimeValueFactory(new SqlDateValueFactory(this.session.getServerSession().getDefaultTimeZone(), this),
                this.zeroDateTimeBehavior);
        this.defaultTimeValueFactory = decorateDateTimeValueFactory(new SqlTimeValueFactory(this.session.getServerSession().getDefaultTimeZone(), this),
                this.zeroDateTimeBehavior);
        this.defaultTimestampValueFactory = decorateDateTimeValueFactory(new SqlTimestampValueFactory(this.session.getServerSession().getDefaultTimeZone()),
                this.zeroDateTimeBehavior);

        this.defaultLocalDateValueFactory = decorateDateTimeValueFactory(new LocalDateValueFactory(this), this.zeroDateTimeBehavior);
        this.defaultLocalTimeValueFactory = decorateDateTimeValueFactory(new LocalTimeValueFactory(this), this.zeroDateTimeBehavior);
        this.defaultLocalDateTimeValueFactory = decorateDateTimeValueFactory(new LocalDateTimeValueFactory(), this.zeroDateTimeBehavior);

        // TODO we always check initial value here (was cached in jdbcCompliantTruncationForReads variable), whatever the setupServerForTruncationChecks() does for writes. It also means that runtime changes of this variable have no effect on reads.
        if (this.connection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_jdbcCompliantTruncation).getInitialValue()) {
            this.byteValueFactory = new IntegerBoundsEnforcer<>(this.byteValueFactory, Byte.MIN_VALUE, Byte.MAX_VALUE);
            this.shortValueFactory = new IntegerBoundsEnforcer<>(this.shortValueFactory, Short.MIN_VALUE, Short.MAX_VALUE);
            this.integerValueFactory = new IntegerBoundsEnforcer<>(this.integerValueFactory, Integer.MIN_VALUE, Integer.MAX_VALUE);
            this.longValueFactory = new IntegerBoundsEnforcer<>(this.longValueFactory, Long.MIN_VALUE, Long.MAX_VALUE);

            this.floatValueFactory = new FloatingPointBoundsEnforcer<>(this.floatValueFactory, -Float.MAX_VALUE, Float.MAX_VALUE);
            this.doubleValueFactory = new FloatingPointBoundsEnforcer<>(this.doubleValueFactory, -Double.MAX_VALUE, Double.MAX_VALUE);
        }

        this.columnDefinition = tuples.getMetadata();
        this.rowData = tuples;
        this.updateCount = this.rowData.size();

        // Check for no results
        if (this.rowData.size() > 0) {
            if (this.updateCount == 1) {
                if (this.thisRow == null) {
                    this.rowData.close(); // empty result set
                    this.updateCount = -1;
                }
            }
        } else {
            this.thisRow = null;
        }

        this.rowData.setOwner(this);

        if (this.columnDefinition.getFields() != null) {
            initializeWithMetadata();
        } // else called by Connection.initializeResultsMetadataFromCache() when cached

        this.useColumnNamesInFindColumn = this.connection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useColumnNamesInFindColumn)
                .getValue();

        setRowPositionValidity();
    }

    @Override
    public void initializeWithMetadata() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            initRowsWithMetadata();

            if (this.profileSQL || this.connection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useUsageAdvisor).getValue()) {
                this.columnUsed = new boolean[this.columnDefinition.getFields().length];
                this.pointOfOrigin = LogUtils.findCallingClassAndMethod(new Throwable());
                this.resultId = resultCounter++;
                this.useUsageAdvisor = this.connection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useUsageAdvisor).getValue();
                this.eventSink = ProfilerEventHandlerFactory.getInstance(this.session);
            }

            if (this.connection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_gatherPerfMetrics).getValue()) {
                this.session.incrementNumberOfResultSetsCreated();

                Set<String> tableNamesSet = new HashSet<>();

                for (int i = 0; i < this.columnDefinition.getFields().length; i++) {
                    Field f = this.columnDefinition.getFields()[i];

                    String tableName = f.getOriginalTableName();

                    if (tableName == null) {
                        tableName = f.getTableName();
                    }

                    if (tableName != null) {
                        if (this.connection.lowerCaseTableNames()) {
                            tableName = tableName.toLowerCase(); // on windows, table
                            // names are not case-sens.
                        }

                        tableNamesSet.add(tableName);
                    }
                }

                this.session.reportNumberOfTablesAccessed(tableNamesSet.size());
            }
        }
    }

    public boolean absolute(int row) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            boolean b;

            if (this.rowData.size() == 0) {
                b = false;
            } else {
                if (row == 0) {
                    beforeFirst();
                    b = false;
                } else if (row == 1) {
                    b = first();
                } else if (row == -1) {
                    b = last();
                } else if (row > this.rowData.size()) {
                    afterLast();
                    b = false;
                } else {
                    if (row < 0) {
                        // adjust to reflect after end of result set
                        int newRowPosition = this.rowData.size() + row + 1;

                        if (newRowPosition <= 0) {
                            beforeFirst();
                            b = false;
                        } else {
                            b = absolute(newRowPosition);
                        }
                    } else {
                        row--; // adjust for index difference
                        this.rowData.setCurrentRow(row);
                        this.thisRow = this.rowData.get(row);
                        b = true;
                    }
                }
            }

            setRowPositionValidity();

            return b;
        }
    }

    public void afterLast() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (this.rowData.size() != 0) {
                this.rowData.afterLast();
                this.thisRow = null;
            }

            setRowPositionValidity();
        }
    }

    public void beforeFirst() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (this.rowData.size() == 0) {
                return;
            }

            this.rowData.beforeFirst();
            this.thisRow = null;

            setRowPositionValidity();
        }
    }

    public void cancelRowUpdates() throws SQLException {
        throw SQLError.notUpdatable();
    }

    /**
     * Ensures that the result set is not closed
     * 
     * @throws SQLException
     *             if the result set is closed
     */
    protected final JdbcConnection checkClosed() throws SQLException {
        JdbcConnection c = this.connection;

        if (c == null) {
            throw SQLError.createSQLException(Messages.getString("ResultSet.Operation_not_allowed_after_ResultSet_closed_144"),
                    MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
        }

        return c;
    }

    /**
     * Checks if columnIndex is within the number of columns in this result set.
     * 
     * @param columnIndex
     *            the index to check
     * 
     * @throws SQLException
     *             if the index is out of bounds
     */
    protected final void checkColumnBounds(int columnIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if ((columnIndex < 1)) {
                throw SQLError.createSQLException(
                        Messages.getString("ResultSet.Column_Index_out_of_range_low",
                                new Object[] { Integer.valueOf(columnIndex), Integer.valueOf(this.columnDefinition.getFields().length) }),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            } else if ((columnIndex > this.columnDefinition.getFields().length)) {
                throw SQLError.createSQLException(
                        Messages.getString("ResultSet.Column_Index_out_of_range_high",
                                new Object[] { Integer.valueOf(columnIndex), Integer.valueOf(this.columnDefinition.getFields().length) }),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            if (this.profileSQL || this.useUsageAdvisor) {
                this.columnUsed[columnIndex - 1] = true;
            }
        }
    }

    /**
     * Ensures that the cursor is positioned on a valid row and that the result
     * set is not closed
     * 
     * @throws SQLException
     *             if the result set is not in a valid state for traversal
     */
    protected void checkRowPos() throws SQLException {
        checkClosed();

        if (!this.onValidRow) {
            throw SQLError.createSQLException(this.invalidRowReason, MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
        }
    }

    private boolean onValidRow = false;
    private String invalidRowReason = null;

    private void setRowPositionValidity() throws SQLException {
        if (!this.rowData.isDynamic() && (this.rowData.size() == 0)) {
            this.invalidRowReason = Messages.getString("ResultSet.Illegal_operation_on_empty_result_set");
            this.onValidRow = false;
        } else if (this.rowData.isBeforeFirst()) {
            this.invalidRowReason = Messages.getString("ResultSet.Before_start_of_result_set_146");
            this.onValidRow = false;
        } else if (this.rowData.isAfterLast()) {
            this.invalidRowReason = Messages.getString("ResultSet.After_end_of_result_set_148");
            this.onValidRow = false;
        } else {
            this.onValidRow = true;
            this.invalidRowReason = null;
        }
    }

    /**
     * After this call, getWarnings returns null until a new warning is reported
     * for this ResultSet
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public void clearWarnings() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            this.warningChain = null;
        }
    }

    public void close() throws SQLException {
        realClose(true);
    }

    public void populateCachedMetaData(CachedResultSetMetaData cachedMetaData) throws SQLException {
        this.columnDefinition.exportTo(cachedMetaData);
        cachedMetaData.setMetadata(getMetaData());
    }

    public void deleteRow() throws SQLException {
        throw SQLError.notUpdatable();
    }

    /*
     * /**
     * TODO: Required by JDBC spec
     */
    /*
     * protected void finalize() throws Throwable {
     * if (!this.isClosed) {
     * realClose(false);
     * }
     * }
     */

    public int findColumn(String columnName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            Integer index = this.columnDefinition.findColumn(columnName, this.useColumnNamesInFindColumn, 1);

            if (index == -1) {
                throw SQLError.createSQLException(
                        Messages.getString("ResultSet.Column____112") + columnName + Messages.getString("ResultSet.___not_found._113"),
                        MysqlErrorNumbers.SQL_STATE_COLUMN_NOT_FOUND, getExceptionInterceptor());
            }

            return index;
        }
    }

    public boolean first() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            boolean b = true;

            if (this.rowData.isEmpty()) {
                b = false;
            } else {
                this.rowData.beforeFirst();
                this.thisRow = this.rowData.next();
            }

            setRowPositionValidity();

            return b;
        }
    }

    /**
     * Decorate a date/time value factory to implement zeroDateTimeBehavior.
     */
    private static <T> ValueFactory<T> decorateDateTimeValueFactory(ValueFactory<T> vf, PropertyDefinitions.ZeroDatetimeBehavior zeroDateTimeBehavior) {
        // enforce zero date/time behavior
        switch (zeroDateTimeBehavior) {
            case CONVERT_TO_NULL:
                return new ZeroDateTimeToNullValueFactory<>(vf);
            case ROUND:
                return new ZeroDateTimeToDefaultValueFactory<>(vf);
            case EXCEPTION:
            default:
                return vf;
        }
    }

    /**
     * Get a non-string value from a row. All requests to obtain non-string values should use this method. This method implements the "indirect" conversion of
     * values that are returned as strings from the server. This is an expensive conversion which first requires interpreting the value as a string in it's
     * given character set and converting it to an ASCII string which can then be parsed as a numeric/date value.
     */
    private <T> T getNonStringValueFromRow(int columnIndex, ValueFactory<T> vf) throws SQLException {
        Field f = this.columnDefinition.getFields()[columnIndex - 1];

        // interpret the string as necessary to create the a value of the requested type
        String encoding = f.getEncoding();
        StringConverter<T> stringConverter = new StringConverter<>(encoding, vf);
        stringConverter.setEventSink(this.eventSink);
        stringConverter.setEmptyStringsConvertToZero(this.emptyStringsConvertToZero.getValue());
        return this.thisRow.getValue(columnIndex - 1, stringConverter);
    }

    /**
     * Get a Date of Timestamp value from a row. This implements the "yearIsDateType=true" behavior.
     */
    private <T> T getDateOrTimestampValueFromRow(int columnIndex, ValueFactory<T> vf) throws SQLException {
        Field f = this.columnDefinition.getFields()[columnIndex - 1];

        // return YEAR values as Dates if necessary
        if (f.getMysqlTypeId() == MysqlType.FIELD_TYPE_YEAR && this.yearIsDateType) {
            return getNonStringValueFromRow(columnIndex, new YearToDateValueFactory<>(vf));
        }
        return getNonStringValueFromRow(columnIndex, new YearToDateValueFactory<>(vf));
    }

    public Array getArray(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    public Array getArray(String colName) throws SQLException {
        return getArray(findColumn(colName));
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return getBinaryStream(columnIndex);
    }

    public InputStream getAsciiStream(String columnName) throws SQLException {
        return getAsciiStream(findColumn(columnName));
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return getNonStringValueFromRow(columnIndex, this.bigDecimalValueFactory);
    }

    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        ValueFactory<BigDecimal> vf = new BigDecimalValueFactory(scale);
        return getNonStringValueFromRow(columnIndex, vf);
    }

    public BigDecimal getBigDecimal(String columnName) throws SQLException {
        return getBigDecimal(findColumn(columnName));
    }

    @Deprecated
    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnName), scale);
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return this.thisRow.getValue(columnIndex - 1, this.binaryStreamValueFactory);
    }

    public InputStream getBinaryStream(String columnName) throws SQLException {
        return getBinaryStream(findColumn(columnName));
    }

    public java.sql.Blob getBlob(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);

        if (this.thisRow.getNull(columnIndex - 1)) {
            return null;
        }

        if (!this.emulateLocators.getValue()) {
            return new Blob(this.thisRow.getBytes(columnIndex - 1), getExceptionInterceptor());
        }

        return new BlobFromLocator(this, columnIndex, getExceptionInterceptor());
    }

    public java.sql.Blob getBlob(String colName) throws SQLException {
        return getBlob(findColumn(colName));
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return getNonStringValueFromRow(columnIndex, this.booleanValueFactory);
    }

    public boolean getBoolean(String columnName) throws SQLException {
        return getBoolean(findColumn(columnName));
    }

    public byte getByte(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return getNonStringValueFromRow(columnIndex, this.byteValueFactory);
    }

    public byte getByte(String columnName) throws SQLException {
        return getByte(findColumn(columnName));
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return this.thisRow.getBytes(columnIndex - 1);
    }

    public byte[] getBytes(String columnName) throws SQLException {
        return getBytes(findColumn(columnName));
    }

    public java.io.Reader getCharacterStream(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        InputStream stream = getBinaryStream(columnIndex);

        if (stream == null) {
            return null;
        }

        Field f = this.columnDefinition.getFields()[columnIndex - 1];
        try {
            return new InputStreamReader(stream, f.getEncoding());
        } catch (UnsupportedEncodingException e) {
            SQLException sqlEx = SQLError.createSQLException("Cannot read value with encoding: " + f.getEncoding(), this.exceptionInterceptor);
            sqlEx.initCause(e);
            throw sqlEx;
        }
    }

    public java.io.Reader getCharacterStream(String columnName) throws SQLException {
        return getCharacterStream(findColumn(columnName));
    }

    public java.sql.Clob getClob(int columnIndex) throws SQLException {
        String asString = getStringForClob(columnIndex);

        if (asString == null) {
            return null;
        }

        return new com.mysql.cj.jdbc.Clob(asString, getExceptionInterceptor());
    }

    public java.sql.Clob getClob(String colName) throws SQLException {
        return getClob(findColumn(colName));
    }

    public Date getDate(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return getDateOrTimestampValueFromRow(columnIndex, this.defaultDateValueFactory);
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        ValueFactory<Date> vf = new SqlDateValueFactory(cal != null ? cal.getTimeZone() : this.session.getServerSession().getDefaultTimeZone(), this);
        return getDateOrTimestampValueFromRow(columnIndex, decorateDateTimeValueFactory(vf, this.zeroDateTimeBehavior));
    }

    public Date getDate(String columnName) throws SQLException {
        return getDate(findColumn(columnName));
    }

    public Date getDate(String columnName, Calendar cal) throws SQLException {
        return getDate(findColumn(columnName), cal);
    }

    public double getDouble(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return getNonStringValueFromRow(columnIndex, this.doubleValueFactory);
    }

    public double getDouble(String columnName) throws SQLException {
        return getDouble(findColumn(columnName));
    }

    public float getFloat(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return getNonStringValueFromRow(columnIndex, this.floatValueFactory);
    }

    public float getFloat(String columnName) throws SQLException {
        return getFloat(findColumn(columnName));
    }

    public int getInt(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return getNonStringValueFromRow(columnIndex, this.integerValueFactory);
    }

    public BigInteger getBigInteger(int columnIndex) throws SQLException {
        String stringVal = getString(columnIndex);
        if (stringVal == null) {
            return null;
        }
        try {
            return new BigInteger(stringVal);
        } catch (NumberFormatException nfe) {
            throw SQLError.createSQLException(
                    Messages.getString("ResultSet.Bad_format_for_BigInteger", new Object[] { Integer.valueOf(columnIndex), stringVal }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }
    }

    public int getInt(String columnName) throws SQLException {
        return getInt(findColumn(columnName));
    }

    public long getLong(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return getNonStringValueFromRow(columnIndex, this.longValueFactory);
    }

    public long getLong(String columnName) throws SQLException {
        return getLong(findColumn(columnName));
    }

    public short getShort(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return getNonStringValueFromRow(columnIndex, this.shortValueFactory);
    }

    public short getShort(String columnName) throws SQLException {
        return getShort(findColumn(columnName));
    }

    public String getString(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);

        Field f = this.columnDefinition.getFields()[columnIndex - 1];
        ValueFactory<String> vf = new StringValueFactory(f.getEncoding());
        // return YEAR values as Dates if necessary
        if (f.getMysqlTypeId() == MysqlType.FIELD_TYPE_YEAR && this.yearIsDateType) {
            vf = new YearToDateValueFactory<>(vf);
        }
        String stringVal = this.thisRow.getValue(columnIndex - 1, vf);

        if (this.padCharsWithSpace && stringVal != null && f.getMysqlTypeId() == MysqlType.FIELD_TYPE_STRING) {
            int maxBytesPerChar = this.session.getServerSession().getMaxBytesPerChar(f.getCollationIndex(), f.getEncoding());
            int fieldLength = (int) f.getLength() /* safe, bytes in a CHAR <= 1024 */ / maxBytesPerChar; /* safe, this will never be 0 */
            return StringUtils.padString(stringVal, fieldLength);
        }

        return stringVal;
    }

    public String getString(String columnName) throws SQLException {
        return getString(findColumn(columnName));
    }

    private String getStringForClob(int columnIndex) throws SQLException {
        String asString = null;

        String forcedEncoding = this.connection.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_clobCharacterEncoding).getStringValue();

        if (forcedEncoding == null) {
            asString = getString(columnIndex);
        } else {
            byte[] asBytes = null;

            asBytes = getBytes(columnIndex);

            if (asBytes != null) {
                asString = StringUtils.toString(asBytes, forcedEncoding);
            }
        }

        return asString;
    }

    public Time getTime(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return getNonStringValueFromRow(columnIndex, this.defaultTimeValueFactory);
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        ValueFactory<Time> vf = new SqlTimeValueFactory(cal != null ? cal.getTimeZone() : this.session.getServerSession().getDefaultTimeZone());
        return getNonStringValueFromRow(columnIndex, decorateDateTimeValueFactory(vf, this.zeroDateTimeBehavior));
    }

    public Time getTime(String columnName) throws SQLException {
        return getTime(findColumn(columnName));
    }

    public Time getTime(String columnName, Calendar cal) throws SQLException {
        return getTime(findColumn(columnName), cal);
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return getDateOrTimestampValueFromRow(columnIndex, this.defaultTimestampValueFactory);
    }

    public LocalDate getLocalDate(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return getDateOrTimestampValueFromRow(columnIndex, this.defaultLocalDateValueFactory);
    }

    public LocalDateTime getLocalDateTime(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return getDateOrTimestampValueFromRow(columnIndex, this.defaultLocalDateTimeValueFactory);
    }

    public LocalTime getLocalTime(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return getNonStringValueFromRow(columnIndex, this.defaultLocalTimeValueFactory);
    }

    /*
     * This method is optimized by saving the configuration for the last-used cal/tz. If it's re-used, we don't need to create a new value factory (and thus
     * calendar, etc) instance
     */
    private TimeZone lastTsCustomTz;
    private ValueFactory<Timestamp> customTsVf;

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);

        TimeZone tz = cal != null ? cal.getTimeZone() : this.session.getServerSession().getDefaultTimeZone();
        if (this.customTsVf != null && tz == this.lastTsCustomTz) {
            return getDateOrTimestampValueFromRow(columnIndex, this.customTsVf);
        }
        ValueFactory<Timestamp> vf = decorateDateTimeValueFactory(new SqlTimestampValueFactory(tz), this.zeroDateTimeBehavior);
        this.lastTsCustomTz = tz;
        this.customTsVf = vf;
        return getDateOrTimestampValueFromRow(columnIndex, vf);
    }

    public Timestamp getTimestamp(String columnName) throws SQLException {
        return getTimestamp(findColumn(columnName));
    }

    public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnName), cal);
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);

        String fieldEncoding = this.columnDefinition.getFields()[columnIndex - 1].getEncoding();
        if (fieldEncoding == null || !fieldEncoding.equals("UTF-8")) {
            throw new SQLException("Can not call getNCharacterStream() when field's charset isn't UTF-8");
        }
        return getCharacterStream(columnIndex);
    }

    public Reader getNCharacterStream(String columnName) throws SQLException {
        return getNCharacterStream(findColumn(columnName));
    }

    public NClob getNClob(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);

        String fieldEncoding = this.columnDefinition.getFields()[columnIndex - 1].getEncoding();
        if (fieldEncoding == null || !fieldEncoding.equals("UTF-8")) {
            throw new SQLException("Can not call getNClob() when field's charset isn't UTF-8");
        }

        String asString = getStringForNClob(columnIndex);

        if (asString == null) {
            return null;
        }

        return new com.mysql.cj.jdbc.NClob(asString, getExceptionInterceptor());
    }

    public NClob getNClob(String columnName) throws SQLException {
        return getNClob(findColumn(columnName));
    }

    private String getStringForNClob(int columnIndex) throws SQLException {
        String asString = null;

        String forcedEncoding = "UTF-8";

        try {
            byte[] asBytes = getBytes(columnIndex);

            if (asBytes != null) {
                asString = new String(asBytes, forcedEncoding);
            }
        } catch (UnsupportedEncodingException uee) {
            throw SQLError.createSQLException("Unsupported character encoding " + forcedEncoding, MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }

        return asString;
    }

    public String getNString(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);

        String fieldEncoding = this.columnDefinition.getFields()[columnIndex - 1].getEncoding();
        if (fieldEncoding == null || !fieldEncoding.equals("UTF-8")) {
            throw new SQLException("Can not call getNString() when field's charset isn't UTF-8");
        }
        return getString(columnIndex);
    }

    public String getNString(String columnName) throws SQLException {
        return getNString(findColumn(columnName));
    }

    public int getConcurrency() throws SQLException {
        return (CONCUR_READ_ONLY);
    }

    public String getCursorName() throws SQLException {
        throw SQLError.createSQLException(Messages.getString("ResultSet.Positioned_Update_not_supported"), MysqlErrorNumbers.SQL_STATE_DRIVER_NOT_CAPABLE,
                getExceptionInterceptor());
    }

    public int getFetchDirection() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return this.fetchDirection;
        }
    }

    public int getFetchSize() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return this.fetchSize;
        }
    }

    /**
     * Returns the first character of the query that this result set was created
     * from.
     * 
     * @return the first character of the query...uppercased
     */
    public char getFirstCharOfQuery() {
        try {
            synchronized (checkClosed().getConnectionMutex()) {
                return this.firstCharOfQuery;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e); // FIXME: Need to evolve interface
        }
    }

    public java.sql.ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();

        return new ResultSetMetaData(this.session, this.columnDefinition.getFields(),
                this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useOldAliasMetadataBehavior).getValue(), this.yearIsDateType,
                getExceptionInterceptor());
    }

    public Object getObject(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);

        int columnIndexMinusOne = columnIndex - 1;

        // we can't completely rely on code below because primitives have default values for null (e.g. int->0)
        if (this.thisRow.getNull(columnIndexMinusOne)) {
            return null;
        }

        Field field = this.columnDefinition.getFields()[columnIndexMinusOne];
        switch (field.getMysqlType()) {
            case BIT:
                // TODO Field sets binary and blob flags if the length of BIT field is > 1; is it needed at all?
                if (field.isBinary() || field.isBlob()) {
                    byte[] data = getBytes(columnIndex);

                    if (this.connection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_autoDeserialize).getValue()) {
                        Object obj = data;

                        if ((data != null) && (data.length >= 2)) {
                            if ((data[0] == -84) && (data[1] == -19)) {
                                // Serialized object?
                                try {
                                    ByteArrayInputStream bytesIn = new ByteArrayInputStream(data);
                                    ObjectInputStream objIn = new ObjectInputStream(bytesIn);
                                    obj = objIn.readObject();
                                    objIn.close();
                                    bytesIn.close();
                                } catch (ClassNotFoundException cnfe) {
                                    throw SQLError.createSQLException(Messages.getString("ResultSet.Class_not_found___91") + cnfe.toString()
                                            + Messages.getString("ResultSet._while_reading_serialized_object_92"), getExceptionInterceptor());
                                } catch (IOException ex) {
                                    obj = data; // not serialized?
                                }
                            } else {
                                return getString(columnIndex);
                            }
                        }

                        return obj;
                    }

                    return data;
                }

                return field.isSingleBit() ? Boolean.valueOf(getBoolean(columnIndex)) : getBytes(columnIndex);

            case BOOLEAN:
                return Boolean.valueOf(getBoolean(columnIndex));

            case TINYINT:
                return Integer.valueOf(getByte(columnIndex));

            case TINYINT_UNSIGNED:
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case INT:
                return Integer.valueOf(getInt(columnIndex));

            case INT_UNSIGNED:
            case BIGINT:
                return Long.valueOf(getLong(columnIndex));

            case BIGINT_UNSIGNED:
                return getBigInteger(columnIndex);

            case DECIMAL:
            case DECIMAL_UNSIGNED:
                String stringVal = getString(columnIndex);

                if (stringVal != null) {
                    if (stringVal.length() == 0) {
                        return new BigDecimal(0);
                    }

                    try {
                        return new BigDecimal(stringVal);
                    } catch (NumberFormatException ex) {
                        throw SQLError.createSQLException(
                                Messages.getString("ResultSet.Bad_format_for_BigDecimal", new Object[] { stringVal, Integer.valueOf(columnIndex) }),
                                MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
                    }
                }
                return null;

            case FLOAT:
            case FLOAT_UNSIGNED:
                return new Float(getFloat(columnIndex));

            case DOUBLE:
            case DOUBLE_UNSIGNED:
                return new Double(getDouble(columnIndex));

            case CHAR:
            case ENUM:
            case SET:
            case VARCHAR:
            case TINYTEXT:
                return getString(columnIndex);

            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
                return getStringForClob(columnIndex);

            case GEOMETRY:
                return getBytes(columnIndex);

            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
            case BLOB:
                if (field.isBinary() || field.isBlob()) {
                    byte[] data = getBytes(columnIndex);

                    if (this.connection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_autoDeserialize).getValue()) {
                        Object obj = data;

                        if ((data != null) && (data.length >= 2)) {
                            if ((data[0] == -84) && (data[1] == -19)) {
                                // Serialized object?
                                try {
                                    ByteArrayInputStream bytesIn = new ByteArrayInputStream(data);
                                    ObjectInputStream objIn = new ObjectInputStream(bytesIn);
                                    obj = objIn.readObject();
                                    objIn.close();
                                    bytesIn.close();
                                } catch (ClassNotFoundException cnfe) {
                                    throw SQLError.createSQLException(Messages.getString("ResultSet.Class_not_found___91") + cnfe.toString()
                                            + Messages.getString("ResultSet._while_reading_serialized_object_92"), getExceptionInterceptor());
                                } catch (IOException ex) {
                                    obj = data; // not serialized?
                                }
                            } else {
                                return getString(columnIndex);
                            }
                        }

                        return obj;
                    }

                    return data;
                }

                return getBytes(columnIndex);

            case YEAR:
                return this.yearIsDateType ? getDate(columnIndex) : Short.valueOf(getShort(columnIndex));

            case DATE:
                return getDate(columnIndex);

            case TIME:
                return getTime(columnIndex);

            case TIMESTAMP:
            case DATETIME:
                return getTimestamp(columnIndex);

            default:
                return getString(columnIndex);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        if (type == null) {
            throw SQLError.createSQLException("Type parameter can not be null", MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        synchronized (checkClosed().getConnectionMutex()) {
            if (type.equals(String.class)) {
                return (T) getString(columnIndex);

            } else if (type.equals(BigDecimal.class)) {
                return (T) getBigDecimal(columnIndex);

            } else if (type.equals(BigInteger.class)) {
                return (T) getBigInteger(columnIndex);

            } else if (type.equals(Boolean.class) || type.equals(Boolean.TYPE)) {
                return (T) Boolean.valueOf(getBoolean(columnIndex));

            } else if (type.equals(Integer.class) || type.equals(Integer.TYPE)) {
                return (T) Integer.valueOf(getInt(columnIndex));

            } else if (type.equals(Long.class) || type.equals(Long.TYPE)) {
                return (T) Long.valueOf(getLong(columnIndex));

            } else if (type.equals(Float.class) || type.equals(Float.TYPE)) {
                return (T) Float.valueOf(getFloat(columnIndex));

            } else if (type.equals(Double.class) || type.equals(Double.TYPE)) {
                return (T) Double.valueOf(getDouble(columnIndex));

            } else if (type.equals(byte[].class)) {
                return (T) getBytes(columnIndex);

            } else if (type.equals(Date.class)) {
                return (T) getDate(columnIndex);

            } else if (type.equals(Time.class)) {
                return (T) getTime(columnIndex);

            } else if (type.equals(Timestamp.class)) {
                return (T) getTimestamp(columnIndex);

            } else if (type.equals(Clob.class)) {
                return (T) getClob(columnIndex);

            } else if (type.equals(Blob.class)) {
                return (T) getBlob(columnIndex);

            } else if (type.equals(Array.class)) {
                return (T) getArray(columnIndex);

            } else if (type.equals(Ref.class)) {
                return (T) getRef(columnIndex);

            } else if (type.equals(URL.class)) {
                return (T) getURL(columnIndex);

            } else if (type.equals(Struct.class)) {
                throw new SQLFeatureNotSupportedException();

            } else if (type.equals(RowId.class)) {
                return (T) getRowId(columnIndex);

            } else if (type.equals(NClob.class)) {
                return (T) getNClob(columnIndex);

            } else if (type.equals(SQLXML.class)) {
                return (T) getSQLXML(columnIndex);

            } else if (type.equals(LocalDate.class)) {
                return (T) getLocalDate(columnIndex);

            } else if (type.equals(LocalDateTime.class)) {
                return (T) getLocalDateTime(columnIndex);

            } else if (type.equals(LocalTime.class)) {
                return (T) getLocalTime(columnIndex);

            } else if (type.equals(OffsetDateTime.class)) {
                try {
                    String odt = getString(columnIndex);
                    return odt == null ? null : (T) OffsetDateTime.parse(odt);
                } catch (DateTimeParseException e) {
                    // Let it continue and try by object deserialization.
                }

            } else if (type.equals(OffsetTime.class)) {
                try {
                    String ot = getString(columnIndex);
                    return ot == null ? null : (T) OffsetTime.parse(getString(columnIndex));
                } catch (DateTimeParseException e) {
                    // Let it continue and try by object deserialization.
                }

            }

            if (this.connection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_autoDeserialize).getValue()) {
                try {
                    return (T) getObject(columnIndex);
                } catch (ClassCastException cce) {
                    SQLException sqlEx = SQLError.createSQLException("Conversion not supported for type " + type.getName(),
                            MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
                    sqlEx.initCause(cce);

                    throw sqlEx;
                }
            }

            throw SQLError.createSQLException("Conversion not supported for type " + type.getName(), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    public Object getObject(int i, java.util.Map<String, Class<?>> map) throws SQLException {
        return getObject(i);
    }

    public Object getObject(String columnName) throws SQLException {
        return getObject(findColumn(columnName));
    }

    public Object getObject(String colName, java.util.Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(colName), map);
    }

    public Object getObjectStoredProc(int columnIndex, int desiredSqlType) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);

        Object value = this.thisRow.getBytes(columnIndex - 1);

        if (value == null) {
            return null;
        }

        Field field = this.columnDefinition.getFields()[columnIndex - 1];

        MysqlType desiredMysqlType = MysqlType.getByJdbcType(desiredSqlType);

        switch (desiredMysqlType) {
            case BIT:
            case BOOLEAN:
                return Boolean.valueOf(getBoolean(columnIndex));

            case TINYINT:
            case TINYINT_UNSIGNED:
                return Integer.valueOf(getInt(columnIndex));

            case SMALLINT:
            case SMALLINT_UNSIGNED:
                return Integer.valueOf(getInt(columnIndex));

            case INT:
            case INT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
                if (!field.isUnsigned() || field.getMysqlTypeId() == MysqlType.FIELD_TYPE_INT24) {
                    return Integer.valueOf(getInt(columnIndex));
                }
                return Long.valueOf(getLong(columnIndex));

            case BIGINT:
                return Long.valueOf(getLong(columnIndex));

            case BIGINT_UNSIGNED:
                return getBigInteger(columnIndex);

            case DECIMAL:
            case DECIMAL_UNSIGNED:
                String stringVal = getString(columnIndex);
                BigDecimal val;

                if (stringVal != null) {
                    if (stringVal.length() == 0) {
                        val = new BigDecimal(0);

                        return val;
                    }

                    try {
                        val = new BigDecimal(stringVal);
                    } catch (NumberFormatException ex) {
                        throw SQLError.createSQLException(
                                Messages.getString("ResultSet.Bad_format_for_BigDecimal", new Object[] { stringVal, Integer.valueOf(columnIndex) }),
                                MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
                    }

                    return val;
                }

                return null;

            case FLOAT:
            case FLOAT_UNSIGNED:
                return new Float(getFloat(columnIndex));

            case DOUBLE:
            case DOUBLE_UNSIGNED:
                return new Double(getDouble(columnIndex));

            case CHAR:
            case ENUM:
            case SET:
            case VARCHAR:
            case TINYTEXT:
                return getString(columnIndex);

            case JSON:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                return getStringForClob(columnIndex);

            case BINARY:
            case GEOMETRY:
            case VARBINARY:
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                return getBytes(columnIndex);

            case YEAR:
            case DATE:
                if (field.getMysqlType() == MysqlType.YEAR && !this.yearIsDateType) {
                    return Short.valueOf(getShort(columnIndex));
                }

                return getDate(columnIndex);

            case TIME:
                return getTime(columnIndex);

            case TIMESTAMP:
                return getTimestamp(columnIndex);

            default:
                return getString(columnIndex);
        }
    }

    public Object getObjectStoredProc(int i, java.util.Map<Object, Object> map, int desiredSqlType) throws SQLException {
        return getObjectStoredProc(i, desiredSqlType);
    }

    public Object getObjectStoredProc(String columnName, int desiredSqlType) throws SQLException {
        return getObjectStoredProc(findColumn(columnName), desiredSqlType);
    }

    public Object getObjectStoredProc(String colName, java.util.Map<Object, Object> map, int desiredSqlType) throws SQLException {
        return getObjectStoredProc(findColumn(colName), map, desiredSqlType);
    }

    public java.sql.Ref getRef(int i) throws SQLException {
        checkColumnBounds(i);
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    public java.sql.Ref getRef(String colName) throws SQLException {
        return getRef(findColumn(colName));
    }

    public int getRow() throws SQLException {
        checkClosed();

        int currentRowNumber = this.rowData.getPosition();
        int row = 0;

        // Non-dynamic result sets can be interrogated for this information
        if (!this.rowData.isDynamic()) {
            if ((currentRowNumber < 0) || this.rowData.isAfterLast() || this.rowData.isEmpty()) {
                row = 0;
            } else {
                row = currentRowNumber + 1;
            }
        } else {
            // dynamic (streaming) can not
            row = currentRowNumber + 1;
        }

        return row;
    }

    public java.sql.Statement getStatement() throws SQLException {
        try {
            synchronized (checkClosed().getConnectionMutex()) {
                if (this.wrapperStatement != null) {
                    return this.wrapperStatement;
                }

                return this.owningStatement;
            }

        } catch (SQLException sqlEx) {
            throw SQLError.createSQLException("Operation not allowed on closed ResultSet.", MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                    getExceptionInterceptor());
        }

    }

    public int getType() throws SQLException {
        return this.resultSetType;
    }

    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        checkRowPos();

        return getBinaryStream(columnIndex);
    }

    @Deprecated
    public InputStream getUnicodeStream(String columnName) throws SQLException {
        return getUnicodeStream(findColumn(columnName));
    }

    public URL getURL(int colIndex) throws SQLException {
        String val = getString(colIndex);

        if (val == null) {
            return null;
        }

        try {
            return new URL(val);
        } catch (MalformedURLException mfe) {
            throw SQLError.createSQLException(Messages.getString("ResultSet.Malformed_URL____104") + val + "'", MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }
    }

    public URL getURL(String colName) throws SQLException {
        String val = getString(colName);

        if (val == null) {
            return null;
        }

        try {
            return new URL(val);
        } catch (MalformedURLException mfe) {
            throw SQLError.createSQLException(Messages.getString("ResultSet.Malformed_URL____107") + val + "'", MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }
    }

    public java.sql.SQLWarning getWarnings() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return this.warningChain;
        }
    }

    public void insertRow() throws SQLException {
        throw SQLError.notUpdatable();
    }

    public boolean isAfterLast() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            boolean b = this.rowData.isAfterLast();

            return b;
        }
    }

    public boolean isBeforeFirst() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return this.rowData.isBeforeFirst();
        }
    }

    public boolean isFirst() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return this.rowData.isFirst();
        }
    }

    public boolean isLast() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return this.rowData.isLast();
        }
    }

    public boolean last() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            boolean b = true;

            if (this.rowData.size() == 0) {
                b = false;
            } else {
                this.rowData.beforeLast();
                this.thisRow = this.rowData.next();
            }

            setRowPositionValidity();

            return b;
        }
    }

    public void moveToCurrentRow() throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void moveToInsertRow() throws SQLException {
        throw SQLError.notUpdatable();
    }

    public boolean next() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            boolean b;

            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
            }

            if (this.rowData.size() == 0) {
                b = false;
            } else {
                this.thisRow = this.rowData.next();

                if (this.thisRow == null) {
                    b = false;
                } else {
                    clearWarnings();

                    b = true;

                }
            }

            setRowPositionValidity();

            return b;
        }
    }

    /**
     * The <i>prev</i> method is not part of JDBC, but because of the architecture of this driver it is possible to move both forward and backward within the
     * result set.
     * 
     * <p>
     * If an input stream from the previous row is open, it is implicitly closed. The ResultSet's warning chain is cleared when a new row is read
     * </p>
     * 
     * @return true if the new current is valid; false if there are no more rows
     * 
     * @exception java.sql.SQLException
     *                if a database access error occurs
     */
    public boolean prev() throws java.sql.SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            int rowIndex = this.rowData.getPosition();

            boolean b = true;

            if ((rowIndex - 1) >= 0) {
                rowIndex--;
                this.rowData.setCurrentRow(rowIndex);
                this.thisRow = this.rowData.get(rowIndex);

                b = true;
            } else if ((rowIndex - 1) == -1) {
                rowIndex--;
                this.rowData.setCurrentRow(rowIndex);
                this.thisRow = null;

                b = false;
            } else {
                b = false;
            }

            setRowPositionValidity();

            return b;
        }
    }

    public boolean previous() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return prev();
        }
    }

    /**
     * Closes this ResultSet and releases resources.
     * 
     * @param calledExplicitly
     *            was realClose called by the standard ResultSet.close() method, or was it closed internally by the
     *            driver?
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public void realClose(boolean calledExplicitly) throws SQLException {
        JdbcConnection locallyScopedConn = this.connection;

        if (locallyScopedConn == null) {
            return; // already closed
        }

        synchronized (locallyScopedConn.getConnectionMutex()) {

            // additional check in case ResultSet was closed
            // while current thread was waiting for lock
            if (this.isClosed) {
                return;
            }

            try {
                if (this.useUsageAdvisor) {

                    // Report on result set closed by driver instead of application

                    if (!calledExplicitly) {
                        this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_WARN, "",
                                (this.owningStatement == null) ? "N/A" : this.owningStatement.getCurrentCatalog(), this.connectionId,
                                (this.owningStatement == null) ? (-1) : this.owningStatement.getId(), this.resultId, System.currentTimeMillis(), 0,
                                Constants.MILLIS_I18N, null, this.pointOfOrigin, Messages.getString("ResultSet.ResultSet_implicitly_closed_by_driver")));
                    }

                    if (this.rowData instanceof ResultsetRowsStatic) {

                        // Report on possibly too-large result sets

                        int resultSetSizeThreshold = locallyScopedConn.getPropertySet()
                                .getIntegerReadableProperty(PropertyDefinitions.PNAME_resultSetSizeThreshold).getValue();
                        if (this.rowData.size() > resultSetSizeThreshold) {
                            this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_WARN, "",
                                    (this.owningStatement == null) ? Messages.getString("ResultSet.N/A_159") : this.owningStatement.getCurrentCatalog(),
                                    this.connectionId, (this.owningStatement == null) ? (-1) : this.owningStatement.getId(), this.resultId,
                                    System.currentTimeMillis(), 0, Constants.MILLIS_I18N, null, this.pointOfOrigin,
                                    Messages.getString("ResultSet.Too_Large_Result_Set",
                                            new Object[] { Integer.valueOf(this.rowData.size()), Integer.valueOf(resultSetSizeThreshold) })));
                        }

                        if (!isLast() && !isAfterLast() && (this.rowData.size() != 0)) {

                            this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_WARN, "",
                                    (this.owningStatement == null) ? Messages.getString("ResultSet.N/A_159") : this.owningStatement.getCurrentCatalog(),
                                    this.connectionId, (this.owningStatement == null) ? (-1) : this.owningStatement.getId(), this.resultId,
                                    System.currentTimeMillis(), 0, Constants.MILLIS_I18N, null, this.pointOfOrigin,
                                    Messages.getString("ResultSet.Possible_incomplete_traversal_of_result_set",
                                            new Object[] { Integer.valueOf(getRow()), Integer.valueOf(this.rowData.size()) })));
                        }
                    }

                    //
                    // Report on any columns that were selected but not referenced
                    //

                    if (this.columnUsed.length > 0 && !this.rowData.wasEmpty()) {
                        StringBuilder buf = new StringBuilder(Messages.getString("ResultSet.The_following_columns_were_never_referenced"));

                        boolean issueWarn = false;

                        for (int i = 0; i < this.columnUsed.length; i++) {
                            if (!this.columnUsed[i]) {
                                if (!issueWarn) {
                                    issueWarn = true;
                                } else {
                                    buf.append(", ");
                                }

                                buf.append(this.columnDefinition.getFields()[i].getFullName());
                            }
                        }

                        if (issueWarn) {
                            this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_WARN, "",
                                    (this.owningStatement == null) ? "N/A" : this.owningStatement.getCurrentCatalog(), this.connectionId,
                                    (this.owningStatement == null) ? (-1) : this.owningStatement.getId(), 0, System.currentTimeMillis(), 0,
                                    Constants.MILLIS_I18N, null, this.pointOfOrigin, buf.toString()));
                        }
                    }
                }
            } finally {
                if (this.owningStatement != null && calledExplicitly) {
                    this.owningStatement.removeOpenResultSet(this);
                }

                SQLException exceptionDuringClose = null;

                if (this.rowData != null) {
                    try {
                        this.rowData.close();
                    } catch (CJException sqlEx) {
                        exceptionDuringClose = SQLExceptionsMapping.translateException(sqlEx);
                    }
                }

                if (this.statementUsedForFetchingRows != null) {
                    try {
                        this.statementUsedForFetchingRows.realClose(true, false);
                    } catch (SQLException sqlEx) {
                        if (exceptionDuringClose != null) {
                            exceptionDuringClose.setNextException(sqlEx);
                        } else {
                            exceptionDuringClose = sqlEx;
                        }
                    }
                }

                this.rowData = null;
                this.columnDefinition = null;
                this.eventSink = null;
                this.warningChain = null;
                this.owningStatement = null;
                this.catalog = null;
                this.serverInfo = null;
                this.thisRow = null;
                this.fastDefaultCal = null;
                this.fastClientCal = null;
                this.connection = null;
                this.session = null;

                this.isClosed = true;

                if (exceptionDuringClose != null) {
                    throw exceptionDuringClose;
                }
            }
        }
    }

    /**
     * Returns true if this ResultSet is closed.
     */
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    public void refreshRow() throws SQLException {
        throw SQLError.notUpdatable();
    }

    public boolean relative(int rows) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (this.rowData.size() == 0) {
                setRowPositionValidity();

                return false;
            }

            this.rowData.moveRowRelative(rows);
            this.thisRow = this.rowData.get(this.rowData.getPosition());

            setRowPositionValidity();

            return (!this.rowData.isAfterLast() && !this.rowData.isBeforeFirst());
        }
    }

    public boolean rowDeleted() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    public boolean rowInserted() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    public boolean rowUpdated() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    public void setFetchDirection(int direction) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if ((direction != FETCH_FORWARD) && (direction != FETCH_REVERSE) && (direction != FETCH_UNKNOWN)) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.Illegal_value_for_fetch_direction_64"),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            this.fetchDirection = direction;
        }
    }

    public void setFetchSize(int rows) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (rows < 0) { /* || rows > getMaxRows() */
                throw SQLError.createSQLException(Messages.getString("ResultSet.Value_must_be_between_0_and_getMaxRows()_66"),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            this.fetchSize = rows;
        }
    }

    /**
     * Sets the first character of the query that this result set was created
     * from.
     * 
     * @param c
     *            the first character of the query...uppercased
     */
    public void setFirstCharOfQuery(char c) {
        try {
            synchronized (checkClosed().getConnectionMutex()) {
                this.firstCharOfQuery = c;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e); // FIXME: Need to evolve public interface
        }
    }

    public void setOwningStatement(JdbcStatement owningStatement) {
        try {
            synchronized (checkClosed().getConnectionMutex()) {
                this.owningStatement = (StatementImpl) owningStatement;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e); // FIXME: Need to evolve public interface
        }
    }

    /**
     * Sets the concurrency
     * 
     * @param concurrencyFlag
     *            CONCUR_UPDATABLE or CONCUR_READONLY
     */
    public synchronized void setResultSetConcurrency(int concurrencyFlag) {
        try {
            synchronized (checkClosed().getConnectionMutex()) {
                this.resultSetConcurrency = concurrencyFlag;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e); // TODO: FIXME: Need to evolve public interface
        }
    }

    /**
     * Sets the result set type
     * 
     * @param typeFlag
     *            SCROLL_SENSITIVE or SCROLL_INSENSITIVE (we only support
     *            SCROLL_INSENSITIVE)
     */
    public synchronized void setResultSetType(int typeFlag) {
        try {
            synchronized (checkClosed().getConnectionMutex()) {
                this.resultSetType = typeFlag;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e); // TODO: FIXME: Need to evolve public interface
        }
    }

    /**
     * Sets server info (if any)
     * 
     * @param info
     *            the server info message
     */
    public void setServerInfo(String info) {
        try {
            synchronized (checkClosed().getConnectionMutex()) {
                this.serverInfo = info;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e); // TODO: FIXME: Need to evolve public interface
        }
    }

    public synchronized void setStatementUsedForFetchingRows(JdbcPreparedStatement stmt) {
        try {
            synchronized (checkClosed().getConnectionMutex()) {
                this.statementUsedForFetchingRows = stmt;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e); // TODO: FIXME: Need to evolve public interface
        }
    }

    /**
     * @param wrapperStatement
     *            The wrapperStatement to set.
     */
    public synchronized void setWrapperStatement(java.sql.Statement wrapperStatement) {
        try {
            synchronized (checkClosed().getConnectionMutex()) {
                this.wrapperStatement = wrapperStatement;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e); // TODO: FIXME: Need to evolve public interface
        }
    }

    @Override
    public String toString() {
        return hasRows() ? super.toString() : "Result set representing update count of " + this.updateCount;
    }

    public void updateArray(int arg0, Array arg1) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    public void updateArray(String arg0, Array arg1) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(int columnIndex, java.io.InputStream x, int length) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateAsciiStream(String columnName, java.io.InputStream x, int length) throws SQLException {
        updateAsciiStream(findColumn(columnName), x, length);
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
        updateBigDecimal(findColumn(columnName), x);
    }

    public void updateBinaryStream(int columnIndex, java.io.InputStream x, int length) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateBinaryStream(String columnName, java.io.InputStream x, int length) throws SQLException {
        updateBinaryStream(findColumn(columnName), x, length);
    }

    public void updateBlob(int arg0, java.sql.Blob arg1) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateBlob(String arg0, java.sql.Blob arg1) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateBoolean(String columnName, boolean x) throws SQLException {
        updateBoolean(findColumn(columnName), x);
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateByte(String columnName, byte x) throws SQLException {
        updateByte(findColumn(columnName), x);
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateBytes(String columnName, byte[] x) throws SQLException {
        updateBytes(findColumn(columnName), x);
    }

    public void updateCharacterStream(int columnIndex, java.io.Reader x, int length) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateCharacterStream(String columnName, java.io.Reader reader, int length) throws SQLException {
        updateCharacterStream(findColumn(columnName), reader, length);
    }

    public void updateClob(int arg0, java.sql.Clob arg1) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    public void updateClob(String columnName, java.sql.Clob clob) throws SQLException {
        updateClob(findColumn(columnName), clob);
    }

    public void updateDate(int columnIndex, java.sql.Date x) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateDate(String columnName, java.sql.Date x) throws SQLException {
        updateDate(findColumn(columnName), x);
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateDouble(String columnName, double x) throws SQLException {
        updateDouble(findColumn(columnName), x);
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateFloat(String columnName, float x) throws SQLException {
        updateFloat(findColumn(columnName), x);
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateInt(String columnName, int x) throws SQLException {
        updateInt(findColumn(columnName), x);
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateLong(String columnName, long x) throws SQLException {
        updateLong(findColumn(columnName), x);
    }

    public void updateNull(int columnIndex) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateNull(String columnName) throws SQLException {
        updateNull(findColumn(columnName));
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateObject(String columnName, Object x) throws SQLException {
        updateObject(findColumn(columnName), x);
    }

    public void updateObject(String columnName, Object x, int scale) throws SQLException {
        updateObject(findColumn(columnName), x);
    }

    public void updateRef(int arg0, Ref arg1) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    public void updateRef(String arg0, Ref arg1) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    public void updateRow() throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateShort(String columnName, short x) throws SQLException {
        updateShort(findColumn(columnName), x);
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateString(String columnName, String x) throws SQLException {
        updateString(findColumn(columnName), x);
    }

    public void updateTime(int columnIndex, java.sql.Time x) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateTime(String columnName, java.sql.Time x) throws SQLException {
        updateTime(findColumn(columnName), x);
    }

    public void updateTimestamp(int columnIndex, java.sql.Timestamp x) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateTimestamp(String columnName, java.sql.Timestamp x) throws SQLException {
        updateTimestamp(findColumn(columnName), x);
    }

    public boolean wasNull() throws SQLException {
        return this.thisRow.wasNull();
    }

    protected ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    /**
     * 
     * @param columnIndex
     * @param x
     * @param length
     * @throws SQLException
     */
    public void updateNCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateNCharacterStream(String columnName, Reader reader, int length) throws SQLException {
        updateNCharacterStream(findColumn(columnName), reader, length);
    }

    public void updateNClob(String columnName, NClob nClob) throws SQLException {
        updateNClob(findColumn(columnName), nClob);
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    public void updateRowId(String columnName, RowId x) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    public int getHoldability() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    public RowId getRowId(int columnIndex) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    public RowId getRowId(String columnLabel) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);

        return new MysqlSQLXML(this, columnIndex, getExceptionInterceptor());
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw SQLError.notUpdatable();

    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x);

    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw SQLError.notUpdatable();

    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw SQLError.notUpdatable();

    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x);
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw SQLError.notUpdatable();

    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        updateBlob(findColumn(columnLabel), inputStream);
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw SQLError.notUpdatable();

    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        updateBlob(findColumn(columnLabel), inputStream, length);
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw SQLError.notUpdatable();

    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader);
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw SQLError.notUpdatable();

    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw SQLError.notUpdatable();

    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        updateClob(findColumn(columnLabel), reader);
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw SQLError.notUpdatable();

    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateClob(findColumn(columnLabel), reader, length);
    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw SQLError.notUpdatable();

    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        updateNCharacterStream(findColumn(columnLabel), reader);

    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw SQLError.notUpdatable();

    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        updateNCharacterStream(findColumn(columnLabel), reader, length);
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw SQLError.notUpdatable();

    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw SQLError.notUpdatable();

    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        updateNClob(findColumn(columnLabel), reader);

    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateNClob(findColumn(columnLabel), reader, length);
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw SQLError.notUpdatable();

    }

    public void updateNString(String columnLabel, String nString) throws SQLException {
        updateNString(findColumn(columnLabel), nString);
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw SQLError.notUpdatable();

    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        updateSQLXML(findColumn(columnLabel), xmlObject);

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
            throw SQLError.createSQLException(Messages.getString("Common.UnableToUnwrap", new Object[] { iface.toString() }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }
    }

    /**
     * Accumulate internal warnings as the SQLWarning chain.
     */
    public synchronized void warningEncountered(String warning) {
        SQLWarning w = new SQLWarning(warning);
        if (this.warningChain == null) {
            this.warningChain = w;
        } else {
            this.warningChain.setNextWarning(w);
        }
    }

    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw SQLError.notUpdatable();
    }

    public ColumnDefinition getMetadata() {
        return this.columnDefinition;
    }

    public com.mysql.cj.jdbc.StatementImpl getOwningStatement() {
        return this.owningStatement;
    }

    @Override
    public void closeOwner(boolean calledExplicitly) {
        try {
            realClose(calledExplicitly);
        } catch (SQLException e) {
            throw ExceptionFactory.createException(e.getMessage(), e);
        }
    }

    @Override
    public JdbcConnection getConnection() {
        return this.connection;
    }

    @Override
    public Session getSession() {
        return this.connection != null ? this.connection.getSession() : null;
    }

    @Override
    public long getConnectionId() {
        return this.connectionId;
    }

    @Override
    public String getPointOfOrigin() {
        return this.pointOfOrigin;
    }

    @Override
    public int getOwnerFetchSize() {
        try {
            return getFetchSize();
        } catch (SQLException e) {
            throw ExceptionFactory.createException(e.getMessage(), e);
        }
    }

    @Override
    public String getCurrentCatalog() {
        return this.owningStatement == null ? "N/A" : this.owningStatement.getCurrentCatalog();
    }

    @Override
    public int getOwningStatementId() {
        return this.owningStatement == null ? -1 : this.owningStatement.getId();
    }

    @Override
    public int getOwningStatementMaxRows() {
        return this.owningStatement == null ? -1 : this.owningStatement.maxRows;
    }

    @Override
    public int getOwningStatementFetchSize() {
        try {
            return this.owningStatement == null ? 0 : this.owningStatement.getFetchSize();
        } catch (SQLException e) {
            throw ExceptionFactory.createException(e.getMessage(), e);
        }
    }

    @Override
    public long getOwningStatementServerId() {
        return this.owningStatement == null ? 0 : this.owningStatement.getServerStatementId();
    }

    @Override
    public Object getSyncMutex() {
        return this.connection != null ? this.connection.getConnectionMutex() : null;
    }

}
