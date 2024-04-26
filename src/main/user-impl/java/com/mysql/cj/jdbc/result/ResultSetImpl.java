/*
 * Copyright (c) 2002, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.jdbc.result;

import java.io.InputStream;
import java.io.InputStreamReader;
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
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.NativeSession;
import com.mysql.cj.Query;
import com.mysql.cj.Session;
import com.mysql.cj.WarningListener;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
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
import com.mysql.cj.jdbc.exceptions.NotUpdatable;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.log.ProfilerEvent;
import com.mysql.cj.log.ProfilerEventHandler;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ResultsetRows;
import com.mysql.cj.protocol.a.result.NativeResultset;
import com.mysql.cj.protocol.a.result.OkPacket;
import com.mysql.cj.result.BigDecimalValueFactory;
import com.mysql.cj.result.BinaryStreamValueFactory;
import com.mysql.cj.result.BooleanValueFactory;
import com.mysql.cj.result.ByteValueFactory;
import com.mysql.cj.result.DoubleValueFactory;
import com.mysql.cj.result.DurationValueFactory;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.FloatValueFactory;
import com.mysql.cj.result.IntegerValueFactory;
import com.mysql.cj.result.LocalDateTimeValueFactory;
import com.mysql.cj.result.LocalDateValueFactory;
import com.mysql.cj.result.LocalTimeValueFactory;
import com.mysql.cj.result.LongValueFactory;
import com.mysql.cj.result.OffsetDateTimeValueFactory;
import com.mysql.cj.result.OffsetTimeValueFactory;
import com.mysql.cj.result.ShortValueFactory;
import com.mysql.cj.result.SqlDateValueFactory;
import com.mysql.cj.result.SqlTimeValueFactory;
import com.mysql.cj.result.SqlTimestampValueFactory;
import com.mysql.cj.result.StringValueFactory;
import com.mysql.cj.result.UtilCalendarValueFactory;
import com.mysql.cj.result.ValueFactory;
import com.mysql.cj.result.ZonedDateTimeValueFactory;
import com.mysql.cj.util.LogUtils;
import com.mysql.cj.util.StringUtils;

public class ResultSetImpl extends NativeResultset implements ResultSetInternalMethods, WarningListener {

    /** Counter used to generate IDs for profiling. */
    static int resultCounter = 1;

    /** The database that was in use when we were created */
    protected String db = null;

    /** Keep track of columns accessed */
    protected boolean[] columnUsed = null;

    /** The Connection instance that created us */
    protected volatile JdbcConnection connection;

    protected NativeSession session = null;

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

    /** Are we read-only or updatable? */
    protected int resultSetConcurrency = 0;

    /** Are we scroll-sensitive/insensitive? */
    protected int resultSetType = 0;

    JdbcPreparedStatement statementUsedForFetchingRows;

    protected boolean useUsageAdvisor = false;
    protected boolean gatherPerfMetrics = false;

    /** Is ResultSet.TYPE_FORWARD_ONLY scroll tolerant? */
    protected boolean scrollTolerant = false;

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
    private ValueFactory<Time> defaultTimeValueFactory;
    private ValueFactory<Timestamp> defaultTimestampValueFactory;

    private ValueFactory<Calendar> defaultUtilCalendarValueFactory;

    private ValueFactory<LocalDate> defaultLocalDateValueFactory;
    private ValueFactory<LocalDateTime> defaultLocalDateTimeValueFactory;
    private ValueFactory<LocalTime> defaultLocalTimeValueFactory;

    private ValueFactory<OffsetTime> defaultOffsetTimeValueFactory;
    private ValueFactory<OffsetDateTime> defaultOffsetDateTimeValueFactory;
    private ValueFactory<ZonedDateTime> defaultZonedDateTimeValueFactory;

    protected RuntimeProperty<Boolean> emulateLocators;

    protected boolean treatMysqlDatetimeAsTimestamp = false;
    protected boolean yearIsDateType = true;

    /**
     * Create a result set for an executeUpdate statement.
     *
     * @param ok
     *            {@link OkPacket}
     * @param conn
     *            the Connection that created us.
     * @param creatorStmt
     *            the Statement that created us.
     */
    public ResultSetImpl(OkPacket ok, JdbcConnection conn, StatementImpl creatorStmt) {
        super(ok);

        this.connection = conn;
        this.owningStatement = creatorStmt;

        if (this.connection != null) {
            this.session = (NativeSession) conn.getSession();
            this.exceptionInterceptor = this.connection.getExceptionInterceptor();

            this.padCharsWithSpace = this.connection.getPropertySet().getBooleanProperty(PropertyKey.padCharsWithSpace).getValue();
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
     *            the Statement that created us.
     *
     * @throws SQLException
     *             if an error occurs
     */
    public ResultSetImpl(ResultsetRows tuples, JdbcConnection conn, StatementImpl creatorStmt) throws SQLException {
        this.connection = conn;
        this.session = (NativeSession) conn.getSession();
        // TODO which database to use, from connection or from statement?
        this.db = creatorStmt != null ? creatorStmt.getCurrentDatabase() : conn.getDatabase();
        this.owningStatement = creatorStmt;

        this.exceptionInterceptor = this.connection.getExceptionInterceptor();

        PropertySet pset = this.connection.getPropertySet();
        this.emulateLocators = pset.getBooleanProperty(PropertyKey.emulateLocators);
        this.padCharsWithSpace = pset.getBooleanProperty(PropertyKey.padCharsWithSpace).getValue();
        this.treatMysqlDatetimeAsTimestamp = pset.getBooleanProperty(PropertyKey.treatMysqlDatetimeAsTimestamp).getValue();
        this.yearIsDateType = pset.getBooleanProperty(PropertyKey.yearIsDateType).getValue();
        this.useUsageAdvisor = pset.getBooleanProperty(PropertyKey.useUsageAdvisor).getValue();
        this.gatherPerfMetrics = pset.getBooleanProperty(PropertyKey.gatherPerfMetrics).getValue();
        this.scrollTolerant = pset.getBooleanProperty(PropertyKey.scrollTolerantForwardOnly).getValue();

        this.booleanValueFactory = new BooleanValueFactory(pset);
        this.byteValueFactory = new ByteValueFactory(pset);
        this.shortValueFactory = new ShortValueFactory(pset);
        this.integerValueFactory = new IntegerValueFactory(pset);
        this.longValueFactory = new LongValueFactory(pset);
        this.floatValueFactory = new FloatValueFactory(pset);
        this.doubleValueFactory = new DoubleValueFactory(pset);
        this.bigDecimalValueFactory = new BigDecimalValueFactory(pset);
        this.binaryStreamValueFactory = new BinaryStreamValueFactory(pset);

        this.defaultTimeValueFactory = new SqlTimeValueFactory(pset, null, this.session.getServerSession().getDefaultTimeZone(), this);
        this.defaultTimestampValueFactory = new SqlTimestampValueFactory(pset, null, this.session.getServerSession().getDefaultTimeZone(),
                this.session.getServerSession().getSessionTimeZone());

        this.defaultUtilCalendarValueFactory = new UtilCalendarValueFactory(pset, this.session.getServerSession().getDefaultTimeZone(),
                this.session.getServerSession().getSessionTimeZone());

        this.defaultLocalDateValueFactory = new LocalDateValueFactory(pset, this);
        this.defaultLocalTimeValueFactory = new LocalTimeValueFactory(pset, this);
        this.defaultLocalDateTimeValueFactory = new LocalDateTimeValueFactory(pset);

        this.defaultOffsetTimeValueFactory = new OffsetTimeValueFactory(pset, this.session.getProtocol().getServerSession().getDefaultTimeZone());
        this.defaultOffsetDateTimeValueFactory = new OffsetDateTimeValueFactory(pset, this.session.getProtocol().getServerSession().getDefaultTimeZone(),
                this.session.getProtocol().getServerSession().getSessionTimeZone());
        this.defaultZonedDateTimeValueFactory = new ZonedDateTimeValueFactory(pset, this.session.getProtocol().getServerSession().getDefaultTimeZone(),
                this.session.getProtocol().getServerSession().getSessionTimeZone());

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

        this.useColumnNamesInFindColumn = pset.getBooleanProperty(PropertyKey.useColumnNamesInFindColumn).getValue();

        setRowPositionValidity();
    }

    @Override
    public void initializeWithMetadata() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            initRowsWithMetadata();

            if (this.useUsageAdvisor) {
                this.columnUsed = new boolean[this.columnDefinition.getFields().length];
                this.pointOfOrigin = LogUtils.findCallingClassAndMethod(new Throwable());
                this.resultId = resultCounter++;
                this.eventSink = this.session.getProfilerEventHandler();
            }

            if (this.gatherPerfMetrics) {
                this.session.getProtocol().getMetricsHolder().incrementNumberOfResultSetsCreated();

                Set<String> tableNamesSet = new HashSet<>();

                for (int i = 0; i < this.columnDefinition.getFields().length; i++) {
                    Field f = this.columnDefinition.getFields()[i];

                    String tableName = f.getOriginalTableName();

                    if (tableName == null) {
                        tableName = f.getTableName();
                    }

                    if (tableName != null) {
                        if (this.connection.lowerCaseTableNames()) {
                            // on windows, table names are not case-sensitive;
                            // adjusting names to hit the same keys in tableNamesSet
                            tableName = tableName.toLowerCase();
                        }

                        tableNamesSet.add(tableName);
                    }
                }

                this.session.getProtocol().getMetricsHolder().reportNumberOfTablesAccessed(tableNamesSet.size());
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR, getExceptionInterceptor());
            }

            if (isStrictlyForwardOnly()) {
                throw ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly"));
            }

            boolean b;

            if (this.rowData.size() == 0) {
                b = false;
            } else if (row == 0) {
                beforeFirst();
                b = false;
            } else if (row == 1) {
                b = first();
            } else if (row == -1) {
                b = last();
            } else if (row > this.rowData.size()) {
                afterLast();
                b = false;
            } else if (row < 0) {
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

            setRowPositionValidity();

            return b;
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void afterLast() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR, getExceptionInterceptor());
            }

            if (isStrictlyForwardOnly()) {
                throw ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly"));
            }

            if (this.rowData.size() != 0) {
                this.rowData.afterLast();
                this.thisRow = null;
            }

            setRowPositionValidity();
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void beforeFirst() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR, getExceptionInterceptor());
            }

            if (isStrictlyForwardOnly()) {
                throw ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly"));
            }

            if (this.rowData.size() == 0) {
                return;
            }

            this.rowData.beforeFirst();
            this.thisRow = null;

            setRowPositionValidity();
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    /**
     * Ensures that the result set is not closed
     *
     * @return connection
     *
     * @throws SQLException
     *             if the result set is closed
     */
    protected final JdbcConnection checkClosed() throws SQLException {
        JdbcConnection c = this.connection;

        if (c == null) {
            throw SQLError.createSQLException(Messages.getString("ResultSet.Operation_not_allowed_after_ResultSet_closed_144"),
                    MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR, getExceptionInterceptor());
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
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (columnIndex < 1) {
                throw SQLError.createSQLException(
                        Messages.getString("ResultSet.Column_Index_out_of_range_low",
                                new Object[] { Integer.valueOf(columnIndex), Integer.valueOf(this.columnDefinition.getFields().length) }),
                        MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            } else if (columnIndex > this.columnDefinition.getFields().length) {
                throw SQLError.createSQLException(
                        Messages.getString("ResultSet.Column_Index_out_of_range_high",
                                new Object[] { Integer.valueOf(columnIndex), Integer.valueOf(this.columnDefinition.getFields().length) }),
                        MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            if (this.useUsageAdvisor) {
                this.columnUsed[columnIndex - 1] = true;
            }
        } finally {
            connectionLock.unlock();
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
            throw SQLError.createSQLException(Messages.getString(this.invalidRowReasonMessageKey), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    getExceptionInterceptor());
        }
    }

    private boolean onValidRow = false;
    private String invalidRowReasonMessageKey = null;

    private void setRowPositionValidity() {
        if (!this.rowData.isDynamic() && this.rowData.size() == 0) {
            this.invalidRowReasonMessageKey = "ResultSet.Illegal_operation_on_empty_result_set";
            this.onValidRow = false;
        } else if (this.rowData.isBeforeFirst()) {
            this.invalidRowReasonMessageKey = "ResultSet.Before_start_of_result_set_146";
            this.onValidRow = false;
        } else if (this.rowData.isAfterLast()) {
            this.invalidRowReasonMessageKey = "ResultSet.After_end_of_result_set_148";
            this.onValidRow = false;
        } else {
            this.onValidRow = true;
            this.invalidRowReasonMessageKey = null;
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            this.warningChain = null;
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void close() throws SQLException {
        realClose(true);
    }

    @Override
    public void populateCachedMetaData(CachedResultSetMetaData cachedMetaData) throws SQLException {
        this.columnDefinition.exportTo(cachedMetaData);
        cachedMetaData.setMetadata(getMetaData());
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public int findColumn(String columnName) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            int index = this.columnDefinition.findColumn(columnName, this.useColumnNamesInFindColumn, 1);

            if (index == -1) {
                throw SQLError.createSQLException(
                        Messages.getString("ResultSet.Column____112") + columnName + Messages.getString("ResultSet.___not_found._113"),
                        MysqlErrorNumbers.SQLSTATE_MYSQL_ER_BAD_FIELD_ERROR, getExceptionInterceptor());
            }

            return index;
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public boolean first() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR, getExceptionInterceptor());
            }

            if (isStrictlyForwardOnly()) {
                throw ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly"));
            }

            boolean b = true;

            if (this.rowData.isEmpty()) {
                b = false;
            } else {
                this.rowData.beforeFirst();
                this.thisRow = this.rowData.next();
            }

            setRowPositionValidity();

            return b;
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(String colName) throws SQLException {
        return getArray(findColumn(colName));
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return getBinaryStream(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(String columnName) throws SQLException {
        return getAsciiStream(findColumn(columnName));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return this.thisRow.getValue(columnIndex - 1, this.bigDecimalValueFactory);
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        ValueFactory<BigDecimal> vf = new BigDecimalValueFactory(this.session.getPropertySet(), scale);
        vf.setPropertySet(this.connection.getPropertySet());
        return this.thisRow.getValue(columnIndex - 1, vf);
    }

    @Override
    public BigDecimal getBigDecimal(String columnName) throws SQLException {
        return getBigDecimal(findColumn(columnName));
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnName), scale);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return this.thisRow.getValue(columnIndex - 1, this.binaryStreamValueFactory);
    }

    @Override
    public InputStream getBinaryStream(String columnName) throws SQLException {
        return getBinaryStream(findColumn(columnName));
    }

    @Override
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

    @Override
    public java.sql.Blob getBlob(String colName) throws SQLException {
        return getBlob(findColumn(colName));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Boolean res = getObject(columnIndex, Boolean.TYPE);
        return res == null ? false : res;
    }

    @Override
    public boolean getBoolean(String columnName) throws SQLException {
        return getBoolean(findColumn(columnName));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        Byte res = getObject(columnIndex, Byte.TYPE);
        return res == null ? (byte) 0 : res;
    }

    @Override
    public byte getByte(String columnName) throws SQLException {
        return getByte(findColumn(columnName));
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return this.thisRow.getBytes(columnIndex - 1);
    }

    @Override
    public byte[] getBytes(String columnName) throws SQLException {
        return getBytes(findColumn(columnName));
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
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

    @Override
    public Reader getCharacterStream(String columnName) throws SQLException {
        return getCharacterStream(findColumn(columnName));
    }

    @Override
    public java.sql.Clob getClob(int columnIndex) throws SQLException {
        String asString = getStringForClob(columnIndex);

        if (asString == null) {
            return null;
        }

        return new com.mysql.cj.jdbc.Clob(asString, getExceptionInterceptor());
    }

    @Override
    public java.sql.Clob getClob(String colName) throws SQLException {
        return getClob(findColumn(colName));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return this.thisRow.getValue(columnIndex - 1,
                new SqlDateValueFactory(this.session.getPropertySet(), null, this.session.getServerSession().getDefaultTimeZone(), this));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return this.thisRow.getValue(columnIndex - 1, new SqlDateValueFactory(this.session.getPropertySet(), cal,
                cal != null ? cal.getTimeZone() : this.session.getServerSession().getDefaultTimeZone(), this));
    }

    @Override
    public Date getDate(String columnName) throws SQLException {
        return getDate(findColumn(columnName));
    }

    @Override
    public Date getDate(String columnName, Calendar cal) throws SQLException {
        return getDate(findColumn(columnName), cal);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Double res = getObject(columnIndex, Double.TYPE);
        return res == null ? (double) 0 : res;
    }

    @Override
    public double getDouble(String columnName) throws SQLException {
        return getDouble(findColumn(columnName));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Float res = getObject(columnIndex, Float.TYPE);
        return res == null ? (float) 0 : res;
    }

    @Override
    public float getFloat(String columnName) throws SQLException {
        return getFloat(findColumn(columnName));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Integer res = getObject(columnIndex, Integer.TYPE);
        return res == null ? 0 : res;
    }

    @Override
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
                    MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }
    }

    @Override
    public int getInt(String columnName) throws SQLException {
        return getInt(findColumn(columnName));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Long res = getObject(columnIndex, Long.TYPE);
        return res == null ? 0L : res;
    }

    @Override
    public long getLong(String columnName) throws SQLException {
        return getLong(findColumn(columnName));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        Short res = getObject(columnIndex, Short.TYPE);
        return res == null ? (short) 0 : res;
    }

    @Override
    public short getShort(String columnName) throws SQLException {
        return getShort(findColumn(columnName));
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);

        Field f = this.columnDefinition.getFields()[columnIndex - 1];
        ValueFactory<String> vf = new StringValueFactory(this.session.getPropertySet());
        String stringVal = this.thisRow.getValue(columnIndex - 1, vf);

        if (this.padCharsWithSpace && stringVal != null && f.getMysqlTypeId() == MysqlType.FIELD_TYPE_STRING) {
            int maxBytesPerChar = this.session.getServerSession().getCharsetSettings().getMaxBytesPerChar(f.getCollationIndex(), f.getEncoding());
            int fieldLength = (int) f.getLength() /* safe, bytes in a CHAR <= 1024 */ / maxBytesPerChar; /* safe, this will never be 0 */
            return StringUtils.padString(stringVal, fieldLength);
        }

        return stringVal;
    }

    @Override
    public String getString(String columnName) throws SQLException {
        return getString(findColumn(columnName));
    }

    private String getStringForClob(int columnIndex) throws SQLException {
        String asString = null;

        String forcedEncoding = this.connection.getPropertySet().getStringProperty(PropertyKey.clobCharacterEncoding).getStringValue();

        if (forcedEncoding == null) {
            asString = getString(columnIndex);
        } else {
            byte[] asBytes = getBytes(columnIndex);

            if (asBytes != null) {
                asString = StringUtils.toString(asBytes, forcedEncoding);
            }
        }

        return asString;
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return this.thisRow.getValue(columnIndex - 1, this.defaultTimeValueFactory);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        ValueFactory<Time> vf = new SqlTimeValueFactory(this.session.getPropertySet(), cal,
                cal != null ? cal.getTimeZone() : this.session.getServerSession().getDefaultTimeZone());
        return this.thisRow.getValue(columnIndex - 1, vf);
    }

    @Override
    public Time getTime(String columnName) throws SQLException {
        return getTime(findColumn(columnName));
    }

    @Override
    public Time getTime(String columnName, Calendar cal) throws SQLException {
        return getTime(findColumn(columnName), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return this.thisRow.getValue(columnIndex - 1, this.defaultTimestampValueFactory);
    }

    public LocalDate getLocalDate(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return this.thisRow.getValue(columnIndex - 1, this.defaultLocalDateValueFactory);
    }

    public LocalDateTime getLocalDateTime(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return this.thisRow.getValue(columnIndex - 1, this.defaultLocalDateTimeValueFactory);
    }

    public LocalTime getLocalTime(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return this.thisRow.getValue(columnIndex - 1, this.defaultLocalTimeValueFactory);
    }

    public Calendar getUtilCalendar(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return this.thisRow.getValue(columnIndex - 1, this.defaultUtilCalendarValueFactory);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        ValueFactory<Timestamp> vf = new SqlTimestampValueFactory(this.session.getPropertySet(), cal, this.session.getServerSession().getDefaultTimeZone(),
                this.session.getServerSession().getSessionTimeZone());
        return this.thisRow.getValue(columnIndex - 1, vf);
    }

    @Override
    public Timestamp getTimestamp(String columnName) throws SQLException {
        return getTimestamp(findColumn(columnName));
    }

    @Override
    public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnName), cal);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);

        String fieldEncoding = this.columnDefinition.getFields()[columnIndex - 1].getEncoding();
        if (fieldEncoding == null || !fieldEncoding.equals("UTF-8")) {
            throw new SQLException("Can not call getNCharacterStream() when field's charset isn't UTF-8");
        }
        return getCharacterStream(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnName) throws SQLException {
        return getNCharacterStream(findColumn(columnName));
    }

    @Override
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

    @Override
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
            throw SQLError.createSQLException("Unsupported character encoding " + forcedEncoding, MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }

        return asString;
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);

        String fieldEncoding = this.columnDefinition.getFields()[columnIndex - 1].getEncoding();
        if (fieldEncoding == null || !fieldEncoding.equals("UTF-8")) {
            throw new SQLException("Can not call getNString() when field's charset isn't UTF-8");
        }
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnName) throws SQLException {
        return getNString(findColumn(columnName));
    }

    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    @Override
    public String getCursorName() throws SQLException {
        throw SQLError.createSQLException(Messages.getString("ResultSet.Positioned_Update_not_supported"), MysqlErrorNumbers.SQLSTATE_CONNJ_DRIVER_NOT_CAPABLE,
                getExceptionInterceptor());
    }

    @Override
    public int getFetchDirection() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            return this.fetchDirection;
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            return this.fetchSize;
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public char getFirstCharOfQuery() {
        try {
            Lock connectionLock = checkClosed().getConnectionLock();
            connectionLock.lock();
            try {
                return this.firstCharOfQuery;
            } finally {
                connectionLock.unlock();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e); // FIXME: Need to evolve interface
        }
    }

    @Override
    public java.sql.ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();

        return new ResultSetMetaData(this.session, this.columnDefinition.getFields(),
                this.session.getPropertySet().getBooleanProperty(PropertyKey.useOldAliasMetadataBehavior).getValue(), this.yearIsDateType,
                getExceptionInterceptor());
    }

    @Override
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
                                MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, getExceptionInterceptor());
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
            case VECTOR:
                return getBytes(columnIndex);

            case YEAR:
                return this.yearIsDateType ? getDate(columnIndex) : Short.valueOf(getShort(columnIndex));

            case DATE:
                return getDate(columnIndex);

            case TIME:
                return getTime(columnIndex);

            case TIMESTAMP:
                return getTimestamp(columnIndex);

            case DATETIME:
                return this.treatMysqlDatetimeAsTimestamp ? getTimestamp(columnIndex) : getLocalDateTime(columnIndex);

            default:
                return getString(columnIndex);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        if (type == null) {
            throw SQLError.createSQLException("Type parameter can not be null", MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (type.equals(String.class)) {
                return (T) getString(columnIndex);

            } else if (type.equals(BigDecimal.class)) {
                return (T) getBigDecimal(columnIndex);

            } else if (type.equals(BigInteger.class)) {
                return (T) getBigInteger(columnIndex);

            } else if (type.equals(Boolean.class) || type.equals(Boolean.TYPE)) {
                checkRowPos();
                checkColumnBounds(columnIndex);
                return (T) this.thisRow.getValue(columnIndex - 1, this.booleanValueFactory);

            } else if (type.equals(Byte.class) || type.equals(Byte.TYPE)) {
                checkRowPos();
                checkColumnBounds(columnIndex);
                return (T) this.thisRow.getValue(columnIndex - 1, this.byteValueFactory);

            } else if (type.equals(Short.class) || type.equals(Short.TYPE)) {
                checkRowPos();
                checkColumnBounds(columnIndex);
                return (T) this.thisRow.getValue(columnIndex - 1, this.shortValueFactory);

            } else if (type.equals(Integer.class) || type.equals(Integer.TYPE)) {
                checkRowPos();
                checkColumnBounds(columnIndex);
                return (T) this.thisRow.getValue(columnIndex - 1, this.integerValueFactory);

            } else if (type.equals(Long.class) || type.equals(Long.TYPE)) {
                checkRowPos();
                checkColumnBounds(columnIndex);
                return (T) this.thisRow.getValue(columnIndex - 1, this.longValueFactory);

            } else if (type.equals(Float.class) || type.equals(Float.TYPE)) {
                checkRowPos();
                checkColumnBounds(columnIndex);
                return (T) this.thisRow.getValue(columnIndex - 1, this.floatValueFactory);

            } else if (type.equals(Double.class) || type.equals(Double.TYPE)) {
                checkRowPos();
                checkColumnBounds(columnIndex);
                return (T) this.thisRow.getValue(columnIndex - 1, this.doubleValueFactory);

            } else if (type.equals(byte[].class)) {
                return (T) getBytes(columnIndex);

            } else if (type.equals(Date.class)) {
                return (T) getDate(columnIndex);

            } else if (type.equals(Time.class)) {
                return (T) getTime(columnIndex);

            } else if (type.equals(Timestamp.class)) {
                return (T) getTimestamp(columnIndex);

            } else if (type.equals(java.util.Date.class)) {
                Timestamp ts = getTimestamp(columnIndex);
                return ts == null ? null : (T) java.util.Date.from(ts.toInstant());

            } else if (type.equals(java.util.Calendar.class)) {
                return (T) getUtilCalendar(columnIndex);

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
                checkRowPos();
                checkColumnBounds(columnIndex);
                return (T) this.thisRow.getValue(columnIndex - 1, this.defaultOffsetDateTimeValueFactory);

            } else if (type.equals(OffsetTime.class)) {
                checkRowPos();
                checkColumnBounds(columnIndex);
                return (T) this.thisRow.getValue(columnIndex - 1, this.defaultOffsetTimeValueFactory);

            } else if (type.equals(ZonedDateTime.class)) {
                checkRowPos();
                checkColumnBounds(columnIndex);
                return (T) this.thisRow.getValue(columnIndex - 1, this.defaultZonedDateTimeValueFactory);

            } else if (type.equals(Duration.class)) {
                checkRowPos();
                checkColumnBounds(columnIndex);
                return (T) this.thisRow.getValue(columnIndex - 1, new DurationValueFactory(this.session.getPropertySet()));
            }

            throw SQLError.createSQLException("Conversion not supported for type " + type.getName(), MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    @Override
    public Object getObject(int i, java.util.Map<String, Class<?>> map) throws SQLException {
        return getObject(i);
    }

    @Override
    public Object getObject(String columnName) throws SQLException {
        return getObject(findColumn(columnName));
    }

    @Override
    public Object getObject(String colName, java.util.Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(colName), map);
    }

    @Override
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
                                MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, getExceptionInterceptor());
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

    @Override
    public Object getObjectStoredProc(int i, java.util.Map<Object, Object> map, int desiredSqlType) throws SQLException {
        return getObjectStoredProc(i, desiredSqlType);
    }

    @Override
    public Object getObjectStoredProc(String columnName, int desiredSqlType) throws SQLException {
        return getObjectStoredProc(findColumn(columnName), desiredSqlType);
    }

    @Override
    public Object getObjectStoredProc(String colName, java.util.Map<Object, Object> map, int desiredSqlType) throws SQLException {
        return getObjectStoredProc(findColumn(colName), map, desiredSqlType);
    }

    @Override
    public java.sql.Ref getRef(int i) throws SQLException {
        checkColumnBounds(i);
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.Ref getRef(String colName) throws SQLException {
        return getRef(findColumn(colName));
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();

        if (!hasRows()) {
            throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                    MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR, getExceptionInterceptor());
        }

        int currentRowNumber = this.rowData.getPosition();
        int row = 0;

        if (!this.rowData.isDynamic()) {
            if (currentRowNumber < 0 || this.rowData.isAfterLast() || this.rowData.isEmpty()) {
                row = 0;
            } else {
                row = currentRowNumber + 1;
            }
        } else {
            if (this.rowData.isBeforeFirst() || this.rowData.isAfterLast() || this.rowData.isEmpty()) {
                row = 0;
            } else {
                row = currentRowNumber;
            }
        }

        return row;
    }

    @Override
    public java.sql.Statement getStatement() throws SQLException {
        try {
            Lock connectionLock = checkClosed().getConnectionLock();
            connectionLock.lock();
            try {
                if (this.wrapperStatement != null) {
                    return this.wrapperStatement;
                }

                return this.owningStatement;
            } finally {
                connectionLock.unlock();
            }

        } catch (SQLException sqlEx) {
            throw SQLError.createSQLException("Operation not allowed on closed ResultSet.", MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    getExceptionInterceptor());
        }
    }

    @Override
    public int getType() throws SQLException {
        return this.resultSetType;
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        checkRowPos();

        return getBinaryStream(columnIndex);
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(String columnName) throws SQLException {
        return getUnicodeStream(findColumn(columnName));
    }

    @Override
    public URL getURL(int colIndex) throws SQLException {
        String val = getString(colIndex);

        if (val == null) {
            return null;
        }

        try {
            return new URL(val);
        } catch (MalformedURLException mfe) {
            throw SQLError.createSQLException(Messages.getString("ResultSet.Malformed_URL____104") + val + "'",
                    MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }
    }

    @Override
    public URL getURL(String colName) throws SQLException {
        String val = getString(colName);

        if (val == null) {
            return null;
        }

        try {
            return new URL(val);
        } catch (MalformedURLException mfe) {
            throw SQLError.createSQLException(Messages.getString("ResultSet.Malformed_URL____107") + val + "'",
                    MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }
    }

    @Override
    public java.sql.SQLWarning getWarnings() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            return this.warningChain;
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void insertRow() throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR, getExceptionInterceptor());
            }
            return this.rowData.isAfterLast();
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR, getExceptionInterceptor());
            }

            return this.rowData.isBeforeFirst();
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public boolean isFirst() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR, getExceptionInterceptor());
            }

            return this.rowData.isFirst();
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public boolean isLast() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR, getExceptionInterceptor());
            }

            return this.rowData.isLast();
        } finally {
            connectionLock.unlock();
        }
    }

    /**
     * Checks whether this ResultSet is scrollable even if its type is ResultSet.TYPE_FORWARD_ONLY. Required for backwards compatibility.
     *
     * @return
     *         <code>true</code> if this result set type is ResultSet.TYPE_FORWARD_ONLY and the connection property 'scrollTolerantForwardOnly' has not been set
     *         to <code>true</code>.
     */
    protected boolean isStrictlyForwardOnly() {
        return this.resultSetType == ResultSet.TYPE_FORWARD_ONLY && !this.scrollTolerant;
    }

    @Override
    public boolean last() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR, getExceptionInterceptor());
            }

            if (isStrictlyForwardOnly()) {
                throw ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly"));
            }

            boolean b = true;

            if (this.rowData.size() == 0) {
                b = false;
            } else {
                this.rowData.beforeLast();
                this.thisRow = this.rowData.next();
            }

            setRowPositionValidity();

            return b;
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public boolean next() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR, getExceptionInterceptor());
            }

            boolean b;

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
        } finally {
            connectionLock.unlock();
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
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {

            int rowIndex = this.rowData.getPosition();

            boolean b = true;

            if (rowIndex - 1 >= 0) {
                rowIndex--;
                this.rowData.setCurrentRow(rowIndex);
                this.thisRow = this.rowData.get(rowIndex);

                b = true;
            } else if (rowIndex - 1 == -1) {
                rowIndex--;
                this.rowData.setCurrentRow(rowIndex);
                this.thisRow = null;

                b = false;
            } else {
                b = false;
            }

            setRowPositionValidity();

            return b;
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public boolean previous() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR, getExceptionInterceptor());
            }

            if (isStrictlyForwardOnly()) {
                throw ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly"));
            }

            return prev();
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void realClose(boolean calledExplicitly) throws SQLException {
        JdbcConnection locallyScopedConn = this.connection;

        if (locallyScopedConn == null) {
            return; // already closed
        }

        Lock connectionLock = locallyScopedConn.getConnectionLock();
        connectionLock.lock();
        try {
            // additional check in case ResultSet was closed while current thread was waiting for lock
            if (this.isClosed) {
                return;
            }

            try {
                if (this.useUsageAdvisor) {

                    if (!calledExplicitly) {
                        this.eventSink.processEvent(ProfilerEvent.TYPE_USAGE, this.session, this.owningStatement, this, 0, new Throwable(),
                                Messages.getString("ResultSet.ResultSet_implicitly_closed_by_driver"));
                    }

                    int resultSetSizeThreshold = locallyScopedConn.getPropertySet().getIntegerProperty(PropertyKey.resultSetSizeThreshold).getValue();
                    if (this.rowData.size() > resultSetSizeThreshold) {
                        this.eventSink.processEvent(ProfilerEvent.TYPE_USAGE, this.session, this.owningStatement, this, 0, new Throwable(),
                                Messages.getString("ResultSet.Too_Large_Result_Set",
                                        new Object[] { Integer.valueOf(this.rowData.size()), Integer.valueOf(resultSetSizeThreshold) }));
                    }

                    if (!isLast() && !isAfterLast() && this.rowData.size() != 0) {
                        this.eventSink.processEvent(ProfilerEvent.TYPE_USAGE, this.session, this.owningStatement, this, 0, new Throwable(),
                                Messages.getString("ResultSet.Possible_incomplete_traversal_of_result_set",
                                        new Object[] { Integer.valueOf(getRow()), Integer.valueOf(this.rowData.size()) }));
                    }

                    // Report on any columns that were selected but not referenced
                    if (this.columnUsed.length > 0 && !this.rowData.wasEmpty()) {
                        StringBuilder buf = new StringBuilder();
                        for (int i = 0; i < this.columnUsed.length; i++) {
                            if (!this.columnUsed[i]) {
                                if (buf.length() > 0) {
                                    buf.append(", ");
                                }
                                buf.append(this.columnDefinition.getFields()[i].getFullName());
                            }
                        }
                        if (buf.length() > 0) {
                            this.eventSink.processEvent(ProfilerEvent.TYPE_USAGE, this.session, this.owningStatement, this, 0, new Throwable(),
                                    Messages.getString("ResultSet.The_following_columns_were_never_referenced", new String[] { buf.toString() }));
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
                this.db = null;
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
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR, getExceptionInterceptor());
            }

            if (isStrictlyForwardOnly()) {
                throw ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly"));
            }

            if (this.rowData.size() == 0) {
                setRowPositionValidity();

                return false;
            }

            this.rowData.moveRowRelative(rows);
            this.thisRow = this.rowData.get(this.rowData.getPosition());

            setRowPositionValidity();

            return !this.rowData.isAfterLast() && !this.rowData.isBeforeFirst();
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (direction != FETCH_FORWARD && direction != FETCH_REVERSE && direction != FETCH_UNKNOWN) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.Illegal_value_for_fetch_direction_64"),
                        MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            if (isStrictlyForwardOnly() && direction != FETCH_FORWARD) {
                String constName = direction == ResultSet.FETCH_REVERSE ? "ResultSet.FETCH_REVERSE" : "ResultSet.FETCH_UNKNOWN";
                throw ExceptionFactory.createException(Messages.getString("ResultSet.Unacceptable_value_for_fetch_direction", new Object[] { constName }));
            }

            this.fetchDirection = direction;
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (rows < 0 && rows != Integer.MIN_VALUE) { /* || rows > getMaxRows() */
                throw SQLError.createSQLException(Messages.getString("ResultSet.Value_must_be_between_0_and_getMaxRows()_66"),
                        MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            this.fetchSize = rows;
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void setFirstCharOfQuery(char c) {
        try {
            Lock connectionLock = checkClosed().getConnectionLock();
            connectionLock.lock();
            try {
                this.firstCharOfQuery = c;
            } finally {
                connectionLock.unlock();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e); // FIXME: Need to evolve public interface
        }
    }

    @Override
    public void setOwningStatement(JdbcStatement owningStatement) {
        try {
            Lock connectionLock = checkClosed().getConnectionLock();
            connectionLock.lock();
            try {
                this.owningStatement = (StatementImpl) owningStatement;
            } finally {
                connectionLock.unlock();
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
    public void setResultSetConcurrency(int concurrencyFlag) {
        this.lock.lock();
        try {
            Lock connectionLock = checkClosed().getConnectionLock();
            connectionLock.lock();
            try {
                this.resultSetConcurrency = concurrencyFlag;
            } finally {
                connectionLock.unlock();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e); // TODO: FIXME: Need to evolve public interface
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Sets the result set type
     *
     * @param typeFlag
     *            SCROLL_SENSITIVE or SCROLL_INSENSITIVE (we only support
     *            SCROLL_INSENSITIVE)
     */
    public void setResultSetType(int typeFlag) {
        this.lock.lock();
        try {
            Lock connectionLock = checkClosed().getConnectionLock();
            connectionLock.lock();
            try {
                this.resultSetType = typeFlag;
            } finally {
                connectionLock.unlock();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e); // TODO: FIXME: Need to evolve public interface
        } finally {
            this.lock.unlock();
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
            Lock connectionLock = checkClosed().getConnectionLock();
            connectionLock.lock();
            try {
                this.serverInfo = info;
            } finally {
                connectionLock.unlock();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e); // TODO: FIXME: Need to evolve public interface
        }
    }

    @Override
    public void setStatementUsedForFetchingRows(JdbcPreparedStatement stmt) {
        this.lock.lock();
        try {
            Lock connectionLock = checkClosed().getConnectionLock();
            connectionLock.lock();
            try {
                this.statementUsedForFetchingRows = stmt;
            } finally {
                connectionLock.unlock();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e); // TODO: FIXME: Need to evolve public interface
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void setWrapperStatement(java.sql.Statement wrapperStatement) {
        this.lock.lock();
        try {
            Lock connectionLock = checkClosed().getConnectionLock();
            connectionLock.lock();
            try {
                this.wrapperStatement = wrapperStatement;
            } finally {
                connectionLock.unlock();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e); // TODO: FIXME: Need to evolve public interface
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public String toString() {
        return hasRows() ? super.toString() : "Result set representing update count of " + this.updateCount;
    }

    @Override
    public void updateArray(int columnIndex, Array arg1) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(String columnLabel, Array arg1) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBlob(int columnIndex, java.sql.Blob arg1) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBlob(String columnLabel, java.sql.Blob arg1) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBoolean(String columnName, boolean x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateByte(String columnName, byte x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBytes(String columnName, byte[] x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateCharacterStream(String columnName, Reader reader, int length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateClob(int columnIndex, java.sql.Clob arg1) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnName, java.sql.Clob clob) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateDate(int columnIndex, java.sql.Date x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateDate(String columnName, java.sql.Date x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateDouble(String columnName, double x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateFloat(String columnName, float x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateInt(String columnName, int x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateLong(String columnName, long x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNClob(String columnName, NClob nClob) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNull(String columnName) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateObject(String columnName, Object x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateObject(String columnName, Object x, int scale) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateRef(int columnIndex, Ref arg1) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(String columnLabel, Ref arg1) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(String columnName, RowId x) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateShort(String columnName, short x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateString(String columnName, String x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateTime(int columnIndex, java.sql.Time x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateTime(String columnName, java.sql.Time x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateTimestamp(int columnIndex, java.sql.Timestamp x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateTimestamp(String columnName, java.sql.Timestamp x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public boolean wasNull() throws SQLException {
        return this.thisRow.wasNull();
    }

    protected ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    @Override
    public int getHoldability() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);

        return new MysqlSQLXML(this, columnIndex, getExceptionInterceptor());
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        checkClosed();

        // This works for classes that aren't actually wrapping anything
        return iface.isInstance(this);
    }

    @Override
    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
        try {
            // This works for classes that aren't actually wrapping anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException(Messages.getString("Common.UnableToUnwrap", new Object[] { iface.toString() }),
                    MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }
    }

    /**
     * Accumulate internal warnings as the SQLWarning chain.
     */
    @Override
    public void warningEncountered(String warning) {
        this.lock.lock();
        try {
            SQLWarning w = new SQLWarning(warning);
            if (this.warningChain == null) {
                this.warningChain = w;
            } else {
                this.warningChain.setNextWarning(w);
            }
        } finally {
            this.lock.unlock();
        }
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
    public Query getOwningQuery() {
        return this.owningStatement;
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
    public Lock getLock() {
        return this.connection != null ? this.connection.getConnectionLock() : null;
    }

}
