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
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;

import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.PropertyDefinitions.DatabaseTerm;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.AssertionFailedException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.ClientPreparedStatement;
import com.mysql.cj.jdbc.CloseOption;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.MysqlSQLXML;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.jdbc.exceptions.NotUpdatable;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.log.ProfilerEvent;
import com.mysql.cj.protocol.ResultsetRow;
import com.mysql.cj.protocol.ResultsetRows;
import com.mysql.cj.protocol.a.result.ByteArrayRow;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.Row;
import com.mysql.cj.util.StringUtils;

/**
 * A result set that is updatable.
 */
public class UpdatableResultSet extends ResultSetImpl {

    /** Marker for 'stream' data when doing INSERT rows */
    final static byte[] STREAM_DATA_MARKER = StringUtils.getBytes("** STREAM DATA **");

    private String charEncoding;

    /** What is the default value for the column? */
    private byte[][] defaultColumnValue;

    /** PreparedStatement used to delete data */
    private ClientPreparedStatement deleter = null;

    private String deleteSQL = null;

    /** PreparedStatement used to insert data */
    protected ClientPreparedStatement inserter = null;

    private String insertSQL = null;

    /** Is this result set updatable? */
    private boolean isUpdatable = false;

    /** Reason the result set is not updatable */
    private String notUpdatableReason = null;

    /** List of primary keys */
    private List<Integer> primaryKeyIndices = null;

    private String qualifiedAndQuotedTableName;

    private String quotedIdChar = null;

    /** PreparedStatement used to refresh data */
    private ClientPreparedStatement refresher;

    private String refreshSQL = null;

    /** The binary data for the 'current' row */
    private Row savedCurrentRow;

    /** PreparedStatement used to delete data */
    protected ClientPreparedStatement updater = null;

    /** SQL for in-place modifcation */
    private String updateSQL = null;

    private boolean populateInserterWithDefaultValues = false;
    private boolean pedantic;

    private boolean hasLongColumnInfo = false;

    private Map<String, Map<String, Map<String, Integer>>> databasesUsedToTablesUsed = null;

    /** Are we on the insert row? */
    private boolean onInsertRow = false;

    /** Are we in the middle of doing updates to the current row? */
    protected boolean doingUpdates = false;

    /**
     * Creates a new ResultSet object.
     *
     * @param tuples
     *            actual row data
     * @param conn
     *            the Connection that created us.
     * @param creatorStmt
     *            statement owning this result set
     *
     * @throws SQLException
     *             if an error occurs
     */
    public UpdatableResultSet(ResultsetRows tuples, JdbcConnection conn, StatementImpl creatorStmt) throws SQLException {
        super(tuples, conn, creatorStmt);
        checkUpdatability();

        this.charEncoding = this.session.getPropertySet().getStringProperty(PropertyKey.characterEncoding).getValue();
        this.populateInserterWithDefaultValues = getSession().getPropertySet().getBooleanProperty(PropertyKey.populateInsertRowWithDefaultValues).getValue();
        this.pedantic = getSession().getPropertySet().getBooleanProperty(PropertyKey.pedantic).getValue();
        this.hasLongColumnInfo = getSession().getServerSession().hasLongColumnInfo();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        boolean ret = super.absolute(row);
        if (this.onInsertRow) {
            this.onInsertRow = false;
        }
        if (this.doingUpdates) {
            this.doingUpdates = false;
        }
        return ret;
    }

    @Override
    public void afterLast() throws SQLException {
        super.afterLast();
        if (this.onInsertRow) {
            this.onInsertRow = false;
        }
        if (this.doingUpdates) {
            this.doingUpdates = false;
        }
    }

    @Override
    public void beforeFirst() throws SQLException {
        super.beforeFirst();
        if (this.onInsertRow) {
            this.onInsertRow = false;
        }
        if (this.doingUpdates) {
            this.doingUpdates = false;
        }
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        if (this.doingUpdates) {
            this.doingUpdates = false;
            this.updater.clearParameters();
        }
    }

    @Override
    protected void checkRowPos() throws SQLException {
        if (!this.onInsertRow) {
            super.checkRowPos();
        }
    }

    /**
     * Is this ResultSet updatable?
     *
     * @throws SQLException
     *             if an error occurs
     */
    public void checkUpdatability() throws SQLException {
        try {
            if (getMetadata() == null) {
                // we've been created to be populated with cached metadata, and we don't have the metadata yet, we'll be called again by
                // Connection.initializeResultsMetadataFromCache() when the metadata has been made available

                return;
            }

            String singleTableName = null;
            String dbName = null;

            int primaryKeyCount = 0;

            Field[] fields = getMetadata().getFields();
            // We can only do this if we know that there is a currently selected database, or if we're talking to a > 4.1 version of MySQL server (as it returns
            // database names in field info)
            if (this.db == null || this.db.length() == 0) {
                this.db = fields[0].getDatabaseName();

                if (this.db == null || this.db.length() == 0) {
                    throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.43"), MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT,
                            getExceptionInterceptor());
                }
            }

            if (fields.length > 0) {
                singleTableName = fields[0].getOriginalTableName();
                dbName = fields[0].getDatabaseName();

                if (singleTableName == null) {
                    singleTableName = fields[0].getTableName();
                    dbName = this.db;
                }

                if (singleTableName == null) {
                    this.isUpdatable = false;
                    this.notUpdatableReason = Messages.getString("NotUpdatableReason.3");

                    return;
                }

                if (fields[0].isPrimaryKey()) {
                    primaryKeyCount++;
                }

                //
                // References only one table?
                //
                for (int i = 1; i < fields.length; i++) {
                    String otherTableName = fields[i].getOriginalTableName();
                    String otherDbName = fields[i].getDatabaseName();

                    if (otherTableName == null) {
                        otherTableName = fields[i].getTableName();
                        otherDbName = this.db;
                    }

                    if (otherTableName == null) {
                        this.isUpdatable = false;
                        this.notUpdatableReason = Messages.getString("NotUpdatableReason.3");

                        return;
                    }

                    if (!otherTableName.equals(singleTableName)) {
                        this.isUpdatable = false;
                        this.notUpdatableReason = Messages.getString("NotUpdatableReason.0");

                        return;
                    }

                    // Can't reference more than one database
                    if (dbName == null || !dbName.equals(otherDbName)) {
                        this.isUpdatable = false;
                        this.notUpdatableReason = Messages.getString("NotUpdatableReason.1");

                        return;
                    }

                    if (fields[i].isPrimaryKey()) {
                        primaryKeyCount++;
                    }
                }
            } else {
                this.isUpdatable = false;
                this.notUpdatableReason = Messages.getString("NotUpdatableReason.3");

                return;
            }

            if (getSession().getPropertySet().getBooleanProperty(PropertyKey.strictUpdates).getValue()) {
                java.sql.DatabaseMetaData dbmd = getConnection().getMetaData();

                java.sql.ResultSet rs = null;
                HashMap<String, String> primaryKeyNames = new HashMap<>();

                try {
                    rs = this.session.getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA
                            ? dbmd.getPrimaryKeys(null, dbName, singleTableName)
                            : dbmd.getPrimaryKeys(dbName, null, singleTableName);

                    while (rs.next()) {
                        String keyName = rs.getString(4);
                        keyName = keyName.toUpperCase();
                        primaryKeyNames.put(keyName, keyName);
                    }
                } finally {
                    if (rs != null) {
                        try {
                            rs.close();
                        } catch (Exception ex) {
                            AssertionFailedException.shouldNotHappen(ex);
                        }

                        rs = null;
                    }
                }

                int existingPrimaryKeysCount = primaryKeyNames.size();

                if (existingPrimaryKeysCount == 0) {
                    this.isUpdatable = false;
                    this.notUpdatableReason = Messages.getString("NotUpdatableReason.5");

                    return; // we can't update tables w/o keys
                }

                //
                // Contains all primary keys?
                //
                for (int i = 0; i < fields.length; i++) {
                    if (fields[i].isPrimaryKey()) {
                        String columnNameUC = fields[i].getName().toUpperCase();

                        if (primaryKeyNames.remove(columnNameUC) == null) {
                            // try original name
                            String originalName = fields[i].getOriginalName();

                            if (originalName != null) {
                                if (primaryKeyNames.remove(originalName.toUpperCase()) == null) {
                                    // we don't know about this key, so give up :(
                                    this.isUpdatable = false;
                                    this.notUpdatableReason = Messages.getString("NotUpdatableReason.6", new Object[] { originalName });

                                    return;
                                }
                            }
                        }
                    }
                }

                this.isUpdatable = primaryKeyNames.isEmpty();

                if (!this.isUpdatable) {
                    this.notUpdatableReason = existingPrimaryKeysCount > 1 ? Messages.getString("NotUpdatableReason.7")
                            : Messages.getString("NotUpdatableReason.4");
                    return;
                }
            }

            //
            // Must have at least one primary key
            //
            if (primaryKeyCount == 0) {
                this.isUpdatable = false;
                this.notUpdatableReason = Messages.getString("NotUpdatableReason.4");

                return;
            }

            this.isUpdatable = true;
            this.notUpdatableReason = null;

            return;
        } catch (SQLException sqlEx) {
            this.isUpdatable = false;
            this.notUpdatableReason = sqlEx.getMessage();
        }
    }

    @Override
    public void deleteRow() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.isUpdatable) {
                throw new NotUpdatable(this.notUpdatableReason);
            }

            if (this.onInsertRow) {
                throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.1"), getExceptionInterceptor());
            } else if (this.rowData.size() == 0) {
                throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.2"), getExceptionInterceptor());
            } else if (isBeforeFirst()) {
                throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.3"), getExceptionInterceptor());
            } else if (isAfterLast()) {
                throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.4"), getExceptionInterceptor());
            }

            if (this.deleter == null) {
                if (this.deleteSQL == null) {
                    generateStatements();
                }

                this.deleter = (ClientPreparedStatement) this.connection.clientPrepareStatement(this.deleteSQL);
            }

            this.deleter.clearParameters();

            int numKeys = this.primaryKeyIndices.size();
            for (int i = 0; i < numKeys; i++) {
                int index = this.primaryKeyIndices.get(i).intValue();
                setParamValue(this.deleter, i + 1, this.thisRow, index, getMetadata().getFields()[index]);
            }

            this.deleter.executeUpdate();
            this.rowData.remove();

            prev(); // position on previous row - Bug#27431
        } finally {
            connectionLock.unlock();
        }
    }

    private void setParamValue(ClientPreparedStatement ps, int psIdx, Row row, int rsIdx, Field field) throws SQLException {
        byte[] val = row.getBytes(rsIdx);
        if (val == null) {
            ps.setNull(psIdx, MysqlType.NULL);
            return;
        }
        switch (field.getMysqlType()) {
            case NULL:
                ps.setNull(psIdx, MysqlType.NULL);
                break;
            case TINYINT:
            case TINYINT_UNSIGNED:
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case INT:
            case YEAR:
                ps.setInt(psIdx, getInt(rsIdx + 1));
                break;
            case INT_UNSIGNED:
            case BIGINT:
                ps.setLong(psIdx, getLong(rsIdx + 1));
                break;
            case BIGINT_UNSIGNED:
                ps.setBigInteger(psIdx, getBigInteger(rsIdx + 1));
                break;
            case CHAR:
            case ENUM:
            case SET:
            case VARCHAR:
            case JSON:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                ps.setString(psIdx, getString(rsIdx + 1));
                break;
            case DATE:
                ps.setDate(psIdx, getDate(rsIdx + 1));
                break;
            case TIMESTAMP:
                ps.setObject(psIdx, getObject(rsIdx + 1, Timestamp.class), MysqlType.TIMESTAMP, field.getDecimals());
                ps.getQueryBindings().getBinding(psIdx - 1, false).setKeepOrigNanos(true);
                break;
            case DATETIME:
                ps.setObject(psIdx, getObject(rsIdx + 1, LocalDateTime.class), MysqlType.DATETIME, field.getDecimals());
                ps.getQueryBindings().getBinding(psIdx - 1, false).setKeepOrigNanos(true);
                break;
            case TIME:
                ps.setTime(psIdx, getTime(rsIdx + 1));
                ps.getQueryBindings().getBinding(psIdx - 1, false).setKeepOrigNanos(true);
                break;
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case FLOAT:
            case FLOAT_UNSIGNED:
            case BOOLEAN:
            case BIT:
                ps.setBytes(psIdx, val, false);
                break;
            /*
             * default, but also explicitly for following types:
             * case Types.BINARY:
             * case Types.BLOB:
             */
            default:
                ps.setBytes(psIdx, val);
                break;
        }
    }

    private void extractDefaultValues() throws SQLException {
        java.sql.DatabaseMetaData dbmd = getConnection().getMetaData();
        this.defaultColumnValue = new byte[getMetadata().getFields().length][];

        java.sql.ResultSet columnsResultSet = null;

        for (Map.Entry<String, Map<String, Map<String, Integer>>> dbEntry : this.databasesUsedToTablesUsed.entrySet()) {
            for (Map.Entry<String, Map<String, Integer>> tableEntry : dbEntry.getValue().entrySet()) {
                String tableName = tableEntry.getKey();
                Map<String, Integer> columnNamesToIndices = tableEntry.getValue();

                try {
                    columnsResultSet = this.session.getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA
                            ? dbmd.getColumns(null, this.db, tableName, "%")
                            : dbmd.getColumns(this.db, null, tableName, "%");

                    while (columnsResultSet.next()) {
                        String columnName = columnsResultSet.getString("COLUMN_NAME");
                        byte[] defaultValue = columnsResultSet.getBytes("COLUMN_DEF");

                        if (columnNamesToIndices.containsKey(columnName)) {
                            int localColumnIndex = columnNamesToIndices.get(columnName).intValue();
                            this.defaultColumnValue[localColumnIndex] = defaultValue;
                        } // else assert?
                    }
                } finally {
                    if (columnsResultSet != null) {
                        columnsResultSet.close();
                        columnsResultSet = null;
                    }
                }
            }
        }
    }

    @Override
    public boolean first() throws SQLException {
        boolean ret = super.first();
        if (this.onInsertRow) {
            this.onInsertRow = false;
        }
        if (this.doingUpdates) {
            this.doingUpdates = false;
        }
        return ret;
    }

    /**
     * Figure out whether or not this ResultSet is updatable, and if so,
     * generate the PreparedStatements to support updates.
     *
     * @throws SQLException
     *             if an error occurs
     * @throws NotUpdatable
     *             if result set was marked as not updatable
     */
    protected void generateStatements() throws SQLException {
        if (!this.isUpdatable) {
            this.doingUpdates = false;
            this.onInsertRow = false;

            throw new NotUpdatable(this.notUpdatableReason);
        }

        String quotedId = getQuotedIdChar();

        Map<String, String> tableNamesSoFar = null;

        if (this.session.getServerSession().isLowerCaseTableNames()) {
            tableNamesSoFar = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            this.databasesUsedToTablesUsed = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        } else {
            tableNamesSoFar = new TreeMap<>();
            this.databasesUsedToTablesUsed = new TreeMap<>();
        }

        this.primaryKeyIndices = new ArrayList<>();

        StringBuilder fieldValues = new StringBuilder();
        StringBuilder keyValues = new StringBuilder();
        StringBuilder columnNames = new StringBuilder();
        StringBuilder insertPlaceHolders = new StringBuilder();
        StringBuilder allTablesBuf = new StringBuilder();
        Map<Integer, String> columnIndicesToTable = new HashMap<>();

        Field[] fields = getMetadata().getFields();

        for (int i = 0; i < fields.length; i++) {
            Map<String, Integer> updColumnNameToIndex = null;

            // FIXME: What about no table?
            if (fields[i].getOriginalTableName() != null) {

                String databaseName = fields[i].getDatabaseName();
                String tableOnlyName = fields[i].getOriginalTableName();

                String fqTableName = StringUtils.getFullyQualifiedName(databaseName, tableOnlyName, quotedId, this.pedantic);

                if (!tableNamesSoFar.containsKey(fqTableName)) {
                    if (!tableNamesSoFar.isEmpty()) {
                        allTablesBuf.append(',');
                    }

                    allTablesBuf.append(fqTableName);
                    tableNamesSoFar.put(fqTableName, fqTableName);
                }

                columnIndicesToTable.put(Integer.valueOf(i), fqTableName);

                updColumnNameToIndex = getColumnsToIndexMapForTableAndDB(databaseName, tableOnlyName);
            } else {
                String tableOnlyName = fields[i].getTableName();

                if (tableOnlyName != null) {

                    String fqTableName = StringUtils.quoteIdentifier(tableOnlyName, quotedId, this.pedantic);

                    if (!tableNamesSoFar.containsKey(fqTableName)) {
                        if (!tableNamesSoFar.isEmpty()) {
                            allTablesBuf.append(',');
                        }

                        allTablesBuf.append(fqTableName);
                        tableNamesSoFar.put(fqTableName, fqTableName);
                    }

                    columnIndicesToTable.put(Integer.valueOf(i), fqTableName);

                    updColumnNameToIndex = getColumnsToIndexMapForTableAndDB(this.db, tableOnlyName);
                }
            }

            String originalColumnName = fields[i].getOriginalName();
            String columnName = this.hasLongColumnInfo && originalColumnName != null && originalColumnName.length() > 0 ? originalColumnName
                    : fields[i].getName();

            if (updColumnNameToIndex != null && columnName != null) {
                updColumnNameToIndex.put(columnName, Integer.valueOf(i));
            }

            String originalTableName = fields[i].getOriginalTableName();
            String tableName = this.hasLongColumnInfo && originalTableName != null && originalTableName.length() > 0 ? originalTableName
                    : fields[i].getTableName();

            String databaseName = fields[i].getDatabaseName();
            String qualifiedColumnName = new StringBuilder() //
                    .append(StringUtils.getFullyQualifiedName(databaseName, tableName, quotedId, this.pedantic)) //
                    .append('.') //
                    .append(StringUtils.quoteIdentifier(columnName, quotedId, this.pedantic)).toString();

            if (fields[i].isPrimaryKey()) {
                this.primaryKeyIndices.add(Integer.valueOf(i));

                if (keyValues.length() > 0) {
                    keyValues.append(" AND ");
                }

                keyValues.append(qualifiedColumnName);
                keyValues.append("<=>");
                keyValues.append("?");
            }

            if (fieldValues.length() == 0) {
                fieldValues.append("SET ");
            } else {
                fieldValues.append(",");
                columnNames.append(",");
                insertPlaceHolders.append(",");
            }

            insertPlaceHolders.append("?");

            columnNames.append(qualifiedColumnName);

            fieldValues.append(qualifiedColumnName);
            fieldValues.append("=?");
        }

        this.qualifiedAndQuotedTableName = allTablesBuf.toString();

        this.updateSQL = "UPDATE " + this.qualifiedAndQuotedTableName + " " + fieldValues.toString() + " WHERE " + keyValues.toString();
        this.insertSQL = "INSERT INTO " + this.qualifiedAndQuotedTableName + " (" + columnNames.toString() + ") VALUES (" + insertPlaceHolders.toString() + ")";
        this.refreshSQL = "SELECT " + columnNames.toString() + " FROM " + this.qualifiedAndQuotedTableName + " WHERE " + keyValues.toString();
        this.deleteSQL = "DELETE FROM " + this.qualifiedAndQuotedTableName + " WHERE " + keyValues.toString();
    }

    private Map<String, Integer> getColumnsToIndexMapForTableAndDB(String databaseName, String tableName) {
        Map<String, Integer> nameToIndex;
        Map<String, Map<String, Integer>> tablesUsedToColumnsMap = this.databasesUsedToTablesUsed.get(databaseName);

        if (tablesUsedToColumnsMap == null) {
            tablesUsedToColumnsMap = this.session.getServerSession().isLowerCaseTableNames() ? new TreeMap<>(String.CASE_INSENSITIVE_ORDER) : new TreeMap<>();
            this.databasesUsedToTablesUsed.put(databaseName, tablesUsedToColumnsMap);
        }

        nameToIndex = tablesUsedToColumnsMap.get(tableName);

        if (nameToIndex == null) {
            nameToIndex = new HashMap<>();
            tablesUsedToColumnsMap.put(tableName, nameToIndex);
        }

        return nameToIndex;
    }

    @Override
    public int getConcurrency() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            return this.isUpdatable ? CONCUR_UPDATABLE : CONCUR_READ_ONLY;
        } finally {
            connectionLock.unlock();
        }
    }

    private String getQuotedIdChar() throws SQLException {
        if (this.quotedIdChar == null) {
            this.quotedIdChar = this.session.getIdentifierQuoteString();
        }

        return this.quotedIdChar;
    }

    @Override
    public void insertRow() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.7"), getExceptionInterceptor());
            }

            this.inserter.executeUpdate();

            long autoIncrementId = this.inserter.getLastInsertID();
            Field[] fields = getMetadata().getFields();
            byte[][] newRow = new byte[fields.length][];

            for (int i = 0; i < fields.length; i++) {
                if (this.inserter.isNull(i + 1)) {
                    newRow[i] = null;
                }

                // WARN: This non-variant only holds if MySQL never allows more than one auto-increment key (which is the way it is _today_)
                if (fields[i].isAutoIncrement() && autoIncrementId > 0) {
                    newRow[i] = StringUtils.getBytes(String.valueOf(autoIncrementId));
                    this.inserter.setBytes(i + 1, newRow[i], false);
                }
            }

            Row resultSetRow = new ByteArrayRow(newRow, getExceptionInterceptor());

            // inserter is always a client-side prepared statement, so it's safe to use it with ByteArrayRow for server-side prepared statement too
            refreshRow(this.inserter, resultSetRow);

            this.rowData.addRow(resultSetRow);
            resetInserter();
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return super.isAfterLast();
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return super.isBeforeFirst();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return super.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        return super.isLast();
    }

    boolean isUpdatable() {
        return this.isUpdatable;
    }

    @Override
    public boolean last() throws SQLException {
        boolean ret = super.last();
        if (this.onInsertRow) {
            this.onInsertRow = false;
        }
        if (this.doingUpdates) {
            this.doingUpdates = false;
        }
        return ret;
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.isUpdatable) {
                throw new NotUpdatable(this.notUpdatableReason);
            }

            if (this.onInsertRow) {
                this.onInsertRow = false;
                this.thisRow = this.savedCurrentRow;
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.isUpdatable) {
                throw new NotUpdatable(this.notUpdatableReason);
            }

            if (this.inserter == null) {
                if (this.insertSQL == null) {
                    generateStatements();
                }

                this.inserter = (ClientPreparedStatement) getConnection().clientPrepareStatement(this.insertSQL);
                this.inserter.getQueryBindings().setColumnDefinition(getMetadata());

                if (this.populateInserterWithDefaultValues) {
                    extractDefaultValues();
                }
            }
            resetInserter();

            Field[] fields = getMetadata().getFields();
            int numFields = fields.length;

            this.onInsertRow = true;
            this.doingUpdates = false;
            this.savedCurrentRow = this.thisRow;
            byte[][] newRowData = new byte[numFields][];
            this.thisRow = new ByteArrayRow(newRowData, getExceptionInterceptor());
            this.thisRow.setMetadata(getMetadata());

            for (int i = 0; i < numFields; i++) {
                if (!this.populateInserterWithDefaultValues) {
                    this.inserter.setBytes(i + 1, StringUtils.getBytes("DEFAULT"), false);
                    newRowData = null;
                } else if (this.defaultColumnValue[i] != null) {
                    Field f = fields[i];

                    switch (f.getMysqlTypeId()) {
                        case MysqlType.FIELD_TYPE_DATE:
                        case MysqlType.FIELD_TYPE_DATETIME:
                        case MysqlType.FIELD_TYPE_TIME:
                        case MysqlType.FIELD_TYPE_TIMESTAMP:

                            if (this.defaultColumnValue[i].length > 7 && this.defaultColumnValue[i][0] == (byte) 'C'
                                    && this.defaultColumnValue[i][1] == (byte) 'U' && this.defaultColumnValue[i][2] == (byte) 'R'
                                    && this.defaultColumnValue[i][3] == (byte) 'R' && this.defaultColumnValue[i][4] == (byte) 'E'
                                    && this.defaultColumnValue[i][5] == (byte) 'N' && this.defaultColumnValue[i][6] == (byte) 'T'
                                    && this.defaultColumnValue[i][7] == (byte) '_') {
                                this.inserter.setBytes(i + 1, this.defaultColumnValue[i], false);
                            } else {
                                this.inserter.setBytes(i + 1, this.defaultColumnValue[i]);
                            }
                            break;

                        default:
                            this.inserter.setBytes(i + 1, this.defaultColumnValue[i]);
                    }

                    // This value _could_ be changed from a getBytes(), so we need a copy....
                    byte[] defaultValueCopy = new byte[this.defaultColumnValue[i].length];
                    System.arraycopy(this.defaultColumnValue[i], 0, defaultValueCopy, 0, defaultValueCopy.length);
                    newRowData[i] = defaultValueCopy;
                } else {
                    this.inserter.setNull(i + 1, MysqlType.NULL);
                    newRowData[i] = null;
                }
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public boolean next() throws SQLException {
        boolean ret = super.next();
        if (this.onInsertRow) {
            this.onInsertRow = false;
        }
        if (this.doingUpdates) {
            this.doingUpdates = false;
        }
        return ret;
    }

    @Override
    public boolean prev() throws SQLException {
        boolean ret = super.prev();
        if (this.onInsertRow) {
            this.onInsertRow = false;
        }
        if (this.doingUpdates) {
            this.doingUpdates = false;
        }
        return ret;
    }

    @Override
    public boolean previous() throws SQLException {
        boolean ret = super.previous();
        if (this.onInsertRow) {
            this.onInsertRow = false;
        }
        if (this.doingUpdates) {
            this.doingUpdates = false;
        }
        return ret;
    }

    /**
     * Close this ResultSet and release resources. By default the close is considered explicit and does not propagate to owner statements.
     */
    @Override
    public void doClose(CloseOption... options) throws SQLException {
        if (this.isClosed) {
            return;
        }

        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            SQLException sqlEx = null;

            if (this.useUsageAdvisor) {
                if (this.deleter == null && this.inserter == null && this.refresher == null && this.updater == null) {
                    this.eventSink.processEvent(ProfilerEvent.TYPE_USAGE, this.session, getOwningStatement(), this, 0, new Throwable(),
                            Messages.getString("UpdatableResultSet.34"));
                }
            }

            try {
                if (this.deleter != null) {
                    this.deleter.close();
                }
            } catch (SQLException ex) {
                sqlEx = ex;
            }

            try {
                if (this.inserter != null) {
                    this.inserter.close();
                }
            } catch (SQLException ex) {
                sqlEx = ex;
            }

            try {
                if (this.refresher != null) {
                    this.refresher.close();
                }
            } catch (SQLException ex) {
                sqlEx = ex;
            }

            try {
                if (this.updater != null) {
                    this.updater.close();
                }
            } catch (SQLException ex) {
                sqlEx = ex;
            }

            super.doClose(options);

            if (sqlEx != null) {
                throw sqlEx;
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void refreshRow() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (isStrictlyForwardOnly()) {
                throw ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly"));
            }

            if (!this.isUpdatable) {
                throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
            }

            if (this.onInsertRow) {
                throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.8"), getExceptionInterceptor());
            } else if (this.rowData.size() == 0) {
                throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.9"), getExceptionInterceptor());
            } else if (isBeforeFirst()) {
                throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.10"), getExceptionInterceptor());
            } else if (isAfterLast()) {
                throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.11"), getExceptionInterceptor());
            }

            refreshRow(this.updater, this.thisRow);
        } finally {
            connectionLock.unlock();
        }
    }

    private void refreshRow(ClientPreparedStatement updateInsertStmt, Row rowToRefresh) throws SQLException {
        if (this.refresher == null) {
            if (this.refreshSQL == null) {
                generateStatements();
            }

            // We're going to copy bytes from refresher results to rowToRefresh, thus we need them to have the same protocol encoding

            this.refresher = ((ResultsetRow) this.thisRow).isBinaryEncoded() ? (ClientPreparedStatement) getConnection().serverPrepareStatement(this.refreshSQL)
                    : (ClientPreparedStatement) getConnection().clientPrepareStatement(this.refreshSQL);

            this.refresher.getQueryBindings().setColumnDefinition(getMetadata());
        }

        this.refresher.clearParameters();

        int numKeys = this.primaryKeyIndices.size();

        for (int i = 0; i < numKeys; i++) {
            byte[] dataFrom = null;
            int index = this.primaryKeyIndices.get(i).intValue();

            if (!this.doingUpdates && !this.onInsertRow) {
                setParamValue(this.refresher, i + 1, this.thisRow, index, getMetadata().getFields()[index]);
                continue;
            }

            dataFrom = updateInsertStmt.getBytesRepresentation(index + 1);

            // Primary keys not set?
            if (updateInsertStmt.isNull(index + 1) || dataFrom.length == 0) {
                setParamValue(this.refresher, i + 1, this.thisRow, index, getMetadata().getFields()[index]);
                continue;
            }

            this.refresher.getQueryBindings().setFromBindValue(i, updateInsertStmt.getQueryBindings().getBindValues()[index]);
        }

        java.sql.ResultSet rs = null;

        try {
            rs = this.refresher.executeQuery();

            int numCols = rs.getMetaData().getColumnCount();

            if (rs.next()) {
                for (int i = 0; i < numCols; i++) {
                    byte[] val = rs.getBytes(i + 1);
                    rowToRefresh.setBytes(i, val == null || rs.wasNull() ? null : val);
                }
            } else {
                throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.12"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        getExceptionInterceptor());
            }
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    // ignore
                }
            }
        }
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        boolean ret = super.relative(rows);
        if (this.onInsertRow) {
            this.onInsertRow = false;
        }
        if (this.doingUpdates) {
            this.doingUpdates = false;
        }
        return ret;
    }

    private void resetInserter() throws SQLException {
        this.inserter.clearParameters();

        for (int i = 0; i < getMetadata().getFields().length; i++) {
            this.inserter.setNull(i + 1, 0);
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
    public void setResultSetConcurrency(int concurrencyFlag) {
        super.setResultSetConcurrency(concurrencyFlag);

        // TODO: FIXME: Issue warning when asked for updatable result set, but result set is not updatable
        //
        // if ((concurrencyFlag == CONCUR_UPDATABLE) && !isUpdatable()) {
        // java.sql.SQLWarning warning = new java.sql.SQLWarning(
        // NotUpdatable.NOT_UPDATABLE_MESSAGE);
        // }
    }

    /**
     * Reset UPDATE prepared statement to value in current row. This_Row MUST
     * point to current, valid row.
     *
     * @throws SQLException
     *             if an error occurs
     */
    protected void syncUpdate() throws SQLException {
        if (this.updater == null) {
            if (this.updateSQL == null) {
                generateStatements();
            }

            this.updater = (ClientPreparedStatement) getConnection().clientPrepareStatement(this.updateSQL);
            this.updater.getQueryBindings().setColumnDefinition(getMetadata());
        }

        Field[] fields = getMetadata().getFields();
        int numFields = fields.length;
        this.updater.clearParameters();

        for (int i = 0; i < numFields; i++) {
            if (this.thisRow.getBytes(i) != null) {
                switch (fields[i].getMysqlType()) {
                    case DATE:
                    case DATETIME:
                    case TIME:
                    case TIMESTAMP:
                        // TODO this is a temporary workaround until Bug#71143 "Calling ResultSet.updateRow should not set all field values in UPDATE" is fixed.
                        // We handle these types separately to avoid fractional seconds truncation (when sendFractionalSeconds=true)
                        // that happens for fields we don't touch with ResultSet.updateNN(). For those fields we should pass the value as is or,
                        // better don't put them into final updater statement as requested by Bug#71143.
                        this.updater.setString(i + 1, getString(i + 1));
                        break;
                    default:
                        this.updater.setObject(i + 1, getObject(i + 1), fields[i].getMysqlType());
                        break;
                }

            } else {
                this.updater.setNull(i + 1, 0);
            }
        }

        int numKeys = this.primaryKeyIndices.size();
        for (int i = 0; i < numKeys; i++) {
            int idx = this.primaryKeyIndices.get(i).intValue();
            setParamValue(this.updater, numFields + i + 1, this.thisRow, idx, fields[idx]);
        }
    }

    @Override
    public void updateRow() throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.isUpdatable) {
                throw new NotUpdatable(this.notUpdatableReason);
            }

            if (this.doingUpdates) {
                this.updater.executeUpdate();
                refreshRow(this.updater, this.thisRow);
                this.doingUpdates = false;
            } else if (this.onInsertRow) {
                throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.44"), getExceptionInterceptor());
            }

            // fixes calling updateRow() and then doing more updates on same row...
            syncUpdate();
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, java.io.InputStream x, int length) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateAsciiStream(int columnIndex, java.io.InputStream x, int length) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setAsciiStream(columnIndex, x, length);
            } else {
                this.inserter.setAsciiStream(columnIndex, x, length);
                this.thisRow.setBytes(columnIndex - 1, STREAM_DATA_MARKER);
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        updateBigDecimal(findColumn(columnLabel), x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setBigDecimal(columnIndex, x);
            } else {
                this.inserter.setBigDecimal(columnIndex, x);
                this.thisRow.setBytes(columnIndex - 1, x == null ? null : StringUtils.getBytes(x.toString()));
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateBinaryStream(String columnLabel, java.io.InputStream x, int length) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, java.io.InputStream x, int length) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setBinaryStream(columnIndex, x, length);
            } else {
                this.inserter.setBinaryStream(columnIndex, x, length);
                this.thisRow.setBytes(columnIndex - 1, x == null ? null : STREAM_DATA_MARKER);
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateBlob(String columnLabel, java.sql.Blob blob) throws SQLException {
        updateBlob(findColumn(columnLabel), blob);
    }

    @Override
    public void updateBlob(int columnIndex, java.sql.Blob blob) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setBlob(columnIndex, blob);
            } else {
                this.inserter.setBlob(columnIndex, blob);
                this.thisRow.setBytes(columnIndex - 1, blob == null ? null : STREAM_DATA_MARKER);
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        updateBoolean(findColumn(columnLabel), x);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setBoolean(columnIndex, x);
            } else {
                this.inserter.setBoolean(columnIndex, x);
                this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex));
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        updateByte(findColumn(columnLabel), x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setByte(columnIndex, x);
            } else {
                this.inserter.setByte(columnIndex, x);
                this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex));
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        updateBytes(findColumn(columnLabel), x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setBytes(columnIndex, x);
            } else {
                this.inserter.setBytes(columnIndex, x);
                this.thisRow.setBytes(columnIndex - 1, x);
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateCharacterStream(String columnLabel, java.io.Reader reader, int length) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, java.io.Reader x, int length) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setCharacterStream(columnIndex, x, length);
            } else {
                this.inserter.setCharacterStream(columnIndex, x, length);
                this.thisRow.setBytes(columnIndex - 1, x == null ? null : STREAM_DATA_MARKER);
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateClob(String columnLabel, Clob clob) throws SQLException {
        updateClob(findColumn(columnLabel), clob);
    }

    @Override
    public void updateClob(int columnIndex, java.sql.Clob clob) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (clob == null) {
                updateNull(columnIndex);
            } else {
                updateCharacterStream(columnIndex, clob.getCharacterStream(), (int) clob.length());
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateDate(String columnLabel, java.sql.Date x) throws SQLException {
        updateDate(findColumn(columnLabel), x);
    }

    @Override
    public void updateDate(int columnIndex, java.sql.Date x) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setDate(columnIndex, x);
            } else {
                this.inserter.setDate(columnIndex, x);
                this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex));
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        updateDouble(findColumn(columnLabel), x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setDouble(columnIndex, x);
            } else {
                this.inserter.setDouble(columnIndex, x);
                this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex));
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        updateFloat(findColumn(columnLabel), x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setFloat(columnIndex, x);
            } else {
                this.inserter.setFloat(columnIndex, x);
                this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex));
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        updateInt(findColumn(columnLabel), x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setInt(columnIndex, x);
            } else {
                this.inserter.setInt(columnIndex, x);
                this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex));
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        updateLong(findColumn(columnLabel), x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setLong(columnIndex, x);
            } else {
                this.inserter.setLong(columnIndex, x);
                this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex));
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        updateNull(findColumn(columnLabel));
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setNull(columnIndex, 0);
            } else {
                this.inserter.setNull(columnIndex, 0);
                this.thisRow.setBytes(columnIndex - 1, null);
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        updateObjectInternal(columnIndex, x, (Integer) null, 0);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scale) throws SQLException {
        updateObject(findColumn(columnLabel), x, scale);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
        updateObjectInternal(columnIndex, x, (Integer) null, scale);
    }

    /**
     * Internal setObject implementation. Although targetType is not part of default ResultSet methods signatures, it is used for type conversions from
     * JDBC42UpdatableResultSet new JDBC 4.2 updateObject() methods.
     *
     * @param columnIndex
     *            column index
     * @param x
     *            value
     * @param targetType
     *            target type
     * @param scaleOrLength
     *            scale or length, depending on target type
     * @throws SQLException
     *             if an error occurs
     */
    protected void updateObjectInternal(int columnIndex, Object x, Integer targetType, int scaleOrLength) throws SQLException {
        try {
            MysqlType targetMysqlType = targetType == null ? null : MysqlType.getByJdbcType(targetType);
            updateObjectInternal(columnIndex, x, targetMysqlType, scaleOrLength);

        } catch (FeatureNotAvailableException nae) {
            throw SQLError.createSQLFeatureNotSupportedException(Messages.getString("Statement.UnsupportedSQLType") + JDBCType.valueOf(targetType),
                    MysqlErrorNumbers.SQLSTATE_CONNJ_DRIVER_NOT_CAPABLE, getExceptionInterceptor());
        }
    }

    /**
     * Internal setObject implementation.
     *
     * @param columnIndex
     *            column index
     * @param x
     *            value
     * @param targetType
     *            target type
     * @param scaleOrLength
     *            scale or length, depending on target type
     * @throws SQLException
     *             if an error occurs
     */
    protected void updateObjectInternal(int columnIndex, Object x, SQLType targetType, int scaleOrLength) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                if (targetType == null) {
                    this.updater.setObject(columnIndex, x);
                } else {
                    this.updater.setObject(columnIndex, x, targetType);
                }
            } else {
                if (targetType == null) {
                    this.inserter.setObject(columnIndex, x);
                } else {
                    this.inserter.setObject(columnIndex, x, targetType);
                }

                this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex));
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        updateObject(findColumn(columnLabel), x, targetSqlType);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        updateObjectInternal(columnIndex, x, targetSqlType, 0);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        updateObject(findColumn(columnLabel), x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        updateObjectInternal(columnIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        updateShort(findColumn(columnLabel), x);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setShort(columnIndex, x);
            } else {
                this.inserter.setShort(columnIndex, x);
                this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex));
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        updateString(findColumn(columnLabel), x);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setString(columnIndex, x);
            } else {
                this.inserter.setString(columnIndex, x);
                this.thisRow.setBytes(columnIndex - 1, x == null ? null : StringUtils.getBytes(x, this.charEncoding));
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateTime(String columnLabel, java.sql.Time x) throws SQLException {
        updateTime(findColumn(columnLabel), x);
    }

    @Override
    public void updateTime(int columnIndex, java.sql.Time x) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setTime(columnIndex, x);
            } else {
                this.inserter.setTime(columnIndex, x);
                this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex));
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateTimestamp(String columnLabel, java.sql.Timestamp x) throws SQLException {
        updateTimestamp(findColumn(columnLabel), x);
    }

    @Override
    public void updateTimestamp(int columnIndex, java.sql.Timestamp x) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setTimestamp(columnIndex, x);
            } else {
                this.inserter.setTimestamp(columnIndex, x);
                this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex));
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setAsciiStream(columnIndex, x);
        } else {
            this.inserter.setAsciiStream(columnIndex, x);
            this.thisRow.setBytes(columnIndex - 1, STREAM_DATA_MARKER);
        }
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setAsciiStream(columnIndex, x, length);
        } else {
            this.inserter.setAsciiStream(columnIndex, x, length);
            this.thisRow.setBytes(columnIndex - 1, STREAM_DATA_MARKER);
        }
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setBinaryStream(columnIndex, x);
        } else {
            this.inserter.setBinaryStream(columnIndex, x);
            this.thisRow.setBytes(columnIndex - 1, x == null ? null : STREAM_DATA_MARKER);
        }
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setBinaryStream(columnIndex, x, length);
        } else {
            this.inserter.setBinaryStream(columnIndex, x, length);
            this.thisRow.setBytes(columnIndex - 1, x == null ? null : STREAM_DATA_MARKER);
        }
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        updateBlob(findColumn(columnLabel), inputStream);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setBlob(columnIndex, inputStream);
        } else {
            this.inserter.setBlob(columnIndex, inputStream);
            this.thisRow.setBytes(columnIndex - 1, inputStream == null ? null : STREAM_DATA_MARKER);
        }
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        updateBlob(findColumn(columnLabel), inputStream, length);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setBlob(columnIndex, inputStream, length);
        } else {
            this.inserter.setBlob(columnIndex, inputStream, length);
            this.thisRow.setBytes(columnIndex - 1, inputStream == null ? null : STREAM_DATA_MARKER);
        }
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setCharacterStream(columnIndex, x);
        } else {
            this.inserter.setCharacterStream(columnIndex, x);
            this.thisRow.setBytes(columnIndex - 1, x == null ? null : STREAM_DATA_MARKER);
        }
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setCharacterStream(columnIndex, x, length);
        } else {
            this.inserter.setCharacterStream(columnIndex, x, length);
            this.thisRow.setBytes(columnIndex - 1, x == null ? null : STREAM_DATA_MARKER);
        }
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        updateClob(findColumn(columnLabel), reader);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        updateCharacterStream(columnIndex, reader);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateClob(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        updateCharacterStream(columnIndex, reader, length);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        updateNCharacterStream(findColumn(columnLabel), reader);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        String fieldEncoding = getMetadata().getFields()[columnIndex - 1].getEncoding();
        if (fieldEncoding == null || !fieldEncoding.equals("UTF-8")) {
            throw new SQLException(Messages.getString("ResultSet.16"));
        }

        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setNCharacterStream(columnIndex, x);
        } else {
            this.inserter.setNCharacterStream(columnIndex, x);
            this.thisRow.setBytes(columnIndex - 1, x == null ? null : STREAM_DATA_MARKER);
        }
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        updateNCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            String fieldEncoding = getMetadata().getFields()[columnIndex - 1].getEncoding();
            if (fieldEncoding == null || !fieldEncoding.equals("UTF-8")) {
                throw new SQLException(Messages.getString("ResultSet.16"));
            }

            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setNCharacterStream(columnIndex, x, length);
            } else {
                this.inserter.setNCharacterStream(columnIndex, x, length);
                this.thisRow.setBytes(columnIndex - 1, x == null ? null : STREAM_DATA_MARKER);
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        updateNClob(findColumn(columnLabel), reader);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        String fieldEncoding = getMetadata().getFields()[columnIndex - 1].getEncoding();
        if (fieldEncoding == null || !fieldEncoding.equals("UTF-8")) {
            throw new SQLException(Messages.getString("ResultSet.17"));
        }
        updateCharacterStream(columnIndex, reader);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateNClob(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        String fieldEncoding = getMetadata().getFields()[columnIndex - 1].getEncoding();
        if (fieldEncoding == null || !fieldEncoding.equals("UTF-8")) {
            throw new SQLException(Messages.getString("ResultSet.17"));
        }
        updateCharacterStream(columnIndex, reader, length);
    }

    @Override
    public void updateNClob(String columnLabel, java.sql.NClob nClob) throws SQLException {
        updateNClob(findColumn(columnLabel), nClob);
    }

    @Override
    public void updateNClob(int columnIndex, java.sql.NClob nClob) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            String fieldEncoding = getMetadata().getFields()[columnIndex - 1].getEncoding();
            if (fieldEncoding == null || !fieldEncoding.equals("UTF-8")) {
                throw new SQLException(Messages.getString("ResultSet.17"));
            }

            if (nClob == null) {
                updateNull(columnIndex);
            } else {
                updateNCharacterStream(columnIndex, nClob.getCharacterStream(), (int) nClob.length());
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        updateSQLXML(findColumn(columnLabel), xmlObject);
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        updateString(columnIndex, ((MysqlSQLXML) xmlObject).getString());
    }

    @Override
    public void updateNString(String columnLabel, String x) throws SQLException {
        updateNString(findColumn(columnLabel), x);
    }

    @Override
    public void updateNString(int columnIndex, String x) throws SQLException {
        Lock connectionLock = checkClosed().getConnectionLock();
        connectionLock.lock();
        try {
            String fieldEncoding = getMetadata().getFields()[columnIndex - 1].getEncoding();
            if (fieldEncoding == null || !fieldEncoding.equals("UTF-8")) {
                throw new SQLException(Messages.getString("ResultSet.18"));
            }

            if (!this.onInsertRow) {
                if (!this.doingUpdates) {
                    this.doingUpdates = true;
                    syncUpdate();
                }

                this.updater.setNString(columnIndex, x);
            } else {
                this.inserter.setNString(columnIndex, x);
                this.thisRow.setBytes(columnIndex - 1, x == null ? null : StringUtils.getBytes(x, fieldEncoding));
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(findColumn(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        String fieldEncoding = getMetadata().getFields()[columnIndex - 1].getEncoding();
        if (fieldEncoding == null || !fieldEncoding.equals("UTF-8")) {
            throw new SQLException(Messages.getString("ResultSet.11"));
        }

        return getCharacterStream(columnIndex);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        String fieldEncoding = getMetadata().getFields()[columnIndex - 1].getEncoding();

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
    public String getNString(String columnLabel) throws SQLException {
        return getNString(findColumn(columnLabel));
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        String fieldEncoding = getMetadata().getFields()[columnIndex - 1].getEncoding();

        if (fieldEncoding == null || !fieldEncoding.equals("UTF-8")) {
            throw new SQLException("Can not call getNString() when field's charset isn't UTF-8");
        }

        return getString(columnIndex);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return new MysqlSQLXML(this, columnIndex, getExceptionInterceptor());
    }

    private String getStringForNClob(int columnIndex) throws SQLException {
        String asString = null;

        String forcedEncoding = "UTF-8";

        try {
            byte[] asBytes = null;

            asBytes = getBytes(columnIndex);

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
    public boolean isClosed() throws SQLException {
        return this.isClosed;
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
            throw SQLError.createSQLException("Unable to unwrap to " + iface.toString(), MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }
    }

}
