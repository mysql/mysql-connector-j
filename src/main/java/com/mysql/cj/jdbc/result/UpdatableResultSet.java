/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.jdbc.result;

import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.mysql.cj.api.ProfilerEvent;
import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.mysqla.result.ResultsetRows;
import com.mysql.cj.api.result.Row;
import com.mysql.cj.core.Constants;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.MysqlType;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.AssertionFailedException;
import com.mysql.cj.core.exceptions.FeatureNotAvailableException;
import com.mysql.cj.core.profiler.ProfilerEventHandlerFactory;
import com.mysql.cj.core.profiler.ProfilerEventImpl;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.jdbc.MysqlSQLXML;
import com.mysql.cj.jdbc.PreparedStatement;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.jdbc.exceptions.NotUpdatable;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqla.result.ByteArrayRow;

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
    private com.mysql.cj.jdbc.PreparedStatement deleter = null;

    private String deleteSQL = null;

    /** PreparedStatement used to insert data */
    protected com.mysql.cj.jdbc.PreparedStatement inserter = null;

    private String insertSQL = null;

    /** Is this result set updateable? */
    private boolean isUpdatable = false;

    /** Reason the result set is not updatable */
    private String notUpdatableReason = null;

    /** List of primary keys */
    private List<Integer> primaryKeyIndicies = null;

    private String qualifiedAndQuotedTableName;

    private String quotedIdChar = null;

    /** PreparedStatement used to refresh data */
    private com.mysql.cj.jdbc.PreparedStatement refresher;

    private String refreshSQL = null;

    /** The binary data for the 'current' row */
    private Row savedCurrentRow;

    /** PreparedStatement used to delete data */
    protected com.mysql.cj.jdbc.PreparedStatement updater = null;

    /** SQL for in-place modifcation */
    private String updateSQL = null;

    private boolean populateInserterWithDefaultValues = false;

    private boolean hasLongColumnInfo = false;

    private Map<String, Map<String, Map<String, Integer>>> databasesUsedToTablesUsed = null;

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
     */
    public UpdatableResultSet(ResultsetRows tuples, JdbcConnection conn, StatementImpl creatorStmt) throws SQLException {
        super(tuples, conn, creatorStmt);
        checkUpdatability();

        this.populateInserterWithDefaultValues = this.getConnection().getPropertySet()
                .getBooleanReadableProperty(PropertyDefinitions.PNAME_populateInsertRowWithDefaultValues).getValue();
        this.hasLongColumnInfo = this.getConnection().getSession().getServerSession().hasLongColumnInfo();
    }

    @Override
    public synchronized boolean absolute(int row) throws SQLException {
        return super.absolute(row);
    }

    @Override
    public synchronized void afterLast() throws SQLException {
        super.afterLast();
    }

    @Override
    public synchronized void beforeFirst() throws SQLException {
        super.beforeFirst();
    }

    @Override
    public synchronized void cancelRowUpdates() throws SQLException {
        checkClosed();

        if (this.doingUpdates) {
            this.doingUpdates = false;
            this.updater.clearParameters();
        }
    }

    @Override
    protected synchronized void checkRowPos() throws SQLException {
        checkClosed();

        if (!this.onInsertRow) {
            super.checkRowPos();
        }
    }

    /**
     * Is this ResultSet updateable?
     * 
     * @throws SQLException
     */
    public void checkUpdatability() throws SQLException {
        try {
            if (this.getMetadata() == null) {
                // we've been created to be populated with cached metadata, and we don't have the metadata yet, we'll be called again by
                // Connection.initializeResultsMetadataFromCache() when the metadata has been made available

                return;
            }

            String singleTableName = null;
            String catalogName = null;

            int primaryKeyCount = 0;

            Field[] fields = this.getMetadata().getFields();
            // We can only do this if we know that there is a currently selected database, or if we're talking to a > 4.1 version of MySQL server (as it returns
            // database names in field info)
            if ((this.catalog == null) || (this.catalog.length() == 0)) {
                this.catalog = fields[0].getDatabaseName();

                if ((this.catalog == null) || (this.catalog.length() == 0)) {
                    throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.43"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                            getExceptionInterceptor());
                }
            }

            if (fields.length > 0) {
                singleTableName = fields[0].getOriginalTableName();
                catalogName = fields[0].getDatabaseName();

                if (singleTableName == null) {
                    singleTableName = fields[0].getTableName();
                    catalogName = this.catalog;
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
                    String otherCatalogName = fields[i].getDatabaseName();

                    if (otherTableName == null) {
                        otherTableName = fields[i].getTableName();
                        otherCatalogName = this.catalog;
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
                    if ((catalogName == null) || !otherCatalogName.equals(catalogName)) {
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

            if (this.getConnection().getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_strictUpdates).getValue()) {
                java.sql.DatabaseMetaData dbmd = this.getConnection().getMetaData();

                java.sql.ResultSet rs = null;
                HashMap<String, String> primaryKeyNames = new HashMap<String, String>();

                try {
                    rs = dbmd.getPrimaryKeys(catalogName, null, singleTableName);

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
                    if (existingPrimaryKeysCount > 1) {
                        this.notUpdatableReason = Messages.getString("NotUpdatableReason.7");
                    } else {
                        this.notUpdatableReason = Messages.getString("NotUpdatableReason.4");
                    }

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
    public synchronized void deleteRow() throws SQLException {
        checkClosed();

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

            this.deleter = (PreparedStatement) this.getConnection().clientPrepareStatement(this.deleteSQL);
        }

        this.deleter.clearParameters();

        int numKeys = this.primaryKeyIndicies.size();

        if (numKeys == 1) {
            int index = this.primaryKeyIndicies.get(0).intValue();
            this.setParamValue(this.deleter, 1, this.thisRow, index, this.getMetadata().getFields()[index].getMysqlType());
        } else {
            for (int i = 0; i < numKeys; i++) {
                int index = this.primaryKeyIndicies.get(i).intValue();
                this.setParamValue(this.deleter, i + 1, this.thisRow, index, this.getMetadata().getFields()[index].getMysqlType());

            }
        }

        this.deleter.executeUpdate();
        this.rowData.remove();

        // position on previous row - Bug#27431
        previous();

    }

    private synchronized void setParamValue(PreparedStatement ps, int psIdx, Row row, int rsIdx, MysqlType mysqlType) throws SQLException {

        byte[] val = row.getBytes(rsIdx);
        if (val == null) {
            ps.setNull(psIdx, MysqlType.NULL);
            return;
        }
        switch (mysqlType) {
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
            case INT_UNSIGNED:
            case YEAR:
                ps.setInt(psIdx, getInt(rsIdx + 1));
                break;
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
            case DATETIME:
                ps.setTimestamp(psIdx, getTimestamp(rsIdx + 1));
                break;
            case TIME:
                ps.setTime(psIdx, getTime(rsIdx + 1));
                break;
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case FLOAT:
            case FLOAT_UNSIGNED:
            case BOOLEAN:
                ps.setBytesNoEscapeNoQuotes(psIdx, val);
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

    private synchronized void extractDefaultValues() throws SQLException {
        java.sql.DatabaseMetaData dbmd = this.getConnection().getMetaData();
        this.defaultColumnValue = new byte[this.getMetadata().getFields().length][];

        java.sql.ResultSet columnsResultSet = null;

        for (Map.Entry<String, Map<String, Map<String, Integer>>> dbEntry : this.databasesUsedToTablesUsed.entrySet()) {
            for (Map.Entry<String, Map<String, Integer>> tableEntry : dbEntry.getValue().entrySet()) {
                String tableName = tableEntry.getKey();
                Map<String, Integer> columnNamesToIndices = tableEntry.getValue();

                try {
                    columnsResultSet = dbmd.getColumns(this.catalog, null, tableName, "%");

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
    public synchronized boolean first() throws SQLException {
        return super.first();
    }

    /**
     * Figure out whether or not this ResultSet is updateable, and if so,
     * generate the PreparedStatements to support updates.
     * 
     * @throws SQLException
     * @throws NotUpdatable
     */
    protected synchronized void generateStatements() throws SQLException {
        if (!this.isUpdatable) {
            this.doingUpdates = false;
            this.onInsertRow = false;

            throw new NotUpdatable(this.notUpdatableReason);
        }

        String quotedId = getQuotedIdChar();

        Map<String, String> tableNamesSoFar = null;

        if (this.getConnection().lowerCaseTableNames()) {
            tableNamesSoFar = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
            this.databasesUsedToTablesUsed = new TreeMap<String, Map<String, Map<String, Integer>>>(String.CASE_INSENSITIVE_ORDER);
        } else {
            tableNamesSoFar = new TreeMap<String, String>();
            this.databasesUsedToTablesUsed = new TreeMap<String, Map<String, Map<String, Integer>>>();
        }

        this.primaryKeyIndicies = new ArrayList<Integer>();

        StringBuilder fieldValues = new StringBuilder();
        StringBuilder keyValues = new StringBuilder();
        StringBuilder columnNames = new StringBuilder();
        StringBuilder insertPlaceHolders = new StringBuilder();
        StringBuilder allTablesBuf = new StringBuilder();
        Map<Integer, String> columnIndicesToTable = new HashMap<Integer, String>();

        boolean firstTime = true;
        boolean keysFirstTime = true;

        Field[] fields = this.getMetadata().getFields();

        for (int i = 0; i < fields.length; i++) {
            StringBuilder tableNameBuffer = new StringBuilder();
            Map<String, Integer> updColumnNameToIndex = null;

            // FIXME: What about no table?
            if (fields[i].getOriginalTableName() != null) {

                String databaseName = fields[i].getDatabaseName();

                if ((databaseName != null) && (databaseName.length() > 0)) {
                    tableNameBuffer.append(quotedId);
                    tableNameBuffer.append(databaseName);
                    tableNameBuffer.append(quotedId);
                    tableNameBuffer.append('.');
                }

                String tableOnlyName = fields[i].getOriginalTableName();

                tableNameBuffer.append(quotedId);
                tableNameBuffer.append(tableOnlyName);
                tableNameBuffer.append(quotedId);

                String fqTableName = tableNameBuffer.toString();

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
                    tableNameBuffer.append(quotedId);
                    tableNameBuffer.append(tableOnlyName);
                    tableNameBuffer.append(quotedId);

                    String fqTableName = tableNameBuffer.toString();

                    if (!tableNamesSoFar.containsKey(fqTableName)) {
                        if (!tableNamesSoFar.isEmpty()) {
                            allTablesBuf.append(',');
                        }

                        allTablesBuf.append(fqTableName);
                        tableNamesSoFar.put(fqTableName, fqTableName);
                    }

                    columnIndicesToTable.put(Integer.valueOf(i), fqTableName);

                    updColumnNameToIndex = getColumnsToIndexMapForTableAndDB(this.catalog, tableOnlyName);
                }
            }

            String originalColumnName = fields[i].getOriginalName();
            String columnName = null;

            if (this.hasLongColumnInfo && originalColumnName != null && originalColumnName.length() > 0) {
                columnName = originalColumnName;
            } else {
                columnName = fields[i].getName();
            }

            if (updColumnNameToIndex != null && columnName != null) {
                updColumnNameToIndex.put(columnName, Integer.valueOf(i));
            }

            String originalTableName = fields[i].getOriginalTableName();
            String tableName = null;

            if (this.hasLongColumnInfo && originalTableName != null && originalTableName.length() > 0) {
                tableName = originalTableName;
            } else {
                tableName = fields[i].getTableName();
            }

            StringBuilder fqcnBuf = new StringBuilder();
            String databaseName = fields[i].getDatabaseName();

            if (databaseName != null && databaseName.length() > 0) {
                fqcnBuf.append(quotedId);
                fqcnBuf.append(databaseName);
                fqcnBuf.append(quotedId);
                fqcnBuf.append('.');
            }

            fqcnBuf.append(quotedId);
            fqcnBuf.append(tableName);
            fqcnBuf.append(quotedId);
            fqcnBuf.append('.');
            fqcnBuf.append(quotedId);
            fqcnBuf.append(columnName);
            fqcnBuf.append(quotedId);

            String qualifiedColumnName = fqcnBuf.toString();

            if (fields[i].isPrimaryKey()) {
                this.primaryKeyIndicies.add(Integer.valueOf(i));

                if (!keysFirstTime) {
                    keyValues.append(" AND ");
                } else {
                    keysFirstTime = false;
                }

                keyValues.append(qualifiedColumnName);
                keyValues.append("<=>");
                keyValues.append("?");
            }

            if (firstTime) {
                firstTime = false;
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
            if (this.getConnection().lowerCaseTableNames()) {
                tablesUsedToColumnsMap = new TreeMap<String, Map<String, Integer>>(String.CASE_INSENSITIVE_ORDER);
            } else {
                tablesUsedToColumnsMap = new TreeMap<String, Map<String, Integer>>();
            }

            this.databasesUsedToTablesUsed.put(databaseName, tablesUsedToColumnsMap);
        }

        nameToIndex = tablesUsedToColumnsMap.get(tableName);

        if (nameToIndex == null) {
            nameToIndex = new HashMap<String, Integer>();
            tablesUsedToColumnsMap.put(tableName, nameToIndex);
        }

        return nameToIndex;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return (this.isUpdatable ? CONCUR_UPDATABLE : CONCUR_READ_ONLY);
    }

    private synchronized String getQuotedIdChar() throws SQLException {
        if (this.quotedIdChar == null) {
            java.sql.DatabaseMetaData dbmd = this.getConnection().getMetaData();
            this.quotedIdChar = dbmd.getIdentifierQuoteString();
        }

        return this.quotedIdChar;
    }

    @Override
    public synchronized void insertRow() throws SQLException {
        checkClosed();

        if (!this.onInsertRow) {
            throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.7"), getExceptionInterceptor());
        }

        this.inserter.executeUpdate();

        long autoIncrementId = this.inserter.getLastInsertID();
        Field[] fields = this.getMetadata().getFields();
        int numFields = fields.length;
        byte[][] newRow = new byte[numFields][];

        for (int i = 0; i < numFields; i++) {
            if (this.inserter.isNull(i)) {
                newRow[i] = null;
            } else {
                newRow[i] = this.inserter.getBytesRepresentation(i);
            }

            // WARN: This non-variant only holds if MySQL never allows more than one auto-increment key (which is the way it is _today_)
            if (fields[i].isAutoIncrement() && autoIncrementId > 0) {
                newRow[i] = StringUtils.getBytes(String.valueOf(autoIncrementId));
                this.inserter.setBytesNoEscapeNoQuotes(i + 1, newRow[i]);
            }
        }

        Row resultSetRow = new ByteArrayRow(newRow, getExceptionInterceptor());

        refreshRow(this.inserter, resultSetRow);

        this.rowData.addRow(resultSetRow);
        resetInserter();
    }

    @Override
    public synchronized boolean isAfterLast() throws SQLException {
        return super.isAfterLast();
    }

    @Override
    public synchronized boolean isBeforeFirst() throws SQLException {
        return super.isBeforeFirst();
    }

    @Override
    public synchronized boolean isFirst() throws SQLException {
        return super.isFirst();
    }

    @Override
    public synchronized boolean isLast() throws SQLException {
        return super.isLast();
    }

    boolean isUpdatable() {
        return this.isUpdatable;
    }

    @Override
    public synchronized boolean last() throws SQLException {
        return super.last();
    }

    @Override
    public synchronized void moveToCurrentRow() throws SQLException {
        checkClosed();

        if (!this.isUpdatable) {
            throw new NotUpdatable(this.notUpdatableReason);
        }

        if (this.onInsertRow) {
            this.onInsertRow = false;
            this.thisRow = this.savedCurrentRow;
        }
    }

    @Override
    public synchronized void moveToInsertRow() throws SQLException {
        checkClosed();

        if (!this.isUpdatable) {
            throw new NotUpdatable(this.notUpdatableReason);
        }

        if (this.inserter == null) {
            if (this.insertSQL == null) {
                generateStatements();
            }

            this.inserter = (PreparedStatement) this.getConnection().clientPrepareStatement(this.insertSQL);
            if (this.populateInserterWithDefaultValues) {
                extractDefaultValues();
            }

            resetInserter();
        } else {
            resetInserter();
        }

        Field[] fields = this.getMetadata().getFields();
        int numFields = fields.length;

        this.onInsertRow = true;
        this.doingUpdates = false;
        this.savedCurrentRow = this.thisRow;
        byte[][] newRowData = new byte[numFields][];
        this.thisRow = new ByteArrayRow(newRowData, getExceptionInterceptor());
        this.thisRow.setMetadata(this.getMetadata());

        for (int i = 0; i < numFields; i++) {
            if (!this.populateInserterWithDefaultValues) {
                this.inserter.setBytesNoEscapeNoQuotes(i + 1, StringUtils.getBytes("DEFAULT"));
                newRowData = null;
            } else {
                if (this.defaultColumnValue[i] != null) {
                    Field f = fields[i];

                    switch (f.getMysqlTypeId()) {
                        case MysqlaConstants.FIELD_TYPE_DATE:
                        case MysqlaConstants.FIELD_TYPE_DATETIME:
                        case MysqlaConstants.FIELD_TYPE_TIME:
                        case MysqlaConstants.FIELD_TYPE_TIMESTAMP:

                            if (this.defaultColumnValue[i].length > 7 && this.defaultColumnValue[i][0] == (byte) 'C'
                                    && this.defaultColumnValue[i][1] == (byte) 'U' && this.defaultColumnValue[i][2] == (byte) 'R'
                                    && this.defaultColumnValue[i][3] == (byte) 'R' && this.defaultColumnValue[i][4] == (byte) 'E'
                                    && this.defaultColumnValue[i][5] == (byte) 'N' && this.defaultColumnValue[i][6] == (byte) 'T'
                                    && this.defaultColumnValue[i][7] == (byte) '_') {
                                this.inserter.setBytesNoEscapeNoQuotes(i + 1, this.defaultColumnValue[i]);

                            } else {
                                this.inserter.setBytes(i + 1, this.defaultColumnValue[i], false, false);
                            }
                            break;

                        default:
                            this.inserter.setBytes(i + 1, this.defaultColumnValue[i], false, false);
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
        }
    }

    @Override
    public synchronized boolean next() throws SQLException {
        return super.next();
    }

    @Override
    public synchronized boolean prev() throws SQLException {
        return super.prev();
    }

    @Override
    public synchronized boolean previous() throws SQLException {
        return super.previous();
    }

    /**
     * Closes this ResultSet and releases resources.
     * 
     * @param calledExplicitly
     *            was realClose called by the standard ResultSet.close() method, or was it closed internally by the
     *            driver?
     * 
     * @throws SQLException
     *             if an error occurs.
     */
    @Override
    public synchronized void realClose(boolean calledExplicitly) throws SQLException {
        if (this.isClosed) {
            return;
        }

        SQLException sqlEx = null;

        if (this.useUsageAdvisor) {
            if ((this.deleter == null) && (this.inserter == null) && (this.refresher == null) && (this.updater == null)) {
                this.eventSink = ProfilerEventHandlerFactory.getInstance(this.session);

                String message = Messages.getString("UpdatableResultSet.34");

                this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_WARN, "",
                        (this.getOwningStatement() == null) ? "N/A" : this.getOwningStatement().getCurrentCatalog(), this.getConnectionId(),
                        (this.getOwningStatement() == null) ? (-1) : this.getOwningStatement().getId(), this.resultId, System.currentTimeMillis(), 0,
                        Constants.MILLIS_I18N, null, this.getPointOfOrigin(), message));
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

        super.realClose(calledExplicitly);

        if (sqlEx != null) {
            throw sqlEx;
        }
    }

    @Override
    public synchronized void refreshRow() throws SQLException {
        checkClosed();

        if (!this.isUpdatable) {
            throw SQLError.notUpdatable();
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
    }

    private synchronized void refreshRow(PreparedStatement updateInsertStmt, Row rowToRefresh) throws SQLException {
        if (this.refresher == null) {
            if (this.refreshSQL == null) {
                generateStatements();
            }

            this.refresher = (PreparedStatement) this.getConnection().clientPrepareStatement(this.refreshSQL);
        }

        this.refresher.clearParameters();

        int numKeys = this.primaryKeyIndicies.size();

        if (numKeys == 1) {
            byte[] dataFrom = null;
            int index = this.primaryKeyIndicies.get(0).intValue();

            if (!this.doingUpdates && !this.onInsertRow) {
                dataFrom = rowToRefresh.getBytes(index);
            } else {
                dataFrom = updateInsertStmt.getBytesRepresentation(index);

                // Primary keys not set?
                if (updateInsertStmt.isNull(index) || (dataFrom.length == 0)) {
                    dataFrom = rowToRefresh.getBytes(index);
                } else {
                    dataFrom = stripBinaryPrefix(dataFrom);
                }
            }

            if (this.getMetadata().getFields()[index].getValueNeedsQuoting()) {
                this.refresher.setBytesNoEscape(1, dataFrom);
            } else {
                this.refresher.setBytesNoEscapeNoQuotes(1, dataFrom);
            }

        } else {
            for (int i = 0; i < numKeys; i++) {
                byte[] dataFrom = null;
                int index = this.primaryKeyIndicies.get(i).intValue();

                if (!this.doingUpdates && !this.onInsertRow) {
                    dataFrom = rowToRefresh.getBytes(index);
                } else {
                    dataFrom = updateInsertStmt.getBytesRepresentation(index);

                    // Primary keys not set?
                    if (updateInsertStmt.isNull(index) || (dataFrom.length == 0)) {
                        dataFrom = rowToRefresh.getBytes(index);
                    } else {
                        dataFrom = stripBinaryPrefix(dataFrom);
                    }
                }

                this.refresher.setBytesNoEscape(i + 1, dataFrom);
            }
        }

        java.sql.ResultSet rs = null;

        try {
            rs = this.refresher.executeQuery();

            int numCols = rs.getMetaData().getColumnCount();

            if (rs.next()) {
                for (int i = 0; i < numCols; i++) {
                    byte[] val = rs.getBytes(i + 1);

                    if ((val == null) || rs.wasNull()) {
                        rowToRefresh.setBytes(i, null);
                    } else {
                        rowToRefresh.setBytes(i, rs.getBytes(i + 1));
                    }
                }
            } else {
                throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.12"), SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
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
    public synchronized boolean relative(int rows) throws SQLException {
        return super.relative(rows);
    }

    private void resetInserter() throws SQLException {
        this.inserter.clearParameters();

        for (int i = 0; i < this.getMetadata().getFields().length; i++) {
            this.inserter.setNull(i + 1, 0);
        }
    }

    @Override
    public synchronized boolean rowDeleted() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public synchronized boolean rowInserted() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public synchronized boolean rowUpdated() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void setResultSetConcurrency(int concurrencyFlag) {
        super.setResultSetConcurrency(concurrencyFlag);

        // TODO: FIXME: Issue warning when asked for updateable result set, but result set is not updatable
        //
        // if ((concurrencyFlag == CONCUR_UPDATABLE) && !isUpdatable()) {
        // java.sql.SQLWarning warning = new java.sql.SQLWarning(
        // NotUpdatable.NOT_UPDATEABLE_MESSAGE);
        // }
    }

    private byte[] stripBinaryPrefix(byte[] dataFrom) {
        return StringUtils.stripEnclosure(dataFrom, "_binary'", "'");
    }

    /**
     * Reset UPDATE prepared statement to value in current row. This_Row MUST
     * point to current, valid row.
     * 
     * @throws SQLException
     */
    protected synchronized void syncUpdate() throws SQLException {
        if (this.updater == null) {
            if (this.updateSQL == null) {
                generateStatements();
            }

            this.updater = (PreparedStatement) this.getConnection().clientPrepareStatement(this.updateSQL);
        }

        Field[] fields = this.getMetadata().getFields();
        int numFields = fields.length;
        this.updater.clearParameters();

        for (int i = 0; i < numFields; i++) {
            if (this.thisRow.getBytes(i) != null) {

                if (fields[i].getValueNeedsQuoting()) {
                    this.updater.setBytes(i + 1, this.thisRow.getBytes(i), fields[i].isBinary(), false);
                } else {
                    this.updater.setBytesNoEscapeNoQuotes(i + 1, this.thisRow.getBytes(i));
                }
            } else {
                this.updater.setNull(i + 1, 0);
            }
        }

        int numKeys = this.primaryKeyIndicies.size();

        if (numKeys == 1) {
            int index = this.primaryKeyIndicies.get(0).intValue();
            this.setParamValue(this.updater, numFields + 1, this.thisRow, index, fields[index].getMysqlType());
        } else {
            for (int i = 0; i < numKeys; i++) {
                int idx = this.primaryKeyIndicies.get(i).intValue();
                this.setParamValue(this.updater, numFields + i + 1, this.thisRow, idx, fields[idx].getMysqlType());
            }
        }
    }

    @Override
    public synchronized void updateAsciiStream(int columnIndex, java.io.InputStream x, int length) throws SQLException {
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
    public synchronized void updateAsciiStream(String columnName, java.io.InputStream x, int length) throws SQLException {
        updateAsciiStream(findColumn(columnName), x, length);
    }

    @Override
    public synchronized void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setBigDecimal(columnIndex, x);
        } else {
            this.inserter.setBigDecimal(columnIndex, x);

            if (x == null) {
                this.thisRow.setBytes(columnIndex - 1, null);
            } else {
                this.thisRow.setBytes(columnIndex - 1, StringUtils.getBytes(x.toString()));
            }
        }
    }

    @Override
    public synchronized void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
        updateBigDecimal(findColumn(columnName), x);
    }

    @Override
    public synchronized void updateBinaryStream(int columnIndex, java.io.InputStream x, int length) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setBinaryStream(columnIndex, x, length);
        } else {
            this.inserter.setBinaryStream(columnIndex, x, length);

            if (x == null) {
                this.thisRow.setBytes(columnIndex - 1, null);
            } else {
                this.thisRow.setBytes(columnIndex - 1, STREAM_DATA_MARKER);
            }
        }
    }

    @Override
    public synchronized void updateBinaryStream(String columnName, java.io.InputStream x, int length) throws SQLException {
        updateBinaryStream(findColumn(columnName), x, length);
    }

    @Override
    public synchronized void updateBlob(int columnIndex, java.sql.Blob blob) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setBlob(columnIndex, blob);
        } else {
            this.inserter.setBlob(columnIndex, blob);

            if (blob == null) {
                this.thisRow.setBytes(columnIndex - 1, null);
            } else {
                this.thisRow.setBytes(columnIndex - 1, STREAM_DATA_MARKER);
            }
        }
    }

    @Override
    public synchronized void updateBlob(String columnName, java.sql.Blob blob) throws SQLException {
        updateBlob(findColumn(columnName), blob);
    }

    @Override
    public synchronized void updateBoolean(int columnIndex, boolean x) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setBoolean(columnIndex, x);
        } else {
            this.inserter.setBoolean(columnIndex, x);

            this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex - 1));
        }
    }

    @Override
    public synchronized void updateBoolean(String columnName, boolean x) throws SQLException {
        updateBoolean(findColumn(columnName), x);
    }

    @Override
    public synchronized void updateByte(int columnIndex, byte x) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setByte(columnIndex, x);
        } else {
            this.inserter.setByte(columnIndex, x);

            this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex - 1));
        }
    }

    @Override
    public synchronized void updateByte(String columnName, byte x) throws SQLException {
        updateByte(findColumn(columnName), x);
    }

    @Override
    public synchronized void updateBytes(int columnIndex, byte[] x) throws SQLException {
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
    }

    @Override
    public synchronized void updateBytes(String columnName, byte[] x) throws SQLException {
        updateBytes(findColumn(columnName), x);
    }

    @Override
    public synchronized void updateCharacterStream(int columnIndex, java.io.Reader x, int length) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setCharacterStream(columnIndex, x, length);
        } else {
            this.inserter.setCharacterStream(columnIndex, x, length);

            if (x == null) {
                this.thisRow.setBytes(columnIndex - 1, null);
            } else {
                this.thisRow.setBytes(columnIndex - 1, STREAM_DATA_MARKER);
            }
        }
    }

    @Override
    public synchronized void updateCharacterStream(String columnName, java.io.Reader reader, int length) throws SQLException {
        updateCharacterStream(findColumn(columnName), reader, length);
    }

    @Override
    public void updateClob(int columnIndex, java.sql.Clob clob) throws SQLException {
        if (clob == null) {
            updateNull(columnIndex);
        } else {
            updateCharacterStream(columnIndex, clob.getCharacterStream(), (int) clob.length());
        }
    }

    @Override
    public synchronized void updateDate(int columnIndex, java.sql.Date x) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setDate(columnIndex, x);
        } else {
            this.inserter.setDate(columnIndex, x);

            this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex - 1));
        }
    }

    @Override
    public synchronized void updateDate(String columnName, java.sql.Date x) throws SQLException {
        updateDate(findColumn(columnName), x);
    }

    @Override
    public synchronized void updateDouble(int columnIndex, double x) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setDouble(columnIndex, x);
        } else {
            this.inserter.setDouble(columnIndex, x);

            this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex - 1));
        }
    }

    @Override
    public synchronized void updateDouble(String columnName, double x) throws SQLException {
        updateDouble(findColumn(columnName), x);
    }

    @Override
    public synchronized void updateFloat(int columnIndex, float x) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setFloat(columnIndex, x);
        } else {
            this.inserter.setFloat(columnIndex, x);

            this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex - 1));
        }
    }

    @Override
    public synchronized void updateFloat(String columnName, float x) throws SQLException {
        updateFloat(findColumn(columnName), x);
    }

    @Override
    public synchronized void updateInt(int columnIndex, int x) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setInt(columnIndex, x);
        } else {
            this.inserter.setInt(columnIndex, x);

            this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex - 1));
        }
    }

    @Override
    public synchronized void updateInt(String columnName, int x) throws SQLException {
        updateInt(findColumn(columnName), x);
    }

    @Override
    public synchronized void updateLong(int columnIndex, long x) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setLong(columnIndex, x);
        } else {
            this.inserter.setLong(columnIndex, x);

            this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex - 1));
        }
    }

    @Override
    public synchronized void updateLong(String columnName, long x) throws SQLException {
        updateLong(findColumn(columnName), x);
    }

    @Override
    public synchronized void updateNull(int columnIndex) throws SQLException {
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
    }

    @Override
    public synchronized void updateNull(String columnName) throws SQLException {
        updateNull(findColumn(columnName));
    }

    @Override
    public synchronized void updateObject(int columnIndex, Object x) throws SQLException {
        updateObjectInternal(columnIndex, x, (Integer) null, 0);
    }

    @Override
    public synchronized void updateObject(int columnIndex, Object x, int scale) throws SQLException {
        updateObjectInternal(columnIndex, x, (Integer) null, scale);
    }

    /**
     * Internal setObject implementation. Although targetType is not part of default ResultSet methods signatures, it is used for type conversions from
     * JDBC42UpdatableResultSet new JDBC 4.2 updateObject() methods.
     * 
     * @param columnIndex
     * @param x
     * @param targetType
     * @param scaleOrLength
     * @throws SQLException
     */
    protected synchronized void updateObjectInternal(int columnIndex, Object x, Integer targetType, int scaleOrLength) throws SQLException {
        try {
            MysqlType targetMysqlType = targetType == null ? null : MysqlType.getByJdbcType(targetType);
            updateObjectInternal(columnIndex, x, targetMysqlType, scaleOrLength);

        } catch (FeatureNotAvailableException nae) {
            throw SQLError.createSQLFeatureNotSupportedException(Messages.getString("Statement.UnsupportedSQLType") + JDBCType.valueOf(targetType),
                    SQLError.SQL_STATE_DRIVER_NOT_CAPABLE, getExceptionInterceptor());
        }
    }

    /**
     * Internal setObject implementation.
     * 
     * @param columnIndex
     * @param x
     * @param targetType
     * @param scaleOrLength
     * @throws SQLException
     */
    protected synchronized void updateObjectInternal(int columnIndex, Object x, SQLType targetType, int scaleOrLength) throws SQLException {
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

            this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex - 1));
        }
    }

    @Override
    public synchronized void updateObject(String columnName, Object x) throws SQLException {
        updateObject(findColumn(columnName), x);
    }

    @Override
    public synchronized void updateObject(String columnName, Object x, int scale) throws SQLException {
        updateObject(findColumn(columnName), x);
    }

    @Override
    public synchronized void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        updateObjectInternal(columnIndex, x, targetSqlType, 0);
    }

    @Override
    public synchronized void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        updateObjectInternal(columnIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public synchronized void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        updateObjectInternal(findColumn(columnLabel), x, targetSqlType, 0);
    }

    @Override
    public synchronized void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        updateObjectInternal(findColumn(columnLabel), x, targetSqlType, scaleOrLength);
    }

    @Override
    public synchronized void updateRow() throws SQLException {
        if (!this.isUpdatable) {
            throw new NotUpdatable(this.notUpdatableReason);
        }

        if (this.doingUpdates) {
            this.updater.executeUpdate();
            refreshRow();
            this.doingUpdates = false;
        } else if (this.onInsertRow) {
            throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.44"), getExceptionInterceptor());
        }

        // fixes calling updateRow() and then doing more updates on same row...
        syncUpdate();
    }

    @Override
    public synchronized void updateShort(int columnIndex, short x) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setShort(columnIndex, x);
        } else {
            this.inserter.setShort(columnIndex, x);

            this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex - 1));
        }
    }

    @Override
    public synchronized void updateShort(String columnName, short x) throws SQLException {
        updateShort(findColumn(columnName), x);
    }

    @Override
    public synchronized void updateString(int columnIndex, String x) throws SQLException {
        checkClosed();

        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setString(columnIndex, x);
        } else {
            this.inserter.setString(columnIndex, x);

            if (x == null) {
                this.thisRow.setBytes(columnIndex - 1, null);
            } else {
                this.thisRow.setBytes(columnIndex - 1, StringUtils.getBytes(x, this.charEncoding));
            }
        }
    }

    @Override
    public synchronized void updateString(String columnName, String x) throws SQLException {
        updateString(findColumn(columnName), x);
    }

    @Override
    public synchronized void updateTime(int columnIndex, java.sql.Time x) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setTime(columnIndex, x);
        } else {
            this.inserter.setTime(columnIndex, x);

            this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex - 1));
        }
    }

    @Override
    public synchronized void updateTime(String columnName, java.sql.Time x) throws SQLException {
        updateTime(findColumn(columnName), x);
    }

    @Override
    public synchronized void updateTimestamp(int columnIndex, java.sql.Timestamp x) throws SQLException {
        if (!this.onInsertRow) {
            if (!this.doingUpdates) {
                this.doingUpdates = true;
                syncUpdate();
            }

            this.updater.setTimestamp(columnIndex, x);
        } else {
            this.inserter.setTimestamp(columnIndex, x);

            this.thisRow.setBytes(columnIndex - 1, this.inserter.getBytesRepresentation(columnIndex - 1));
        }
    }

    @Override
    public synchronized void updateTimestamp(String columnName, java.sql.Timestamp x) throws SQLException {
        updateTimestamp(findColumn(columnName), x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw SQLError.notUpdatable();

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw SQLError.notUpdatable();

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw SQLError.notUpdatable();

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw SQLError.notUpdatable();

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw SQLError.notUpdatable();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw SQLError.notUpdatable();

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw SQLError.notUpdatable();

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw SQLError.notUpdatable();

    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw SQLError.notUpdatable();

    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw SQLError.notUpdatable();

    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw SQLError.notUpdatable();

    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        updateNCharacterStream(columnIndex, x, (int) length);

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw SQLError.notUpdatable();

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw SQLError.notUpdatable();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw SQLError.notUpdatable();

    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw SQLError.notUpdatable();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        updateBlob(findColumn(columnLabel), inputStream);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        updateBlob(findColumn(columnLabel), inputStream, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        updateClob(findColumn(columnLabel), reader);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateClob(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        updateNCharacterStream(findColumn(columnLabel), reader);

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        updateNCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        updateNClob(findColumn(columnLabel), reader);

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateNClob(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        updateSQLXML(findColumn(columnLabel), xmlObject);

    }

    @Override
    public synchronized void updateNCharacterStream(int columnIndex, java.io.Reader x, int length) throws SQLException {
        String fieldEncoding = this.getMetadata().getFields()[columnIndex - 1].getEncoding();
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

            if (x == null) {
                this.thisRow.setBytes(columnIndex - 1, null);
            } else {
                this.thisRow.setBytes(columnIndex - 1, STREAM_DATA_MARKER);
            }
        }
    }

    @Override
    public synchronized void updateNCharacterStream(String columnName, java.io.Reader reader, int length) throws SQLException {
        updateNCharacterStream(findColumn(columnName), reader, length);
    }

    @Override
    public void updateNClob(int columnIndex, java.sql.NClob nClob) throws SQLException {
        String fieldEncoding = this.getMetadata().getFields()[columnIndex - 1].getEncoding();
        if (fieldEncoding == null || !fieldEncoding.equals("UTF-8")) {
            throw new SQLException(Messages.getString("ResultSet.17"));
        }

        if (nClob == null) {
            updateNull(columnIndex);
        } else {
            updateNCharacterStream(columnIndex, nClob.getCharacterStream(), (int) nClob.length());
        }
    }

    @Override
    public void updateNClob(String columnName, java.sql.NClob nClob) throws SQLException {
        updateNClob(findColumn(columnName), nClob);
    }

    @Override
    public synchronized void updateNString(int columnIndex, String x) throws SQLException {
        String fieldEncoding = this.getMetadata().getFields()[columnIndex - 1].getEncoding();
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

            if (x == null) {
                this.thisRow.setBytes(columnIndex - 1, null);
            } else {
                this.thisRow.setBytes(columnIndex - 1, StringUtils.getBytes(x, fieldEncoding));
            }
        }
    }

    @Override
    public synchronized void updateNString(String columnName, String x) throws SQLException {
        updateNString(findColumn(columnName), x);
    }

    @Override
    public int getHoldability() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        String fieldEncoding = this.getMetadata().getFields()[columnIndex - 1].getEncoding();
        if (fieldEncoding == null || !fieldEncoding.equals("UTF-8")) {
            throw new SQLException(Messages.getString("ResultSet.11"));
        }

        return getCharacterStream(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnName) throws SQLException {
        return getNCharacterStream(findColumn(columnName));
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        String fieldEncoding = this.getMetadata().getFields()[columnIndex - 1].getEncoding();

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

    @Override
    public String getNString(int columnIndex) throws SQLException {
        String fieldEncoding = this.getMetadata().getFields()[columnIndex - 1].getEncoding();

        if (fieldEncoding == null || !fieldEncoding.equals("UTF-8")) {
            throw new SQLException("Can not call getNString() when field's charset isn't UTF-8");
        }

        return getString(columnIndex);
    }

    // The following routines simply convert the columnName into a columnIndex and then call the appropriate routine above.
    @Override
    public String getNString(String columnName) throws SQLException {
        return getNString(findColumn(columnName));
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return getRowId(findColumn(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return new MysqlSQLXML(this, columnIndex, getExceptionInterceptor());
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
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
            throw SQLError.createSQLException("Unsupported character encoding " + forcedEncoding, SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }

        return asString;
    }

    @Override
    public synchronized boolean isClosed() throws SQLException {
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
            throw SQLError.createSQLException("Unable to unwrap to " + iface.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }
    }

}
