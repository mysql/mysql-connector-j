/*
 Copyright  2002-2007 MySQL AB, 2008 Sun Microsystems

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL
 as it is applied to this software. View the full text of the
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA



 */
package com.mysql.jdbc;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.mysql.jdbc.profiler.ProfilerEvent;
import com.mysql.jdbc.profiler.ProfilerEventHandlerFactory;

/**
 * A result set that is updatable.
 *
 * @author Mark Matthews
 */
public class UpdatableResultSet extends ResultSetImpl {
	/** Marker for 'stream' data when doing INSERT rows */
	protected final static byte[] STREAM_DATA_MARKER = "** STREAM DATA **" //$NON-NLS-1$
	.getBytes();

	protected SingleByteCharsetConverter charConverter;

	private String charEncoding;

	/** What is the default value for the column? */
	private byte[][] defaultColumnValue;

	/** PreparedStatement used to delete data */
	private com.mysql.jdbc.PreparedStatement deleter = null;

	private String deleteSQL = null;

	private boolean initializedCharConverter = false;

	/** PreparedStatement used to insert data */
	protected com.mysql.jdbc.PreparedStatement inserter = null;

	private String insertSQL = null;

	/** Is this result set updateable? */
	private boolean isUpdatable = false;

	/** Reason the result set is not updatable */
	private String notUpdatableReason = null;

	/** List of primary keys */
	private List primaryKeyIndicies = null;

	private String qualifiedAndQuotedTableName;

	private String quotedIdChar = null;

	/** PreparedStatement used to refresh data */
	private com.mysql.jdbc.PreparedStatement refresher;

	private String refreshSQL = null;

	/** The binary data for the 'current' row */
	private ResultSetRow savedCurrentRow;

	/** PreparedStatement used to delete data */
	protected com.mysql.jdbc.PreparedStatement updater = null;

	/** SQL for in-place modifcation */
	private String updateSQL = null;

	private boolean populateInserterWithDefaultValues = false;

	private Map databasesUsedToTablesUsed = null;
	

	/**
	 * Creates a new ResultSet object.
	 *
	 * @param catalog
	 *            the database in use when we were created
	 * @param fields
	 *            an array of Field objects (basically, the ResultSet MetaData)
	 * @param tuples
	 *            actual row data
	 * @param conn
	 *            the Connection that created us.
	 * @param creatorStmt
	 *            DOCUMENT ME!
	 *
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	protected UpdatableResultSet(String catalog, Field[] fields, RowData tuples,
			ConnectionImpl conn, StatementImpl creatorStmt) throws SQLException {
		super(catalog, fields, tuples, conn, creatorStmt);
		checkUpdatability();
		this.populateInserterWithDefaultValues =
			this.connection.getPopulateInsertRowWithDefaultValues();
	}

	/**
	 * JDBC 2.0
	 *
	 * <p>
	 * Move to an absolute row number in the result set.
	 * </p>
	 *
	 * <p>
	 * If row is positive, moves to an absolute row with respect to the
	 * beginning of the result set. The first row is row 1, the second is row 2,
	 * etc.
	 * </p>
	 *
	 * <p>
	 * If row is negative, moves to an absolute row position with respect to the
	 * end of result set. For example, calling absolute(-1) positions the cursor
	 * on the last row, absolute(-2) indicates the next-to-last row, etc.
	 * </p>
	 *
	 * <p>
	 * An attempt to position the cursor beyond the first/last row in the result
	 * set, leaves the cursor before/after the first/last row, respectively.
	 * </p>
	 *
	 * <p>
	 * Note: Calling absolute(1) is the same as calling first(). Calling
	 * absolute(-1) is the same as calling last().
	 * </p>
	 *
	 * @param row
	 *            DOCUMENT ME!
	 *
	 * @return true if on the result set, false if off.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs, or row is 0, or result
	 *                set type is TYPE_FORWARD_ONLY.
	 */
	public synchronized boolean absolute(int row) throws SQLException {
		return super.absolute(row);
	}

	/**
	 * JDBC 2.0
	 *
	 * <p>
	 * Moves to the end of the result set, just after the last row. Has no
	 * effect if the result set contains no rows.
	 * </p>
	 *
	 * @exception SQLException
	 *                if a database-access error occurs, or result set type is
	 *                TYPE_FORWARD_ONLY.
	 */
	public synchronized void afterLast() throws SQLException {
		super.afterLast();
	}

	/**
	 * JDBC 2.0
	 *
	 * <p>
	 * Moves to the front of the result set, just before the first row. Has no
	 * effect if the result set contains no rows.
	 * </p>
	 *
	 * @exception SQLException
	 *                if a database-access error occurs, or result set type is
	 *                TYPE_FORWARD_ONLY
	 */
	public synchronized void beforeFirst() throws SQLException {
		super.beforeFirst();
	}

	/**
	 * JDBC 2.0 The cancelRowUpdates() method may be called after calling an
	 * updateXXX() method(s) and before calling updateRow() to rollback the
	 * updates made to a row. If no updates have been made or updateRow() has
	 * already been called, then this method has no effect.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs, or if called when on
	 *                the insert row.
	 */
	public synchronized void cancelRowUpdates() throws SQLException {
		checkClosed();

		if (this.doingUpdates) {
			this.doingUpdates = false;
			this.updater.clearParameters();
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see com.mysql.jdbc.ResultSet#checkRowPos()
	 */
	protected void checkRowPos() throws SQLException {
		checkClosed();

		if (!this.onInsertRow) {
			super.checkRowPos();
		}
	}

	/**
	 * Is this ResultSet updateable?
	 *
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	protected void checkUpdatability() throws SQLException {
		try {
			if (this.fields == null) {
				// we've been created to be populated with cached
				// metadata, and we don't have the metadata yet,
				// we'll be called again by
				// Connection.initializeResultsMetadataFromCache()
				// when the metadata has been made available
	
				return;
			}
	
			String singleTableName = null;
			String catalogName = null;
	
			int primaryKeyCount = 0;
	
			// We can only do this if we know that there is a currently
			// selected database, or if we're talking to a > 4.1 version
			// of MySQL server (as it returns database names in field
			// info)
			//
			if ((this.catalog == null) || (this.catalog.length() == 0)) {
				this.catalog = this.fields[0].getDatabaseName();
	
				if ((this.catalog == null) || (this.catalog.length() == 0)) {
					throw SQLError.createSQLException(Messages
							.getString("UpdatableResultSet.43") //$NON-NLS-1$
							, SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor()); //$NON-NLS-1$ //$NON-NLS-2$
				}
			}
	
			if (this.fields.length > 0) {
				singleTableName = this.fields[0].getOriginalTableName();
				catalogName = this.fields[0].getDatabaseName();
	
				if (singleTableName == null) {
					singleTableName = this.fields[0].getTableName();
					catalogName = this.catalog;
				}
	
				if (singleTableName != null && singleTableName.length() == 0) {
					this.isUpdatable = false;
					this.notUpdatableReason = Messages.getString("NotUpdatableReason.3");
	
					return;
				}
	
				if (this.fields[0].isPrimaryKey()) {
					primaryKeyCount++;
				}
	
				//
				// References only one table?
				//
				for (int i = 1; i < this.fields.length; i++) {
					String otherTableName = this.fields[i].getOriginalTableName();
					String otherCatalogName = this.fields[i].getDatabaseName();
	
					if (otherTableName == null) {
						otherTableName = this.fields[i].getTableName();
						otherCatalogName = this.catalog;
					}
	
					if (otherTableName != null && otherTableName.length() == 0) {
						this.isUpdatable = false;
						this.notUpdatableReason = Messages.getString("NotUpdatableReason.3");
	
						return;
					}
	
					if ((singleTableName == null)
							|| !otherTableName.equals(singleTableName)) {
						this.isUpdatable = false;
						this.notUpdatableReason = Messages.getString("NotUpdatableReason.0");
	
						return;
					}
	
					// Can't reference more than one database
					if ((catalogName == null)
							|| !otherCatalogName.equals(catalogName)) {
						this.isUpdatable = false;
						this.notUpdatableReason = Messages.getString("NotUpdatableReason.1");
	
						return;
					}
	
					if (this.fields[i].isPrimaryKey()) {
						primaryKeyCount++;
					}
				}
	
				if ((singleTableName == null) || (singleTableName.length() == 0)) {
					this.isUpdatable = false;
					this.notUpdatableReason = Messages.getString("NotUpdatableReason.2");
	
					return;
				}
			} else {
				this.isUpdatable = false;
				this.notUpdatableReason = Messages.getString("NotUpdatableReason.3");
	
				return;
			}
	
			if (this.connection.getStrictUpdates()) {
				java.sql.DatabaseMetaData dbmd = this.connection.getMetaData();
	
				java.sql.ResultSet rs = null;
				HashMap primaryKeyNames = new HashMap();
	
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
				for (int i = 0; i < this.fields.length; i++) {
					if (this.fields[i].isPrimaryKey()) {
						String columnNameUC = this.fields[i].getName()
								.toUpperCase();
	
						if (primaryKeyNames.remove(columnNameUC) == null) {
							// try original name
							String originalName = this.fields[i].getOriginalName();
	
							if (originalName != null) {
								if (primaryKeyNames.remove(originalName
										.toUpperCase()) == null) {
									// we don't know about this key, so give up :(
									this.isUpdatable = false;
									this.notUpdatableReason = Messages.getString("NotUpdatableReason.6",
											new Object[] {originalName});
	
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

	/**
	 * JDBC 2.0 Delete the current row from the result set and the underlying
	 * database. Cannot be called when on the insert row.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs, or if called when on
	 *                the insert row.
	 * @throws SQLException
	 *             if the ResultSet is not updatable or some other error occurs
	 */
	public synchronized void deleteRow() throws SQLException {
		checkClosed();

		if (!this.isUpdatable) {
			throw new NotUpdatable(this.notUpdatableReason);
		}

		if (this.onInsertRow) {
			throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.1"), getExceptionInterceptor()); //$NON-NLS-1$
		} else if (this.rowData.size() == 0) {
			throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.2"), getExceptionInterceptor()); //$NON-NLS-1$
		} else if (isBeforeFirst()) {
			throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.3"), getExceptionInterceptor()); //$NON-NLS-1$
		} else if (isAfterLast()) {
			throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.4"), getExceptionInterceptor()); //$NON-NLS-1$
		}

		if (this.deleter == null) {
			if (this.deleteSQL == null) {
				generateStatements();
			}

			this.deleter = (PreparedStatement) this.connection
					.clientPrepareStatement(this.deleteSQL);
		}

		this.deleter.clearParameters();

		String characterEncoding = null;

		if (this.connection.getUseUnicode()) {
			characterEncoding = this.connection.getEncoding();
		}

		int numKeys = this.primaryKeyIndicies.size();

		if (numKeys == 1) {
			int index = ((Integer) this.primaryKeyIndicies.get(0))
					.intValue();
			this.setParamValue(this.deleter, 1, this.thisRow, 
					index, this.fields[index].getSQLType());
		} else {
			for (int i = 0; i < numKeys; i++) {
				int index = ((Integer) this.primaryKeyIndicies.get(i))
						.intValue();
				this.setParamValue(this.deleter, i + 1, this.thisRow, 
						index, this.fields[index].getSQLType());

			}
		}

		this.deleter.executeUpdate();
		this.rowData.removeRow(this.rowData.getCurrentRowNumber());
		
		// position on previous row - Bug#27431
		previous();
		
	}
	
	private synchronized void setParamValue(PreparedStatement ps, int psIdx,
			ResultSetRow row, int rsIdx, int sqlType) throws SQLException {
		
		byte[] val = row.getColumnValue(rsIdx);
		if(val == null){
			ps.setNull(psIdx, Types.NULL);
			return;
		}
		switch (sqlType) {
			case Types.NULL:
				ps.setNull(psIdx, Types.NULL);
				break;
			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
				ps.setInt(psIdx, row.getInt(rsIdx));
				break;
			case Types.BIGINT:
				ps.setLong(psIdx, row.getLong(rsIdx));
				break;
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
			case Types.DECIMAL:
			case Types.NUMERIC:
				ps.setString(psIdx, row.getString(rsIdx, this.charEncoding, this.connection));
				break;
			case Types.DATE:
				ps.setDate(psIdx, row.getDateFast(rsIdx, this.connection, this, this.fastDateCal), this.fastDateCal);
				break;
			case Types.TIMESTAMP:
				ps.setTimestamp(psIdx, row.getTimestampFast(rsIdx, this.fastDateCal, this.defaultTimeZone, false, this.connection, this));
				break;
			case Types.TIME:
				ps.setTime(psIdx, row.getTimeFast(rsIdx, this.fastDateCal, this.defaultTimeZone, false, this.connection, this));
				break;
			case Types.FLOAT:
			case Types.DOUBLE:
			case Types.REAL:
			case Types.BOOLEAN:
				ps.setBytesNoEscapeNoQuotes(psIdx, val);
				break;
			/* default, but also explicitly for following types: 
			case Types.BINARY:
			case Types.BLOB:
			*/
			default:
				ps.setBytes(psIdx, val);
				break;
		}
		
	}

	private synchronized void extractDefaultValues() throws SQLException {
		java.sql.DatabaseMetaData dbmd = this.connection.getMetaData();
		this.defaultColumnValue = new byte[this.fields.length][];
		
		java.sql.ResultSet columnsResultSet = null;
		Iterator referencedDbs = this.databasesUsedToTablesUsed.entrySet().iterator();

		while (referencedDbs.hasNext()) {
		    Map.Entry dbEntry = (Map.Entry)referencedDbs.next();
		    String databaseName = dbEntry.getKey().toString();

		    Iterator referencedTables = ((Map)dbEntry.getValue()).entrySet().iterator();

		    while (referencedTables.hasNext()) {
		        Map.Entry tableEntry = (Map.Entry)referencedTables.next();
		        String tableName = tableEntry.getKey().toString();
		        Map columnNamesToIndices = (Map)tableEntry.getValue();

        		try {
        			columnsResultSet = dbmd.getColumns(this.catalog, null,
        			        tableName, "%"); //$NON-NLS-1$

        			while (columnsResultSet.next()) {
        				String columnName = columnsResultSet.getString("COLUMN_NAME"); //$NON-NLS-1$
        				byte[] defaultValue = columnsResultSet.getBytes("COLUMN_DEF"); //$NON-NLS-1$

        				if (columnNamesToIndices.containsKey(columnName)) {
        				    int localColumnIndex = ((Integer)columnNamesToIndices.get(columnName)).intValue();

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

	/**
	 * JDBC 2.0
	 *
	 * <p>
	 * Moves to the first row in the result set.
	 * </p>
	 *
	 * @return true if on a valid row, false if no rows in the result set.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs, or result set type is
	 *                TYPE_FORWARD_ONLY.
	 */
	public synchronized boolean first() throws SQLException {
		return super.first();
	}

	/**
	 * Figure out whether or not this ResultSet is updateable, and if so,
	 * generate the PreparedStatements to support updates.
	 *
	 * @throws SQLException
	 *             DOCUMENT ME!
	 * @throws NotUpdatable
	 *             DOCUMENT ME!
	 */
	protected synchronized void generateStatements() throws SQLException {
		if (!this.isUpdatable) {
			this.doingUpdates = false;
			this.onInsertRow = false;

			throw new NotUpdatable(this.notUpdatableReason);
		}

		String quotedId = getQuotedIdChar();

		Map tableNamesSoFar = null;

		if (this.connection.lowerCaseTableNames()) {
		    tableNamesSoFar = new TreeMap(String.CASE_INSENSITIVE_ORDER);
		    this.databasesUsedToTablesUsed = new TreeMap(String.CASE_INSENSITIVE_ORDER);
		} else {
		    tableNamesSoFar = new TreeMap();
		    this.databasesUsedToTablesUsed = new TreeMap();
		}

		this.primaryKeyIndicies = new ArrayList();

		StringBuffer fieldValues = new StringBuffer();
		StringBuffer keyValues = new StringBuffer();
		StringBuffer columnNames = new StringBuffer();
		StringBuffer insertPlaceHolders = new StringBuffer();
		StringBuffer allTablesBuf = new StringBuffer();
		Map columnIndicesToTable = new HashMap();

		boolean firstTime = true;
		boolean keysFirstTime = true;

		String equalsStr = this.connection.versionMeetsMinimum(3, 23, 0) ? "<=>"
				: "=";

		for (int i = 0; i < this.fields.length; i++) {
		    StringBuffer tableNameBuffer = new StringBuffer();
		    Map updColumnNameToIndex = null;

		    // FIXME: What about no table?
		    if (this.fields[i].getOriginalTableName() != null) {

	            String databaseName = this.fields[i].getDatabaseName();

	            if ((databaseName != null) && (databaseName.length() > 0)) {
	                tableNameBuffer.append(quotedId);
	                tableNameBuffer.append(databaseName);
	                tableNameBuffer.append(quotedId);
	                tableNameBuffer.append('.');
	            }

	            String tableOnlyName = this.fields[i].getOriginalTableName();

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

	            columnIndicesToTable.put(new Integer(i), fqTableName);

	            updColumnNameToIndex = getColumnsToIndexMapForTableAndDB(databaseName, tableOnlyName);
	        } else {
	            String tableOnlyName = this.fields[i].getTableName();

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

                    columnIndicesToTable.put(new Integer(i), fqTableName);

                    updColumnNameToIndex = getColumnsToIndexMapForTableAndDB(this.catalog, tableOnlyName);
	            }
	        }

			String originalColumnName = this.fields[i].getOriginalName();
			String columnName = null;

			if (this.connection.getIO().hasLongColumnInfo()
					&& (originalColumnName != null)
					&& (originalColumnName.length() > 0)) {
				columnName = originalColumnName;
			} else {
				columnName = this.fields[i].getName();
			}

			if (updColumnNameToIndex != null && columnName != null) {
			    updColumnNameToIndex.put(columnName, new Integer(i));
			}

			String originalTableName = this.fields[i].getOriginalTableName();
            String tableName = null;

            if (this.connection.getIO().hasLongColumnInfo()
                    && (originalTableName != null)
                    && (originalTableName.length() > 0)) {
                tableName = originalTableName;
            } else {
                tableName = this.fields[i].getTableName();
            }

            StringBuffer fqcnBuf = new StringBuffer();
            String databaseName = this.fields[i].getDatabaseName();

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
            
			if (this.fields[i].isPrimaryKey()) {
				this.primaryKeyIndicies.add(Constants.integerValueOf(i));

				if (!keysFirstTime) {
					keyValues.append(" AND "); //$NON-NLS-1$
				} else {
					keysFirstTime = false;
				}

				keyValues.append(qualifiedColumnName);
				keyValues.append(equalsStr);
				keyValues.append("?"); //$NON-NLS-1$
			}

			if (firstTime) {
				firstTime = false;
				fieldValues.append("SET "); //$NON-NLS-1$
			} else {
				fieldValues.append(","); //$NON-NLS-1$
				columnNames.append(","); //$NON-NLS-1$
				insertPlaceHolders.append(","); //$NON-NLS-1$
			}

			insertPlaceHolders.append("?"); //$NON-NLS-1$

			columnNames.append(qualifiedColumnName);

			fieldValues.append(qualifiedColumnName);
			fieldValues.append("=?"); //$NON-NLS-1$
		}

		this.qualifiedAndQuotedTableName = allTablesBuf.toString();

		this.updateSQL = "UPDATE " + this.qualifiedAndQuotedTableName + " " //$NON-NLS-1$ //$NON-NLS-2$
				+ fieldValues.toString() //$NON-NLS-1$ //$NON-NLS-2$
				+ " WHERE " + keyValues.toString(); //$NON-NLS-1$
		this.insertSQL = "INSERT INTO " + this.qualifiedAndQuotedTableName //$NON-NLS-1$
				+ " (" + columnNames.toString() //$NON-NLS-1$ //$NON-NLS-2$
				+ ") VALUES (" + insertPlaceHolders.toString() + ")"; //$NON-NLS-1$ //$NON-NLS-2$
		this.refreshSQL = "SELECT " + columnNames.toString() + " FROM " //$NON-NLS-1$ //$NON-NLS-2$
				+ this.qualifiedAndQuotedTableName //$NON-NLS-1$ //$NON-NLS-2$
				+ " WHERE " + keyValues.toString(); //$NON-NLS-1$
		this.deleteSQL = "DELETE FROM " + this.qualifiedAndQuotedTableName //$NON-NLS-1$
				+ " WHERE " //$NON-NLS-1$ //$NON-NLS-2$
				+ keyValues.toString();
	}

    private Map getColumnsToIndexMapForTableAndDB(String databaseName, String tableName) {
        Map nameToIndex;
        Map tablesUsedToColumnsMap = (Map)this.databasesUsedToTablesUsed.get(databaseName);

        if (tablesUsedToColumnsMap == null) {
            if (this.connection.lowerCaseTableNames()) {
                tablesUsedToColumnsMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
            } else {
                tablesUsedToColumnsMap = new TreeMap();
            }

            this.databasesUsedToTablesUsed.put(databaseName, tablesUsedToColumnsMap);
        }

        nameToIndex = (Map)tablesUsedToColumnsMap.get(tableName);

        if (nameToIndex == null) {
            nameToIndex = new HashMap();
            tablesUsedToColumnsMap.put(tableName, nameToIndex);
        }

        return nameToIndex;
    }

	private synchronized SingleByteCharsetConverter getCharConverter()
			throws SQLException {
		if (!this.initializedCharConverter) {
			this.initializedCharConverter = true;

			if (this.connection.getUseUnicode()) {
				this.charEncoding = connection.getEncoding();
				this.charConverter = this.connection
						.getCharsetConverter(this.charEncoding);
			}
		}

		return this.charConverter;
	}

	/**
	 * JDBC 2.0 Return the concurrency of this result set. The concurrency used
	 * is determined by the statement that created the result set.
	 *
	 * @return the concurrency type, CONCUR_READ_ONLY, etc.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public int getConcurrency() throws SQLException {
		return (this.isUpdatable ? CONCUR_UPDATABLE : CONCUR_READ_ONLY);
	}

	private synchronized String getQuotedIdChar() throws SQLException {
		if (this.quotedIdChar == null) {
			boolean useQuotedIdentifiers = this.connection
					.supportsQuotedIdentifiers();

			if (useQuotedIdentifiers) {
				java.sql.DatabaseMetaData dbmd = this.connection.getMetaData();
				this.quotedIdChar = dbmd.getIdentifierQuoteString();
			} else {
				this.quotedIdChar = ""; //$NON-NLS-1$
			}
		}

		return this.quotedIdChar;
	}

	/**
	 * JDBC 2.0 Insert the contents of the insert row into the result set and
	 * the database. Must be on the insert row when this method is called.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs, if called when not on
	 *                the insert row, or if all non-nullable columns in the
	 *                insert row have not been given a value
	 */
	public synchronized void insertRow() throws SQLException {
		checkClosed();

		if (!this.onInsertRow) {
			throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.7"), getExceptionInterceptor()); //$NON-NLS-1$
		}

		this.inserter.executeUpdate();

		long autoIncrementId = this.inserter.getLastInsertID();
		int numFields = this.fields.length;
		byte[][] newRow = new byte[numFields][];

		for (int i = 0; i < numFields; i++) {
			if (this.inserter.isNull(i)) {
				newRow[i] = null;
			} else {
				newRow[i] = this.inserter.getBytesRepresentation(i);
			}

			//
			// WARN: This non-variant only holds if MySQL never allows more
			// than one auto-increment key (which is the way it is _today_)
			//
			if (this.fields[i].isAutoIncrement() && autoIncrementId > 0) {
				newRow[i] = String.valueOf(autoIncrementId).getBytes();
				this.inserter.setBytesNoEscapeNoQuotes(i + 1, newRow[i]);
			}
		}

		ResultSetRow resultSetRow = new ByteArrayRow(newRow, getExceptionInterceptor());

		refreshRow(this.inserter, resultSetRow);

		this.rowData.addRow(resultSetRow);
		resetInserter();
	}

	/**
	 * JDBC 2.0
	 *
	 * <p>
	 * Determine if the cursor is after the last row in the result set.
	 * </p>
	 *
	 * @return true if after the last row, false otherwise. Returns false when
	 *         the result set contains no rows.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public synchronized boolean isAfterLast() throws SQLException {
		return super.isAfterLast();
	}

	/**
	 * JDBC 2.0
	 *
	 * <p>
	 * Determine if the cursor is before the first row in the result set.
	 * </p>
	 *
	 * @return true if before the first row, false otherwise. Returns false when
	 *         the result set contains no rows.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public synchronized boolean isBeforeFirst() throws SQLException {
		return super.isBeforeFirst();
	}

	/**
	 * JDBC 2.0
	 *
	 * <p>
	 * Determine if the cursor is on the first row of the result set.
	 * </p>
	 *
	 * @return true if on the first row, false otherwise.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public synchronized boolean isFirst() throws SQLException {
		return super.isFirst();
	}

	/**
	 * JDBC 2.0
	 *
	 * <p>
	 * Determine if the cursor is on the last row of the result set. Note:
	 * Calling isLast() may be expensive since the JDBC driver might need to
	 * fetch ahead one row in order to determine whether the current row is the
	 * last row in the result set.
	 * </p>
	 *
	 * @return true if on the last row, false otherwise.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public synchronized boolean isLast() throws SQLException {
		return super.isLast();
	}

	boolean isUpdatable() {
		return this.isUpdatable;
	}

	/**
	 * JDBC 2.0
	 *
	 * <p>
	 * Moves to the last row in the result set.
	 * </p>
	 *
	 * @return true if on a valid row, false if no rows in the result set.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs, or result set type is
	 *                TYPE_FORWARD_ONLY.
	 */
	public synchronized boolean last() throws SQLException {
		return super.last();
	}

	/**
	 * JDBC 2.0 Move the cursor to the remembered cursor position, usually the
	 * current row. Has no effect unless the cursor is on the insert row.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs, or the result set is
	 *                not updatable
	 * @throws SQLException
	 *             if the ResultSet is not updatable or some other error occurs
	 */
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

	/**
	 * JDBC 2.0 Move to the insert row. The current cursor position is
	 * remembered while the cursor is positioned on the insert row. The insert
	 * row is a special row associated with an updatable result set. It is
	 * essentially a buffer where a new row may be constructed by calling the
	 * updateXXX() methods prior to inserting the row into the result set. Only
	 * the updateXXX(), getXXX(), and insertRow() methods may be called when the
	 * cursor is on the insert row. All of the columns in a result set must be
	 * given a value each time this method is called before calling insertRow().
	 * UpdateXXX()must be called before getXXX() on a column.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs, or the result set is
	 *                not updatable
	 * @throws NotUpdatable
	 *             DOCUMENT ME!
	 */
	public synchronized void moveToInsertRow() throws SQLException {
		checkClosed();

		if (!this.isUpdatable) {
			throw new NotUpdatable(this.notUpdatableReason);
		}

		if (this.inserter == null) {
			if (this.insertSQL == null) {
				generateStatements();
			}

			this.inserter = (PreparedStatement) this.connection
					.clientPrepareStatement(this.insertSQL);
			if (this.populateInserterWithDefaultValues) {
				extractDefaultValues();
			}

			resetInserter();
		} else {
			resetInserter();
		}

		int numFields = this.fields.length;

		this.onInsertRow = true;
		this.doingUpdates = false;
		this.savedCurrentRow = this.thisRow;
		byte[][] newRowData = new byte[numFields][];
		this.thisRow = new ByteArrayRow(newRowData, getExceptionInterceptor());

		for (int i = 0; i < numFields; i++) {
			if (!this.populateInserterWithDefaultValues) {
				this.inserter.setBytesNoEscapeNoQuotes(i + 1,
						"DEFAULT".getBytes());
				newRowData = null;
			} else {
				if (this.defaultColumnValue[i] != null) {
					Field f = this.fields[i];

					switch (f.getMysqlType()) {
					case MysqlDefs.FIELD_TYPE_DATE:
					case MysqlDefs.FIELD_TYPE_DATETIME:
					case MysqlDefs.FIELD_TYPE_NEWDATE:
					case MysqlDefs.FIELD_TYPE_TIME:
					case MysqlDefs.FIELD_TYPE_TIMESTAMP:

						if (this.defaultColumnValue[i].length > 7
								&& this.defaultColumnValue[i][0] == (byte) 'C'
								&& this.defaultColumnValue[i][1] == (byte) 'U'
								&& this.defaultColumnValue[i][2] == (byte) 'R'
								&& this.defaultColumnValue[i][3] == (byte) 'R'
								&& this.defaultColumnValue[i][4] == (byte) 'E'
								&& this.defaultColumnValue[i][5] == (byte) 'N'
								&& this.defaultColumnValue[i][6] == (byte) 'T'
								&& this.defaultColumnValue[i][7] == (byte) '_') {
							this.inserter.setBytesNoEscapeNoQuotes(i + 1,
									this.defaultColumnValue[i]);

							break;
						}
					default:
						this.inserter.setBytes(i + 1, this.defaultColumnValue[i],
								false, false);
					}

					// This value _could_ be changed from a getBytes(), so we
					// need a copy....
					byte[] defaultValueCopy = new byte[this.defaultColumnValue[i].length];
					System.arraycopy(defaultColumnValue[i], 0, defaultValueCopy, 0,
							defaultValueCopy.length);
					newRowData[i] = defaultValueCopy;
				} else {
					this.inserter.setNull(i + 1, java.sql.Types.NULL);
					newRowData[i] = null;
				}
			}
		}
	}

	// ---------------------------------------------------------------------
	// Updates
	// ---------------------------------------------------------------------

	/**
	 * A ResultSet is initially positioned before its first row, the first call
	 * to next makes the first row the current row; the second call makes the
	 * second row the current row, etc.
	 *
	 * <p>
	 * If an input stream from the previous row is open, it is implicitly
	 * closed. The ResultSet's warning chain is cleared when a new row is read
	 * </p>
	 *
	 * @return true if the new current is valid; false if there are no more rows
	 *
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public synchronized boolean next() throws SQLException {
		return super.next();
	}

	/**
	 * The prev method is not part of JDBC, but because of the architecture of
	 * this driver it is possible to move both forward and backward within the
	 * result set.
	 *
	 * <p>
	 * If an input stream from the previous row is open, it is implicitly
	 * closed. The ResultSet's warning chain is cleared when a new row is read
	 * </p>
	 *
	 * @return true if the new current is valid; false if there are no more rows
	 *
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public synchronized boolean prev() throws SQLException {
		return super.prev();
	}

	/**
	 * JDBC 2.0
	 *
	 * <p>
	 * Moves to the previous row in the result set.
	 * </p>
	 *
	 * <p>
	 * Note: previous() is not the same as relative(-1) since it makes sense to
	 * call previous() when there is no current row.
	 * </p>
	 *
	 * @return true if on a valid row, false if off the result set.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs, or result set type is
	 *                TYPE_FORWAR_DONLY.
	 */
	public synchronized boolean previous() throws SQLException {
		return super.previous();
	}

	/**
	 * Closes this ResultSet, releasing all resources.
	 *
	 * @param calledExplicitly
	 *            was this called from close()?
	 *
	 * @throws SQLException
	 *             if an error occurs.
	 */
	public void realClose(boolean calledExplicitly) throws SQLException {
		if (this.isClosed) {
			return;
		}

		SQLException sqlEx = null;

		if (this.useUsageAdvisor) {
			if ((this.deleter == null) && (this.inserter == null)
					&& (this.refresher == null) && (this.updater == null)) {
				this.eventSink = ProfilerEventHandlerFactory.getInstance(this.connection);

				String message = Messages.getString("UpdatableResultSet.34"); //$NON-NLS-1$

				this.eventSink.consumeEvent(new ProfilerEvent(
						ProfilerEvent.TYPE_WARN,
						"", //$NON-NLS-1$
						(this.owningStatement == null) ? "N/A" //$NON-NLS-1$
								: this.owningStatement.currentCatalog, //$NON-NLS-1$
						this.connectionId,
						(this.owningStatement == null) ? (-1)
								: this.owningStatement.getId(), this.resultId,
						System.currentTimeMillis(), 0, Constants.MILLIS_I18N,
						null, this.pointOfOrigin, message));
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

	/**
	 * JDBC 2.0 Refresh the value of the current row with its current value in
	 * the database. Cannot be called when on the insert row. The refreshRow()
	 * method provides a way for an application to explicitly tell the JDBC
	 * driver to refetch a row(s) from the database. An application may want to
	 * call refreshRow() when caching or prefetching is being done by the JDBC
	 * driver to fetch the latest value of a row from the database. The JDBC
	 * driver may actually refresh multiple rows at once if the fetch size is
	 * greater than one. All values are refetched subject to the transaction
	 * isolation level and cursor sensitivity. If refreshRow() is called after
	 * calling updateXXX(), but before calling updateRow() then the updates made
	 * to the row are lost. Calling refreshRow() frequently will likely slow
	 * performance.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs, or if called when on
	 *                the insert row.
	 * @throws NotUpdatable
	 *             DOCUMENT ME!
	 */
	public synchronized void refreshRow() throws SQLException {
		checkClosed();

		if (!this.isUpdatable) {
			throw new NotUpdatable();
		}

		if (this.onInsertRow) {
			throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.8"), getExceptionInterceptor()); //$NON-NLS-1$
		} else if (this.rowData.size() == 0) {
			throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.9"), getExceptionInterceptor()); //$NON-NLS-1$
		} else if (isBeforeFirst()) {
			throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.10"), getExceptionInterceptor()); //$NON-NLS-1$
		} else if (isAfterLast()) {
			throw SQLError.createSQLException(Messages.getString("UpdatableResultSet.11"), getExceptionInterceptor()); //$NON-NLS-1$
		}

		refreshRow(this.updater, this.thisRow);
	}

	private synchronized void refreshRow(PreparedStatement updateInsertStmt,
			ResultSetRow rowToRefresh) throws SQLException {
		if (this.refresher == null) {
			if (this.refreshSQL == null) {
				generateStatements();
			}

			this.refresher = (PreparedStatement) this.connection
					.clientPrepareStatement(this.refreshSQL);
		}

		this.refresher.clearParameters();

		int numKeys = this.primaryKeyIndicies.size();

		if (numKeys == 1) {
			byte[] dataFrom = null;
			int index = ((Integer) this.primaryKeyIndicies.get(0)).intValue();

			if (!this.doingUpdates && !this.onInsertRow) {
				dataFrom = (byte[]) rowToRefresh.getColumnValue(index);
			} else {
				dataFrom = updateInsertStmt.getBytesRepresentation(index);

				// Primary keys not set?
				if (updateInsertStmt.isNull(index) || (dataFrom.length == 0)) {
					dataFrom = (byte[]) rowToRefresh.getColumnValue(index);
				} else {
					dataFrom = stripBinaryPrefix(dataFrom);
				}
			}

			this.refresher.setBytesNoEscape(1, dataFrom);
		} else {
			for (int i = 0; i < numKeys; i++) {
				byte[] dataFrom = null;
				int index = ((Integer) this.primaryKeyIndicies.get(i))
						.intValue();

				if (!this.doingUpdates && !this.onInsertRow) {
					dataFrom = (byte[]) rowToRefresh.getColumnValue(index);
				} else {
					dataFrom = updateInsertStmt.getBytesRepresentation(index);

					// Primary keys not set?
					if (updateInsertStmt.isNull(index) || (dataFrom.length == 0)) {
						dataFrom = (byte[]) rowToRefresh.getColumnValue(index);
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
						rowToRefresh.setColumnValue(i, null);
					} else {
						rowToRefresh.setColumnValue(i, rs.getBytes(i + 1));
					}
				}
			} else {
				throw SQLError.createSQLException(Messages
						.getString("UpdatableResultSet.12"), //$NON-NLS-1$
						SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor()); //$NON-NLS-1$
			}
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException ex) {
					; // ignore
				}
			}
		}
	}

	/**
	 * JDBC 2.0
	 *
	 * <p>
	 * Moves a relative number of rows, either positive or negative. Attempting
	 * to move beyond the first/last row in the result set positions the cursor
	 * before/after the the first/last row. Calling relative(0) is valid, but
	 * does not change the cursor position.
	 * </p>
	 *
	 * <p>
	 * Note: Calling relative(1) is different than calling next() since is makes
	 * sense to call next() when there is no current row, for example, when the
	 * cursor is positioned before the first row or after the last row of the
	 * result set.
	 * </p>
	 *
	 * @param rows
	 *            DOCUMENT ME!
	 *
	 * @return true if on a row, false otherwise.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs, or there is no current
	 *                row, or result set type is TYPE_FORWARD_ONLY.
	 */
	public synchronized boolean relative(int rows) throws SQLException {
		return super.relative(rows);
	}

	private void resetInserter() throws SQLException {
		this.inserter.clearParameters();

		for (int i = 0; i < this.fields.length; i++) {
			this.inserter.setNull(i + 1, 0);
		}
	}

	/**
	 * JDBC 2.0 Determine if this row has been deleted. A deleted row may leave
	 * a visible "hole" in a result set. This method can be used to detect holes
	 * in a result set. The value returned depends on whether or not the result
	 * set can detect deletions.
	 *
	 * @return true if deleted and deletes are detected
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 * @throws NotImplemented
	 *             DOCUMENT ME!
	 *
	 * @see DatabaseMetaData#deletesAreDetected
	 */
	public synchronized boolean rowDeleted() throws SQLException {
		throw SQLError.notImplemented();
	}

	/**
	 * JDBC 2.0 Determine if the current row has been inserted. The value
	 * returned depends on whether or not the result set can detect visible
	 * inserts.
	 *
	 * @return true if inserted and inserts are detected
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 * @throws NotImplemented
	 *             DOCUMENT ME!
	 *
	 * @see DatabaseMetaData#insertsAreDetected
	 */
	public synchronized boolean rowInserted() throws SQLException {
		throw SQLError.notImplemented();
	}

	/**
	 * JDBC 2.0 Determine if the current row has been updated. The value
	 * returned depends on whether or not the result set can detect updates.
	 *
	 * @return true if the row has been visibly updated by the owner or another,
	 *         and updates are detected
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 * @throws NotImplemented
	 *             DOCUMENT ME!
	 *
	 * @see DatabaseMetaData#updatesAreDetected
	 */
	public synchronized boolean rowUpdated() throws SQLException {
		throw SQLError.notImplemented();
	}

	/**
	 * Sets the concurrency type of this result set
	 *
	 * @param concurrencyFlag
	 *            the type of concurrency that this ResultSet should support.
	 */
	protected void setResultSetConcurrency(int concurrencyFlag) {
		super.setResultSetConcurrency(concurrencyFlag);

		//
		// FIXME: Issue warning when asked for updateable result set, but result
		// set is not
		// updatable
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
	 *             DOCUMENT ME!
	 */
	protected synchronized void syncUpdate() throws SQLException {
		if (this.updater == null) {
			if (this.updateSQL == null) {
				generateStatements();
			}

			this.updater = (PreparedStatement) this.connection
					.clientPrepareStatement(this.updateSQL);
		}

		int numFields = this.fields.length;
		this.updater.clearParameters();

		for (int i = 0; i < numFields; i++) {
			if (this.thisRow.getColumnValue(i) != null) {
				this.updater.setBytes(i + 1, (byte[]) this.thisRow.getColumnValue(i),
						this.fields[i].isBinary(), false);
			} else {
				this.updater.setNull(i + 1, 0);
			}
		}

		int numKeys = this.primaryKeyIndicies.size();

		if (numKeys == 1) {
			int index = ((Integer) this.primaryKeyIndicies.get(0)).intValue();
			this.setParamValue(this.updater, numFields + 1, this.thisRow, index , 
					this.fields[index].getSQLType());
		} else {
			for (int i = 0; i < numKeys; i++) {
				int idx = ((Integer)this.primaryKeyIndicies.get(i)).intValue();
				this.setParamValue(this.updater, numFields + i + 1, this.thisRow, 
						idx , this.fields[idx].getSQLType());
			}
		}
	}

	/**
	 * JDBC 2.0 Update a column with an ascii stream value. The updateXXX()
	 * methods are used to update column values in the current row, or the
	 * insert row. The updateXXX() methods do not update the underlying
	 * database, instead the updateRow() or insertRow() methods are called to
	 * update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @param length
	 *            the length of the stream
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateAsciiStream(int columnIndex,
			java.io.InputStream x, int length) throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setAsciiStream(columnIndex, x, length);
		} else {
			this.inserter.setAsciiStream(columnIndex, x, length);
			this.thisRow.setColumnValue(columnIndex - 1, STREAM_DATA_MARKER);
		}
	}

	/**
	 * JDBC 2.0 Update a column with an ascii stream value. The updateXXX()
	 * methods are used to update column values in the current row, or the
	 * insert row. The updateXXX() methods do not update the underlying
	 * database, instead the updateRow() or insertRow() methods are called to
	 * update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 * @param length
	 *            of the stream
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateAsciiStream(String columnName,
			java.io.InputStream x, int length) throws SQLException {
		updateAsciiStream(findColumn(columnName), x, length);
	}

	/**
	 * JDBC 2.0 Update a column with a BigDecimal value. The updateXXX() methods
	 * are used to update column values in the current row, or the insert row.
	 * The updateXXX() methods do not update the underlying database, instead
	 * the updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setBigDecimal(columnIndex, x);
		} else {
			this.inserter.setBigDecimal(columnIndex, x);

			if (x == null) {
				this.thisRow.setColumnValue(columnIndex - 1, null);
			} else {
				this.thisRow.setColumnValue(columnIndex - 1, x.toString().getBytes());
			}
		}
	}

	/**
	 * JDBC 2.0 Update a column with a BigDecimal value. The updateXXX() methods
	 * are used to update column values in the current row, or the insert row.
	 * The updateXXX() methods do not update the underlying database, instead
	 * the updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateBigDecimal(String columnName, BigDecimal x)
			throws SQLException {
		updateBigDecimal(findColumn(columnName), x);
	}

	/**
	 * JDBC 2.0 Update a column with a binary stream value. The updateXXX()
	 * methods are used to update column values in the current row, or the
	 * insert row. The updateXXX() methods do not update the underlying
	 * database, instead the updateRow() or insertRow() methods are called to
	 * update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @param length
	 *            the length of the stream
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateBinaryStream(int columnIndex,
			java.io.InputStream x, int length) throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setBinaryStream(columnIndex, x, length);
		} else {
			this.inserter.setBinaryStream(columnIndex, x, length);

			if (x == null) {
				this.thisRow.setColumnValue(columnIndex - 1, null);
			} else {
				this.thisRow.setColumnValue(columnIndex - 1, STREAM_DATA_MARKER);
			}
		}
	}

	/**
	 * JDBC 2.0 Update a column with a binary stream value. The updateXXX()
	 * methods are used to update column values in the current row, or the
	 * insert row. The updateXXX() methods do not update the underlying
	 * database, instead the updateRow() or insertRow() methods are called to
	 * update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 * @param length
	 *            of the stream
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateBinaryStream(String columnName,
			java.io.InputStream x, int length) throws SQLException {
		updateBinaryStream(findColumn(columnName), x, length);
	}

	/**
	 * @see ResultSetInternalMethods#updateBlob(int, Blob)
	 */
	public synchronized void updateBlob(int columnIndex, java.sql.Blob blob)
			throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setBlob(columnIndex, blob);
		} else {
			this.inserter.setBlob(columnIndex, blob);

			if (blob == null) {
				this.thisRow.setColumnValue(columnIndex - 1, null);
			} else {
				this.thisRow.setColumnValue(columnIndex - 1, STREAM_DATA_MARKER);
			}
		}
	}

	/**
	 * @see ResultSetInternalMethods#updateBlob(String, Blob)
	 */
	public synchronized void updateBlob(String columnName, java.sql.Blob blob)
			throws SQLException {
		updateBlob(findColumn(columnName), blob);
	}

	/**
	 * JDBC 2.0 Update a column with a boolean value. The updateXXX() methods
	 * are used to update column values in the current row, or the insert row.
	 * The updateXXX() methods do not update the underlying database, instead
	 * the updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateBoolean(int columnIndex, boolean x)
			throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setBoolean(columnIndex, x);
		} else {
			this.inserter.setBoolean(columnIndex, x);

			this.thisRow.setColumnValue(columnIndex - 1, this.inserter
					.getBytesRepresentation(columnIndex - 1));
		}
	}

	/**
	 * JDBC 2.0 Update a column with a boolean value. The updateXXX() methods
	 * are used to update column values in the current row, or the insert row.
	 * The updateXXX() methods do not update the underlying database, instead
	 * the updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateBoolean(String columnName, boolean x)
			throws SQLException {
		updateBoolean(findColumn(columnName), x);
	}

	/**
	 * JDBC 2.0 Update a column with a byte value. The updateXXX() methods are
	 * used to update column values in the current row, or the insert row. The
	 * updateXXX() methods do not update the underlying database, instead the
	 * updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateByte(int columnIndex, byte x)
			throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setByte(columnIndex, x);
		} else {
			this.inserter.setByte(columnIndex, x);

			this.thisRow.setColumnValue(columnIndex - 1, this.inserter
					.getBytesRepresentation(columnIndex - 1));
		}
	}

	/**
	 * JDBC 2.0 Update a column with a byte value. The updateXXX() methods are
	 * used to update column values in the current row, or the insert row. The
	 * updateXXX() methods do not update the underlying database, instead the
	 * updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateByte(String columnName, byte x)
			throws SQLException {
		updateByte(findColumn(columnName), x);
	}

	/**
	 * JDBC 2.0 Update a column with a byte array value. The updateXXX() methods
	 * are used to update column values in the current row, or the insert row.
	 * The updateXXX() methods do not update the underlying database, instead
	 * the updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateBytes(int columnIndex, byte[] x)
			throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setBytes(columnIndex, x);
		} else {
			this.inserter.setBytes(columnIndex, x);

			this.thisRow.setColumnValue(columnIndex - 1, x);
		}
	}

	/**
	 * JDBC 2.0 Update a column with a byte array value. The updateXXX() methods
	 * are used to update column values in the current row, or the insert row.
	 * The updateXXX() methods do not update the underlying database, instead
	 * the updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateBytes(String columnName, byte[] x)
			throws SQLException {
		updateBytes(findColumn(columnName), x);
	}

	/**
	 * JDBC 2.0 Update a column with a character stream value. The updateXXX()
	 * methods are used to update column values in the current row, or the
	 * insert row. The updateXXX() methods do not update the underlying
	 * database, instead the updateRow() or insertRow() methods are called to
	 * update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @param length
	 *            the length of the stream
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateCharacterStream(int columnIndex,
			java.io.Reader x, int length) throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setCharacterStream(columnIndex, x, length);
		} else {
			this.inserter.setCharacterStream(columnIndex, x, length);

			if (x == null) {
				this.thisRow.setColumnValue(columnIndex - 1, null);
			} else {
				this.thisRow.setColumnValue(columnIndex - 1, STREAM_DATA_MARKER);
			}
		}
	}

	/**
	 * JDBC 2.0 Update a column with a character stream value. The updateXXX()
	 * methods are used to update column values in the current row, or the
	 * insert row. The updateXXX() methods do not update the underlying
	 * database, instead the updateRow() or insertRow() methods are called to
	 * update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param reader
	 *            the new column value
	 * @param length
	 *            of the stream
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateCharacterStream(String columnName,
			java.io.Reader reader, int length) throws SQLException {
		updateCharacterStream(findColumn(columnName), reader, length);
	}

	/**
	 * @see ResultSetInternalMethods#updateClob(int, Clob)
	 */
	public void updateClob(int columnIndex, java.sql.Clob clob)
			throws SQLException {
		if (clob == null) {
			updateNull(columnIndex);
		} else {
			updateCharacterStream(columnIndex, clob.getCharacterStream(),
					(int) clob.length());
		}
	}

	/**
	 * JDBC 2.0 Update a column with a Date value. The updateXXX() methods are
	 * used to update column values in the current row, or the insert row. The
	 * updateXXX() methods do not update the underlying database, instead the
	 * updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateDate(int columnIndex, java.sql.Date x)
			throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setDate(columnIndex, x);
		} else {
			this.inserter.setDate(columnIndex, x);

			this.thisRow.setColumnValue(columnIndex - 1, this.inserter
					.getBytesRepresentation(columnIndex - 1));
		}
	}

	/**
	 * JDBC 2.0 Update a column with a Date value. The updateXXX() methods are
	 * used to update column values in the current row, or the insert row. The
	 * updateXXX() methods do not update the underlying database, instead the
	 * updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateDate(String columnName, java.sql.Date x)
			throws SQLException {
		updateDate(findColumn(columnName), x);
	}

	/**
	 * JDBC 2.0 Update a column with a Double value. The updateXXX() methods are
	 * used to update column values in the current row, or the insert row. The
	 * updateXXX() methods do not update the underlying database, instead the
	 * updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateDouble(int columnIndex, double x)
			throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setDouble(columnIndex, x);
		} else {
			this.inserter.setDouble(columnIndex, x);

			this.thisRow.setColumnValue(columnIndex - 1, this.inserter
					.getBytesRepresentation(columnIndex - 1));
		}
	}

	/**
	 * JDBC 2.0 Update a column with a double value. The updateXXX() methods are
	 * used to update column values in the current row, or the insert row. The
	 * updateXXX() methods do not update the underlying database, instead the
	 * updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateDouble(String columnName, double x)
			throws SQLException {
		updateDouble(findColumn(columnName), x);
	}

	/**
	 * JDBC 2.0 Update a column with a float value. The updateXXX() methods are
	 * used to update column values in the current row, or the insert row. The
	 * updateXXX() methods do not update the underlying database, instead the
	 * updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateFloat(int columnIndex, float x)
			throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setFloat(columnIndex, x);
		} else {
			this.inserter.setFloat(columnIndex, x);

			this.thisRow.setColumnValue(columnIndex - 1, this.inserter
					.getBytesRepresentation(columnIndex - 1));
		}
	}

	/**
	 * JDBC 2.0 Update a column with a float value. The updateXXX() methods are
	 * used to update column values in the current row, or the insert row. The
	 * updateXXX() methods do not update the underlying database, instead the
	 * updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateFloat(String columnName, float x)
			throws SQLException {
		updateFloat(findColumn(columnName), x);
	}

	/**
	 * JDBC 2.0 Update a column with an integer value. The updateXXX() methods
	 * are used to update column values in the current row, or the insert row.
	 * The updateXXX() methods do not update the underlying database, instead
	 * the updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateInt(int columnIndex, int x)
			throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setInt(columnIndex, x);
		} else {
			this.inserter.setInt(columnIndex, x);

			this.thisRow.setColumnValue(columnIndex - 1, this.inserter
					.getBytesRepresentation(columnIndex - 1));
		}
	}

	/**
	 * JDBC 2.0 Update a column with an integer value. The updateXXX() methods
	 * are used to update column values in the current row, or the insert row.
	 * The updateXXX() methods do not update the underlying database, instead
	 * the updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateInt(String columnName, int x)
			throws SQLException {
		updateInt(findColumn(columnName), x);
	}

	/**
	 * JDBC 2.0 Update a column with a long value. The updateXXX() methods are
	 * used to update column values in the current row, or the insert row. The
	 * updateXXX() methods do not update the underlying database, instead the
	 * updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateLong(int columnIndex, long x)
			throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setLong(columnIndex, x);
		} else {
			this.inserter.setLong(columnIndex, x);

			this.thisRow.setColumnValue(columnIndex - 1, this.inserter
					.getBytesRepresentation(columnIndex - 1));
		}
	}

	/**
	 * JDBC 2.0 Update a column with a long value. The updateXXX() methods are
	 * used to update column values in the current row, or the insert row. The
	 * updateXXX() methods do not update the underlying database, instead the
	 * updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateLong(String columnName, long x)
			throws SQLException {
		updateLong(findColumn(columnName), x);
	}

	/**
	 * JDBC 2.0 Give a nullable column a null value. The updateXXX() methods are
	 * used to update column values in the current row, or the insert row. The
	 * updateXXX() methods do not update the underlying database, instead the
	 * updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateNull(int columnIndex) throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setNull(columnIndex, 0);
		} else {
			this.inserter.setNull(columnIndex, 0);

			this.thisRow.setColumnValue(columnIndex - 1, null);
		}
	}

	/**
	 * JDBC 2.0 Update a column with a null value. The updateXXX() methods are
	 * used to update column values in the current row, or the insert row. The
	 * updateXXX() methods do not update the underlying database, instead the
	 * updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateNull(String columnName) throws SQLException {
		updateNull(findColumn(columnName));
	}

	/**
	 * JDBC 2.0 Update a column with an Object value. The updateXXX() methods
	 * are used to update column values in the current row, or the insert row.
	 * The updateXXX() methods do not update the underlying database, instead
	 * the updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateObject(int columnIndex, Object x)
			throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setObject(columnIndex, x);
		} else {
			this.inserter.setObject(columnIndex, x);

			this.thisRow.setColumnValue(columnIndex - 1, this.inserter
					.getBytesRepresentation(columnIndex - 1));
		}
	}

	/**
	 * JDBC 2.0 Update a column with an Object value. The updateXXX() methods
	 * are used to update column values in the current row, or the insert row.
	 * The updateXXX() methods do not update the underlying database, instead
	 * the updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @param scale
	 *            For java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types
	 *            this is the number of digits after the decimal. For all other
	 *            types this value will be ignored.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateObject(int columnIndex, Object x, int scale)
			throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setObject(columnIndex, x);
		} else {
			this.inserter.setObject(columnIndex, x);

			this.thisRow.setColumnValue(columnIndex - 1, this.inserter
					.getBytesRepresentation(columnIndex - 1));
		}
	}

	/**
	 * JDBC 2.0 Update a column with an Object value. The updateXXX() methods
	 * are used to update column values in the current row, or the insert row.
	 * The updateXXX() methods do not update the underlying database, instead
	 * the updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateObject(String columnName, Object x)
			throws SQLException {
		updateObject(findColumn(columnName), x);
	}

	/**
	 * JDBC 2.0 Update a column with an Object value. The updateXXX() methods
	 * are used to update column values in the current row, or the insert row.
	 * The updateXXX() methods do not update the underlying database, instead
	 * the updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 * @param scale
	 *            For java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types
	 *            this is the number of digits after the decimal. For all other
	 *            types this value will be ignored.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateObject(String columnName, Object x, int scale)
			throws SQLException {
		updateObject(findColumn(columnName), x);
	}

	/**
	 * JDBC 2.0 Update the underlying database with the new contents of the
	 * current row. Cannot be called when on the insert row.
	 *
	 * @exception SQLException
	 *                if a database-access error occurs, or if called when on
	 *                the insert row
	 * @throws NotUpdatable
	 *             DOCUMENT ME!
	 */
	public synchronized void updateRow() throws SQLException {
		if (!this.isUpdatable) {
			throw new NotUpdatable(this.notUpdatableReason);
		}

		if (this.doingUpdates) {
			this.updater.executeUpdate();
			refreshRow();
			this.doingUpdates = false;
		}

		//
		// fixes calling updateRow() and then doing more
		// updates on same row...
		syncUpdate();
	}

	/**
	 * JDBC 2.0 Update a column with a short value. The updateXXX() methods are
	 * used to update column values in the current row, or the insert row. The
	 * updateXXX() methods do not update the underlying database, instead the
	 * updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateShort(int columnIndex, short x)
			throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setShort(columnIndex, x);
		} else {
			this.inserter.setShort(columnIndex, x);

			this.thisRow.setColumnValue(columnIndex - 1, this.inserter
					.getBytesRepresentation(columnIndex - 1));
		}
	}

	/**
	 * JDBC 2.0 Update a column with a short value. The updateXXX() methods are
	 * used to update column values in the current row, or the insert row. The
	 * updateXXX() methods do not update the underlying database, instead the
	 * updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateShort(String columnName, short x)
			throws SQLException {
		updateShort(findColumn(columnName), x);
	}

	/**
	 * JDBC 2.0 Update a column with a String value. The updateXXX() methods are
	 * used to update column values in the current row, or the insert row. The
	 * updateXXX() methods do not update the underlying database, instead the
	 * updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateString(int columnIndex, String x)
			throws SQLException {
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
				this.thisRow.setColumnValue(columnIndex - 1, null);
			} else {
				if (getCharConverter() != null) {
					this.thisRow.setColumnValue(columnIndex - 1, StringUtils.getBytes(x,
							this.charConverter, this.charEncoding,
							this.connection.getServerCharacterEncoding(),
							this.connection.parserKnowsUnicode(), getExceptionInterceptor()));
				} else {
					this.thisRow.setColumnValue(columnIndex - 1, x.getBytes());
				}
			}
		}
	}

	/**
	 * JDBC 2.0 Update a column with a String value. The updateXXX() methods are
	 * used to update column values in the current row, or the insert row. The
	 * updateXXX() methods do not update the underlying database, instead the
	 * updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateString(String columnName, String x)
			throws SQLException {
		updateString(findColumn(columnName), x);
	}

	/**
	 * JDBC 2.0 Update a column with a Time value. The updateXXX() methods are
	 * used to update column values in the current row, or the insert row. The
	 * updateXXX() methods do not update the underlying database, instead the
	 * updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateTime(int columnIndex, java.sql.Time x)
			throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setTime(columnIndex, x);
		} else {
			this.inserter.setTime(columnIndex, x);

			this.thisRow.setColumnValue(columnIndex - 1, this.inserter
					.getBytesRepresentation(columnIndex - 1));
		}
	}

	/**
	 * JDBC 2.0 Update a column with a Time value. The updateXXX() methods are
	 * used to update column values in the current row, or the insert row. The
	 * updateXXX() methods do not update the underlying database, instead the
	 * updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateTime(String columnName, java.sql.Time x)
			throws SQLException {
		updateTime(findColumn(columnName), x);
	}

	/**
	 * JDBC 2.0 Update a column with a Timestamp value. The updateXXX() methods
	 * are used to update column values in the current row, or the insert row.
	 * The updateXXX() methods do not update the underlying database, instead
	 * the updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateTimestamp(int columnIndex,
			java.sql.Timestamp x) throws SQLException {
		if (!this.onInsertRow) {
			if (!this.doingUpdates) {
				this.doingUpdates = true;
				syncUpdate();
			}

			this.updater.setTimestamp(columnIndex, x);
		} else {
			this.inserter.setTimestamp(columnIndex, x);

			this.thisRow.setColumnValue(columnIndex - 1, this.inserter
					.getBytesRepresentation(columnIndex - 1));
		}
	}

	/**
	 * JDBC 2.0 Update a column with a Timestamp value. The updateXXX() methods
	 * are used to update column values in the current row, or the insert row.
	 * The updateXXX() methods do not update the underlying database, instead
	 * the updateRow() or insertRow() methods are called to update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 *
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public synchronized void updateTimestamp(String columnName,
			java.sql.Timestamp x) throws SQLException {
		updateTimestamp(findColumn(columnName), x);
	}
}
