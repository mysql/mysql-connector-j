/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * The representation (mapping) in the JavaTM programming language of an SQL BLOB value. An SQL BLOB is a built-in type that stores a Binary Large Object
 * as a column value in a row of a database table. The driver implements Blob using an SQL locator(BLOB), which means that a Blob object contains a logical
 * pointer to the SQL BLOB data rather than the data itself. A Blob object is valid for the duration of the transaction in which is was created. Methods in
 * the interfaces ResultSet, CallableStatement, and PreparedStatement, such as getBlob and setBlob allow a programmer to access an SQL BLOB value. The Blob
 * interface provides methods for getting the length of an SQL BLOB (Binary Large Object) value, for materializing a BLOB value on the client, and for
 * determining the position of a pattern of bytes within a BLOB value. This class is new in the JDBC 2.0 API.
 */
public class BlobFromLocator implements java.sql.Blob {
    private List<String> primaryKeyColumns = null;

    private List<String> primaryKeyValues = null;

    /** The ResultSet that created this BLOB */
    private ResultSetImpl creatorResultSet;

    private String blobColumnName = null;

    private String tableName = null;

    private int numColsInResultSet = 0;

    private int numPrimaryKeys = 0;

    private String quotedId;

    private ExceptionInterceptor exceptionInterceptor;

    /**
     * Creates an updatable BLOB that can update in-place
     */
    BlobFromLocator(ResultSetImpl creatorResultSetToSet, int blobColumnIndex, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        this.exceptionInterceptor = exceptionInterceptor;
        this.creatorResultSet = creatorResultSetToSet;

        this.numColsInResultSet = this.creatorResultSet.fields.length;
        this.quotedId = this.creatorResultSet.connection.getMetaData().getIdentifierQuoteString();

        if (this.numColsInResultSet > 1) {
            this.primaryKeyColumns = new ArrayList<String>();
            this.primaryKeyValues = new ArrayList<String>();

            for (int i = 0; i < this.numColsInResultSet; i++) {
                if (this.creatorResultSet.fields[i].isPrimaryKey()) {
                    StringBuilder keyName = new StringBuilder();
                    keyName.append(this.quotedId);

                    String originalColumnName = this.creatorResultSet.fields[i].getOriginalName();

                    if ((originalColumnName != null) && (originalColumnName.length() > 0)) {
                        keyName.append(originalColumnName);
                    } else {
                        keyName.append(this.creatorResultSet.fields[i].getName());
                    }

                    keyName.append(this.quotedId);

                    this.primaryKeyColumns.add(keyName.toString());
                    this.primaryKeyValues.add(this.creatorResultSet.getString(i + 1));
                }
            }
        } else {
            notEnoughInformationInQuery();
        }

        this.numPrimaryKeys = this.primaryKeyColumns.size();

        if (this.numPrimaryKeys == 0) {
            notEnoughInformationInQuery();
        }

        if (this.creatorResultSet.fields[0].getOriginalTableName() != null) {
            StringBuilder tableNameBuffer = new StringBuilder();

            String databaseName = this.creatorResultSet.fields[0].getDatabaseName();

            if ((databaseName != null) && (databaseName.length() > 0)) {
                tableNameBuffer.append(this.quotedId);
                tableNameBuffer.append(databaseName);
                tableNameBuffer.append(this.quotedId);
                tableNameBuffer.append('.');
            }

            tableNameBuffer.append(this.quotedId);
            tableNameBuffer.append(this.creatorResultSet.fields[0].getOriginalTableName());
            tableNameBuffer.append(this.quotedId);

            this.tableName = tableNameBuffer.toString();
        } else {
            StringBuilder tableNameBuffer = new StringBuilder();

            tableNameBuffer.append(this.quotedId);
            tableNameBuffer.append(this.creatorResultSet.fields[0].getTableName());
            tableNameBuffer.append(this.quotedId);

            this.tableName = tableNameBuffer.toString();
        }

        this.blobColumnName = this.quotedId + this.creatorResultSet.getString(blobColumnIndex) + this.quotedId;
    }

    private void notEnoughInformationInQuery() throws SQLException {
        throw SQLError.createSQLException("Emulated BLOB locators must come from a ResultSet with only one table selected, and all primary keys selected",
                SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
    }

    /**
     * @see Blob#setBinaryStream(long)
     */
    public OutputStream setBinaryStream(long indexToWriteAt) throws SQLException {
        throw SQLError.notImplemented();
    }

    /**
     * Retrieves the BLOB designated by this Blob instance as a stream.
     * 
     * @return this BLOB represented as a binary stream of bytes.
     * 
     * @throws SQLException
     *             if a database error occurs
     */
    public java.io.InputStream getBinaryStream() throws SQLException {
        // TODO: Make fetch size configurable
        return new BufferedInputStream(new LocatorInputStream(), this.creatorResultSet.connection.getLocatorFetchBufferSize());
    }

    /**
     * @see Blob#setBytes(long, byte[], int, int)
     */
    public int setBytes(long writeAt, byte[] bytes, int offset, int length) throws SQLException {
        java.sql.PreparedStatement pStmt = null;

        if ((offset + length) > bytes.length) {
            length = bytes.length - offset;
        }

        byte[] bytesToWrite = new byte[length];
        System.arraycopy(bytes, offset, bytesToWrite, 0, length);

        // FIXME: Needs to use identifiers for column/table names
        StringBuilder query = new StringBuilder("UPDATE ");
        query.append(this.tableName);
        query.append(" SET ");
        query.append(this.blobColumnName);
        query.append(" = INSERT(");
        query.append(this.blobColumnName);
        query.append(", ");
        query.append(writeAt);
        query.append(", ");
        query.append(length);
        query.append(", ?) WHERE ");

        query.append(this.primaryKeyColumns.get(0));
        query.append(" = ?");

        for (int i = 1; i < this.numPrimaryKeys; i++) {
            query.append(" AND ");
            query.append(this.primaryKeyColumns.get(i));
            query.append(" = ?");
        }

        try {
            // FIXME: Have this passed in instead
            pStmt = this.creatorResultSet.connection.prepareStatement(query.toString());

            pStmt.setBytes(1, bytesToWrite);

            for (int i = 0; i < this.numPrimaryKeys; i++) {
                pStmt.setString(i + 2, this.primaryKeyValues.get(i));
            }

            int rowsUpdated = pStmt.executeUpdate();

            if (rowsUpdated != 1) {
                throw SQLError.createSQLException("BLOB data not found! Did primary keys change?", SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
            }
        } finally {
            if (pStmt != null) {
                try {
                    pStmt.close();
                } catch (SQLException sqlEx) {
                    // do nothing
                }

                pStmt = null;
            }
        }

        return (int) length();
    }

    /**
     * @see Blob#setBytes(long, byte[])
     */
    public int setBytes(long writeAt, byte[] bytes) throws SQLException {
        return setBytes(writeAt, bytes, 0, bytes.length);
    }

    /**
     * Returns as an array of bytes, part or all of the BLOB value that this
     * Blob object designates.
     * 
     * @param pos
     *            where to start the part of the BLOB
     * @param length
     *            the length of the part of the BLOB you want returned.
     * 
     * @return the bytes stored in the blob starting at position <code>pos</code> and having a length of <code>length</code>.
     * 
     * @throws SQLException
     *             if a database error occurs
     */
    public byte[] getBytes(long pos, int length) throws SQLException {
        java.sql.PreparedStatement pStmt = null;

        try {

            pStmt = createGetBytesStatement();

            return getBytesInternal(pStmt, pos, length);
        } finally {
            if (pStmt != null) {
                try {
                    pStmt.close();
                } catch (SQLException sqlEx) {
                    // do nothing
                }

                pStmt = null;
            }
        }
    }

    /**
     * Returns the number of bytes in the BLOB value designated by this Blob
     * object.
     * 
     * @return the length of this blob
     * 
     * @throws SQLException
     *             if a database error occurs
     */
    public long length() throws SQLException {
        java.sql.ResultSet blobRs = null;
        java.sql.PreparedStatement pStmt = null;

        // FIXME: Needs to use identifiers for column/table names
        StringBuilder query = new StringBuilder("SELECT LENGTH(");
        query.append(this.blobColumnName);
        query.append(") FROM ");
        query.append(this.tableName);
        query.append(" WHERE ");

        query.append(this.primaryKeyColumns.get(0));
        query.append(" = ?");

        for (int i = 1; i < this.numPrimaryKeys; i++) {
            query.append(" AND ");
            query.append(this.primaryKeyColumns.get(i));
            query.append(" = ?");
        }

        try {
            // FIXME: Have this passed in instead
            pStmt = this.creatorResultSet.connection.prepareStatement(query.toString());

            for (int i = 0; i < this.numPrimaryKeys; i++) {
                pStmt.setString(i + 1, this.primaryKeyValues.get(i));
            }

            blobRs = pStmt.executeQuery();

            if (blobRs.next()) {
                return blobRs.getLong(1);
            }

            throw SQLError.createSQLException("BLOB data not found! Did primary keys change?", SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
        } finally {
            if (blobRs != null) {
                try {
                    blobRs.close();
                } catch (SQLException sqlEx) {
                    // do nothing
                }

                blobRs = null;
            }

            if (pStmt != null) {
                try {
                    pStmt.close();
                } catch (SQLException sqlEx) {
                    // do nothing
                }

                pStmt = null;
            }
        }
    }

    /**
     * Finds the position of the given pattern in this BLOB.
     * 
     * @param pattern
     *            the pattern to find
     * @param start
     *            where to start finding the pattern
     * 
     * @return the position where the pattern is found in the BLOB, -1 if not
     *         found
     * 
     * @throws SQLException
     *             if a database error occurs
     */
    public long position(java.sql.Blob pattern, long start) throws SQLException {
        return position(pattern.getBytes(0, (int) pattern.length()), start);
    }

    /**
     * @see java.sql.Blob#position(byte[], long)
     */
    public long position(byte[] pattern, long start) throws SQLException {
        java.sql.ResultSet blobRs = null;
        java.sql.PreparedStatement pStmt = null;

        // FIXME: Needs to use identifiers for column/table names
        StringBuilder query = new StringBuilder("SELECT LOCATE(");
        query.append("?, ");
        query.append(this.blobColumnName);
        query.append(", ");
        query.append(start);
        query.append(") FROM ");
        query.append(this.tableName);
        query.append(" WHERE ");

        query.append(this.primaryKeyColumns.get(0));
        query.append(" = ?");

        for (int i = 1; i < this.numPrimaryKeys; i++) {
            query.append(" AND ");
            query.append(this.primaryKeyColumns.get(i));
            query.append(" = ?");
        }

        try {
            // FIXME: Have this passed in instead
            pStmt = this.creatorResultSet.connection.prepareStatement(query.toString());
            pStmt.setBytes(1, pattern);

            for (int i = 0; i < this.numPrimaryKeys; i++) {
                pStmt.setString(i + 2, this.primaryKeyValues.get(i));
            }

            blobRs = pStmt.executeQuery();

            if (blobRs.next()) {
                return blobRs.getLong(1);
            }

            throw SQLError.createSQLException("BLOB data not found! Did primary keys change?", SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
        } finally {
            if (blobRs != null) {
                try {
                    blobRs.close();
                } catch (SQLException sqlEx) {
                    // do nothing
                }

                blobRs = null;
            }

            if (pStmt != null) {
                try {
                    pStmt.close();
                } catch (SQLException sqlEx) {
                    // do nothing
                }

                pStmt = null;
            }
        }
    }

    /**
     * @see Blob#truncate(long)
     */
    public void truncate(long length) throws SQLException {
        java.sql.PreparedStatement pStmt = null;

        // FIXME: Needs to use identifiers for column/table names
        StringBuilder query = new StringBuilder("UPDATE ");
        query.append(this.tableName);
        query.append(" SET ");
        query.append(this.blobColumnName);
        query.append(" = LEFT(");
        query.append(this.blobColumnName);
        query.append(", ");
        query.append(length);
        query.append(") WHERE ");

        query.append(this.primaryKeyColumns.get(0));
        query.append(" = ?");

        for (int i = 1; i < this.numPrimaryKeys; i++) {
            query.append(" AND ");
            query.append(this.primaryKeyColumns.get(i));
            query.append(" = ?");
        }

        try {
            // FIXME: Have this passed in instead
            pStmt = this.creatorResultSet.connection.prepareStatement(query.toString());

            for (int i = 0; i < this.numPrimaryKeys; i++) {
                pStmt.setString(i + 1, this.primaryKeyValues.get(i));
            }

            int rowsUpdated = pStmt.executeUpdate();

            if (rowsUpdated != 1) {
                throw SQLError.createSQLException("BLOB data not found! Did primary keys change?", SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
            }
        } finally {
            if (pStmt != null) {
                try {
                    pStmt.close();
                } catch (SQLException sqlEx) {
                    // do nothing
                }

                pStmt = null;
            }
        }
    }

    java.sql.PreparedStatement createGetBytesStatement() throws SQLException {
        StringBuilder query = new StringBuilder("SELECT SUBSTRING(");

        query.append(this.blobColumnName);
        query.append(", ");
        query.append("?");
        query.append(", ");
        query.append("?");
        query.append(") FROM ");
        query.append(this.tableName);
        query.append(" WHERE ");

        query.append(this.primaryKeyColumns.get(0));
        query.append(" = ?");

        for (int i = 1; i < this.numPrimaryKeys; i++) {
            query.append(" AND ");
            query.append(this.primaryKeyColumns.get(i));
            query.append(" = ?");
        }

        return this.creatorResultSet.connection.prepareStatement(query.toString());
    }

    byte[] getBytesInternal(java.sql.PreparedStatement pStmt, long pos, int length) throws SQLException {

        java.sql.ResultSet blobRs = null;

        try {

            pStmt.setLong(1, pos);
            pStmt.setInt(2, length);

            for (int i = 0; i < this.numPrimaryKeys; i++) {
                pStmt.setString(i + 3, this.primaryKeyValues.get(i));
            }

            blobRs = pStmt.executeQuery();

            if (blobRs.next()) {
                return ((com.mysql.jdbc.ResultSetImpl) blobRs).getBytes(1, true);
            }

            throw SQLError.createSQLException("BLOB data not found! Did primary keys change?", SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
        } finally {
            if (blobRs != null) {
                try {
                    blobRs.close();
                } catch (SQLException sqlEx) {
                    // do nothing
                }

                blobRs = null;
            }
        }
    }

    class LocatorInputStream extends InputStream {
        long currentPositionInBlob = 0;

        long length = 0;

        java.sql.PreparedStatement pStmt = null;

        LocatorInputStream() throws SQLException {
            this.length = length();
            this.pStmt = createGetBytesStatement();
        }

        @SuppressWarnings("synthetic-access")
        LocatorInputStream(long pos, long len) throws SQLException {
            this.length = pos + len;
            this.currentPositionInBlob = pos;
            long blobLength = length();

            if (pos + len > blobLength) {
                throw SQLError.createSQLException(
                        Messages.getString("Blob.invalidStreamLength", new Object[] { Long.valueOf(blobLength), Long.valueOf(pos), Long.valueOf(len) }),
                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT, BlobFromLocator.this.exceptionInterceptor);
            }

            if (pos < 1) {
                throw SQLError.createSQLException(Messages.getString("Blob.invalidStreamPos"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        BlobFromLocator.this.exceptionInterceptor);
            }

            if (pos > blobLength) {
                throw SQLError.createSQLException(Messages.getString("Blob.invalidStreamPos"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        BlobFromLocator.this.exceptionInterceptor);
            }
        }

        @Override
        public int read() throws IOException {
            if (this.currentPositionInBlob + 1 > this.length) {
                return -1;
            }

            try {
                byte[] asBytes = getBytesInternal(this.pStmt, (this.currentPositionInBlob++) + 1, 1);

                if (asBytes == null) {
                    return -1;
                }

                return asBytes[0];
            } catch (SQLException sqlEx) {
                throw new IOException(sqlEx.toString());
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.io.InputStream#read(byte[], int, int)
         */
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (this.currentPositionInBlob + 1 > this.length) {
                return -1;
            }

            try {
                byte[] asBytes = getBytesInternal(this.pStmt, (this.currentPositionInBlob) + 1, len);

                if (asBytes == null) {
                    return -1;
                }

                System.arraycopy(asBytes, 0, b, off, asBytes.length);

                this.currentPositionInBlob += asBytes.length;

                return asBytes.length;
            } catch (SQLException sqlEx) {
                throw new IOException(sqlEx.toString());
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.io.InputStream#read(byte[])
         */
        @Override
        public int read(byte[] b) throws IOException {
            if (this.currentPositionInBlob + 1 > this.length) {
                return -1;
            }

            try {
                byte[] asBytes = getBytesInternal(this.pStmt, (this.currentPositionInBlob) + 1, b.length);

                if (asBytes == null) {
                    return -1;
                }

                System.arraycopy(asBytes, 0, b, 0, asBytes.length);

                this.currentPositionInBlob += asBytes.length;

                return asBytes.length;
            } catch (SQLException sqlEx) {
                throw new IOException(sqlEx.toString());
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.io.InputStream#close()
         */
        @Override
        public void close() throws IOException {
            if (this.pStmt != null) {
                try {
                    this.pStmt.close();
                } catch (SQLException sqlEx) {
                    throw new IOException(sqlEx.toString());
                }
            }

            super.close();
        }
    }

    public void free() throws SQLException {
        this.creatorResultSet = null;
        this.primaryKeyColumns = null;
        this.primaryKeyValues = null;
    }

    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        return new LocatorInputStream(pos, length);
    }
}
