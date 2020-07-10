/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates.
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;

import com.mysql.cj.Constants;
import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.result.ResultSetInternalMethods;
import com.mysql.cj.protocol.OutputStreamWatcher;
import com.mysql.cj.protocol.WatchableOutputStream;
import com.mysql.cj.protocol.WatchableStream;

/**
 * The representation (mapping) in the JavaTM programming language of an SQL BLOB value. An SQL BLOB is a built-in type that stores a Binary Large Object
 * as a column value in a row of a database table. The driver implements Blob using an SQL locator(BLOB), which means that a Blob object contains a logical
 * pointer to the SQL BLOB data rather than the data itself. A Blob object is valid for the duration of the transaction in which is was created. Methods in
 * the interfaces ResultSet, CallableStatement, and PreparedStatement, such as getBlob and setBlob allow a programmer to access an SQL BLOB value. The Blob
 * interface provides methods for getting the length of an SQL BLOB (Binary Large Object) value, for materializing a BLOB value on the client, and for
 * determining the position of a pattern of bytes within a BLOB value. This class is new in the JDBC 2.0 API.
 */
public class Blob implements java.sql.Blob, OutputStreamWatcher {

    //
    // This is a real brain-dead implementation of BLOB. Once I add streamability to the I/O for MySQL this will be more efficiently implemented
    // (except for the position() method, ugh).
    //

    /** The binary data that makes up this BLOB */
    private byte[] binaryData = null;
    private boolean isClosed = false;
    private ExceptionInterceptor exceptionInterceptor;

    /**
     * Creates a Blob without data
     * 
     * @param exceptionInterceptor
     *            exception interceptor
     */
    Blob(ExceptionInterceptor exceptionInterceptor) {
        setBinaryData(Constants.EMPTY_BYTE_ARRAY);
        this.exceptionInterceptor = exceptionInterceptor;
    }

    /**
     * Creates a BLOB encapsulating the given binary data
     * 
     * @param data
     *            data to fill the Blob
     * @param exceptionInterceptor
     *            exception interceptor
     */
    public Blob(byte[] data, ExceptionInterceptor exceptionInterceptor) {
        setBinaryData(data);
        this.exceptionInterceptor = exceptionInterceptor;
    }

    /**
     * Creates an updatable BLOB that can update in-place (not implemented yet).
     * 
     * @param data
     *            data to fill the Blob
     * @param creatorResultSetToSet
     *            result set
     * @param columnIndexToSet
     *            column index
     */
    Blob(byte[] data, ResultSetInternalMethods creatorResultSetToSet, int columnIndexToSet) {
        setBinaryData(data);
    }

    private synchronized byte[] getBinaryData() {
        return this.binaryData;
    }

    @Override
    public synchronized java.io.InputStream getBinaryStream() throws SQLException {
        checkClosed();

        return new ByteArrayInputStream(getBinaryData());
    }

    @Override
    public synchronized byte[] getBytes(long pos, int length) throws SQLException {
        checkClosed();

        if (pos < 1) {
            throw SQLError.createSQLException(Messages.getString("Blob.2"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        pos--;

        if (pos > this.binaryData.length) {
            throw SQLError.createSQLException(Messages.getString("Blob.3"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        if (pos + length > this.binaryData.length) {
            throw SQLError.createSQLException(Messages.getString("Blob.4"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        byte[] newData = new byte[length];
        System.arraycopy(getBinaryData(), (int) (pos), newData, 0, length);

        return newData;
    }

    @Override
    public synchronized long length() throws SQLException {
        checkClosed();

        return getBinaryData().length;
    }

    @Override
    public synchronized long position(byte[] pattern, long start) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public synchronized long position(java.sql.Blob pattern, long start) throws SQLException {
        checkClosed();

        return position(pattern.getBytes(0, (int) pattern.length()), start);
    }

    private synchronized void setBinaryData(byte[] newBinaryData) {
        this.binaryData = newBinaryData;
    }

    @Override
    public synchronized OutputStream setBinaryStream(long indexToWriteAt) throws SQLException {
        checkClosed();

        if (indexToWriteAt < 1) {
            throw SQLError.createSQLException(Messages.getString("Blob.0"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        WatchableOutputStream bytesOut = new WatchableOutputStream();
        bytesOut.setWatcher(this);

        if (indexToWriteAt > 0) {
            bytesOut.write(this.binaryData, 0, (int) (indexToWriteAt - 1));
        }

        return bytesOut;
    }

    @Override
    public synchronized int setBytes(long writeAt, byte[] bytes) throws SQLException {
        checkClosed();

        return setBytes(writeAt, bytes, 0, bytes.length);
    }

    @Override
    public synchronized int setBytes(long writeAt, byte[] bytes, int offset, int length) throws SQLException {
        checkClosed();

        OutputStream bytesOut = setBinaryStream(writeAt);

        try {
            bytesOut.write(bytes, offset, length);
        } catch (IOException ioEx) {
            SQLException sqlEx = SQLError.createSQLException(Messages.getString("Blob.1"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                    this.exceptionInterceptor);
            sqlEx.initCause(ioEx);

            throw sqlEx;
        } finally {
            try {
                bytesOut.close();
            } catch (IOException doNothing) {
                // do nothing
            }
        }

        return length;
    }

    public synchronized void streamClosed(byte[] byteData) {
        this.binaryData = byteData;
    }

    @Override
    public synchronized void streamClosed(WatchableStream out) {
        int streamSize = out.size();

        if (streamSize < this.binaryData.length) {
            out.write(this.binaryData, streamSize, this.binaryData.length - streamSize);
        }

        this.binaryData = out.toByteArray();
    }

    @Override
    public synchronized void truncate(long len) throws SQLException {
        checkClosed();

        if (len < 0) {
            throw SQLError.createSQLException(Messages.getString("Blob.5"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        if (len > this.binaryData.length) {
            throw SQLError.createSQLException(Messages.getString("Blob.6"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        // TODO: Do this without copying byte[]s by maintaining some end pointer on the original data

        byte[] newData = new byte[(int) len];
        System.arraycopy(getBinaryData(), 0, newData, 0, (int) len);
        this.binaryData = newData;
    }

    @Override
    public synchronized void free() throws SQLException {
        this.binaryData = null;
        this.isClosed = true;
    }

    @Override
    public synchronized InputStream getBinaryStream(long pos, long length) throws SQLException {
        checkClosed();

        if (pos < 1) {
            throw SQLError.createSQLException(Messages.getString("Blob.2"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        pos--;

        if (pos > this.binaryData.length) {
            throw SQLError.createSQLException(Messages.getString("Blob.6"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        if (pos + length > this.binaryData.length) {
            throw SQLError.createSQLException(Messages.getString("Blob.4"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        return new ByteArrayInputStream(getBinaryData(), (int) pos, (int) length);
    }

    private synchronized void checkClosed() throws SQLException {
        if (this.isClosed) {
            throw SQLError.createSQLException(Messages.getString("Blob.7"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }
    }
}
