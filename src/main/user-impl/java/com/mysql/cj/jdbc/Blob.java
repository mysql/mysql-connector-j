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

package com.mysql.cj.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    private final Lock lock = new ReentrantLock();

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

    private byte[] getBinaryData() {
        this.lock.lock();
        try {
            return this.binaryData;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public java.io.InputStream getBinaryStream() throws SQLException {
        this.lock.lock();
        try {
            checkClosed();
            return new ByteArrayInputStream(getBinaryData());
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        this.lock.lock();
        try {
            checkClosed();

            if (pos < 1) {
                throw SQLError.createSQLException(Messages.getString("Blob.2"), MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            }

            pos--;

            if (pos > this.binaryData.length) {
                throw SQLError.createSQLException(Messages.getString("Blob.3"), MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            }

            if (pos + length > this.binaryData.length) {
                throw SQLError.createSQLException(Messages.getString("Blob.4"), MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            }

            byte[] newData = new byte[length];
            System.arraycopy(getBinaryData(), (int) pos, newData, 0, length);

            return newData;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public long length() throws SQLException {
        this.lock.lock();
        try {
            checkClosed();

            return getBinaryData().length;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        this.lock.lock();
        try {
            throw SQLError.createSQLFeatureNotSupportedException();
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public long position(java.sql.Blob pattern, long start) throws SQLException {
        this.lock.lock();
        try {
            checkClosed();

            return position(pattern.getBytes(0, (int) pattern.length()), start);
        } finally {
            this.lock.unlock();
        }
    }

    private void setBinaryData(byte[] newBinaryData) {
        this.lock.lock();
        try {
            this.binaryData = newBinaryData;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public OutputStream setBinaryStream(long indexToWriteAt) throws SQLException {
        this.lock.lock();
        try {
            checkClosed();

            if (indexToWriteAt < 1) {
                throw SQLError.createSQLException(Messages.getString("Blob.0"), MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            }

            WatchableOutputStream bytesOut = new WatchableOutputStream();
            bytesOut.setWatcher(this);

            if (indexToWriteAt > 0) {
                bytesOut.write(this.binaryData, 0, (int) (indexToWriteAt - 1));
            }

            return bytesOut;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public int setBytes(long writeAt, byte[] bytes) throws SQLException {
        this.lock.lock();
        try {
            checkClosed();

            return setBytes(writeAt, bytes, 0, bytes.length);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public int setBytes(long writeAt, byte[] bytes, int offset, int length) throws SQLException {
        this.lock.lock();
        try {
            checkClosed();

            OutputStream bytesOut = setBinaryStream(writeAt);

            try {
                bytesOut.write(bytes, offset, length);
            } catch (IOException ioEx) {
                SQLException sqlEx = SQLError.createSQLException(Messages.getString("Blob.1"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
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
        } finally {
            this.lock.unlock();
        }
    }

    public void streamClosed(byte[] byteData) {
        this.lock.lock();
        try {
            this.binaryData = byteData;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void streamClosed(WatchableStream out) {
        this.lock.lock();
        try {
            int streamSize = out.size();

            if (streamSize < this.binaryData.length) {
                out.write(this.binaryData, streamSize, this.binaryData.length - streamSize);
            }

            this.binaryData = out.toByteArray();
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void truncate(long len) throws SQLException {
        this.lock.lock();
        try {
            checkClosed();

            if (len < 0) {
                throw SQLError.createSQLException(Messages.getString("Blob.5"), MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            }

            if (len > this.binaryData.length) {
                throw SQLError.createSQLException(Messages.getString("Blob.6"), MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            }

            // TODO: Do this without copying byte[]s by maintaining some end pointer on the original data

            byte[] newData = new byte[(int) len];
            System.arraycopy(getBinaryData(), 0, newData, 0, (int) len);
            this.binaryData = newData;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void free() throws SQLException {
        this.lock.lock();
        try {
            this.binaryData = null;
            this.isClosed = true;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        this.lock.lock();
        try {
            checkClosed();

            if (pos < 1) {
                throw SQLError.createSQLException(Messages.getString("Blob.2"), MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            }

            pos--;

            if (pos > this.binaryData.length) {
                throw SQLError.createSQLException(Messages.getString("Blob.6"), MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            }

            if (pos + length > this.binaryData.length) {
                throw SQLError.createSQLException(Messages.getString("Blob.4"), MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            }

            return new ByteArrayInputStream(getBinaryData(), (int) pos, (int) length);
        } finally {
            this.lock.unlock();
        }
    }

    private void checkClosed() throws SQLException {
        this.lock.lock();
        try {
            if (this.isClosed) {
                throw SQLError.createSQLException(Messages.getString("Blob.7"), MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            }
        } finally {
            this.lock.unlock();
        }
    }

}
