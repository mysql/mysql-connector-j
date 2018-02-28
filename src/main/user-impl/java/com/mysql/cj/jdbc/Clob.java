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

package com.mysql.cj.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.SQLException;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.protocol.OutputStreamWatcher;
import com.mysql.cj.protocol.WatchableOutputStream;
import com.mysql.cj.protocol.WatchableStream;
import com.mysql.cj.protocol.WatchableWriter;
import com.mysql.cj.protocol.WriterWatcher;
import com.mysql.cj.util.StringUtils;

/**
 * Simplistic implementation of java.sql.Clob for MySQL Connector/J
 */
public class Clob implements java.sql.Clob, OutputStreamWatcher, WriterWatcher {
    private String charData;
    private ExceptionInterceptor exceptionInterceptor;

    Clob(ExceptionInterceptor exceptionInterceptor) {
        this.charData = "";
        this.exceptionInterceptor = exceptionInterceptor;
    }

    public Clob(String charDataInit, ExceptionInterceptor exceptionInterceptor) {
        this.charData = charDataInit;
        this.exceptionInterceptor = exceptionInterceptor;
    }

    /**
     * @see java.sql.Clob#getAsciiStream()
     */
    public InputStream getAsciiStream() throws SQLException {
        if (this.charData != null) {
            return new ByteArrayInputStream(StringUtils.getBytes(this.charData));
        }

        return null;
    }

    /**
     * @see java.sql.Clob#getCharacterStream()
     */
    public Reader getCharacterStream() throws SQLException {
        if (this.charData != null) {
            return new StringReader(this.charData);
        }

        return null;
    }

    /**
     * @see java.sql.Clob#getSubString(long, int)
     */
    public String getSubString(long startPos, int length) throws SQLException {
        if (startPos < 1) {
            throw SQLError.createSQLException(Messages.getString("Clob.6"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        int adjustedStartPos = (int) startPos - 1;
        int adjustedEndIndex = adjustedStartPos + length;

        if (this.charData != null) {
            if (adjustedEndIndex > this.charData.length()) {
                throw SQLError.createSQLException(Messages.getString("Clob.7"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            }

            return this.charData.substring(adjustedStartPos, adjustedEndIndex);
        }

        return null;
    }

    /**
     * @see java.sql.Clob#length()
     */
    public long length() throws SQLException {
        if (this.charData != null) {
            return this.charData.length();
        }

        return 0;
    }

    /**
     * @see java.sql.Clob#position(Clob, long)
     */
    public long position(java.sql.Clob arg0, long arg1) throws SQLException {
        return position(arg0.getSubString(1L, (int) arg0.length()), arg1);
    }

    /**
     * @see java.sql.Clob#position(String, long)
     */
    public long position(String stringToFind, long startPos) throws SQLException {
        if (startPos < 1) {
            throw SQLError.createSQLException(Messages.getString("Clob.8", new Object[] { startPos }), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        }

        if (this.charData != null) {
            if ((startPos - 1) > this.charData.length()) {
                throw SQLError.createSQLException(Messages.getString("Clob.10"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            }

            int pos = this.charData.indexOf(stringToFind, (int) (startPos - 1));

            return (pos == -1) ? (-1) : (pos + 1);
        }

        return -1;
    }

    /**
     * @see java.sql.Clob#setAsciiStream(long)
     */
    public OutputStream setAsciiStream(long indexToWriteAt) throws SQLException {
        if (indexToWriteAt < 1) {
            throw SQLError.createSQLException(Messages.getString("Clob.0"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        WatchableOutputStream bytesOut = new WatchableOutputStream();
        bytesOut.setWatcher(this);

        if (indexToWriteAt > 0) {
            bytesOut.write(StringUtils.getBytes(this.charData), 0, (int) (indexToWriteAt - 1));
        }

        return bytesOut;
    }

    /**
     * @see java.sql.Clob#setCharacterStream(long)
     */
    public Writer setCharacterStream(long indexToWriteAt) throws SQLException {
        if (indexToWriteAt < 1) {
            throw SQLError.createSQLException(Messages.getString("Clob.1"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        WatchableWriter writer = new WatchableWriter();
        writer.setWatcher(this);

        //
        // Don't call write() if nothing to write...
        //
        if (indexToWriteAt > 1) {
            writer.write(this.charData, 0, (int) (indexToWriteAt - 1));
        }

        return writer;
    }

    /**
     * @see java.sql.Clob#setString(long, String)
     */
    public int setString(long pos, String str) throws SQLException {
        if (pos < 1) {
            throw SQLError.createSQLException(Messages.getString("Clob.2"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        if (str == null) {
            throw SQLError.createSQLException(Messages.getString("Clob.3"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        StringBuilder charBuf = new StringBuilder(this.charData);

        pos--;

        int strLength = str.length();

        charBuf.replace((int) pos, (int) (pos + strLength), str);

        this.charData = charBuf.toString();

        return strLength;
    }

    /**
     * @see java.sql.Clob#setString(long, String, int, int)
     */
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        if (pos < 1) {
            throw SQLError.createSQLException(Messages.getString("Clob.4"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        if (str == null) {
            throw SQLError.createSQLException(Messages.getString("Clob.5"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        StringBuilder charBuf = new StringBuilder(this.charData);

        pos--;

        try {
            String replaceString = str.substring(offset, offset + len);

            charBuf.replace((int) pos, (int) (pos + replaceString.length()), replaceString);
        } catch (StringIndexOutOfBoundsException e) {
            throw SQLError.createSQLException(e.getMessage(), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, e, this.exceptionInterceptor);
        }

        this.charData = charBuf.toString();

        return len;
    }

    public void streamClosed(WatchableStream out) {
        int streamSize = out.size();

        if (streamSize < this.charData.length()) {
            out.write(StringUtils.getBytes(this.charData), streamSize, this.charData.length() - streamSize);
        }

        this.charData = StringUtils.toAsciiString(out.toByteArray());
    }

    /**
     * @see java.sql.Clob#truncate(long)
     */
    public void truncate(long length) throws SQLException {
        if (length > this.charData.length()) {
            throw SQLError.createSQLException(
                    Messages.getString("Clob.11") + this.charData.length() + Messages.getString("Clob.12") + length + Messages.getString("Clob.13"),
                    this.exceptionInterceptor);
        }

        this.charData = this.charData.substring(0, (int) length);
    }

    public void writerClosed(char[] charDataBeingWritten) {
        this.charData = new String(charDataBeingWritten);
    }

    public void writerClosed(WatchableWriter out) {
        int dataLength = out.size();

        if (dataLength < this.charData.length()) {
            out.write(this.charData, dataLength, this.charData.length() - dataLength);
        }

        this.charData = out.toString();
    }

    public void free() throws SQLException {
        this.charData = null;
    }

    public Reader getCharacterStream(long pos, long length) throws SQLException {
        return new StringReader(getSubString(pos, (int) length));
    }
}
