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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.SQLException;

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

    Clob(String charDataInit, ExceptionInterceptor exceptionInterceptor) {
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
            throw SQLError.createSQLException(Messages.getString("Clob.6"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        int adjustedStartPos = (int) startPos - 1;
        int adjustedEndIndex = adjustedStartPos + length;

        if (this.charData != null) {
            if (adjustedEndIndex > this.charData.length()) {
                throw SQLError.createSQLException(Messages.getString("Clob.7"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
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
        return position(arg0.getSubString(0L, (int) arg0.length()), arg1);
    }

    /**
     * @see java.sql.Clob#position(String, long)
     */
    public long position(String stringToFind, long startPos) throws SQLException {
        if (startPos < 1) {
            throw SQLError.createSQLException(Messages.getString("Clob.8") + startPos + Messages.getString("Clob.9"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        }

        if (this.charData != null) {
            if ((startPos - 1) > this.charData.length()) {
                throw SQLError.createSQLException(Messages.getString("Clob.10"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
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
            throw SQLError.createSQLException(Messages.getString("Clob.0"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
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
            throw SQLError.createSQLException(Messages.getString("Clob.1"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
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
            throw SQLError.createSQLException(Messages.getString("Clob.2"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        if (str == null) {
            throw SQLError.createSQLException(Messages.getString("Clob.3"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        StringBuffer charBuf = new StringBuffer(this.charData);

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
            throw SQLError.createSQLException(Messages.getString("Clob.4"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        if (str == null) {
            throw SQLError.createSQLException(Messages.getString("Clob.5"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        StringBuffer charBuf = new StringBuffer(this.charData);

        pos--;

        String replaceString = str.substring(offset, len);

        charBuf.replace((int) pos, (int) (pos + replaceString.length()), replaceString);

        this.charData = charBuf.toString();

        return len;
    }

    /**
     * @see com.mysql.jdbc.OutputStreamWatcher#streamClosed(byte[])
     */
    public void streamClosed(WatchableOutputStream out) {
        int streamSize = out.size();

        if (streamSize < this.charData.length()) {
            try {
                out.write(StringUtils.getBytes(this.charData, null, null, false, null, this.exceptionInterceptor), streamSize, this.charData.length()
                        - streamSize);
            } catch (SQLException ex) {
                //
            }
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

    /**
     * @see com.mysql.jdbc.WriterWatcher#writerClosed(char[])
     */
    public void writerClosed(char[] charDataBeingWritten) {
        this.charData = new String(charDataBeingWritten);
    }

    /**
     * @see com.mysql.jdbc.WriterWatcher#writerClosed(char[])
     */
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
