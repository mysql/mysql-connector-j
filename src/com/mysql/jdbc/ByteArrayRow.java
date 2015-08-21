/*
  Copyright (c) 2007, 2014, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * A RowHolder implementation that is for cached results (a-la mysql_store_result()).
 */
public class ByteArrayRow extends ResultSetRow {

    byte[][] internalRowData;

    public ByteArrayRow(byte[][] internalRowData, ExceptionInterceptor exceptionInterceptor) {
        super(exceptionInterceptor);

        this.internalRowData = internalRowData;
    }

    @Override
    public byte[] getColumnValue(int index) throws SQLException {
        return this.internalRowData[index];
    }

    @Override
    public void setColumnValue(int index, byte[] value) throws SQLException {
        this.internalRowData[index] = value;
    }

    @Override
    public String getString(int index, String encoding, MySQLConnection conn) throws SQLException {
        byte[] columnData = this.internalRowData[index];

        if (columnData == null) {
            return null;
        }

        return getString(encoding, conn, columnData, 0, columnData.length);
    }

    @Override
    public boolean isNull(int index) throws SQLException {
        return this.internalRowData[index] == null;
    }

    @Override
    public boolean isFloatingPointNumber(int index) throws SQLException {
        byte[] numAsBytes = this.internalRowData[index];

        if (this.internalRowData[index] == null || this.internalRowData[index].length == 0) {
            return false;
        }

        for (int i = 0; i < numAsBytes.length; i++) {
            if (((char) numAsBytes[i] == 'e') || ((char) numAsBytes[i] == 'E')) {
                return true;
            }
        }

        return false;
    }

    @Override
    public long length(int index) throws SQLException {
        if (this.internalRowData[index] == null) {
            return 0;
        }

        return this.internalRowData[index].length;
    }

    @Override
    public int getInt(int columnIndex) {
        if (this.internalRowData[columnIndex] == null) {
            return 0;
        }

        return StringUtils.getInt(this.internalRowData[columnIndex]);
    }

    @Override
    public long getLong(int columnIndex) {
        if (this.internalRowData[columnIndex] == null) {
            return 0;
        }

        return StringUtils.getLong(this.internalRowData[columnIndex]);
    }

    @Override
    public Timestamp getTimestampFast(int columnIndex, Calendar targetCalendar, TimeZone tz, boolean rollForward, MySQLConnection conn, ResultSetImpl rs)
            throws SQLException {
        byte[] columnValue = this.internalRowData[columnIndex];

        if (columnValue == null) {
            return null;
        }

        return getTimestampFast(columnIndex, this.internalRowData[columnIndex], 0, columnValue.length, targetCalendar, tz, rollForward, conn, rs);
    }

    @Override
    public double getNativeDouble(int columnIndex) throws SQLException {
        if (this.internalRowData[columnIndex] == null) {
            return 0;
        }

        return getNativeDouble(this.internalRowData[columnIndex], 0);
    }

    @Override
    public float getNativeFloat(int columnIndex) throws SQLException {
        if (this.internalRowData[columnIndex] == null) {
            return 0;
        }

        return getNativeFloat(this.internalRowData[columnIndex], 0);
    }

    @Override
    public int getNativeInt(int columnIndex) throws SQLException {
        if (this.internalRowData[columnIndex] == null) {
            return 0;
        }

        return getNativeInt(this.internalRowData[columnIndex], 0);
    }

    @Override
    public long getNativeLong(int columnIndex) throws SQLException {
        if (this.internalRowData[columnIndex] == null) {
            return 0;
        }

        return getNativeLong(this.internalRowData[columnIndex], 0);
    }

    @Override
    public short getNativeShort(int columnIndex) throws SQLException {
        if (this.internalRowData[columnIndex] == null) {
            return 0;
        }

        return getNativeShort(this.internalRowData[columnIndex], 0);
    }

    @Override
    public Timestamp getNativeTimestamp(int columnIndex, Calendar targetCalendar, TimeZone tz, boolean rollForward, MySQLConnection conn, ResultSetImpl rs)
            throws SQLException {
        byte[] bits = this.internalRowData[columnIndex];

        if (bits == null) {
            return null;
        }

        return getNativeTimestamp(bits, 0, bits.length, targetCalendar, tz, rollForward, conn, rs);
    }

    @Override
    public void closeOpenStreams() {
        // no-op for this type
    }

    @Override
    public InputStream getBinaryInputStream(int columnIndex) throws SQLException {
        if (this.internalRowData[columnIndex] == null) {
            return null;
        }

        return new ByteArrayInputStream(this.internalRowData[columnIndex]);
    }

    @Override
    public Reader getReader(int columnIndex) throws SQLException {
        InputStream stream = getBinaryInputStream(columnIndex);

        if (stream == null) {
            return null;
        }

        try {
            return new InputStreamReader(stream, this.metadata[columnIndex].getEncoding());
        } catch (UnsupportedEncodingException e) {
            SQLException sqlEx = SQLError.createSQLException("", this.exceptionInterceptor);

            sqlEx.initCause(e);

            throw sqlEx;
        }
    }

    @Override
    public Time getTimeFast(int columnIndex, Calendar targetCalendar, TimeZone tz, boolean rollForward, MySQLConnection conn, ResultSetImpl rs)
            throws SQLException {
        byte[] columnValue = this.internalRowData[columnIndex];

        if (columnValue == null) {
            return null;
        }

        return getTimeFast(columnIndex, this.internalRowData[columnIndex], 0, columnValue.length, targetCalendar, tz, rollForward, conn, rs);
    }

    @Override
    public Date getDateFast(int columnIndex, MySQLConnection conn, ResultSetImpl rs, Calendar targetCalendar) throws SQLException {
        byte[] columnValue = this.internalRowData[columnIndex];

        if (columnValue == null) {
            return null;
        }

        return getDateFast(columnIndex, this.internalRowData[columnIndex], 0, columnValue.length, conn, rs, targetCalendar);
    }

    @Override
    public Object getNativeDateTimeValue(int columnIndex, Calendar targetCalendar, int jdbcType, int mysqlType, TimeZone tz, boolean rollForward,
            MySQLConnection conn, ResultSetImpl rs) throws SQLException {
        byte[] columnValue = this.internalRowData[columnIndex];

        if (columnValue == null) {
            return null;
        }

        return getNativeDateTimeValue(columnIndex, columnValue, 0, columnValue.length, targetCalendar, jdbcType, mysqlType, tz, rollForward, conn, rs);
    }

    @Override
    public Date getNativeDate(int columnIndex, MySQLConnection conn, ResultSetImpl rs, Calendar cal) throws SQLException {
        byte[] columnValue = this.internalRowData[columnIndex];

        if (columnValue == null) {
            return null;
        }

        return getNativeDate(columnIndex, columnValue, 0, columnValue.length, conn, rs, cal);
    }

    @Override
    public Time getNativeTime(int columnIndex, Calendar targetCalendar, TimeZone tz, boolean rollForward, MySQLConnection conn, ResultSetImpl rs)
            throws SQLException {
        byte[] columnValue = this.internalRowData[columnIndex];

        if (columnValue == null) {
            return null;
        }

        return getNativeTime(columnIndex, columnValue, 0, columnValue.length, targetCalendar, tz, rollForward, conn, rs);
    }

    @Override
    public int getBytesSize() {
        if (this.internalRowData == null) {
            return 0;
        }

        int bytesSize = 0;

        for (int i = 0; i < this.internalRowData.length; i++) {
            if (this.internalRowData[i] != null) {
                bytesSize += this.internalRowData[i].length;
            }
        }

        return bytesSize;
    }
}