/*
  Copyright (c) 2007, 2018, Oracle and/or its affiliates. All rights reserved.

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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

/**
 * A RowHolder implementation that holds one row packet (which is re-used by the driver, and thus saves memory allocations), and tries when possible to avoid
 * allocations to break out the results as individual byte[]s.
 * 
 * (this isn't possible when doing things like reading floating point values).
 */
public class BufferRow extends ResultSetRow {
    private Buffer rowFromServer;

    /**
     * The beginning of the row packet
     */
    private int homePosition = 0;

    /**
     * The home position before the is-null bitmask for server-side prepared statement result sets
     */
    private int preNullBitmaskHomePosition = 0;

    /**
     * The last-requested index, used as an optimization, if you ask for the same index, we won't seek to find it. If you ask for an index that is >
     * than the last one requested, we start seeking from the last requested index.
     */
    private int lastRequestedIndex = -1;

    /**
     * The position of the last-requested index, optimization in concert with lastRequestedIndex.
     */
    private int lastRequestedPos;

    /**
     * The metadata of the fields of this result set.
     */
    private Field[] metadata;

    /**
     * Is this a row from a server-side prepared statement? If so, they're encoded differently, so we have different ways of finding where each column is, and
     * unpacking them.
     */
    private boolean isBinaryEncoded;

    /**
     * If binary-encoded, the NULL status of each column is at the beginning of the row, so we
     */
    private boolean[] isNull;

    private List<InputStream> openStreams;

    public BufferRow(Buffer buf, Field[] fields, boolean isBinaryEncoded, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        super(exceptionInterceptor);

        this.rowFromServer = buf;
        this.metadata = fields;
        this.isBinaryEncoded = isBinaryEncoded;
        this.homePosition = this.rowFromServer.getPosition();
        this.preNullBitmaskHomePosition = this.homePosition;

        if (fields != null) {
            setMetadata(fields);
        }
    }

    @Override
    public synchronized void closeOpenStreams() {
        if (this.openStreams != null) {
            // This would've looked slicker in a "for" loop but we want to skip over streams that fail to close (they probably won't ever) to be more robust and
            // close everything we _can_

            Iterator<InputStream> iter = this.openStreams.iterator();

            while (iter.hasNext()) {

                try {
                    iter.next().close();
                } catch (IOException e) {
                    // ignore - it can't really happen in this case
                }
            }

            this.openStreams.clear();
        }
    }

    private int findAndSeekToOffset(int index) throws SQLException {
        if (!this.isBinaryEncoded) {

            if (index == 0) {
                this.lastRequestedIndex = 0;
                this.lastRequestedPos = this.homePosition;
                this.rowFromServer.setPosition(this.homePosition);

                return 0;
            }

            if (index == this.lastRequestedIndex) {
                this.rowFromServer.setPosition(this.lastRequestedPos);

                return this.lastRequestedPos;
            }

            int startingIndex = 0;

            if (index > this.lastRequestedIndex) {
                if (this.lastRequestedIndex >= 0) {
                    startingIndex = this.lastRequestedIndex;
                } else {
                    startingIndex = 0;
                }

                this.rowFromServer.setPosition(this.lastRequestedPos);
            } else {
                this.rowFromServer.setPosition(this.homePosition);
            }

            for (int i = startingIndex; i < index; i++) {
                this.rowFromServer.fastSkipLenByteArray();
            }

            this.lastRequestedIndex = index;
            this.lastRequestedPos = this.rowFromServer.getPosition();

            return this.lastRequestedPos;
        }

        return findAndSeekToOffsetForBinaryEncoding(index);
    }

    private int findAndSeekToOffsetForBinaryEncoding(int index) throws SQLException {
        if (index == 0) {
            this.lastRequestedIndex = 0;
            this.lastRequestedPos = this.homePosition;
            this.rowFromServer.setPosition(this.homePosition);

            return 0;
        }

        if (index == this.lastRequestedIndex) {
            this.rowFromServer.setPosition(this.lastRequestedPos);

            return this.lastRequestedPos;
        }

        int startingIndex = 0;

        if (index > this.lastRequestedIndex) {
            if (this.lastRequestedIndex >= 0) {
                startingIndex = this.lastRequestedIndex;
            } else {
                // First-time "scan"
                startingIndex = 0;
                this.lastRequestedPos = this.homePosition;
            }

            this.rowFromServer.setPosition(this.lastRequestedPos);
        } else {
            this.rowFromServer.setPosition(this.homePosition);
        }

        for (int i = startingIndex; i < index; i++) {
            if (this.isNull[i]) {
                continue;
            }

            int curPosition = this.rowFromServer.getPosition();

            switch (this.metadata[i].getMysqlType()) {
                case MysqlDefs.FIELD_TYPE_NULL:
                    break; // for dummy binds

                case MysqlDefs.FIELD_TYPE_TINY:

                    this.rowFromServer.setPosition(curPosition + 1);
                    break;

                case MysqlDefs.FIELD_TYPE_SHORT:
                case MysqlDefs.FIELD_TYPE_YEAR:
                    this.rowFromServer.setPosition(curPosition + 2);

                    break;
                case MysqlDefs.FIELD_TYPE_LONG:
                case MysqlDefs.FIELD_TYPE_INT24:
                    this.rowFromServer.setPosition(curPosition + 4);

                    break;
                case MysqlDefs.FIELD_TYPE_LONGLONG:
                    this.rowFromServer.setPosition(curPosition + 8);

                    break;
                case MysqlDefs.FIELD_TYPE_FLOAT:
                    this.rowFromServer.setPosition(curPosition + 4);

                    break;
                case MysqlDefs.FIELD_TYPE_DOUBLE:
                    this.rowFromServer.setPosition(curPosition + 8);

                    break;
                case MysqlDefs.FIELD_TYPE_TIME:
                    this.rowFromServer.fastSkipLenByteArray();

                    break;
                case MysqlDefs.FIELD_TYPE_DATE:

                    this.rowFromServer.fastSkipLenByteArray();

                    break;
                case MysqlDefs.FIELD_TYPE_DATETIME:
                case MysqlDefs.FIELD_TYPE_TIMESTAMP:
                    this.rowFromServer.fastSkipLenByteArray();

                    break;
                case MysqlDefs.FIELD_TYPE_TINY_BLOB:
                case MysqlDefs.FIELD_TYPE_MEDIUM_BLOB:
                case MysqlDefs.FIELD_TYPE_LONG_BLOB:
                case MysqlDefs.FIELD_TYPE_BLOB:
                case MysqlDefs.FIELD_TYPE_VAR_STRING:
                case MysqlDefs.FIELD_TYPE_VARCHAR:
                case MysqlDefs.FIELD_TYPE_STRING:
                case MysqlDefs.FIELD_TYPE_DECIMAL:
                case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
                case MysqlDefs.FIELD_TYPE_GEOMETRY:
                case MysqlDefs.FIELD_TYPE_BIT:
                case MysqlDefs.FIELD_TYPE_JSON:
                    this.rowFromServer.fastSkipLenByteArray();

                    break;

                default:
                    throw SQLError.createSQLException(
                            Messages.getString("MysqlIO.97") + this.metadata[i].getMysqlType() + Messages.getString("MysqlIO.98") + (i + 1)
                                    + Messages.getString("MysqlIO.99") + this.metadata.length + Messages.getString("MysqlIO.100"),
                            SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
            }
        }

        this.lastRequestedIndex = index;
        this.lastRequestedPos = this.rowFromServer.getPosition();

        return this.lastRequestedPos;
    }

    @Override
    public synchronized InputStream getBinaryInputStream(int columnIndex) throws SQLException {
        if (this.isBinaryEncoded) {
            if (isNull(columnIndex)) {
                return null;
            }
        }

        findAndSeekToOffset(columnIndex);

        long length = this.rowFromServer.readFieldLength();

        int offset = this.rowFromServer.getPosition();

        if (length == Buffer.NULL_LENGTH) {
            return null;
        }

        InputStream stream = new ByteArrayInputStream(this.rowFromServer.getByteBuffer(), offset, (int) length);

        if (this.openStreams == null) {
            this.openStreams = new LinkedList<InputStream>();
        }

        return stream;
    }

    @Override
    public byte[] getColumnValue(int index) throws SQLException {
        findAndSeekToOffset(index);

        if (!this.isBinaryEncoded) {
            return this.rowFromServer.readLenByteArray(0);
        }

        if (this.isNull[index]) {
            return null;
        }

        switch (this.metadata[index].getMysqlType()) {
            case MysqlDefs.FIELD_TYPE_NULL:
                return null;

            case MysqlDefs.FIELD_TYPE_TINY:
                return new byte[] { this.rowFromServer.readByte() };

            case MysqlDefs.FIELD_TYPE_SHORT:
            case MysqlDefs.FIELD_TYPE_YEAR:
                return this.rowFromServer.getBytes(2);

            case MysqlDefs.FIELD_TYPE_LONG:
            case MysqlDefs.FIELD_TYPE_INT24:
                return this.rowFromServer.getBytes(4);

            case MysqlDefs.FIELD_TYPE_LONGLONG:
                return this.rowFromServer.getBytes(8);

            case MysqlDefs.FIELD_TYPE_FLOAT:
                return this.rowFromServer.getBytes(4);

            case MysqlDefs.FIELD_TYPE_DOUBLE:
                return this.rowFromServer.getBytes(8);

            case MysqlDefs.FIELD_TYPE_TIME:
            case MysqlDefs.FIELD_TYPE_DATE:
            case MysqlDefs.FIELD_TYPE_DATETIME:
            case MysqlDefs.FIELD_TYPE_TIMESTAMP:
            case MysqlDefs.FIELD_TYPE_TINY_BLOB:
            case MysqlDefs.FIELD_TYPE_MEDIUM_BLOB:
            case MysqlDefs.FIELD_TYPE_LONG_BLOB:
            case MysqlDefs.FIELD_TYPE_BLOB:
            case MysqlDefs.FIELD_TYPE_VAR_STRING:
            case MysqlDefs.FIELD_TYPE_VARCHAR:
            case MysqlDefs.FIELD_TYPE_STRING:
            case MysqlDefs.FIELD_TYPE_DECIMAL:
            case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
            case MysqlDefs.FIELD_TYPE_GEOMETRY:
            case MysqlDefs.FIELD_TYPE_BIT:
            case MysqlDefs.FIELD_TYPE_JSON:
                return this.rowFromServer.readLenByteArray(0);

            default:
                throw SQLError.createSQLException(
                        Messages.getString("MysqlIO.97") + this.metadata[index].getMysqlType() + Messages.getString("MysqlIO.98") + (index + 1)
                                + Messages.getString("MysqlIO.99") + this.metadata.length + Messages.getString("MysqlIO.100"),
                        SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
        }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {

        findAndSeekToOffset(columnIndex);

        long length = this.rowFromServer.readFieldLength();

        int offset = this.rowFromServer.getPosition();

        if (length == Buffer.NULL_LENGTH) {
            return 0;
        }

        return StringUtils.getInt(this.rowFromServer.getByteBuffer(), offset, offset + (int) length);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        findAndSeekToOffset(columnIndex);

        long length = this.rowFromServer.readFieldLength();

        int offset = this.rowFromServer.getPosition();

        if (length == Buffer.NULL_LENGTH) {
            return 0;
        }

        return StringUtils.getLong(this.rowFromServer.getByteBuffer(), offset, offset + (int) length);
    }

    @Override
    public double getNativeDouble(int columnIndex) throws SQLException {
        if (isNull(columnIndex)) {
            return 0;
        }

        findAndSeekToOffset(columnIndex);

        int offset = this.rowFromServer.getPosition();

        return getNativeDouble(this.rowFromServer.getByteBuffer(), offset);
    }

    @Override
    public float getNativeFloat(int columnIndex) throws SQLException {
        if (isNull(columnIndex)) {
            return 0;
        }

        findAndSeekToOffset(columnIndex);

        int offset = this.rowFromServer.getPosition();

        return getNativeFloat(this.rowFromServer.getByteBuffer(), offset);
    }

    @Override
    public int getNativeInt(int columnIndex) throws SQLException {
        if (isNull(columnIndex)) {
            return 0;
        }

        findAndSeekToOffset(columnIndex);

        int offset = this.rowFromServer.getPosition();

        return getNativeInt(this.rowFromServer.getByteBuffer(), offset);
    }

    @Override
    public long getNativeLong(int columnIndex) throws SQLException {
        if (isNull(columnIndex)) {
            return 0;
        }

        findAndSeekToOffset(columnIndex);

        int offset = this.rowFromServer.getPosition();

        return getNativeLong(this.rowFromServer.getByteBuffer(), offset);
    }

    @Override
    public short getNativeShort(int columnIndex) throws SQLException {
        if (isNull(columnIndex)) {
            return 0;
        }

        findAndSeekToOffset(columnIndex);

        int offset = this.rowFromServer.getPosition();

        return getNativeShort(this.rowFromServer.getByteBuffer(), offset);
    }

    @Override
    public Timestamp getNativeTimestamp(int columnIndex, Calendar targetCalendar, TimeZone tz, boolean rollForward, MySQLConnection conn, ResultSetImpl rs)
            throws SQLException {
        if (isNull(columnIndex)) {
            return null;
        }

        findAndSeekToOffset(columnIndex);

        long length = this.rowFromServer.readFieldLength();

        int offset = this.rowFromServer.getPosition();

        return getNativeTimestamp(this.rowFromServer.getByteBuffer(), offset, (int) length, targetCalendar, tz, rollForward, conn, rs);
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
    public String getString(int columnIndex, String encoding, MySQLConnection conn) throws SQLException {
        if (this.isBinaryEncoded) {
            if (isNull(columnIndex)) {
                return null;
            }
        }

        findAndSeekToOffset(columnIndex);

        long length = this.rowFromServer.readFieldLength();

        if (length == Buffer.NULL_LENGTH) {
            return null;
        }

        if (length == 0) {
            return "";
        }

        // TODO: I don't like this, would like to push functionality back to the buffer class somehow

        int offset = this.rowFromServer.getPosition();

        return getString(encoding, conn, this.rowFromServer.getByteBuffer(), offset, (int) length);
    }

    @Override
    public Time getTimeFast(int columnIndex, Calendar targetCalendar, TimeZone tz, boolean rollForward, MySQLConnection conn, ResultSetImpl rs)
            throws SQLException {
        if (isNull(columnIndex)) {
            return null;
        }

        findAndSeekToOffset(columnIndex);

        long length = this.rowFromServer.readFieldLength();

        int offset = this.rowFromServer.getPosition();

        return getTimeFast(columnIndex, this.rowFromServer.getByteBuffer(), offset, (int) length, targetCalendar, tz, rollForward, conn, rs);
    }

    @Override
    public Timestamp getTimestampFast(int columnIndex, Calendar targetCalendar, TimeZone tz, boolean rollForward, MySQLConnection conn, ResultSetImpl rs,
            boolean useGmtMillis, boolean useJDBCCompliantTimezoneShift) throws SQLException {
        if (isNull(columnIndex)) {
            return null;
        }

        findAndSeekToOffset(columnIndex);

        long length = this.rowFromServer.readFieldLength();

        int offset = this.rowFromServer.getPosition();

        return getTimestampFast(columnIndex, this.rowFromServer.getByteBuffer(), offset, (int) length, targetCalendar, tz, rollForward, conn, rs, useGmtMillis,
                useJDBCCompliantTimezoneShift);
    }

    @Override
    public boolean isFloatingPointNumber(int index) throws SQLException {
        if (this.isBinaryEncoded) {
            switch (this.metadata[index].getSQLType()) {
                case Types.FLOAT:
                case Types.DOUBLE:
                case Types.DECIMAL:
                case Types.NUMERIC:
                    return true;
                default:
                    return false;
            }
        }

        findAndSeekToOffset(index);

        long length = this.rowFromServer.readFieldLength();

        if (length == Buffer.NULL_LENGTH) {
            return false;
        }

        if (length == 0) {
            return false;
        }

        int offset = this.rowFromServer.getPosition();
        byte[] buffer = this.rowFromServer.getByteBuffer();

        for (int i = 0; i < (int) length; i++) {
            char c = (char) buffer[offset + i];

            if ((c == 'e') || (c == 'E')) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean isNull(int index) throws SQLException {
        if (!this.isBinaryEncoded) {
            findAndSeekToOffset(index);

            return this.rowFromServer.readFieldLength() == Buffer.NULL_LENGTH;
        }

        return this.isNull[index];
    }

    @Override
    public long length(int index) throws SQLException {
        findAndSeekToOffset(index);

        long length = this.rowFromServer.readFieldLength();

        if (length == Buffer.NULL_LENGTH) {
            return 0;
        }

        return length;
    }

    @Override
    public void setColumnValue(int index, byte[] value) throws SQLException {
        throw new OperationNotSupportedException();
    }

    @Override
    public ResultSetRow setMetadata(Field[] f) throws SQLException {
        super.setMetadata(f);

        if (this.isBinaryEncoded) {
            setupIsNullBitmask();
        }

        return this;
    }

    /**
     * Unpacks the bitmask at the head of the row packet that tells us what
     * columns hold null values, and sets the "home" position directly after the
     * bitmask.
     */
    private void setupIsNullBitmask() throws SQLException {
        if (this.isNull != null) {
            return; // we've already done this
        }

        this.rowFromServer.setPosition(this.preNullBitmaskHomePosition);

        int nullCount = (this.metadata.length + 9) / 8;

        byte[] nullBitMask = new byte[nullCount];

        for (int i = 0; i < nullCount; i++) {
            nullBitMask[i] = this.rowFromServer.readByte();
        }

        this.homePosition = this.rowFromServer.getPosition();

        this.isNull = new boolean[this.metadata.length];

        int nullMaskPos = 0;
        int bit = 4; // first two bits are reserved for future use

        for (int i = 0; i < this.metadata.length; i++) {

            this.isNull[i] = ((nullBitMask[nullMaskPos] & bit) != 0);

            if (((bit <<= 1) & 255) == 0) {
                bit = 1; /* To next byte */

                nullMaskPos++;
            }
        }
    }

    @Override
    public Date getDateFast(int columnIndex, MySQLConnection conn, ResultSetImpl rs, Calendar targetCalendar) throws SQLException {
        if (isNull(columnIndex)) {
            return null;
        }

        findAndSeekToOffset(columnIndex);

        long length = this.rowFromServer.readFieldLength();

        int offset = this.rowFromServer.getPosition();

        return getDateFast(columnIndex, this.rowFromServer.getByteBuffer(), offset, (int) length, conn, rs, targetCalendar);
    }

    @Override
    public java.sql.Date getNativeDate(int columnIndex, MySQLConnection conn, ResultSetImpl rs, Calendar cal) throws SQLException {
        if (isNull(columnIndex)) {
            return null;
        }

        findAndSeekToOffset(columnIndex);

        long length = this.rowFromServer.readFieldLength();

        int offset = this.rowFromServer.getPosition();

        return getNativeDate(columnIndex, this.rowFromServer.getByteBuffer(), offset, (int) length, conn, rs, cal);
    }

    @Override
    public Object getNativeDateTimeValue(int columnIndex, Calendar targetCalendar, int jdbcType, int mysqlType, TimeZone tz, boolean rollForward,
            MySQLConnection conn, ResultSetImpl rs) throws SQLException {
        if (isNull(columnIndex)) {
            return null;
        }

        findAndSeekToOffset(columnIndex);

        long length = this.rowFromServer.readFieldLength();

        int offset = this.rowFromServer.getPosition();

        return getNativeDateTimeValue(columnIndex, this.rowFromServer.getByteBuffer(), offset, (int) length, targetCalendar, jdbcType, mysqlType, tz,
                rollForward, conn, rs);
    }

    @Override
    public Time getNativeTime(int columnIndex, Calendar targetCalendar, TimeZone tz, boolean rollForward, MySQLConnection conn, ResultSetImpl rs)
            throws SQLException {
        if (isNull(columnIndex)) {
            return null;
        }

        findAndSeekToOffset(columnIndex);

        long length = this.rowFromServer.readFieldLength();

        int offset = this.rowFromServer.getPosition();

        return getNativeTime(columnIndex, this.rowFromServer.getByteBuffer(), offset, (int) length, targetCalendar, tz, rollForward, conn, rs);
    }

    @Override
    public int getBytesSize() {
        return this.rowFromServer.getBufLength();
    }
}