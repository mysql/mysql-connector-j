/*
 * Copyright (c) 2002, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.log;

import java.util.Date;

import com.mysql.cj.util.LogUtils;
import com.mysql.cj.util.StringUtils;

public class ProfilerEventImpl implements ProfilerEvent {

    private byte eventType;
    private String hostName;
    private String database;
    private long connectionId;
    private int statementId;
    private int resultSetId;
    private long eventCreationTime;
    private long eventDuration;
    private String durationUnits;
    private String eventCreationPointDesc;
    private String message;

    /**
     * Creates a new profiler event
     *
     * @param eventType
     *            the event type (from the constants TYPE_????)
     * @param hostName
     *            the hostname where the event occurs
     * @param db
     *            the database in use
     * @param connectionId
     *            the connection id (-1 if N/A)
     * @param statementId
     *            the statement id (-1 if N/A)
     * @param resultSetId
     *            the result set id (-1 if N/A)
     * @param eventDuration
     *            how long did the event last?
     * @param durationUnits
     *            time units user for eventDuration
     * @param eventCreationPoint
     *            event creation point as a Throwable
     * @param message
     *            optional message
     */
    public ProfilerEventImpl(byte eventType, String hostName, String db, long connectionId, int statementId, int resultSetId, long eventDuration,
            String durationUnits, Throwable eventCreationPoint, String message) {
        this(eventType, hostName, db, connectionId, statementId, resultSetId, System.currentTimeMillis(), eventDuration, durationUnits,
                LogUtils.findCallingClassAndMethod(eventCreationPoint), message);
    }

    private ProfilerEventImpl(byte eventType, String hostName, String db, long connectionId, int statementId, int resultSetId, long eventCreationTime,
            long eventDuration, String durationUnits, String eventCreationPointDesc, String message) {
        // null-strings are stored as empty strings to get consistent results with pack/unpack
        this.eventType = eventType;
        this.hostName = hostName == null ? "" : hostName;
        this.database = db == null ? "" : db;
        this.connectionId = connectionId;
        this.statementId = statementId;
        this.resultSetId = resultSetId;
        this.eventCreationTime = eventCreationTime;
        this.eventDuration = eventDuration;
        this.durationUnits = durationUnits == null ? "" : durationUnits;
        this.eventCreationPointDesc = eventCreationPointDesc == null ? "" : eventCreationPointDesc;
        this.message = message == null ? "" : message;
    }

    @Override
    public byte getEventType() {
        return this.eventType;
    }

    @Override
    public String getHostName() {
        return this.hostName;
    }

    @Override
    public String getDatabase() {
        return this.database;
    }

    @Override
    public long getConnectionId() {
        return this.connectionId;
    }

    @Override
    public int getStatementId() {
        return this.statementId;
    }

    @Override
    public int getResultSetId() {
        return this.resultSetId;
    }

    @Override
    public long getEventCreationTime() {
        return this.eventCreationTime;
    }

    @Override
    public long getEventDuration() {
        return this.eventDuration;
    }

    @Override
    public String getDurationUnits() {
        return this.durationUnits;
    }

    @Override
    public String getEventCreationPointAsString() {
        return this.eventCreationPointDesc;
    }

    @Override
    public String getMessage() {
        return this.message;
    }

    /**
     * Returns a representation of this event as a String.
     *
     * @return a String representation of this event.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("[");

        switch (this.getEventType()) {
            case TYPE_EXECUTE:
                buf.append("EXECUTE");
                break;
            case TYPE_FETCH:
                buf.append("FETCH");
                break;
            case TYPE_OBJECT_CREATION:
                buf.append("CONSTRUCT");
                break;
            case TYPE_PREPARE:
                buf.append("PREPARE");
                break;
            case TYPE_QUERY:
                buf.append("QUERY");
                break;
            case TYPE_USAGE:
                buf.append("USAGE ADVISOR");
                break;
            case TYPE_SLOW_QUERY:
                buf.append("SLOW QUERY");
                break;
            default:
                buf.append("UNKNOWN");
        }
        buf.append("] ");

        buf.append(this.message);

        buf.append(" [Created on: ");
        buf.append(new Date(this.eventCreationTime));
        buf.append(", duration: ");
        buf.append(this.eventDuration);
        buf.append(", connection-id: ");
        buf.append(this.connectionId);
        buf.append(", statement-id: ");
        buf.append(this.statementId);
        buf.append(", resultset-id: ");
        buf.append(this.resultSetId);
        buf.append(",");
        buf.append(this.eventCreationPointDesc);
        buf.append("]");

        return buf.toString();
    }

    /**
     * Unpacks a binary representation of this event.
     *
     * @param buf
     *            the binary representation of this event
     * @return the unpacked Event
     */
    public static ProfilerEvent unpack(byte[] buf) {
        int pos = 0;

        byte eventType = buf[pos++];

        byte[] host = readBytes(buf, pos);
        pos += 4 + host.length;

        byte[] db = readBytes(buf, pos);
        pos += 4 + db.length;

        long connectionId = readLong(buf, pos);
        pos += 8;
        int statementId = readInt(buf, pos);
        pos += 4;
        int resultSetId = readInt(buf, pos);
        pos += 4;
        long eventCreationTime = readLong(buf, pos);
        pos += 8;
        long eventDuration = readLong(buf, pos);
        pos += 8;

        byte[] eventDurationUnits = readBytes(buf, pos);
        pos += 4 + eventDurationUnits.length;

        byte[] eventCreationAsBytes = readBytes(buf, pos);
        pos += 4 + eventCreationAsBytes.length;

        byte[] message = readBytes(buf, pos);
        pos += 4 + message.length;

        // TODO charset?
        return new ProfilerEventImpl(eventType, StringUtils.toString(host, "ISO8859_1"), StringUtils.toString(db, "ISO8859_1"), connectionId, statementId,
                resultSetId, eventCreationTime, eventDuration, StringUtils.toString(eventDurationUnits, "ISO8859_1"),
                StringUtils.toString(eventCreationAsBytes, "ISO8859_1"), StringUtils.toString(message, "ISO8859_1"));
    }

    @Override
    public byte[] pack() {
        // TODO charset (Bug#41172 ?)
        byte[] hostNameAsBytes = StringUtils.getBytes(this.hostName, "ISO8859_1");
        byte[] dbAsBytes = StringUtils.getBytes(this.database, "ISO8859_1");
        byte[] durationUnitsAsBytes = StringUtils.getBytes(this.durationUnits, "ISO8859_1");
        byte[] eventCreationAsBytes = StringUtils.getBytes(this.eventCreationPointDesc, "ISO8859_1");
        byte[] messageAsBytes = StringUtils.getBytes(this.message, "ISO8859_1");

        int len = /* eventType */ 1 + /* hostName */ 4 + hostNameAsBytes.length + + /* db */ (4 + dbAsBytes.length) + /* connectionId */ 8 + /* statementId */ 4
                + /* resultSetId */ 4 + /* eventCreationTime */ 8 + /* eventDuration */ 8 + /* durationUnits */ 4 + durationUnitsAsBytes.length
                + /* eventCreationPointDesc */ 4 + eventCreationAsBytes.length + /* message */ 4 + messageAsBytes.length;

        byte[] buf = new byte[len];
        int pos = 0;
        buf[pos++] = this.eventType;
        pos = writeBytes(hostNameAsBytes, buf, pos);
        pos = writeBytes(dbAsBytes, buf, pos);
        pos = writeLong(this.connectionId, buf, pos);
        pos = writeInt(this.statementId, buf, pos);
        pos = writeInt(this.resultSetId, buf, pos);
        pos = writeLong(this.eventCreationTime, buf, pos);
        pos = writeLong(this.eventDuration, buf, pos);
        pos = writeBytes(durationUnitsAsBytes, buf, pos);
        pos = writeBytes(eventCreationAsBytes, buf, pos);
        pos = writeBytes(messageAsBytes, buf, pos);

        return buf;
    }

    private static int writeInt(int i, byte[] buf, int pos) {
        buf[pos++] = (byte) (i & 0xff);
        buf[pos++] = (byte) (i >>> 8);
        buf[pos++] = (byte) (i >>> 16);
        buf[pos++] = (byte) (i >>> 24);
        return pos;
    }

    private static int writeLong(long l, byte[] buf, int pos) {
        buf[pos++] = (byte) (l & 0xff);
        buf[pos++] = (byte) (l >>> 8);
        buf[pos++] = (byte) (l >>> 16);
        buf[pos++] = (byte) (l >>> 24);
        buf[pos++] = (byte) (l >>> 32);
        buf[pos++] = (byte) (l >>> 40);
        buf[pos++] = (byte) (l >>> 48);
        buf[pos++] = (byte) (l >>> 56);
        return pos;
    }

    private static int writeBytes(byte[] msg, byte[] buf, int pos) {
        pos = writeInt(msg.length, buf, pos);
        System.arraycopy(msg, 0, buf, pos, msg.length);
        return pos + msg.length;
    }

    private static int readInt(byte[] buf, int pos) {
        return buf[pos++] & 0xff | (buf[pos++] & 0xff) << 8 | (buf[pos++] & 0xff) << 16 | (buf[pos++] & 0xff) << 24;
    }

    private static long readLong(byte[] buf, int pos) {
        return buf[pos++] & 0xff | (long) (buf[pos++] & 0xff) << 8 | (long) (buf[pos++] & 0xff) << 16 | (long) (buf[pos++] & 0xff) << 24
                | (long) (buf[pos++] & 0xff) << 32 | (long) (buf[pos++] & 0xff) << 40 | (long) (buf[pos++] & 0xff) << 48 | (long) (buf[pos++] & 0xff) << 56;
    }

    private static byte[] readBytes(byte[] buf, int pos) {
        int length = readInt(buf, pos);
        byte[] msg = new byte[length];
        System.arraycopy(buf, pos + 4, msg, 0, length);
        return msg;
    }

}
