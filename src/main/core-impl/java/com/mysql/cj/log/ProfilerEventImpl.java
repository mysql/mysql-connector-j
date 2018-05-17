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

package com.mysql.cj.log;

import java.util.Date;

import com.mysql.cj.util.StringUtils;

public class ProfilerEventImpl implements ProfilerEvent {

    /**
     * Type of event
     */
    private byte eventType;

    /**
     * Associated connection (-1 for none)
     */
    protected long connectionId;

    /**
     * Associated statement (-1 for none)
     */
    protected int statementId;

    /**
     * Associated result set (-1 for none)
     */
    protected int resultSetId;

    /**
     * When was the event created?
     */
    protected long eventCreationTime;

    /**
     * How long did the event last?
     */
    protected long eventDuration;

    /**
     * What units was the duration measured in?
     */
    protected String durationUnits;

    /**
     * The hostname the event occurred on (as an index into a dictionary, used
     * by 'remote' profilers for efficiency)?
     */
    protected int hostNameIndex;

    /**
     * The hostname the event occurred on
     */
    protected String hostName;

    /**
     * The catalog the event occurred on (as an index into a dictionary, used by
     * 'remote' profilers for efficiency)?
     */
    protected int catalogIndex;

    /**
     * The catalog the event occurred on
     */
    protected String catalog;

    /**
     * Where was the event created (as an index into a dictionary, used by
     * 'remote' profilers for efficiency)?
     */
    protected int eventCreationPointIndex;

    /**
     * Where was the event created (as a string description of the
     * eventCreationPoint)?
     */
    protected String eventCreationPointDesc;

    /**
     * Optional event message
     */
    protected String message;

    /**
     * Creates a new profiler event
     * 
     * @param eventType
     *            the event type (from the constants TYPE_????)
     * @param hostName
     *            the hostname where the event occurs
     * @param catalog
     *            the catalog in use
     * @param connectionId
     *            the connection id (-1 if N/A)
     * @param statementId
     *            the statement id (-1 if N/A)
     * @param resultSetId
     *            the result set id (-1 if N/A)
     * @param eventCreationTime
     *            when was the event created?
     * @param eventDuration
     *            how long did the event last?
     * @param durationUnits
     *            time units user for eventDuration
     * @param eventCreationPointDesc
     *            event creation point as a string
     * @param eventCreationPoint
     *            event creation point as a Throwable
     * @param message
     *            optional message
     */
    public ProfilerEventImpl(byte eventType, String hostName, String catalog, long connectionId, int statementId, int resultSetId, long eventCreationTime,
            long eventDuration, String durationUnits, String eventCreationPointDesc, String eventCreationPoint, String message) {
        this.setEventType(eventType);
        this.connectionId = connectionId;
        this.statementId = statementId;
        this.resultSetId = resultSetId;
        this.eventCreationTime = eventCreationTime;
        this.eventDuration = eventDuration;
        this.durationUnits = durationUnits;
        this.eventCreationPointDesc = eventCreationPointDesc;
        this.message = message;
    }

    public String getEventCreationPointAsString() {
        return this.eventCreationPointDesc;
    }

    /**
     * Returns a representation of this event as a String.
     * 
     * @return a String representation of this event.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

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

            case TYPE_WARN:
                buf.append("WARN");
                break;
            case TYPE_SLOW_QUERY:
                buf.append("SLOW QUERY");
                break;
            default:
                buf.append("UNKNOWN");
        }

        buf.append(" created: ");
        buf.append(new Date(this.eventCreationTime));
        buf.append(" duration: ");
        buf.append(this.eventDuration);
        buf.append(" connection: ");
        buf.append(this.connectionId);
        buf.append(" statement: ");
        buf.append(this.statementId);
        buf.append(" resultset: ");
        buf.append(this.resultSetId);

        if (this.message != null) {
            buf.append(" message: ");
            buf.append(this.message);

        }

        if (this.eventCreationPointDesc != null) {
            buf.append("\n\nEvent Created at:\n");
            buf.append(this.eventCreationPointDesc);
        }

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
        long connectionId = readInt(buf, pos);
        pos += 8;
        int statementId = readInt(buf, pos);
        pos += 4;
        int resultSetId = readInt(buf, pos);
        pos += 4;
        long eventCreationTime = readLong(buf, pos);
        pos += 8;
        long eventDuration = readLong(buf, pos);
        pos += 4;

        byte[] eventDurationUnits = readBytes(buf, pos);
        pos += 4;

        if (eventDurationUnits != null) {
            pos += eventDurationUnits.length;
        }

        readInt(buf, pos);
        pos += 4;
        byte[] eventCreationAsBytes = readBytes(buf, pos);
        pos += 4;

        if (eventCreationAsBytes != null) {
            pos += eventCreationAsBytes.length;
        }

        byte[] message = readBytes(buf, pos);
        pos += 4;

        if (message != null) {
            pos += message.length;
        }

        return new ProfilerEventImpl(eventType, "", "", connectionId, statementId, resultSetId, eventCreationTime, eventDuration,
                StringUtils.toString(eventDurationUnits, "ISO8859_1"), StringUtils.toString(eventCreationAsBytes, "ISO8859_1"), null,
                StringUtils.toString(message, "ISO8859_1"));
    }

    public byte[] pack() {

        int len = 1 + 4 + 4 + 4 + 8 + 4 + 4;

        byte[] eventCreationAsBytes = null;

        getEventCreationPointAsString();

        if (this.eventCreationPointDesc != null) {
            eventCreationAsBytes = StringUtils.getBytes(this.eventCreationPointDesc, "ISO8859_1");
            len += (4 + eventCreationAsBytes.length);
        } else {
            len += 4;
        }

        byte[] messageAsBytes = null;

        if (this.message != null) {
            messageAsBytes = StringUtils.getBytes(this.message, "ISO8859_1");
            len += (4 + messageAsBytes.length);
        } else {
            len += 4;
        }

        byte[] durationUnitsAsBytes = null;

        if (this.durationUnits != null) {
            durationUnitsAsBytes = StringUtils.getBytes(this.durationUnits, "ISO8859_1");
            len += (4 + durationUnitsAsBytes.length);
        } else {
            len += 4;
            durationUnitsAsBytes = StringUtils.getBytes("", "ISO8859_1");
        }

        byte[] buf = new byte[len];

        int pos = 0;

        buf[pos++] = this.getEventType();
        pos = writeLong(this.connectionId, buf, pos);
        pos = writeInt(this.statementId, buf, pos);
        pos = writeInt(this.resultSetId, buf, pos);
        pos = writeLong(this.eventCreationTime, buf, pos);
        pos = writeLong(this.eventDuration, buf, pos);
        pos = writeBytes(durationUnitsAsBytes, buf, pos);
        pos = writeInt(this.eventCreationPointIndex, buf, pos);

        if (eventCreationAsBytes != null) {
            pos = writeBytes(eventCreationAsBytes, buf, pos);
        } else {
            pos = writeInt(0, buf, pos);
        }

        if (messageAsBytes != null) {
            pos = writeBytes(messageAsBytes, buf, pos);
        } else {
            pos = writeInt(0, buf, pos);
        }

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
        return (buf[pos++] & 0xff) | ((buf[pos++] & 0xff) << 8) | ((buf[pos++] & 0xff) << 16) | ((buf[pos++] & 0xff) << 24);

    }

    private static long readLong(byte[] buf, int pos) {
        return (buf[pos++] & 0xff) | ((long) (buf[pos++] & 0xff) << 8) | ((long) (buf[pos++] & 0xff) << 16) | ((long) (buf[pos++] & 0xff) << 24)
                | ((long) (buf[pos++] & 0xff) << 32) | ((long) (buf[pos++] & 0xff) << 40) | ((long) (buf[pos++] & 0xff) << 48)
                | ((long) (buf[pos++] & 0xff) << 56);
    }

    private static byte[] readBytes(byte[] buf, int pos) {
        int length = readInt(buf, pos);

        pos += 4;

        byte[] msg = new byte[length];
        System.arraycopy(buf, pos, msg, 0, length);

        return msg;
    }

    public String getCatalog() {
        return this.catalog;
    }

    public long getConnectionId() {
        return this.connectionId;
    }

    public long getEventCreationTime() {
        return this.eventCreationTime;
    }

    public long getEventDuration() {
        return this.eventDuration;
    }

    public String getDurationUnits() {
        return this.durationUnits;
    }

    public byte getEventType() {
        return this.eventType;
    }

    public int getResultSetId() {
        return this.resultSetId;
    }

    public int getStatementId() {
        return this.statementId;
    }

    public String getMessage() {
        return this.message;
    }

    public void setEventType(byte eventType) {
        this.eventType = eventType;
    }

}
