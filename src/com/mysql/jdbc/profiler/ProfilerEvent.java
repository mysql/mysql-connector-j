/*
  Copyright (c) 2002, 2019, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc.profiler;

import java.util.Date;

import com.mysql.jdbc.StringUtils;
import com.mysql.jdbc.log.LogUtils;

public class ProfilerEvent {
    /**
     * Profiler event for usage advisor
     */
    public static final byte TYPE_USAGE = 0;

    /**
     * An alias for TYPE_USAGE
     */
    public static final byte TYPE_WARN = 0;

    /**
     * Profiler creating object type event
     */
    public static final byte TYPE_OBJECT_CREATION = 1;

    /**
     * Profiler event for prepared statements being prepared
     */
    public static final byte TYPE_PREPARE = 2;

    /**
     * Profiler event for a query being executed
     */
    public static final byte TYPE_QUERY = 3;

    /**
     * Profiler event for prepared statements being executed
     */
    public static final byte TYPE_EXECUTE = 4;

    /**
     * Profiler event for result sets being retrieved
     */
    public static final byte TYPE_FETCH = 5;

    /**
     * Profiler event for slow query
     */
    public static final byte TYPE_SLOW_QUERY = 6;

    /**
     * Not available value.
     */
    public static final byte NA = -1;

    /**
     * Type of event
     */
    protected byte eventType;

    /**
     * The hostname the event occurred on
     */
    protected String hostName;

    /**
     * The catalog the event occurred on
     */
    protected String catalog;

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
     * Where was the event created (as a string description of the
     * eventCreationPoint)?
     */
    protected String eventCreationPointDesc;

    /**
     * Optional event message
     */
    protected String message;

    /**
     * The hostname the event occurred on (as an index into a dictionary, used
     * by 'remote' profilers for efficiency)?
     */
    public int hostNameIndex;

    /**
     * The catalog the event occurred on (as an index into a dictionary, used by
     * 'remote' profilers for efficiency)?
     */
    public int catalogIndex;

    /**
     * Where was the event created (as an index into a dictionary, used by
     * 'remote' profilers for efficiency)?
     */
    public int eventCreationPointIndex;

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
     * @param eventDuration
     *            how long did the event last?
     * @param durationUnits
     *            what units was the duration measured in?
     * @param eventCreationPoint
     *            event creation point as a Throwable
     * @param message
     *            optional message
     */
    public ProfilerEvent(byte eventType, String hostName, String catalog, long connectionId, int statementId, int resultSetId, long eventDuration,
            String durationUnits, Throwable eventCreationPoint, String message) {

        this(eventType, hostName, catalog, connectionId, statementId, resultSetId, System.currentTimeMillis(), eventDuration, durationUnits,
                LogUtils.findCallingClassAndMethod(eventCreationPoint), message, NA, NA, NA);
    }

    private ProfilerEvent(byte eventType, String hostName, String catalog, long connectionId, int statementId, int resultSetId, long eventCreationTime,
            long eventDuration, String durationUnits, String eventCreationPointDesc, String message, int hostNameIndex, int catalogIndex,
            int eventCreationPointIndex) {
        // null-strings are stored as empty strings to get consistent results with pack/unpack
        this.eventType = eventType;
        this.hostName = hostName == null ? "" : hostName;
        this.catalog = catalog == null ? "" : catalog;
        this.connectionId = connectionId;
        this.statementId = statementId;
        this.resultSetId = resultSetId;
        this.eventCreationTime = eventCreationTime;
        this.eventDuration = eventDuration;
        this.durationUnits = durationUnits == null ? "" : durationUnits;
        this.eventCreationPointDesc = eventCreationPointDesc == null ? "" : eventCreationPointDesc;
        this.message = message == null ? "" : message;
        this.hostNameIndex = hostNameIndex;
        this.catalogIndex = catalogIndex;
        this.eventCreationPointIndex = eventCreationPointIndex;
    }

    /**
     * Returns the event type
     * 
     * @return the event type
     */
    public byte getEventType() {
        return this.eventType;
    }

    /**
     * Returns the host name the event occurred on.
     * 
     * @return host name
     */
    public String getHostName() {
        return this.hostName;
    }

    /**
     * Returns the catalog the event occurred on.
     * 
     * @return the catalog in use
     */
    public String getCatalog() {
        return this.catalog;
    }

    /**
     * Returns the id of the associated connection (-1 for none).
     * 
     * @return the connection in use
     */
    public long getConnectionId() {
        return this.connectionId;
    }

    /**
     * Returns the id of the associated statement (-1 for none).
     * 
     * @return the statement in use
     */
    public int getStatementId() {
        return this.statementId;
    }

    /**
     * Returns the id of the associated result set (-1 for none).
     * 
     * @return the result set in use
     */
    public int getResultSetId() {
        return this.resultSetId;
    }

    /**
     * Returns the time (in System.currentTimeMillis() form) when this event was created.
     * 
     * @return the time this event was created
     */
    public long getEventCreationTime() {
        return this.eventCreationTime;
    }

    /**
     * Returns the duration of the event in milliseconds
     * 
     * @return the duration of the event in milliseconds
     */
    public long getEventDuration() {
        return this.eventDuration;
    }

    /**
     * Returns the units for getEventDuration()
     * 
     * @return name of duration units
     */
    public String getDurationUnits() {
        return this.durationUnits;
    }

    /**
     * Returns the description of where the event was created.
     * 
     * @return a description of where this event was created.
     */
    public String getEventCreationPointAsString() {
        return this.eventCreationPointDesc;
    }

    /**
     * Returns the optional message for this event
     * 
     * @return the message stored in this event
     */
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
        buf.append(", hostNameIndex: ");
        buf.append(this.hostNameIndex);
        buf.append(", catalogIndex: ");
        buf.append(this.catalogIndex);
        buf.append(", eventCreationPointIndex: ");
        buf.append(this.eventCreationPointIndex);
        buf.append("]");

        return buf.toString();
    }

    /**
     * Unpacks a binary representation of this event.
     * 
     * @param buf
     *            the binary representation of this event
     * @return the unpacked Event
     * @throws Exception
     *             if an error occurs while unpacking the event
     */
    public static ProfilerEvent unpack(byte[] buf) throws Exception {
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

        int hostNameIndex = readInt(buf, pos);
        pos += 4;
        int catalogIndex = readInt(buf, pos);
        pos += 4;
        int eventCreationPointIndex = readInt(buf, pos);
        pos += 4;

        // TODO charset?
        return new ProfilerEvent(eventType, StringUtils.toString(host, "ISO8859_1"), StringUtils.toString(db, "ISO8859_1"), connectionId, statementId,
                resultSetId, eventCreationTime, eventDuration, StringUtils.toString(eventDurationUnits, "ISO8859_1"),
                StringUtils.toString(eventCreationAsBytes, "ISO8859_1"), StringUtils.toString(message, "ISO8859_1"), hostNameIndex, catalogIndex,
                eventCreationPointIndex);
    }

    /**
     * Creates a binary representation of this event.
     * 
     * @return a binary representation of this event
     * @throws Exception
     *             if an error occurs while packing this event.
     */
    public byte[] pack() throws Exception {

        // TODO charset (Bug#41172 ?)
        byte[] hostNameAsBytes = StringUtils.getBytes(this.hostName, "ISO8859_1");
        byte[] dbAsBytes = StringUtils.getBytes(this.catalog, "ISO8859_1");
        byte[] durationUnitsAsBytes = StringUtils.getBytes(this.durationUnits, "ISO8859_1");
        byte[] eventCreationAsBytes = StringUtils.getBytes(this.eventCreationPointDesc, "ISO8859_1");
        byte[] messageAsBytes = StringUtils.getBytes(this.message, "ISO8859_1");

        int len = /* eventType */ 1 + /* hostName */ (4 + hostNameAsBytes.length) + + /* db */ (4 + dbAsBytes.length) + /* connectionId */ 8
                + /* statementId */ 4 + /* resultSetId */ 4 + /* eventCreationTime */ 8 + /* eventDuration */ 8
                + /* durationUnits */ (4 + durationUnitsAsBytes.length) + /* eventCreationPointDesc */ (4 + eventCreationAsBytes.length)
                + /* message */ (4 + messageAsBytes.length) + /* hostNameIndex */ 4 + /* catalogIndex */ 4 + /* eventCreationPointIndex */ 4;

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

        pos = writeInt(this.hostNameIndex, buf, pos);
        pos = writeInt(this.catalogIndex, buf, pos);
        pos = writeInt(this.eventCreationPointIndex, buf, pos);

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
        byte[] msg = new byte[length];
        System.arraycopy(buf, pos + 4, msg, 0, length);
        return msg;
    }
}