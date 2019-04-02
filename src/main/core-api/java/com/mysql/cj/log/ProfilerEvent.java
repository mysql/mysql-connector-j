/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
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

public interface ProfilerEvent {
    /**
     * Profiler event for usage advisor
     */
    public static final byte TYPE_USAGE = 0;

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
     * Returns the event type
     * 
     * @return the event type
     */
    byte getEventType();

    /**
     * Returns the host name the event occurred on.
     * 
     * @return host name
     */
    String getHostName();

    /**
     * Returns the database the event occurred on.
     * 
     * @return the database in use
     */
    String getDatabase();

    /**
     * Returns the id of the associated connection (-1 for none).
     * 
     * @return the connection in use
     */
    long getConnectionId();

    /**
     * Returns the id of the associated statement (-1 for none).
     * 
     * @return the statement in use
     */
    int getStatementId();

    /**
     * Returns the id of the associated result set (-1 for none).
     * 
     * @return the result set in use
     */
    int getResultSetId();

    /**
     * Returns the time (in System.currentTimeMillis() form) when this event was created.
     * 
     * @return the time this event was created
     */
    long getEventCreationTime();

    /**
     * Returns the duration of the event in milliseconds
     * 
     * @return the duration of the event in milliseconds
     */
    long getEventDuration();

    /**
     * Returns the units for getEventDuration()
     * 
     * @return name of duration units
     */
    String getDurationUnits();

    /**
     * Returns the description of where the event was created.
     * 
     * @return a description of where this event was created.
     */
    String getEventCreationPointAsString();

    /**
     * Returns the optional message for this event
     * 
     * @return the message stored in this event
     */
    String getMessage();

    /**
     * Creates a binary representation of this event.
     * 
     * @return a binary representation of this event
     */
    byte[] pack();

}
