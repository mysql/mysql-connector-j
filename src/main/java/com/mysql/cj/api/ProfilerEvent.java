/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api;

public interface ProfilerEvent {
    /**
     * A Profiler warning event
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
     * Returns the event type flag
     * 
     * @return the event type flag
     */
    byte getEventType();

    void setEventType(byte eventType);

    /**
     * Returns the duration of the event in milliseconds
     * 
     * @return the duration of the event in milliseconds
     */
    long getEventDuration();

    /**
     * Returns the units for getEventDuration()
     */
    String getDurationUnits();

    /**
     * Returns the id of the connection in use when this event was created.
     * 
     * @return the connection in use
     */
    long getConnectionId();

    /**
     * Returns the id of the result set in use when this event was created.
     * 
     * @return the result set in use
     */
    int getResultSetId();

    /**
     * Returns the id of the statement in use when this event was created.
     * 
     * @return the statement in use
     */
    int getStatementId();

    /**
     * Returns the optional message for this event
     * 
     * @return the message stored in this event
     */
    String getMessage();

    /**
     * Returns the time (in System.currentTimeMillis() form) when this event was
     * created
     * 
     * @return the time this event was created
     */
    long getEventCreationTime();

    /**
     * Returns the catalog in use
     * 
     * @return the catalog in use
     */
    String getCatalog();

    /**
     * Returns the description of when this event was created.
     * 
     * @return a description of when this event was created.
     */
    String getEventCreationPointAsString();

    /**
     * Creates a binary representation of this event.
     * 
     * @return a binary representation of this event
     */
    byte[] pack();

}
