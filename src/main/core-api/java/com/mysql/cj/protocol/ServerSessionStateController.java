/*
 * Copyright (c) 2021, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol;

import java.util.ArrayList;
import java.util.List;

import com.mysql.cj.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.exceptions.ExceptionFactory;

public interface ServerSessionStateController {

    public static int SESSION_TRACK_SYSTEM_VARIABLES = 0x00;
    public static int SESSION_TRACK_SCHEMA = 0x01;
    public static int SESSION_TRACK_STATE_CHANGE = 0x02;
    public static int SESSION_TRACK_GTIDS = 0x03;
    public static int SESSION_TRACK_TRANSACTION_CHARACTERISTICS = 0x04;
    public static int SESSION_TRACK_TRANSACTION_STATE = 0x05;

    /**
     * Set the object containing server session changes collected from the latest query execution. Used internally.
     *
     * @param changes
     *            {@link ServerSessionStateChanges} object.
     *
     */
    default void setSessionStateChanges(ServerSessionStateChanges changes) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    /**
     * Get the object containing server session changes collected from the latest query execution.
     * <p>
     * Please note that the driver could issue some queries internally. With that there is no guarantee that all session changes are reflected in the
     * {@link ServerSessionStateChanges} object after the recent user's query. If this is an issue, a {@link SessionStateChangesListener} can be added via
     * {@link #addSessionStateChangesListener(SessionStateChangesListener)} to catch all session changes.
     * </p>
     *
     * @return {@link ServerSessionStateChanges} object.
     */
    default ServerSessionStateChanges getSessionStateChanges() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @FunctionalInterface
    public static interface SessionStateChangesListener {

        void handleSessionStateChanges(ServerSessionStateChanges changes);

    }

    /**
     * Add the {@link SessionStateChangesListener} that will process {@link ServerSessionStateChanges} on it's arrival.
     *
     * @param l
     *            {@link SessionStateChangesListener} object.
     */
    default void addSessionStateChangesListener(SessionStateChangesListener l) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    /**
     * Remove {@link SessionStateChangesListener}.
     *
     * @param l
     *            {@link SessionStateChangesListener} object.
     */
    default void removeSessionStateChangesListener(SessionStateChangesListener l) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    /**
     * The object containing server session changes collected from the latest query execution.
     * <p>
     * Driver is getting these changes when connection property trackSessionState=true and server supports session tracking.
     * </p>
     *
     */
    public static interface ServerSessionStateChanges {

        List<SessionStateChange> getSessionStateChangesList();

    }

    /**
     * A single server session change record.
     * <p>
     * The server messages about session changes are parsed according to their known types:
     * <ul>
     * <li>{@link #SESSION_TRACK_SYSTEM_VARIABLES} - two values, the system variable name and it's new value;</li>
     * <li>{@link #SESSION_TRACK_SCHEMA} - single value, the new schema name;</li>
     * <li>{@link #SESSION_TRACK_STATE_CHANGE} - single value, "1" or "0";</li>
     * <li>{@link #SESSION_TRACK_GTIDS} - single value, list of GTIDs as reported by server;</li>
     * <li>{@link #SESSION_TRACK_TRANSACTION_CHARACTERISTICS} - single value, transaction characteristics statement;</li>
     * <li>{@link #SESSION_TRACK_TRANSACTION_STATE} - single value, transaction state record.</li>
     * </ul>
     * <p>
     * For the unknown change type the raw payload is written into the single value.
     * </p>
     * <p>
     * See more details in the <a href="https://dev.mysql.com/doc/refman/8.0/en/session-state-tracking.html">server documentation</a>.
     * </p>
     */
    public static class SessionStateChange {

        private int type;
        private List<String> values = new ArrayList<>();

        public SessionStateChange(int type) {
            this.type = type;
        }

        public int getType() {
            return this.type;
        }

        public List<String> getValues() {
            return this.values;
        }

        public SessionStateChange addValue(String value) {
            this.values.add(value);
            return this;
        }

    }

}
