/*
 * Copyright (c) 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.telemetry;

/**
 * List of supported telemetry attributes and operation values.
 */
public enum TelemetryAttribute {

    // Call-level attributes:
    DB_NAME("db.name"), //
    DB_OPERATION("db.operation"), //
    DB_STATEMENT("db.statement"), //

    // Connection-level attributes:
    DB_CONNECTION_STRING("db.connection_string"), //
    DB_SYSTEM("db.system"), //
    DB_USER("db.user"), //
    NETWORK_PEER_ADDRESS("network.peer.address"), //
    NETWORK_PEER_PORT("network.peer.port"), //
    NETWORK_TRANSPORT("network.transport"), //
    SERVER_ADDRESS("server.address"), //
    SERVER_PORT("server.port"), //

    // General thread attributes:
    THREAD_ID("thread.id"), //
    THREAD_NAME("thread.name");

    private String key = null;

    private TelemetryAttribute(String key) {
        this.key = key;
    }

    public String getKey() {
        return this.key;
    }

    /*
     * Most common operation values.
     */
    public static final String DB_SYSTEM_DEFAULT = "mysql";
    public static final String NETWORK_TRANSPORT_TCP = "tcp";
    public static final String NETWORK_TRANSPORT_UNIX = "unix";
    public static final String NETWORK_TRANSPORT_PIPE = "pipe";
    public static final String STATEMENT_SUFFIX = " (...)";
    public static final String OPERATION_BATCH = "(SQL batch)";
    public static final String OPERATION_COMMIT = "COMMIT";
    public static final String OPERATION_CREATE = "CREATE";
    public static final String OPERATION_EXPLAIN = "EXPLAIN";
    public static final String OPERATION_INIT_DB = "INIT_DB";
    public static final String OPERATION_KILL = "KILL";
    public static final String OPERATION_PING = "PING";
    public static final String OPERATION_ROLLBACK = "ROLLBACK";
    public static final String OPERATION_SELECT = "SELECT";
    public static final String OPERATION_SET = "SET";
    public static final String OPERATION_SHOW = "SHOW";
    public static final String OPERATION_SHUTDOWN = "SHUTDOWN";
    public static final String OPERATION_USE = "USE";

}
