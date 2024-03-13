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
 * List of telemetry span names.
 */
public enum TelemetrySpanName {

    CANCEL_QUERY("Cancel query"), //
    CHANGE_DATABASE("Change database"), //
    COMMIT("Commit"), //
    CONNECTION_CREATE("Create connection"), //
    CONNECTION_RESET("Reset connection"), //
    CREATE_DATABASE("Create database"), //
    EXPLAIN_QUERY("Explain query"), //
    GET_INNODB_STATUS("Get InnoDB status"), //
    GET_PROCESS_HOST("Get process host"), //
    GET_VARIABLE("Get variable '%s'"), //
    LOAD_COLLATIONS("Load collations"), //
    LOAD_VARIABLES("Load server variables"), //
    PING("Ping"), //
    ROLLBACK("Rollback"),//
    ROUTINE_EXECUTE("Execute stored routine"), //
    ROUTINE_EXECUTE_BATCH("Batch execute stored routine"), //
    ROUTINE_PREPARE("Prepare stored routine"), //
    SET_CHARSET("Set character set"), //
    SET_OPTION_MULTI_STATEMENTS("Set multi-statements '%s'"), //
    SET_TRANSACTION_ACCESS_MODE("Set transaction access mode '%s'"), //
    SET_TRANSACTION_ISOLATION("Set transaction isolation"), //
    SET_VARIABLE("Set variable '%s'"), //
    SET_VARIABLES("Set variable(s)"), //
    SHOW_WARNINGS("Show warnings"), //
    SHUTDOWN("Shutdown"), //
    STMT_DEALLOCATE_PREPARED("Deallocate prepared statement"), //
    STMT_EXECUTE("Execute statement"), //
    STMT_EXECUTE_BATCH("Batch execute statement"), //
    STMT_EXECUTE_BATCH_PREPARED("Batch execute prepared statement"), //
    STMT_EXECUTE_PREPARED("Execute prepared statement"), //
    STMT_FETCH_PREPARED("Fetch rows for prepared statement"), //
    STMT_PREPARE("Prepare statement"), //
    STMT_RESET_PREPARED("Reset prepared statement"), //
    STMT_SEND_LONG_DATA("Send long data for prepared statement"), //
    USE_DATABASE("Use database");

    private String name = "";

    private TelemetrySpanName(String name) {
        this.name = name;
    }

    public String getName(Object... args) {
        if (args.length > 0) {
            return String.format(this.name, args);
        }
        return this.name;
    }

}
