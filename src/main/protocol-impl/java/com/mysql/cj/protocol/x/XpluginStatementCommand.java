/*
 * Copyright (c) 2016, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.x;

public enum XpluginStatementCommand {

    XPLUGIN_STMT_CREATE_COLLECTION("create_collection"), XPLUGIN_STMT_CREATE_COLLECTION_INDEX("create_collection_index"),
    XPLUGIN_STMT_DROP_COLLECTION("drop_collection"), XPLUGIN_STMT_DROP_COLLECTION_INDEX("drop_collection_index"),
    XPLUGIN_STMT_MODIFY_COLLECTION_OPTIONS("modify_collection_options"), XPLUGIN_STMT_PING("ping"), XPLUGIN_STMT_LIST_OBJECTS("list_objects"),
    XPLUGIN_STMT_ENABLE_NOTICES("enable_notices"), XPLUGIN_STMT_DISABLE_NOTICES("disable_notices"), XPLUGIN_STMT_LIST_NOTICES("list_notices");
    // TODO add support for "ping", "list_clients", "kill_client" and "ensure_collection" commands

    public String commandName;

    private XpluginStatementCommand(String commandName) {
        this.commandName = commandName;
    }

}
