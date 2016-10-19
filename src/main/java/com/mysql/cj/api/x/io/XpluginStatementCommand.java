/*
  Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api.x.io;

public enum XpluginStatementCommand {
    XPLUGIN_STMT_CREATE_COLLECTION("create_collection"), XPLUGIN_STMT_CREATE_COLLECTION_INDEX("create_collection_index"),
    XPLUGIN_STMT_DROP_COLLECTION("drop_collection"), XPLUGIN_STMT_DROP_COLLECTION_INDEX("drop_collection_index"), XPLUGIN_STMT_PING("ping"),
    XPLUGIN_STMT_LIST_OBJECTS("list_objects"), XPLUGIN_STMT_ENABLE_NOTICES("enable_notices"), XPLUGIN_STMT_DISABLE_NOTICES("disable_notices"),
    XPLUGIN_STMT_LIST_NOTICES("list_notices"); // TODO add support for "ping", "list_clients", "kill_client" and "ensure_collection" commands

    public String commandName;

    private XpluginStatementCommand(String commandName) {
        this.commandName = commandName;
    }
}
