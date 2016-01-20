/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqlx;

import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.UpdateOperation;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.ColumnIdentifier;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.Expr;

public class UpdateSpec {
    public static enum UpdateType {
        // these must mirror exactly the names of UpdateOperation.UpdateType in mysqlx_crud.proto
        ITEM_REMOVE, ITEM_SET, ITEM_REPLACE, ITEM_MERGE, ARRAY_INSERT, ARRAY_APPEND
    }

    private UpdateOperation.UpdateType updateType;
    private ColumnIdentifier source;
    private Expr value;

    public UpdateSpec(UpdateType updateType, String source) {
        this.updateType = UpdateOperation.UpdateType.valueOf(updateType.name());
        // accomodate parser's documentField() handling by removing "$"
        if (source.length() > 0 && source.charAt(0) == '$') {
            source = source.substring(1);
        }
        this.source = new ExprParser(source, false).documentField().getIdentifier();
    }

    public Object getUpdateType() {
        return this.updateType;
    }

    public Object getSource() {
        return this.source;
    }

    public UpdateSpec setValue(Object value) {
        this.value = ExprUtil.argObjectToExpr(value, false);
        return this;
    }

    public Object getValue() {
        return this.value;
    }
}
