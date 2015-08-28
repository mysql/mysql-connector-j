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

package com.mysql.cj.mysqlx.devapi;

import com.mysql.cj.api.x.SelectStatement;
import com.mysql.cj.mysqlx.FindParams;
import com.mysql.cj.mysqlx.TableFindParams;

public class SelectStatementImpl implements SelectStatement {
    private TableImpl table;
    private FindParams findParams = new TableFindParams();

    /* package private */SelectStatementImpl(TableImpl table, String projection) {
        this.table = table;
        if (projection != null && projection.length() > 0) {
            this.findParams.setFields(projection);
        }
    }

    public RowsImpl execute() {
        return this.table.getSession().getMysqlxSession().selectRows(this.table.getSchema().getName(), this.table.getName(), this.findParams);
    }

    public SelectStatement clearBindings() {
        this.findParams.clearArgs();
        return this;
    }

    public SelectStatement bind(String argName, Object value) {
        this.findParams.addArg(argName, value);
        return this;
    }

    public SelectStatement where(String searchCondition) {
        this.findParams.setCriteria(searchCondition);
        return this;
    }

    public SelectStatement groupBy(String groupBy) {
        this.findParams.setGrouping(groupBy);
        return this;
    }

    public SelectStatement having(String having) {
        this.findParams.setGroupingCriteria(having);
        return this;
    }

    public SelectStatement orderBy(String sortFields) {
        this.findParams.setOrder(sortFields);
        return this;
    }

    public SelectStatement limit(long numberOfRows) {
        this.findParams.setLimit(numberOfRows);
        return this;
    }

    public SelectStatement offset(long limitOffset) {
        this.findParams.setOffset(limitOffset);
        return this;
    }
}
