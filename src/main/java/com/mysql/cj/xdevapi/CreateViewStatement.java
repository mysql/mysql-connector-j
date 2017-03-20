/*
  Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.xdevapi;

import com.mysql.cj.api.xdevapi.Schema;
import com.mysql.cj.api.xdevapi.ViewCreate;
import com.mysql.cj.api.xdevapi.ViewCreateDefined;
import com.mysql.cj.core.Messages;
import com.mysql.cj.x.core.MysqlxSession;
import com.mysql.cj.x.core.XDevAPIError;

public class CreateViewStatement extends AbstractViewDDLStatement<ViewCreate, ViewCreateDefined> implements ViewCreate {

    public CreateViewStatement(MysqlxSession mysqlxSession, Schema sch, String viewName, boolean replace) {
        if (mysqlxSession == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "mysqlxSession" }));
        }
        if (sch == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "sch" }));
        }
        if (viewName == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "viewName" }));
        }
        this.mysqlxSession = mysqlxSession;
        this.schema = sch;
        this.viewName = viewName;
        this.replace = replace;
    }

    @Override
    ViewCreate self() {
        return this;
    }

    @Override
    ViewCreateDefined selfDefined() {
        return new CreateViewDefinedStatement(this.mysqlxSession, this);
    }

}
