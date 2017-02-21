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

import com.mysql.cj.api.xdevapi.Table;
import com.mysql.cj.api.xdevapi.ViewCreateDefined;
import com.mysql.cj.x.core.MysqlxSession;

public class CreateViewDefinedStatement extends AbstractViewDDLStatement<ViewCreateDefined, ViewCreateDefined> implements ViewCreateDefined {

    public CreateViewDefinedStatement(MysqlxSession mysqlxSession, CreateViewStatement st) {
        this.mysqlxSession = mysqlxSession;
        this.schema = st.schema;
        this.viewName = st.viewName;
        this.replace = st.replace;
        this.columns = st.columns;
        this.alg = st.alg;
        this.sec = st.sec;
        this.definer = st.definer;
        this.findParams = st.findParams;
        this.checkOpt = st.checkOpt;
    }

    @Override
    ViewCreateDefined self() {
        return this;
    }

    @Override
    ViewCreateDefined selfDefined() {
        return this;
    }

    @Override
    public Table execute() {
        this.mysqlxSession.createView(this.schema.getName(), this.viewName, this.replace, this.columns, this.alg, this.sec, this.definer, this.findParams,
                this.checkOpt);
        return this.schema.getTable(this.viewName);
    }

}
