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

import java.util.ArrayList;
import java.util.List;

import com.mysql.cj.api.xdevapi.Schema;
import com.mysql.cj.api.xdevapi.SelectStatement;
import com.mysql.cj.api.xdevapi.ViewDDL;
import com.mysql.cj.core.Messages;
import com.mysql.cj.x.core.MysqlxSession;
import com.mysql.cj.x.core.XDevAPIError;

public abstract class AbstractViewDDLStatement<T extends ViewDDL<T, D>, D extends ViewDDL<D, D>> implements ViewDDL<T, D> {

    protected MysqlxSession mysqlxSession;
    protected Schema schema;
    protected String viewName;
    protected boolean replace = false;

    protected List<String> columns = new ArrayList<>();
    protected ViewAlgorithm alg = null;
    protected ViewSqlSecurity sec = null;
    protected String definer = null;
    protected FindParams findParams = null;
    protected ViewCheckOption checkOpt = null;

    abstract T self();

    abstract D selfDefined();

    @Override
    public T columns(String... columnStrLst) {
        if (columnStrLst == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "columnStrLst" }));
        }

        for (String c : columnStrLst) {
            if (c == null) {
                throw new XDevAPIError(Messages.getString("CreateTableStatement.1", new String[] { "columnStrLst" }));
            }
            this.columns.add(c);
        }

        return self();
    }

    @Override
    public T algorithm(ViewAlgorithm algorithm) {
        this.alg = algorithm;
        return self();
    }

    @Override
    public T security(ViewSqlSecurity sqlSecurity) {
        this.sec = sqlSecurity;
        return self();
    }

    @Override
    public T definer(String userStr) {
        this.definer = userStr;
        return self();
    }

    @Override
    public D definedAs(SelectStatement selectStatement) {
        if (selectStatement == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "selectStatement" }));
        }
        this.findParams = selectStatement.getFindParams().clone();
        return selfDefined();
    }

    @Override
    public T withCheckOption(ViewCheckOption checkOption) {
        this.checkOpt = checkOption;
        return self();
    }

}
