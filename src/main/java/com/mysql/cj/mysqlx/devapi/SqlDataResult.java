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

package com.mysql.cj.mysqlx.devapi;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Supplier;

import com.mysql.cj.api.result.RowList;
import com.mysql.cj.api.x.SqlResult;
import com.mysql.cj.core.exceptions.FeatureNotAvailableException;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.core.result.Field;

/**
 * SQL result with data. Implemented as a thin layer over {@link RowResultImpl}.
 */
public class SqlDataResult extends RowResultImpl implements SqlResult {
    public SqlDataResult(ArrayList<Field> metadata, TimeZone defaultTimeZone, RowList rows, Supplier<StatementExecuteOk> completer) {
        super(metadata, defaultTimeZone, rows, completer);
    }

    public boolean nextResult() {
        throw new FeatureNotAvailableException("Not a multi-result");
    }

    public long getAffectedItemsCount() {
        return getStatementExecuteOk().getRowsAffected();
    }

    public Long getAutoIncrementValue() {
        return getStatementExecuteOk().getLastInsertId();
    }

    public List<String> getLastDocumentIds() {
        throw new FeatureNotAvailableException("Document IDs are not assigned for SQL statements");
    }
}
