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

import java.util.Iterator;
import java.util.List;

import com.mysql.cj.api.x.Result;
import com.mysql.cj.api.x.Warning;
import com.mysql.cj.core.io.StatementExecuteOk;

/**
 * A result from a statement that doesn't return a set of rows.
 */
public class UpdateResult implements Result {
    private StatementExecuteOk ok;
    private List<String> lastDocIds;

    /**
     * Create a new result.
     *
     * @param updates
     *            the response from the server
     * @param lastDocIds
     *            the (optional) IDs of the inserted documents
     */
    public UpdateResult(StatementExecuteOk ok, List<String> lastDocIds) {
        this.ok = ok;
        this.lastDocIds = lastDocIds;
    }

    public long getAffectedItemsCount() {
        return this.ok.getRowsAffected();
    }

    public Long getAutoIncrementValue() {
        return this.ok.getLastInsertId();
    }

    public List<String> getLastDocumentIds() {
        return this.lastDocIds;
    }

    public int getWarningsCount() {
        return this.ok.getWarnings().size();
    }

    public Iterator<Warning> getWarnings() {
        return this.ok.getWarnings().iterator();
    }
}
