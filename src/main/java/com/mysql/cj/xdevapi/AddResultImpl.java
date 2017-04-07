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

import java.util.List;

import com.mysql.cj.api.xdevapi.AddResult;
import com.mysql.cj.x.core.StatementExecuteOk;
import com.mysql.cj.x.core.XDevAPIError;

/**
 * A result from the collection.add() statement.
 */
public class AddResultImpl extends UpdateResult implements AddResult {
    private List<String> lastDocIds;

    /**
     * Create a new result.
     *
     * @param ok
     *            the response from the server
     * @param lastDocIds
     *            the (optional) IDs of the inserted documents
     */
    public AddResultImpl(StatementExecuteOk ok, List<String> lastDocIds) {
        super(ok);
        this.lastDocIds = lastDocIds;
    }

    @Override
    public List<String> getDocumentIds() {
        return this.lastDocIds;
    }

    @Override
    public String getDocumentId() {
        if (this.lastDocIds.size() > 1) {
            throw new XDevAPIError("Method getDocumentId() is allowed only for a single document add() result.");
        }
        return this.lastDocIds.get(this.lastDocIds.size() - 1);
    }

}
