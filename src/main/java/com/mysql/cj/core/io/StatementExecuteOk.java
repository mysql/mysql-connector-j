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

package com.mysql.cj.core.io;

import java.util.List;

import com.mysql.cj.api.x.Warning;

/**
 * The returned information from a successfully executed statement. All fields are optional and may be <i>null</i>.
 *
 * @todo can/should we put warnings here too?
 */
public class StatementExecuteOk {
    private long rowsAffected;
    private Long lastInsertId;
    private List<Warning> warnings;

    // TODO; use of Warning here is not cross-protocol, need an abstract version
    public StatementExecuteOk(long rowsAffected, Long lastInsertId, List<Warning> warnings) {
        this.rowsAffected = rowsAffected;
        this.lastInsertId = lastInsertId;
        this.warnings = warnings; // should NOT be null
    }

    public long getRowsAffected() {
        return this.rowsAffected;
    }

    public Long getLastInsertId() {
        return this.lastInsertId;
    }

    public List<Warning> getWarnings() {
        return this.warnings;
    }
}
