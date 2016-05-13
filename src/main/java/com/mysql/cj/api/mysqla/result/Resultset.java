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

package com.mysql.cj.api.mysqla.result;

/**
 * Represents ProtocolText::Resultset or ProtocolBinary::Resultset structure.
 * 
 * See:
 * http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
 * http://dev.mysql.com/doc/internals/en/binary-protocol-resultset.html
 *
 */
public interface Resultset {

    /**
     * Sometimes the driver doesn't have metadata before consuming the result set rows (because it's cached),
     * or need to coerce the metadata returned by queries into that required by the particular specification
     * (eg metadata returned by metadata queries into that required by the JDBC specification).
     * So it can call this to set it after the fact.
     * 
     * @param metadata
     *            field-level metadata for the result set
     */
    void setColumnDefinition(ColumnDefinition metadata);

    ColumnDefinition getColumnDefinition();

    /**
     * Does the result set contain rows, or is it the result of a DDL or DML statement?
     */
    boolean hasRows();

    /**
     * Set metadata of this Resultset to ResultsetRows it contains.
     */
    void initRowsWithMetadata();

    /**
     * The id (used when profiling) to identify us
     */
    int getResultId();
}
