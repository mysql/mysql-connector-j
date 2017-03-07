/*
  Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api.xdevapi;

import com.mysql.cj.xdevapi.FindParams;

/**
 * A statement representing a SELECT query.
 */
public interface SelectStatement extends DataStatement<SelectStatement, RowResult, Row> {
    /**
     * Add/replace the search condition for this query.
     * 
     * @param searchCondition
     *            search condition expression
     * @return {@link SelectStatement}
     */
    SelectStatement where(String searchCondition);

    /**
     * Add/replace the aggregation fields for this query.
     * 
     * @param groupBy
     *            groupBy expression
     * @return {@link SelectStatement}
     */
    SelectStatement groupBy(String... groupBy);

    /**
     * Add/replace the aggregate criteria for this query.
     * 
     * @param having
     *            having expression
     * @return {@link SelectStatement}
     */
    SelectStatement having(String having);

    /**
     * Add/replace the order specification for this query.
     * 
     * @param sortFields
     *            sort expression
     * @return {@link SelectStatement}
     */
    SelectStatement orderBy(String... sortFields);

    /**
     * Add/replace the row limit for this query.
     * 
     * @param numberOfRows
     *            limit
     * @return {@link SelectStatement}
     */
    SelectStatement limit(long numberOfRows);

    /**
     * Add/replace the row offset for this query.
     * 
     * @param limitOffset
     *            limit offset
     * @return {@link SelectStatement}
     */
    SelectStatement offset(long limitOffset);

    /**
     * Return FindParams defined for this statement.
     * 
     * @return {@link FindParams}
     */
    FindParams getFindParams();
}
