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

package com.mysql.cj.api.xdevapi;

/**
 * Common for {@link ViewCreate} and {@link ViewUpdate}
 */
public interface ViewDDL<T, D> {

    public enum ViewAlgorithm {
        /**
         * MySQL chooses which algorithm to use
         */
        UNDEFINED, /**
                    * The text of a statement that refers to the view and the view definition are merged
                    */
        MERGE, /**
                * The view are retrieved into a temporary table
                */
        TEMPTABLE;
    };

    public enum ViewSqlSecurity {
        INVOKER, DEFINER;
    };

    public enum ViewCheckOption {
        /**
         * The view WHERE clause is checked, but no underlying views are checked
         */
        LOCAL, /**
                * The view WHERE clause is checked, then checking recurses to underlying views
                */
        CASCADED;
    };

    /**
     * Defines the column names of the View.
     * 
     * @param columnStrLst
     * @return
     */
    T columns(String... columnStrLst);

    /**
     * Defines the View's algorithm.
     * 
     * @param algorithm
     * @return
     */
    T algorithm(ViewAlgorithm algorithm);

    /**
     * Defines the View's security scheme.
     * 
     * @param sqlSecurity
     * @return
     */
    T security(ViewSqlSecurity sqlSecurity);

    /**
     * Defines the View's definer.
     * 
     * @param userStr
     * @return
     */
    T definer(String userStr);

    /**
     * Defines the table select statement to generate the View.
     * 
     * @param selectStatement
     * @return
     */
    D definedAs(SelectStatement selectStatement);

    /**
     * Set insert/update constraints on the View.
     * 
     * @param checkOption
     * @return
     */
    T withCheckOption(ViewCheckOption checkOption);

}
