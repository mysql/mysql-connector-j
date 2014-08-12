/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

package com.mysql.jdbc.integration.jboss;

import java.sql.SQLException;

import org.jboss.resource.adapter.jdbc.vendor.MySQLExceptionSorter;

/**
 * Exception sorter used for JBoss to make recovery of downed/stale connections work more consistently.
 */
public final class ExtendedMysqlExceptionSorter extends MySQLExceptionSorter {

    static final long serialVersionUID = -2454582336945931069L;

    /*
     * (non-Javadoc)
     * 
     * @see org.jboss.resource.adapter.jdbc.ExceptionSorter#isExceptionFatal(java.sql.SQLException)
     */
    @Override
    public boolean isExceptionFatal(SQLException ex) {
        String sqlState = ex.getSQLState();

        if (sqlState != null && sqlState.startsWith("08")) {
            return true;
        }

        return super.isExceptionFatal(ex);
    }

}
