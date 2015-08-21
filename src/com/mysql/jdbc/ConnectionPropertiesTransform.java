/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Implement this interface, and pass the class name as the 'propertiesTransform' property in your JDBC URL, and the driver will pass the properties it has
 * parsed to your transform implementation so that you can modify/substitute/add any that you desire.
 */
public interface ConnectionPropertiesTransform {
    /**
     * The JDBC driver will call this method if the user has loaded your
     * implementation of this interface by specifying the 'propertiesTransform'
     * property in their JDBC URL.
     * 
     * @param props
     *            the properties as passed by the driver (never null)
     * 
     * @return the same properties with any transformations that your
     *         implementation has made
     * 
     * @throws SQLException
     *             if a transform can not be made for any reason.
     */
    public Properties transformProperties(Properties props) throws SQLException;
}
