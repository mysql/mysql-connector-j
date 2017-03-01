/*
  Copyright (c) 2002, 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.jdbc.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Utilities for dealing with result sets (used in testcases and profiler).
 */
public class ResultSetUtil {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void resultSetToMap(Map mappedValues, ResultSet rs) throws SQLException {
        while (rs.next()) {
            mappedValues.put(rs.getObject(1), rs.getObject(2));
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void resultSetToMap(Map mappedValues, java.sql.ResultSet rs, int key, int value) throws SQLException {
        while (rs.next()) {
            mappedValues.put(rs.getObject(key), rs.getObject(value));
        }
    }

    /**
     * Given a ResultSet and an index into the columns of that ResultSet, read
     * binary data from the column which represents a serialized object, and
     * re-create the object.
     * 
     * @param resultSet
     *            the ResultSet to use.
     * @param index
     *            an index into the ResultSet.
     * @return the object if it can be de-serialized
     * @throws SQLException
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws Exception
     *             if an error occurs
     */
    // TODO not used ?
    public static Object readObject(java.sql.ResultSet resultSet, int index) throws IOException, SQLException, ClassNotFoundException {
        ObjectInputStream objIn = new ObjectInputStream(resultSet.getBinaryStream(index));
        Object obj = objIn.readObject();
        objIn.close();

        return obj;
    }

}
