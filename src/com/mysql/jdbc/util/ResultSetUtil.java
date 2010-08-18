/*
 Copyright (c) 2002, 2010, Oracle and/or its affiliates. All rights reserved.
 

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
package com.mysql.jdbc.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Utilities for dealing with result sets (used in testcases and profiler).
 * 
 * @author Mark Matthews
 * 
 * @version $Id$
 */
public class ResultSetUtil {

	public static StringBuffer appendResultSetSlashGStyle(
			StringBuffer appendTo, ResultSet rs) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();

		int numFields = rsmd.getColumnCount();
		int maxWidth = 0;

		String[] fieldNames = new String[numFields];

		for (int i = 0; i < numFields; i++) {
			fieldNames[i] = rsmd.getColumnLabel(i + 1);

			if (fieldNames[i].length() > maxWidth) {
				maxWidth = fieldNames[i].length();
			}
		}

		int rowCount = 1;

		while (rs.next()) {
			appendTo.append("*************************** ");
			appendTo.append(rowCount++);
			appendTo.append(". row ***************************\n");

			for (int i = 0; i < numFields; i++) {
				int leftPad = maxWidth - fieldNames[i].length();

				for (int j = 0; j < leftPad; j++) {
					appendTo.append(" ");
				}

				appendTo.append(fieldNames[i]);
				appendTo.append(": ");

				String stringVal = rs.getString(i + 1);

				if (stringVal != null) {
					appendTo.append(stringVal);
				} else {
					appendTo.append("NULL");
				}

				appendTo.append("\n");
			}

			appendTo.append("\n");
		}

		return appendTo;
	}
}
