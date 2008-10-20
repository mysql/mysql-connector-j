/*
 Copyright  2002-2004 MySQL AB, 2008 Sun Microsystems

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA


 
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
