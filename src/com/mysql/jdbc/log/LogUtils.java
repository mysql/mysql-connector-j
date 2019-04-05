/*
  Copyright (c) 2005, 2019, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc.log;

import com.mysql.jdbc.Util;

public class LogUtils {

    public static final String CALLER_INFORMATION_NOT_AVAILABLE = "Caller information not available";

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private static final int LINE_SEPARATOR_LENGTH = LINE_SEPARATOR.length();

    public static String findCallingClassAndMethod(Throwable t) {
        String stackTraceAsString = Util.stackTraceToString(t);

        String callingClassAndMethod = CALLER_INFORMATION_NOT_AVAILABLE;

        int endInternalMethods = stackTraceAsString.lastIndexOf("com.mysql.jdbc");

        if (endInternalMethods != -1) {
            int endOfLine = -1;
            int compliancePackage = stackTraceAsString.indexOf("com.mysql.jdbc.compliance", endInternalMethods);

            if (compliancePackage != -1) {
                endOfLine = compliancePackage - LINE_SEPARATOR_LENGTH;
            } else {
                endOfLine = stackTraceAsString.indexOf(LINE_SEPARATOR, endInternalMethods);
            }

            if (endOfLine != -1) {
                int nextEndOfLine = stackTraceAsString.indexOf(LINE_SEPARATOR, endOfLine + LINE_SEPARATOR_LENGTH);

                if (nextEndOfLine != -1) {
                    callingClassAndMethod = stackTraceAsString.substring(endOfLine + LINE_SEPARATOR_LENGTH, nextEndOfLine);
                } else {
                    callingClassAndMethod = stackTraceAsString.substring(endOfLine + LINE_SEPARATOR_LENGTH);
                }
            }
        }

        if (!callingClassAndMethod.startsWith("\tat ") && !callingClassAndMethod.startsWith("at ")) {
            return "at " + callingClassAndMethod;
        }

        return callingClassAndMethod;
    }
}
