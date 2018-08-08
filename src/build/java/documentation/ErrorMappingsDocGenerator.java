/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package documentation;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.mysql.cj.exceptions.MysqlErrorNumbers;

/**
 * Creates XML file describing mapping of MySQL error #'s to SQL92 and X/Open states.
 */
public class ErrorMappingsDocGenerator {

    public static void main(String[] args) throws Exception {
        dumpSqlStatesMappingsAsXml();
    }

    public static void dumpSqlStatesMappingsAsXml() throws Exception {
        TreeMap<Integer, Integer> allErrorNumbers = new TreeMap<>();
        Map<Object, String> mysqlErrorNumbersToNames = new HashMap<>();

        //      Integer errorNumber = null;

        // 
        // First create a list of all 'known' error numbers that are mapped.
        //
        for (Integer errorNumber : MysqlErrorNumbers.mysqlToSql99State.keySet()) {
            allErrorNumbers.put(errorNumber, errorNumber);
        }

        //
        // Now create a list of the actual MySQL error numbers we know about
        //
        java.lang.reflect.Field[] possibleFields = MysqlErrorNumbers.class.getDeclaredFields();

        for (int i = 0; i < possibleFields.length; i++) {
            String fieldName = possibleFields[i].getName();

            if (fieldName.startsWith("ER_")) {
                mysqlErrorNumbersToNames.put(possibleFields[i].get(null), fieldName);
            }
        }

        System.out.println("<ErrorMappings>");

        for (Integer errorNumber : allErrorNumbers.keySet()) {
            String sql92State = MysqlErrorNumbers.mysqlToSql99(errorNumber.intValue());

            System.out.println("   <ErrorMapping mysqlErrorNumber=\"" + errorNumber + "\" mysqlErrorName=\"" + mysqlErrorNumbersToNames.get(errorNumber)
                    + "\" legacySqlState=\"" + "" + "\" sql92SqlState=\"" + ((sql92State == null) ? "" : sql92State) + "\"/>");
        }

        System.out.println("</ErrorMappings>");
    }
}
