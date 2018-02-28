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

package com.mysql.cj.jdbc.admin;

import java.sql.DriverManager;
import java.sql.ResultSet;

import com.mysql.cj.util.TimeUtil;

/**
 * Dumps the timezone of the MySQL server represented by the JDBC url given on the commandline (or localhost/test if none provided).
 */
public class TimezoneDump {
    private static final String DEFAULT_URL = "jdbc:mysql:///test";

    /**
     * Constructor for TimezoneDump.
     */
    public TimezoneDump() {
        super();
    }

    /**
     * Entry point for program when called from the command line.
     * 
     * @param args
     *            command-line args. Arg 1 is JDBC URL.
     * @throws Exception
     *             if any errors occur
     */
    public static void main(String[] args) throws Exception {
        String jdbcUrl = DEFAULT_URL;

        if ((args.length == 1) && (args[0] != null)) {
            jdbcUrl = args[0];
        }

        Class.forName("com.mysql.cj.jdbc.Driver").newInstance();

        ResultSet rs = null;

        try {
            rs = DriverManager.getConnection(jdbcUrl).createStatement().executeQuery("SHOW VARIABLES LIKE 'timezone'");

            while (rs.next()) {
                String timezoneFromServer = rs.getString(2);
                System.out.println("MySQL timezone name: " + timezoneFromServer);

                String canonicalTimezone = TimeUtil.getCanonicalTimezone(timezoneFromServer, null);
                System.out.println("Java timezone name: " + canonicalTimezone);
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }
}
