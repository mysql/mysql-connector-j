/*
 Copyright  2002-2004 MySQL AB, 2008 Sun Microsystems
 All rights reserved. Use is subject to license terms.

  The MySQL Connector/J is licensed under the terms of the GPL,
  like most MySQL Connectors. There are special exceptions to the
  terms and conditions of the GPL as it is applied to this software,
  see the FLOSS License Exception available on mysql.com.

  This program is free software; you can redistribute it and/or
  modify it under the terms of the GNU General Public License as
  published by the Free Software Foundation; version 2 of the
  License.

  This program is distributed in the hope that it will be useful,  
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  02110-1301 USA



 */
package com.mysql.jdbc.util;

import java.sql.DriverManager;
import java.sql.ResultSet;

import com.mysql.jdbc.TimeUtil;

/**
 * Dumps the timezone of the MySQL server represented by the JDBC url given on
 * the commandline (or localhost/test if none provided).
 * 
 * @author Mark Matthews
 */
public class TimezoneDump {
	// ~ Static fields/initializers
	// ---------------------------------------------

	private static final String DEFAULT_URL = "jdbc:mysql:///test";

	// ~ Constructors
	// -----------------------------------------------------------

	/**
	 * Constructor for TimezoneDump.
	 */
	public TimezoneDump() {
		super();
	}

	// ~ Methods
	// ----------------------------------------------------------------

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

		Class.forName("com.mysql.jdbc.Driver").newInstance();

		ResultSet rs = DriverManager.getConnection(jdbcUrl).createStatement()
				.executeQuery("SHOW VARIABLES LIKE 'timezone'");

		while (rs.next()) {
			String timezoneFromServer = rs.getString(2);
			System.out.println("MySQL timezone name: " + timezoneFromServer);

			String canonicalTimezone = TimeUtil
					.getCanoncialTimezone(timezoneFromServer, null);
			System.out.println("Java timezone name: " + canonicalTimezone);
		}
	}
}
