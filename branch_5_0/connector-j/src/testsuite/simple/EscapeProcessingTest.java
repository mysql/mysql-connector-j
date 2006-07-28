/*
 Copyright (C) 2002-2004 MySQL AB

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
package testsuite.simple;

import java.sql.Connection;
import java.util.Properties;
import java.util.TimeZone;

import testsuite.BaseTestCase;

/**
 * Tests escape processing
 * 
 * @author Mark Matthews
 */
public class EscapeProcessingTest extends BaseTestCase {
	// ~ Constructors
	// -----------------------------------------------------------

	/**
	 * Constructor for EscapeProcessingTest.
	 * 
	 * @param name
	 *            the test to run
	 */
	public EscapeProcessingTest(String name) {
		super(name);
	}

	// ~ Methods
	// ----------------------------------------------------------------

	/**
	 * Tests the escape processing functionality
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	public void testEscapeProcessing() throws Exception {
		String results = "select dayname (abs(now())),   -- Today    \n"
				+ "           '1997-05-24',  -- a date                    \n"
				+ "           '10:30:29',  -- a time                     \n"
				+ "           '1997-05-24 10:30:29', -- a timestamp  \n"
				+ "          '{string data with { or } will not be altered'   \n"
				+ "--  Also note that you can safely include { and } in comments";

		String exSql = "select {fn dayname ({fn abs({fn now()})})},   -- Today    \n"
				+ "           {d '1997-05-24'},  -- a date                    \n"
				+ "           {t '10:30:29' },  -- a time                     \n"
				+ "           {ts '1997-05-24 10:30:29.123'}, -- a timestamp  \n"
				+ "          '{string data with { or } will not be altered'   \n"
				+ "--  Also note that you can safely include { and } in comments";

		String escapedSql = this.conn.nativeSQL(exSql);

		assertTrue(results.equals(escapedSql));

	}

	/**
	 * Runs all test cases in this test suite
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(EscapeProcessingTest.class);
	}

	/**
	 * JDBC-4.0 spec will allow either SQL_ or not for type in {fn convert ...}
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testConvertEscape() throws Exception {
		assertEquals(conn.nativeSQL("{fn convert(abcd, SQL_INTEGER)}"), conn
				.nativeSQL("{fn convert(abcd, INTEGER)}"));
	}
	
	/**
	 * Tests that the escape tokenizer converts timestamp values
	 * wrt. timezones when useTimezone=true.
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testTimestampConversion() throws Exception {
		TimeZone currentTimezone = TimeZone.getDefault();
		String[] availableIds = TimeZone.getAvailableIDs(currentTimezone.getRawOffset() + (3600 * 1000 * 2));
		String newTimezone = null;
		
		if (availableIds.length > 0) {
			newTimezone = availableIds[0];
		} else {
			newTimezone = "UTC"; // punt
		}
		
		Properties props = new Properties();
		
		props.setProperty("useTimezone", "true");
		props.setProperty("serverTimezone", newTimezone);
		Connection tzConn = null;
		
		try {
			String escapeToken = "SELECT {ts '2002-11-12 10:00:00'} {t '05:11:02'}";
			tzConn = getConnectionWithProps(props);
			assertTrue(!tzConn.nativeSQL(escapeToken).equals(this.conn.nativeSQL(escapeToken)));
		} finally {
			if (tzConn != null) {
				tzConn.close();
			}
		}
		
	}
}
