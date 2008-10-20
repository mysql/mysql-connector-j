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
package testsuite.simple;

import java.sql.SQLException;

import testsuite.BaseTestCase;

/**
 * 
 * @author Mark Matthews
 * @version $Id$
 */
public class NumbersTest extends BaseTestCase {
	// ~ Static fields/initializers
	// ---------------------------------------------

	private static final long TEST_BIGINT_VALUE = 6147483647L;

	// ~ Constructors
	// -----------------------------------------------------------

	/**
	 * Creates a new NumbersTest object.
	 * 
	 * @param name
	 *            DOCUMENT ME!
	 */
	public NumbersTest(String name) {
		super(name);
	}

	// ~ Methods
	// ----------------------------------------------------------------

	/**
	 * Runs all test cases in this test suite
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(NumbersTest.class);
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @throws Exception
	 *             DOCUMENT ME!
	 */
	public void setUp() throws Exception {
		super.setUp();
		createTestTable();
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public void testNumbers() throws SQLException {
		this.rs = this.stmt.executeQuery("SELECT * from number_test");

		while (this.rs.next()) {
			long minBigInt = this.rs.getLong(1);
			long maxBigInt = this.rs.getLong(2);
			long testBigInt = this.rs.getLong(3);
			assertTrue("Minimum bigint not stored correctly",
					(minBigInt == Long.MIN_VALUE));
			assertTrue("Maximum bigint not stored correctly",
					(maxBigInt == Long.MAX_VALUE));
			assertTrue("Test bigint not stored correctly",
					(TEST_BIGINT_VALUE == testBigInt));
		}
	}

	private void createTestTable() throws SQLException {
		try {
			this.stmt.executeUpdate("DROP TABLE number_test");
		} catch (SQLException sqlEx) {
			;
		}

		this.stmt
				.executeUpdate("CREATE TABLE number_test (minBigInt bigint, maxBigInt bigint, testBigInt bigint)");
		this.stmt
				.executeUpdate("INSERT INTO number_test (minBigInt,maxBigInt,testBigInt) values ("
						+ Long.MIN_VALUE
						+ ","
						+ Long.MAX_VALUE
						+ ","
						+ TEST_BIGINT_VALUE + ")");
	}
}
