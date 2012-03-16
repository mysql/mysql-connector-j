/*
 Copyright (c) 2002, 2011, Oracle and/or its affiliates. All rights reserved.
 

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
package testsuite.regression;

import java.sql.ResultSetMetaData;

import testsuite.BaseTestCase;

/**
 * Tests various number-handling issues that have arrisen in the JDBC driver at
 * one time or another.
 * 
 * @author Mark Matthews
 */
public class NumbersRegressionTest extends BaseTestCase {
	/**
	 * Constructor for NumbersRegressionTest.
	 * 
	 * @param name
	 *            the test name
	 */
	public NumbersRegressionTest(String name) {
		super(name);
	}

	/**
	 * Runs all test cases
	 * 
	 * @param args
	 *            command-line args
	 * 
	 * @throws Exception
	 *             if any errors occur
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(NumbersRegressionTest.class);
	}

	/**
	 * Tests that BIGINT retrieval works correctly
	 * 
	 * @throws Exception
	 *             if any errors occur
	 */
	public void testBigInt() throws Exception {
		createTable("bigIntRegression", "(val BIGINT NOT NULL)");
		this.stmt
				.executeUpdate("INSERT INTO bigIntRegression VALUES (6692730313872877584)");
		this.rs = this.stmt.executeQuery("SELECT val FROM bigIntRegression");

		while (this.rs.next()) {
			// check retrieval
			long retrieveAsLong = this.rs.getLong(1);
			assertTrue(retrieveAsLong == 6692730313872877584L);
		}

		this.rs.close();
		this.stmt.executeUpdate("DROP TABLE IF EXISTS bigIntRegression");

		String bigIntAsString = "6692730313872877584";

		long parsedBigIntAsLong = Long.parseLong(bigIntAsString);

		// check JDK parsing
		assertTrue(bigIntAsString.equals(String.valueOf(parsedBigIntAsLong)));
	}

	/**
	 * Tests correct type assignment for MySQL FLOAT and REAL datatypes.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testFloatsAndReals() throws Exception {
		createTable("floatsAndReals",
				"(floatCol FLOAT, realCol REAL, doubleCol DOUBLE)");
		this.stmt.executeUpdate("INSERT INTO floatsAndReals VALUES (0, 0, 0)");

		this.rs = this.stmt
				.executeQuery("SELECT floatCol, realCol, doubleCol FROM floatsAndReals");

		ResultSetMetaData rsmd = this.rs.getMetaData();

		this.rs.next();

		assertTrue(rsmd.getColumnClassName(1).equals("java.lang.Float"));
		assertTrue(this.rs.getObject(1).getClass().getName()
				.equals("java.lang.Float"));

		assertTrue(rsmd.getColumnClassName(2).equals("java.lang.Double"));
		assertTrue(this.rs.getObject(2).getClass().getName()
				.equals("java.lang.Double"));

		assertTrue(rsmd.getColumnClassName(3).equals("java.lang.Double"));
		assertTrue(this.rs.getObject(3).getClass().getName()
				.equals("java.lang.Double"));

	}

	/**
	 * Tests that ResultSetMetaData precision and scale methods work correctly
	 * for all numeric types.
	 * 
	 * @throws Exception
	 *             if any errors occur
	 */
	public void testPrecisionAndScale() throws Exception {
		testPrecisionForType("TINYINT", 8, -1, false);
		testPrecisionForType("TINYINT", 8, -1, true);
		testPrecisionForType("SMALLINT", 8, -1, false);
		testPrecisionForType("SMALLINT", 8, -1, true);
		testPrecisionForType("MEDIUMINT", 8, -1, false);
		testPrecisionForType("MEDIUMINT", 8, -1, true);
		testPrecisionForType("INT", 8, -1, false);
		testPrecisionForType("INT", 8, -1, true);
		testPrecisionForType("BIGINT", 8, -1, false);
		testPrecisionForType("BIGINT", 8, -1, true);

		testPrecisionForType("FLOAT", 8, 4, false);
		testPrecisionForType("FLOAT", 8, 4, true);
		testPrecisionForType("DOUBLE", 8, 4, false);
		testPrecisionForType("DOUBLE", 8, 4, true);

		testPrecisionForType("DECIMAL", 8, 4, false);
		testPrecisionForType("DECIMAL", 8, 4, true);

		testPrecisionForType("DECIMAL", 9, 0, false);
		testPrecisionForType("DECIMAL", 9, 0, true);
	}

	private void testPrecisionForType(String typeName, int m, int d,
			boolean unsigned) throws Exception {
		try {
			this.stmt
					.executeUpdate("DROP TABLE IF EXISTS precisionAndScaleRegression");

			StringBuffer createStatement = new StringBuffer(
					"CREATE TABLE precisionAndScaleRegression ( val ");
			createStatement.append(typeName);
			createStatement.append("(");
			createStatement.append(m);

			if (d != -1) {
				createStatement.append(",");
				createStatement.append(d);
			}

			createStatement.append(")");

			if (unsigned) {
				createStatement.append(" UNSIGNED ");
			}

			createStatement.append(" NOT NULL)");

			this.stmt.executeUpdate(createStatement.toString());

			this.rs = this.stmt
					.executeQuery("SELECT val FROM precisionAndScaleRegression");

			ResultSetMetaData rsmd = this.rs.getMetaData();
			assertTrue(
					"Precision returned incorrectly for type " + typeName
							+ ", " + m + " != rsmd.getPrecision() = "
							+ rsmd.getPrecision(1), rsmd.getPrecision(1) == m);

			if (d != -1) {
				assertTrue(
						"Scale returned incorrectly for type " + typeName
								+ ", " + d + " != rsmd.getScale() = "
								+ rsmd.getScale(1), rsmd.getScale(1) == d);
			}
		} finally {
			if (this.rs != null) {
				try {
					this.rs.close();
				} catch (Exception ex) {
					// ignore
				}
			}

			this.stmt
					.executeUpdate("DROP TABLE IF EXISTS precisionAndScaleRegression");
		}
	}

	public void testIntShouldReturnLong() throws Exception {
		createTable("testIntRetLong", "(field1 INT)");
		this.stmt.executeUpdate("INSERT INTO testIntRetLong VALUES (1)");

		this.rs = this.stmt.executeQuery("SELECT * FROM testIntRetLong");
		this.rs.next();

		assertTrue(this.rs.getObject(1).getClass()
				.equals(java.lang.Integer.class));
	}

	/**
	 * Tests fix for BUG#5729, UNSIGNED BIGINT returned incorrectly
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testBug5729() throws Exception {
		if (versionMeetsMinimum(4, 1)) {
			String valueAsString = "1095923280000";

			createTable("testBug5729", "(field1 BIGINT UNSIGNED)");
			this.stmt.executeUpdate("INSERT INTO testBug5729 VALUES ("
					+ valueAsString + ")");

			this.rs = this.conn.prepareStatement("SELECT * FROM testBug5729")
					.executeQuery();
			this.rs.next();

			assertTrue(this.rs.getObject(1).toString().equals(valueAsString));
		}
	}

	/**
	 * Tests fix for BUG#8484 - ResultSet.getBigDecimal() throws exception when
	 * rounding would need to occur to set scale.
	 * 
	 * @throws Exception
	 *             if the test fails
	 * @deprecated
	 */
	public void testBug8484() throws Exception {
		createTable("testBug8484",
				"(field1 DECIMAL(16, 8), field2 varchar(32))");
		this.stmt
				.executeUpdate("INSERT INTO testBug8484 VALUES (12345678.12345678, '')");
		this.rs = this.stmt
				.executeQuery("SELECT field1, field2 FROM testBug8484");
		this.rs.next();
		assertEquals("12345678.123", this.rs.getBigDecimal(1, 3).toString());
		assertEquals("0.000", this.rs.getBigDecimal(2, 3).toString());

		this.pstmt = this.conn
				.prepareStatement("SELECT field1, field2 FROM testBug8484");
		this.rs = this.pstmt.executeQuery();
		this.rs.next();
		assertEquals("12345678.123", this.rs.getBigDecimal(1, 3).toString());
		assertEquals("0.000", this.rs.getBigDecimal(2, 3).toString());
	}
}
