/*
  Copyright (c) 2002, 2013, Oracle and/or its affiliates. All rights reserved.

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
package testsuite.regression.jdbc4;

import java.sql.DatabaseMetaData;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import com.mysql.jdbc.ConnectionProperties;
import com.mysql.jdbc.Util;

import testsuite.BaseTestCase;

public class MetaDataRegressionTest extends BaseTestCase {
	/**
	 * Creates a new MetaDataRegressionTest.
	 * 
	 * @param name
	 *            the name of the test
	 */
	public MetaDataRegressionTest(String name) {
		super(name);
	}

	/**
	 * Runs all test cases in this test suite
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(MetaDataRegressionTest.class);
	}

	/**
	 * Tests fix for BUG#68307 - getFunctionColumns() returns incorrect "COLUMN_TYPE" information. This is a JDBC4
	 * feature.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug68307() throws Exception {
		createFunction("testBug68307_func", "(func_param_in INT) RETURNS INT DETERMINISTIC RETURN 1");

		createProcedure("testBug68307_proc",
				"(IN proc_param_in INT, OUT proc_param_out INT, INOUT proc_param_inout INT) SELECT 1");

		// test metadata from MySQL
		DatabaseMetaData testDbMetaData = conn.getMetaData();
		checkFunctionColumnTypeForBug68307("MySQL", testDbMetaData);
		checkProcedureColumnTypeForBug68307("MySQL", testDbMetaData);

		// test metadata from I__S
		Connection connUseIS = getConnectionWithProps("useInformationSchema=true");
		testDbMetaData = connUseIS.getMetaData();
		checkFunctionColumnTypeForBug68307("I__S", testDbMetaData);
		checkProcedureColumnTypeForBug68307("I__S", testDbMetaData);
		connUseIS.close();
	}

	private void checkFunctionColumnTypeForBug68307(String testAgainst, DatabaseMetaData testDbMetaData) throws Exception {
		rs = testDbMetaData.getFunctionColumns(null, null, "testBug68307_%", "%");

		while (rs.next()) {
			String message = testAgainst + ", function <" + rs.getString("FUNCTION_NAME") + "."
					+ rs.getString("COLUMN_NAME") + ">";
			if (rs.getString("COLUMN_NAME") == null || rs.getString("COLUMN_NAME").length() == 0) {
				assertEquals(message, DatabaseMetaData.functionReturn, rs.getShort("COLUMN_TYPE"));
			} else if (rs.getString("COLUMN_NAME").endsWith("_in")) {
				assertEquals(message, DatabaseMetaData.functionColumnIn, rs.getShort("COLUMN_TYPE"));
			} else if (rs.getString("COLUMN_NAME").endsWith("_inout")) {
				assertEquals(message, DatabaseMetaData.functionColumnInOut, rs.getShort("COLUMN_TYPE"));
			} else if (rs.getString("COLUMN_NAME").endsWith("_out")) {
				assertEquals(message, DatabaseMetaData.functionColumnOut, rs.getShort("COLUMN_TYPE"));
			} else {
				fail("Column '" + rs.getString("FUNCTION_NAME") + "." + rs.getString("COLUMN_NAME")
						+ "' not expected within test case.");
			}
		}
	}

	private void checkProcedureColumnTypeForBug68307(String testAgainst, DatabaseMetaData testDbMetaData) throws Exception {
		rs = testDbMetaData.getProcedureColumns(null, null, "testBug68307_%", "%");

		while (rs.next()) {
			String message = testAgainst + ", procedure <" + rs.getString("PROCEDURE_NAME") + "."
					+ rs.getString("COLUMN_NAME") + ">";
			if (rs.getString("COLUMN_NAME") == null || rs.getString("COLUMN_NAME").length() == 0) {
				assertEquals(message, DatabaseMetaData.procedureColumnReturn, rs.getShort("COLUMN_TYPE"));
			} else if (rs.getString("COLUMN_NAME").endsWith("_in")) {
				assertEquals(message, DatabaseMetaData.procedureColumnIn, rs.getShort("COLUMN_TYPE"));
			} else if (rs.getString("COLUMN_NAME").endsWith("_inout")) {
				assertEquals(message, DatabaseMetaData.procedureColumnInOut, rs.getShort("COLUMN_TYPE"));
			} else if (rs.getString("COLUMN_NAME").endsWith("_out")) {
				assertEquals(message, DatabaseMetaData.procedureColumnOut, rs.getShort("COLUMN_TYPE"));
			} else {
				fail("Column '" + rs.getString("FUNCTION_NAME") + "." + rs.getString("COLUMN_NAME")
						+ "' not expected within test case.");
			}
		}
	}

	/**
	 * Tests fix for BUG#44451 - getTables does not return resultset with expected columns.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug44451() throws Exception {
		String methodName;
		List<String> expectedFields;
		String[] testStepDescription = new String[] { "MySQL MetaData", "I__S MetaData" };
		Connection connUseIS = getConnectionWithProps("useInformationSchema=true");
		Connection[] testConnections = new Connection[] { conn, connUseIS };

		methodName = "getClientInfoProperties()";
		expectedFields = Arrays.asList("NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION");
		for (int i = 0; i < testStepDescription.length; i++) {
			DatabaseMetaData testDbMetaData = testConnections[i].getMetaData();
			rs = testDbMetaData.getClientInfoProperties();
			checkReturnedColumnsForBug44451(testStepDescription[i], methodName, expectedFields, rs);
			rs.close();
		}

		methodName = "getFunctions()";
		expectedFields = Arrays.asList("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME");
		for (int i = 0; i < testStepDescription.length; i++) {
			DatabaseMetaData testDbMetaData = testConnections[i].getMetaData();
			rs = testDbMetaData.getFunctions(null, null, "%");
			checkReturnedColumnsForBug44451(testStepDescription[i], methodName, expectedFields, rs);
			rs.close();
		}
		
		connUseIS.close();
	}
	
	private void checkReturnedColumnsForBug44451(String stepDescription, String methodName, List<String> expectedFields,
			ResultSet resultSetToCheck) throws Exception {
		ResultSetMetaData rsMetaData = resultSetToCheck.getMetaData();
		int numberOfColumns = rsMetaData.getColumnCount();

		assertEquals(stepDescription + ", wrong column count in method '" + methodName + "'.", expectedFields.size(),
				numberOfColumns);
		for (int i = 0; i < numberOfColumns; i++) {
			int position = i + 1;
			assertEquals(stepDescription + ", wrong column at position '" + position + "' in method '" + methodName
					+ "'.", expectedFields.get(i), rsMetaData.getColumnName(position));
		}
		rs.close();
	}
	
	/**
	 * Tests fix for BUG#69298 - Different outcome from DatabaseMetaData.getFunctions() when using I__S.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug69298() throws Exception {
		Connection testConn;

		createFunction("testBug69298_func",
				"(param_func INT) RETURNS INT COMMENT 'testBug69298_func comment' DETERMINISTIC RETURN 1");
		createProcedure("testBug69298_proc", "(IN param_proc INT) COMMENT 'testBug69298_proc comment' SELECT 1");

		// test with standard connection
		assertFalse("Property useInformationSchema should be false",
				((ConnectionProperties) conn).getUseInformationSchema());
		assertTrue("Property getProceduresReturnsFunctions should be true",
				((ConnectionProperties) conn).getGetProceduresReturnsFunctions());
		checkGetFunctionsForBug69298("Std. Connection MetaData", conn);
		checkGetFunctionColumnsForBug69298("Std. Connection MetaData", conn);
		checkGetProceduresForBug69298("Std. Connection MetaData", conn);
		checkGetProcedureColumnsForBug69298("Std. Connection MetaData", conn);

		// test with property useInformationSchema=true
		testConn = getConnectionWithProps("useInformationSchema=true");
		assertTrue("Property useInformationSchema should be true",
				((ConnectionProperties) testConn).getUseInformationSchema());
		assertTrue("Property getProceduresReturnsFunctions should be true",
				((ConnectionProperties) testConn).getGetProceduresReturnsFunctions());
		checkGetFunctionsForBug69298("Prop. useInfoSchema(1) MetaData", testConn);
		checkGetFunctionColumnsForBug69298("Prop. useInfoSchema(1) MetaData", testConn);
		checkGetProceduresForBug69298("Prop. useInfoSchema(1) MetaData", testConn);
		checkGetProcedureColumnsForBug69298("Prop. useInfoSchema(1) MetaData", testConn);
		testConn.close();

		// test with property getProceduresReturnsFunctions=false
		testConn = getConnectionWithProps("getProceduresReturnsFunctions=false");
		assertFalse("Property useInformationSchema should be false",
				((ConnectionProperties) testConn).getUseInformationSchema());
		assertFalse("Property getProceduresReturnsFunctions should be false",
				((ConnectionProperties) testConn).getGetProceduresReturnsFunctions());
		checkGetFunctionsForBug69298("Prop. getProcRetFunc(0) MetaData", testConn);
		checkGetFunctionColumnsForBug69298("Prop. getProcRetFunc(0) MetaData", testConn);
		checkGetProceduresForBug69298("Prop. getProcRetFunc(0) MetaData", testConn);
		checkGetProcedureColumnsForBug69298("Prop. getProcRetFunc(0) MetaData", testConn);
		testConn.close();

		// test with property useInformationSchema=true & getProceduresReturnsFunctions=false
		testConn = getConnectionWithProps("useInformationSchema=true,getProceduresReturnsFunctions=false");
		assertTrue("Property useInformationSchema should be true",
				((ConnectionProperties) testConn).getUseInformationSchema());
		assertFalse("Property getProceduresReturnsFunctions should be false",
				((ConnectionProperties) testConn).getGetProceduresReturnsFunctions());
		checkGetFunctionsForBug69298("Prop. useInfoSchema(1) + getProcRetFunc(0) MetaData", testConn);
		checkGetFunctionColumnsForBug69298("Prop. useInfoSchema(1) + getProcRetFunc(0) MetaData", testConn);
		checkGetProceduresForBug69298("Prop. useInfoSchema(1) + getProcRetFunc(0) MetaData", testConn);
		checkGetProcedureColumnsForBug69298("Prop. useInfoSchema(1) + getProcRetFunc(0) MetaData", testConn);
		testConn.close();
	}

	private void checkGetFunctionsForBug69298(String stepDescription, Connection testConn) throws Exception {
		DatabaseMetaData testDbMetaData = testConn.getMetaData();
		ResultSet proceduresMD = testDbMetaData.getFunctions(null, null, "testBug69298_%");
		String sd = stepDescription + " gerFunctions() ";

		assertTrue(sd + "one row expected.", proceduresMD.next());

		// function: testBug69298_func
		assertEquals(sd + "-> FUNCTION_CAT", testConn.getCatalog(), proceduresMD.getString("FUNCTION_CAT"));
		assertEquals(sd + "-> FUNCTION_SCHEM", null, proceduresMD.getString("FUNCTION_SCHEM"));
		assertEquals(sd + "-> FUNCTION_NAME", "testBug69298_func", proceduresMD.getString("FUNCTION_NAME"));
		assertEquals(sd + "-> REMARKS", "testBug69298_func comment", proceduresMD.getString("REMARKS"));
		assertEquals(sd + "-> FUNCTION_TYPE", DatabaseMetaData.functionNoTable, proceduresMD.getShort("FUNCTION_TYPE"));
		assertEquals(sd + "-> SPECIFIC_NAME", "testBug69298_func", proceduresMD.getString("SPECIFIC_NAME"));

		assertFalse(stepDescription + "no more rows expected.", proceduresMD.next());
	}

	private void checkGetFunctionColumnsForBug69298(String stepDescription, Connection testConn) throws Exception {
		DatabaseMetaData testDbMetaData = testConn.getMetaData();
		ResultSet proceduresMD = testDbMetaData.getFunctionColumns(null, null, "testBug69298_%", "%");
		String sd = stepDescription + " getFunctionColumns() ";

		assertTrue(sd + "one row expected.", proceduresMD.next());

		// function column: testBug69298_func return
		assertEquals(sd + "-> FUNCTION_CAT", testConn.getCatalog(), proceduresMD.getString("FUNCTION_CAT"));
		assertEquals(sd + "-> FUNCTION_SCHEM", null, proceduresMD.getString("FUNCTION_SCHEM"));
		assertEquals(sd + "-> FUNCTION_NAME", "testBug69298_func", proceduresMD.getString("FUNCTION_NAME"));
		assertEquals(sd + "-> COLUMN_NAME", "", proceduresMD.getString("COLUMN_NAME"));
		assertEquals(sd + "-> COLUMN_TYPE", DatabaseMetaData.functionReturn, proceduresMD.getShort("COLUMN_TYPE"));
		assertEquals(sd + "-> DATA_TYPE", Types.INTEGER, proceduresMD.getInt("DATA_TYPE"));
		assertEquals(sd + "-> TYPE_NAME", "INT", proceduresMD.getString("TYPE_NAME"));
		assertEquals(sd + "-> PRECISION", 10, proceduresMD.getInt("PRECISION"));
		assertEquals(sd + "-> LENGTH", 10, proceduresMD.getInt("LENGTH"));
		assertEquals(sd + "-> SCALE", 0, proceduresMD.getShort("SCALE"));
		assertEquals(sd + "-> RADIX", 10, proceduresMD.getShort("RADIX"));
		assertEquals(sd + "-> NULLABLE", DatabaseMetaData.functionNullable, proceduresMD.getShort("NULLABLE"));
		assertEquals(sd + "-> REMARKS", null, proceduresMD.getString("REMARKS"));
		assertEquals(sd + "-> CHAR_OCTET_LENGTH", 0, proceduresMD.getInt("CHAR_OCTET_LENGTH"));
		assertEquals(sd + "-> ORDINAL_POSITION", 0, proceduresMD.getInt("ORDINAL_POSITION"));
		assertEquals(sd + "-> IS_NULLABLE", "YES", proceduresMD.getString("IS_NULLABLE"));
		assertEquals(sd + "-> SPECIFIC_NAME", "testBug69298_func", proceduresMD.getString("SPECIFIC_NAME"));

		assertTrue(sd + "two rows expected.", proceduresMD.next());

		// function column: testBug69298_func.param_func
		assertEquals(sd + "-> FUNCTION_CAT", testConn.getCatalog(), proceduresMD.getString("FUNCTION_CAT"));
		assertEquals(sd + "-> FUNCTION_SCHEM", null, proceduresMD.getString("FUNCTION_SCHEM"));
		assertEquals(sd + "-> FUNCTION_NAME", "testBug69298_func", proceduresMD.getString("FUNCTION_NAME"));
		assertEquals(sd + "-> COLUMN_NAME", "param_func", proceduresMD.getString("COLUMN_NAME"));
		assertEquals(sd + "-> COLUMN_TYPE", DatabaseMetaData.functionColumnIn, proceduresMD.getShort("COLUMN_TYPE"));
		assertEquals(sd + "-> DATA_TYPE", Types.INTEGER, proceduresMD.getInt("DATA_TYPE"));
		assertEquals(sd + "-> TYPE_NAME", "INT", proceduresMD.getString("TYPE_NAME"));
		assertEquals(sd + "-> PRECISION", 10, proceduresMD.getInt("PRECISION"));
		assertEquals(sd + "-> LENGTH", 10, proceduresMD.getInt("LENGTH"));
		assertEquals(sd + "-> SCALE", 0, proceduresMD.getShort("SCALE"));
		assertEquals(sd + "-> RADIX", 10, proceduresMD.getShort("RADIX"));
		assertEquals(sd + "-> NULLABLE", DatabaseMetaData.functionNullable, proceduresMD.getShort("NULLABLE"));
		assertEquals(sd + "-> REMARKS", null, proceduresMD.getString("REMARKS"));
		assertEquals(sd + "-> CHAR_OCTET_LENGTH", 0, proceduresMD.getInt("CHAR_OCTET_LENGTH"));
		assertEquals(sd + "-> ORDINAL_POSITION", 1, proceduresMD.getInt("ORDINAL_POSITION"));
		assertEquals(sd + "-> IS_NULLABLE", "YES", proceduresMD.getString("IS_NULLABLE"));
		assertEquals(sd + "-> SPECIFIC_NAME", "testBug69298_func", proceduresMD.getString("SPECIFIC_NAME"));

		assertFalse(sd + "no more rows expected.", proceduresMD.next());
	}

	private void checkGetProceduresForBug69298(String stepDescription, Connection testConn) throws Exception {
		DatabaseMetaData testDbMetaData = testConn.getMetaData();
		ResultSet proceduresMD = testDbMetaData.getProcedures(null, null, "testBug69298_%");
		String sd = stepDescription + " getProcedures() ";
		boolean isGetProceduresReturnsFunctions = ((ConnectionProperties) testConn).getGetProceduresReturnsFunctions();

		assertTrue(sd + "one row expected.", proceduresMD.next());

		if (isGetProceduresReturnsFunctions) {
			// function: testBug69298_func
			assertEquals(sd + "-> PROCEDURE_CAT", testConn.getCatalog(), proceduresMD.getString("PROCEDURE_CAT"));
			assertEquals(sd + "-> PROCEDURE_SCHEM", null, proceduresMD.getString("PROCEDURE_SCHEM"));
			assertEquals(sd + "-> PROCEDURE_NAME", "testBug69298_func", proceduresMD.getString("PROCEDURE_NAME"));
			assertEquals(sd + "-> REMARKS", "testBug69298_func comment", proceduresMD.getString("REMARKS"));
			assertEquals(sd + "-> PROCEDURE_TYPE", DatabaseMetaData.procedureReturnsResult,
					proceduresMD.getShort("PROCEDURE_TYPE"));
			assertEquals(sd + "-> SPECIFIC_NAME", "testBug69298_func", proceduresMD.getString("SPECIFIC_NAME"));

			assertTrue(sd + "two rows expected.", proceduresMD.next());
		}

		// procedure: testBug69298_proc
		assertEquals(sd + "-> PROCEDURE_CAT", testConn.getCatalog(), proceduresMD.getString("PROCEDURE_CAT"));
		assertEquals(sd + "-> PROCEDURE_SCHEM", null, proceduresMD.getString("PROCEDURE_SCHEM"));
		assertEquals(sd + "-> PROCEDURE_NAME", "testBug69298_proc", proceduresMD.getString("PROCEDURE_NAME"));
		assertEquals(sd + "-> REMARKS", "testBug69298_proc comment", proceduresMD.getString("REMARKS"));
		assertEquals(sd + "-> PROCEDURE_TYPE", DatabaseMetaData.procedureNoResult,
				proceduresMD.getShort("PROCEDURE_TYPE"));
		assertEquals(sd + "-> SPECIFIC_NAME", "testBug69298_proc", proceduresMD.getString("SPECIFIC_NAME"));

		assertFalse(stepDescription + "no more rows expected.", proceduresMD.next());
	}

	private void checkGetProcedureColumnsForBug69298(String stepDescription, Connection testConn) throws Exception {
		DatabaseMetaData testDbMetaData = testConn.getMetaData();
		ResultSet proceduresMD = testDbMetaData.getProcedureColumns(null, null, "testBug69298_%", "%");
		String sd = stepDescription + " getProcedureColumns() ";
		boolean isGetProceduresReturnsFunctions = ((ConnectionProperties) testConn).getGetProceduresReturnsFunctions();

		assertTrue(sd + "one row expected.", proceduresMD.next());

		if (isGetProceduresReturnsFunctions) {
			// function column: testBug69298_func return
			assertEquals(sd + "-> PROCEDURE_CAT", testConn.getCatalog(), proceduresMD.getString("PROCEDURE_CAT"));
			assertEquals(sd + "-> PROCEDURE_SCHEM", null, proceduresMD.getString("PROCEDURE_SCHEM"));
			assertEquals(sd + "-> PROCEDURE_NAME", "testBug69298_func", proceduresMD.getString("PROCEDURE_NAME"));
			assertEquals(sd + "-> COLUMN_NAME", "", proceduresMD.getString("COLUMN_NAME"));
			assertEquals(sd + "-> COLUMN_TYPE", DatabaseMetaData.procedureColumnReturn,
					proceduresMD.getShort("COLUMN_TYPE"));
			assertEquals(sd + "-> DATA_TYPE", Types.INTEGER, proceduresMD.getInt("DATA_TYPE"));
			assertEquals(sd + "-> TYPE_NAME", "INT", proceduresMD.getString("TYPE_NAME"));
			assertEquals(sd + "-> PRECISION", 10, proceduresMD.getInt("PRECISION"));
			assertEquals(sd + "-> LENGTH", 10, proceduresMD.getInt("LENGTH"));
			assertEquals(sd + "-> SCALE", 0, proceduresMD.getShort("SCALE"));
			assertEquals(sd + "-> RADIX", 10, proceduresMD.getShort("RADIX"));
			assertEquals(sd + "-> NULLABLE", DatabaseMetaData.procedureNullable, proceduresMD.getShort("NULLABLE"));
			assertEquals(sd + "-> REMARKS", null, proceduresMD.getString("REMARKS"));
			assertEquals(sd + "-> COLUMN_DEF", null, proceduresMD.getString("COLUMN_DEF"));
			assertEquals(sd + "-> SQL_DATA_TYPE", 0, proceduresMD.getInt("SQL_DATA_TYPE"));
			assertEquals(sd + "-> SQL_DATETIME_SUB", 0, proceduresMD.getInt("SQL_DATETIME_SUB"));
			assertEquals(sd + "-> CHAR_OCTET_LENGTH", 0, proceduresMD.getInt("CHAR_OCTET_LENGTH"));
			assertEquals(sd + "-> ORDINAL_POSITION", 0, proceduresMD.getInt("ORDINAL_POSITION"));
			assertEquals(sd + "-> IS_NULLABLE", "YES", proceduresMD.getString("IS_NULLABLE"));
			assertEquals(sd + "-> SPECIFIC_NAME", "testBug69298_func", proceduresMD.getString("SPECIFIC_NAME"));

			assertTrue(sd + "two rows expected.", proceduresMD.next());

			// function column: testBug69298_func.param_func
			assertEquals(sd + "-> PROCEDURE_CAT", testConn.getCatalog(), proceduresMD.getString("PROCEDURE_CAT"));
			assertEquals(sd + "-> PROCEDURE_SCHEM", null, proceduresMD.getString("PROCEDURE_SCHEM"));
			assertEquals(sd + "-> PROCEDURE_NAME", "testBug69298_func", proceduresMD.getString("PROCEDURE_NAME"));
			assertEquals(sd + "-> COLUMN_NAME", "param_func", proceduresMD.getString("COLUMN_NAME"));
			assertEquals(sd + "-> COLUMN_TYPE", DatabaseMetaData.procedureColumnIn,
					proceduresMD.getShort("COLUMN_TYPE"));
			assertEquals(sd + "-> DATA_TYPE", Types.INTEGER, proceduresMD.getInt("DATA_TYPE"));
			assertEquals(sd + "-> TYPE_NAME", "INT", proceduresMD.getString("TYPE_NAME"));
			assertEquals(sd + "-> PRECISION", 10, proceduresMD.getInt("PRECISION"));
			assertEquals(sd + "-> LENGTH", 10, proceduresMD.getInt("LENGTH"));
			assertEquals(sd + "-> SCALE", 0, proceduresMD.getShort("SCALE"));
			assertEquals(sd + "-> RADIX", 10, proceduresMD.getShort("RADIX"));
			assertEquals(sd + "-> NULLABLE", DatabaseMetaData.procedureNullable, proceduresMD.getShort("NULLABLE"));
			assertEquals(sd + "-> REMARKS", null, proceduresMD.getString("REMARKS"));
			assertEquals(sd + "-> COLUMN_DEF", null, proceduresMD.getString("COLUMN_DEF"));
			assertEquals(sd + "-> SQL_DATA_TYPE", 0, proceduresMD.getInt("SQL_DATA_TYPE"));
			assertEquals(sd + "-> SQL_DATETIME_SUB", 0, proceduresMD.getInt("SQL_DATETIME_SUB"));
			assertEquals(sd + "-> CHAR_OCTET_LENGTH", 0, proceduresMD.getInt("CHAR_OCTET_LENGTH"));
			assertEquals(sd + "-> ORDINAL_POSITION", 1, proceduresMD.getInt("ORDINAL_POSITION"));
			assertEquals(sd + "-> IS_NULLABLE", "YES", proceduresMD.getString("IS_NULLABLE"));
			assertEquals(sd + "-> SPECIFIC_NAME", "testBug69298_func", proceduresMD.getString("SPECIFIC_NAME"));

			assertTrue(sd + "three rows expected.", proceduresMD.next());
		}

		// procedure column: testBug69298_proc.param_proc
		assertEquals(sd + "-> PROCEDURE_CAT", testConn.getCatalog(), proceduresMD.getString("PROCEDURE_CAT"));
		assertEquals(sd + "-> PROCEDURE_SCHEM", null, proceduresMD.getString("PROCEDURE_SCHEM"));
		assertEquals(sd + "-> PROCEDURE_NAME", "testBug69298_proc", proceduresMD.getString("PROCEDURE_NAME"));
		assertEquals(sd + "-> COLUMN_NAME", "param_proc", proceduresMD.getString("COLUMN_NAME"));
		assertEquals(sd + "-> COLUMN_TYPE", DatabaseMetaData.procedureColumnIn, proceduresMD.getShort("COLUMN_TYPE"));
		assertEquals(sd + "-> DATA_TYPE", Types.INTEGER, proceduresMD.getInt("DATA_TYPE"));
		assertEquals(sd + "-> TYPE_NAME", "INT", proceduresMD.getString("TYPE_NAME"));
		assertEquals(sd + "-> PRECISION", 10, proceduresMD.getInt("PRECISION"));
		assertEquals(sd + "-> LENGTH", 10, proceduresMD.getInt("LENGTH"));
		assertEquals(sd + "-> SCALE", 0, proceduresMD.getShort("SCALE"));
		assertEquals(sd + "-> RADIX", 10, proceduresMD.getShort("RADIX"));
		assertEquals(sd + "-> NULLABLE", DatabaseMetaData.procedureNullable, proceduresMD.getShort("NULLABLE"));
		assertEquals(sd + "-> REMARKS", null, proceduresMD.getString("REMARKS"));
		assertEquals(sd + "-> COLUMN_DEF", null, proceduresMD.getString("COLUMN_DEF"));
		assertEquals(sd + "-> SQL_DATA_TYPE", 0, proceduresMD.getInt("SQL_DATA_TYPE"));
		assertEquals(sd + "-> SQL_DATETIME_SUB", 0, proceduresMD.getInt("SQL_DATETIME_SUB"));
		assertEquals(sd + "-> CHAR_OCTET_LENGTH", 0, proceduresMD.getInt("CHAR_OCTET_LENGTH"));
		assertEquals(sd + "-> ORDINAL_POSITION", 1, proceduresMD.getInt("ORDINAL_POSITION"));
		assertEquals(sd + "-> IS_NULLABLE", "YES", proceduresMD.getString("IS_NULLABLE"));
		assertEquals(sd + "-> SPECIFIC_NAME", "testBug69298_proc", proceduresMD.getString("SPECIFIC_NAME"));

		assertFalse(sd + "no more rows expected.", proceduresMD.next());
	}
}
