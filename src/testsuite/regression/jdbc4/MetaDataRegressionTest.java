/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

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
		ResultSet functionsMD = testDbMetaData.getFunctions(null, null, "testBug69298_%");
		String sd = stepDescription + " getFunctions() ";

		assertTrue(sd + "one row expected.", functionsMD.next());

		// function: testBug69298_func
		assertEquals(sd + "-> FUNCTION_CAT", testConn.getCatalog(), functionsMD.getString("FUNCTION_CAT"));
		assertEquals(sd + "-> FUNCTION_SCHEM", null, functionsMD.getString("FUNCTION_SCHEM"));
		assertEquals(sd + "-> FUNCTION_NAME", "testBug69298_func", functionsMD.getString("FUNCTION_NAME"));
		assertEquals(sd + "-> REMARKS", "testBug69298_func comment", functionsMD.getString("REMARKS"));
		assertEquals(sd + "-> FUNCTION_TYPE", DatabaseMetaData.functionNoTable, functionsMD.getShort("FUNCTION_TYPE"));
		assertEquals(sd + "-> SPECIFIC_NAME", "testBug69298_func", functionsMD.getString("SPECIFIC_NAME"));

		assertFalse(stepDescription + "no more rows expected.", functionsMD.next());
	}

	private void checkGetFunctionColumnsForBug69298(String stepDescription, Connection testConn) throws Exception {
		DatabaseMetaData testDbMetaData = testConn.getMetaData();
		ResultSet funcColsMD = testDbMetaData.getFunctionColumns(null, null, "testBug69298_%", "%");
		String sd = stepDescription + " getFunctionColumns() ";

		assertTrue(sd + "1st of 2 rows expected.", funcColsMD.next());

		// function column: testBug69298_func return
		assertEquals(sd + "-> FUNCTION_CAT", testConn.getCatalog(), funcColsMD.getString("FUNCTION_CAT"));
		assertEquals(sd + "-> FUNCTION_SCHEM", null, funcColsMD.getString("FUNCTION_SCHEM"));
		assertEquals(sd + "-> FUNCTION_NAME", "testBug69298_func", funcColsMD.getString("FUNCTION_NAME"));
		assertEquals(sd + "-> COLUMN_NAME", "", funcColsMD.getString("COLUMN_NAME"));
		assertEquals(sd + "-> COLUMN_TYPE", DatabaseMetaData.functionReturn, funcColsMD.getShort("COLUMN_TYPE"));
		assertEquals(sd + "-> DATA_TYPE", Types.INTEGER, funcColsMD.getInt("DATA_TYPE"));
		assertEquals(sd + "-> TYPE_NAME", "INT", funcColsMD.getString("TYPE_NAME"));
		assertEquals(sd + "-> PRECISION", 10, funcColsMD.getInt("PRECISION"));
		assertEquals(sd + "-> LENGTH", 10, funcColsMD.getInt("LENGTH"));
		assertEquals(sd + "-> SCALE", 0, funcColsMD.getShort("SCALE"));
		assertEquals(sd + "-> RADIX", 10, funcColsMD.getShort("RADIX"));
		assertEquals(sd + "-> NULLABLE", DatabaseMetaData.functionNullable, funcColsMD.getShort("NULLABLE"));
		assertEquals(sd + "-> REMARKS", null, funcColsMD.getString("REMARKS"));
		assertEquals(sd + "-> CHAR_OCTET_LENGTH", 0, funcColsMD.getInt("CHAR_OCTET_LENGTH"));
		assertEquals(sd + "-> ORDINAL_POSITION", 0, funcColsMD.getInt("ORDINAL_POSITION"));
		assertEquals(sd + "-> IS_NULLABLE", "YES", funcColsMD.getString("IS_NULLABLE"));
		assertEquals(sd + "-> SPECIFIC_NAME", "testBug69298_func", funcColsMD.getString("SPECIFIC_NAME"));

		assertTrue(sd + "2nd of 2 rows expected.", funcColsMD.next());

		// function column: testBug69298_func.param_func
		assertEquals(sd + "-> FUNCTION_CAT", testConn.getCatalog(), funcColsMD.getString("FUNCTION_CAT"));
		assertEquals(sd + "-> FUNCTION_SCHEM", null, funcColsMD.getString("FUNCTION_SCHEM"));
		assertEquals(sd + "-> FUNCTION_NAME", "testBug69298_func", funcColsMD.getString("FUNCTION_NAME"));
		assertEquals(sd + "-> COLUMN_NAME", "param_func", funcColsMD.getString("COLUMN_NAME"));
		assertEquals(sd + "-> COLUMN_TYPE", DatabaseMetaData.functionColumnIn, funcColsMD.getShort("COLUMN_TYPE"));
		assertEquals(sd + "-> DATA_TYPE", Types.INTEGER, funcColsMD.getInt("DATA_TYPE"));
		assertEquals(sd + "-> TYPE_NAME", "INT", funcColsMD.getString("TYPE_NAME"));
		assertEquals(sd + "-> PRECISION", 10, funcColsMD.getInt("PRECISION"));
		assertEquals(sd + "-> LENGTH", 10, funcColsMD.getInt("LENGTH"));
		assertEquals(sd + "-> SCALE", 0, funcColsMD.getShort("SCALE"));
		assertEquals(sd + "-> RADIX", 10, funcColsMD.getShort("RADIX"));
		assertEquals(sd + "-> NULLABLE", DatabaseMetaData.functionNullable, funcColsMD.getShort("NULLABLE"));
		assertEquals(sd + "-> REMARKS", null, funcColsMD.getString("REMARKS"));
		assertEquals(sd + "-> CHAR_OCTET_LENGTH", 0, funcColsMD.getInt("CHAR_OCTET_LENGTH"));
		assertEquals(sd + "-> ORDINAL_POSITION", 1, funcColsMD.getInt("ORDINAL_POSITION"));
		assertEquals(sd + "-> IS_NULLABLE", "YES", funcColsMD.getString("IS_NULLABLE"));
		assertEquals(sd + "-> SPECIFIC_NAME", "testBug69298_func", funcColsMD.getString("SPECIFIC_NAME"));

		assertFalse(sd + "no more rows expected.", funcColsMD.next());
	}

	private void checkGetProceduresForBug69298(String stepDescription, Connection testConn) throws Exception {
		DatabaseMetaData testDbMetaData = testConn.getMetaData();
		ResultSet proceduresMD = testDbMetaData.getProcedures(null, null, "testBug69298_%");
		String sd = stepDescription + " getProcedures() ";
		boolean isGetProceduresReturnsFunctions = ((ConnectionProperties) testConn).getGetProceduresReturnsFunctions();

		if (isGetProceduresReturnsFunctions) {
			assertTrue(sd + "1st of 2 rows expected.", proceduresMD.next());

			// function: testBug69298_func
			assertEquals(sd + "-> PROCEDURE_CAT", testConn.getCatalog(), proceduresMD.getString("PROCEDURE_CAT"));
			assertEquals(sd + "-> PROCEDURE_SCHEM", null, proceduresMD.getString("PROCEDURE_SCHEM"));
			assertEquals(sd + "-> PROCEDURE_NAME", "testBug69298_func", proceduresMD.getString("PROCEDURE_NAME"));
			assertEquals(sd + "-> REMARKS", "testBug69298_func comment", proceduresMD.getString("REMARKS"));
			assertEquals(sd + "-> PROCEDURE_TYPE", DatabaseMetaData.procedureReturnsResult,
					proceduresMD.getShort("PROCEDURE_TYPE"));
			assertEquals(sd + "-> SPECIFIC_NAME", "testBug69298_func", proceduresMD.getString("SPECIFIC_NAME"));

			assertTrue(sd + "2nd of 2 rows expected.", proceduresMD.next());
		} else {
			assertTrue(sd + "one row expected.", proceduresMD.next());
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
		ResultSet procColsMD = testDbMetaData.getProcedureColumns(null, null, "testBug69298_%", "%");
		String sd = stepDescription + " getProcedureColumns() ";
		boolean isGetProceduresReturnsFunctions = ((ConnectionProperties) testConn).getGetProceduresReturnsFunctions();

		if (isGetProceduresReturnsFunctions) {
			assertTrue(sd + "1st of 3 rows expected.", procColsMD.next());

			// function column: testBug69298_func return
			assertEquals(sd + "-> PROCEDURE_CAT", testConn.getCatalog(), procColsMD.getString("PROCEDURE_CAT"));
			assertEquals(sd + "-> PROCEDURE_SCHEM", null, procColsMD.getString("PROCEDURE_SCHEM"));
			assertEquals(sd + "-> PROCEDURE_NAME", "testBug69298_func", procColsMD.getString("PROCEDURE_NAME"));
			assertEquals(sd + "-> COLUMN_NAME", "", procColsMD.getString("COLUMN_NAME"));
			assertEquals(sd + "-> COLUMN_TYPE", DatabaseMetaData.procedureColumnReturn,
					procColsMD.getShort("COLUMN_TYPE"));
			assertEquals(sd + "-> DATA_TYPE", Types.INTEGER, procColsMD.getInt("DATA_TYPE"));
			assertEquals(sd + "-> TYPE_NAME", "INT", procColsMD.getString("TYPE_NAME"));
			assertEquals(sd + "-> PRECISION", 10, procColsMD.getInt("PRECISION"));
			assertEquals(sd + "-> LENGTH", 10, procColsMD.getInt("LENGTH"));
			assertEquals(sd + "-> SCALE", 0, procColsMD.getShort("SCALE"));
			assertEquals(sd + "-> RADIX", 10, procColsMD.getShort("RADIX"));
			assertEquals(sd + "-> NULLABLE", DatabaseMetaData.procedureNullable, procColsMD.getShort("NULLABLE"));
			assertEquals(sd + "-> REMARKS", null, procColsMD.getString("REMARKS"));
			assertEquals(sd + "-> COLUMN_DEF", null, procColsMD.getString("COLUMN_DEF"));
			assertEquals(sd + "-> SQL_DATA_TYPE", 0, procColsMD.getInt("SQL_DATA_TYPE"));
			assertEquals(sd + "-> SQL_DATETIME_SUB", 0, procColsMD.getInt("SQL_DATETIME_SUB"));
			assertEquals(sd + "-> CHAR_OCTET_LENGTH", 0, procColsMD.getInt("CHAR_OCTET_LENGTH"));
			assertEquals(sd + "-> ORDINAL_POSITION", 0, procColsMD.getInt("ORDINAL_POSITION"));
			assertEquals(sd + "-> IS_NULLABLE", "YES", procColsMD.getString("IS_NULLABLE"));
			assertEquals(sd + "-> SPECIFIC_NAME", "testBug69298_func", procColsMD.getString("SPECIFIC_NAME"));

			assertTrue(sd + "2nd of 3 rows expected.", procColsMD.next());

			// function column: testBug69298_func.param_func
			assertEquals(sd + "-> PROCEDURE_CAT", testConn.getCatalog(), procColsMD.getString("PROCEDURE_CAT"));
			assertEquals(sd + "-> PROCEDURE_SCHEM", null, procColsMD.getString("PROCEDURE_SCHEM"));
			assertEquals(sd + "-> PROCEDURE_NAME", "testBug69298_func", procColsMD.getString("PROCEDURE_NAME"));
			assertEquals(sd + "-> COLUMN_NAME", "param_func", procColsMD.getString("COLUMN_NAME"));
			assertEquals(sd + "-> COLUMN_TYPE", DatabaseMetaData.procedureColumnIn, procColsMD.getShort("COLUMN_TYPE"));
			assertEquals(sd + "-> DATA_TYPE", Types.INTEGER, procColsMD.getInt("DATA_TYPE"));
			assertEquals(sd + "-> TYPE_NAME", "INT", procColsMD.getString("TYPE_NAME"));
			assertEquals(sd + "-> PRECISION", 10, procColsMD.getInt("PRECISION"));
			assertEquals(sd + "-> LENGTH", 10, procColsMD.getInt("LENGTH"));
			assertEquals(sd + "-> SCALE", 0, procColsMD.getShort("SCALE"));
			assertEquals(sd + "-> RADIX", 10, procColsMD.getShort("RADIX"));
			assertEquals(sd + "-> NULLABLE", DatabaseMetaData.procedureNullable, procColsMD.getShort("NULLABLE"));
			assertEquals(sd + "-> REMARKS", null, procColsMD.getString("REMARKS"));
			assertEquals(sd + "-> COLUMN_DEF", null, procColsMD.getString("COLUMN_DEF"));
			assertEquals(sd + "-> SQL_DATA_TYPE", 0, procColsMD.getInt("SQL_DATA_TYPE"));
			assertEquals(sd + "-> SQL_DATETIME_SUB", 0, procColsMD.getInt("SQL_DATETIME_SUB"));
			assertEquals(sd + "-> CHAR_OCTET_LENGTH", 0, procColsMD.getInt("CHAR_OCTET_LENGTH"));
			assertEquals(sd + "-> ORDINAL_POSITION", 1, procColsMD.getInt("ORDINAL_POSITION"));
			assertEquals(sd + "-> IS_NULLABLE", "YES", procColsMD.getString("IS_NULLABLE"));
			assertEquals(sd + "-> SPECIFIC_NAME", "testBug69298_func", procColsMD.getString("SPECIFIC_NAME"));

			assertTrue(sd + "3rd of 3 rows expected.", procColsMD.next());
		} else {
			assertTrue(sd + "one row expected.", procColsMD.next());
		}

		// procedure column: testBug69298_proc.param_proc
		assertEquals(sd + "-> PROCEDURE_CAT", testConn.getCatalog(), procColsMD.getString("PROCEDURE_CAT"));
		assertEquals(sd + "-> PROCEDURE_SCHEM", null, procColsMD.getString("PROCEDURE_SCHEM"));
		assertEquals(sd + "-> PROCEDURE_NAME", "testBug69298_proc", procColsMD.getString("PROCEDURE_NAME"));
		assertEquals(sd + "-> COLUMN_NAME", "param_proc", procColsMD.getString("COLUMN_NAME"));
		assertEquals(sd + "-> COLUMN_TYPE", DatabaseMetaData.procedureColumnIn, procColsMD.getShort("COLUMN_TYPE"));
		assertEquals(sd + "-> DATA_TYPE", Types.INTEGER, procColsMD.getInt("DATA_TYPE"));
		assertEquals(sd + "-> TYPE_NAME", "INT", procColsMD.getString("TYPE_NAME"));
		assertEquals(sd + "-> PRECISION", 10, procColsMD.getInt("PRECISION"));
		assertEquals(sd + "-> LENGTH", 10, procColsMD.getInt("LENGTH"));
		assertEquals(sd + "-> SCALE", 0, procColsMD.getShort("SCALE"));
		assertEquals(sd + "-> RADIX", 10, procColsMD.getShort("RADIX"));
		assertEquals(sd + "-> NULLABLE", DatabaseMetaData.procedureNullable, procColsMD.getShort("NULLABLE"));
		assertEquals(sd + "-> REMARKS", null, procColsMD.getString("REMARKS"));
		assertEquals(sd + "-> COLUMN_DEF", null, procColsMD.getString("COLUMN_DEF"));
		assertEquals(sd + "-> SQL_DATA_TYPE", 0, procColsMD.getInt("SQL_DATA_TYPE"));
		assertEquals(sd + "-> SQL_DATETIME_SUB", 0, procColsMD.getInt("SQL_DATETIME_SUB"));
		assertEquals(sd + "-> CHAR_OCTET_LENGTH", 0, procColsMD.getInt("CHAR_OCTET_LENGTH"));
		assertEquals(sd + "-> ORDINAL_POSITION", 1, procColsMD.getInt("ORDINAL_POSITION"));
		assertEquals(sd + "-> IS_NULLABLE", "YES", procColsMD.getString("IS_NULLABLE"));
		assertEquals(sd + "-> SPECIFIC_NAME", "testBug69298_proc", procColsMD.getString("SPECIFIC_NAME"));

		assertFalse(sd + "no more rows expected.", procColsMD.next());
	}

	/**
	 * Tests fix for BUG#17248345 - GETFUNCTIONCOLUMNS() METHOD RETURNS COLUMNS OF PROCEDURE. (this happens when
	 * functions and procedures have a common name)
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug17248345() throws Exception {
		Connection testConn;

		// create one stored procedure and one function with same name
		createProcedure("testBug17248345", "(IN proccol INT) SELECT 1");
		createFunction("testBug17248345", "(funccol INT) RETURNS INT DETERMINISTIC RETURN 1");

		// test with standard connection (getProceduresReturnsFunctions=true & useInformationSchema=false)
		assertFalse("Property useInformationSchema should be false",
				((ConnectionProperties) conn).getUseInformationSchema());
		assertTrue("Property getProceduresReturnsFunctions should be true",
				((ConnectionProperties) conn).getGetProceduresReturnsFunctions());
		checkMetaDataInfoForBug17248345(conn);

		// test with property useInformationSchema=true (getProceduresReturnsFunctions=true)
		testConn = getConnectionWithProps("useInformationSchema=true");
		assertTrue("Property useInformationSchema should be true",
				((ConnectionProperties) testConn).getUseInformationSchema());
		assertTrue("Property getProceduresReturnsFunctions should be true",
				((ConnectionProperties) testConn).getGetProceduresReturnsFunctions());
		checkMetaDataInfoForBug17248345(testConn);
		testConn.close();

		// test with property getProceduresReturnsFunctions=false (useInformationSchema=false)
		testConn = getConnectionWithProps("getProceduresReturnsFunctions=false");
		assertFalse("Property useInformationSchema should be false",
				((ConnectionProperties) testConn).getUseInformationSchema());
		assertFalse("Property getProceduresReturnsFunctions should be false",
				((ConnectionProperties) testConn).getGetProceduresReturnsFunctions());
		checkMetaDataInfoForBug17248345(testConn);
		testConn.close();

		// test with property useInformationSchema=true & getProceduresReturnsFunctions=false
		testConn = getConnectionWithProps("useInformationSchema=true,getProceduresReturnsFunctions=false");
		assertTrue("Property useInformationSchema should be true",
				((ConnectionProperties) testConn).getUseInformationSchema());
		assertFalse("Property getProceduresReturnsFunctions should be false",
				((ConnectionProperties) testConn).getGetProceduresReturnsFunctions());
		checkMetaDataInfoForBug17248345(testConn);
		testConn.close();
	}

	private void checkMetaDataInfoForBug17248345(Connection testConn) throws Exception {
		DatabaseMetaData testDbMetaData = testConn.getMetaData();
		ResultSet rsMD;
		boolean useInfoSchema = ((ConnectionProperties) testConn).getUseInformationSchema();
		boolean getProcRetFunc = ((ConnectionProperties) testConn).getGetProceduresReturnsFunctions();
		String stepDescription = "Prop. useInfoSchema(" + (useInfoSchema ? 1 : 0) + ") + getProcRetFunc("
				+ (getProcRetFunc ? 1 : 0) + "):";
		String sd;

		// getFunctions() must return 1 record.
		sd = stepDescription + " getFunctions() ";
		rsMD = testDbMetaData.getFunctions(null, null, "testBug17248345");
		assertTrue(sd + "one row expected.", rsMD.next());
		assertEquals(sd + " -> FUNCTION_NAME", "testBug17248345", rsMD.getString("FUNCTION_NAME"));
		assertFalse(sd + "no more rows expected.", rsMD.next());

		// getFunctionColumns() must return 2 records (func return + func param).
		sd = stepDescription + " getFunctionColumns() ";
		rsMD = testDbMetaData.getFunctionColumns(null, null, "testBug17248345", "%");
		assertTrue(sd + "1st of 2 rows expected.", rsMD.next());
		assertEquals(sd + " -> FUNCTION_NAME", "testBug17248345", rsMD.getString("FUNCTION_NAME"));
		assertEquals(sd + " -> COLUMN_NAME", "", rsMD.getString("COLUMN_NAME"));
		assertTrue(sd + "2nd of 2 rows expected.", rsMD.next());
		assertEquals(sd + " -> FUNCTION_NAME", "testBug17248345", rsMD.getString("FUNCTION_NAME"));
		assertEquals(sd + " -> COLUMN_NAME", "funccol", rsMD.getString("COLUMN_NAME"));
		assertFalse(sd + "no more rows expected.", rsMD.next());

		// getProcedures() must return 1 or 2 records, depending on if getProceduresReturnsFunctions is false or true
		// respectively. When exists a procedure and a function with same name, function is returned first.
		sd = stepDescription + " getProcedures() ";
		rsMD = testDbMetaData.getProcedures(null, null, "testBug17248345");
		if (getProcRetFunc) {
			assertTrue(sd + "1st of 2 rows expected.", rsMD.next());
			assertEquals(sd + " -> PROCEDURE_NAME", "testBug17248345", rsMD.getString("PROCEDURE_NAME"));
			assertTrue(sd + "2nd of 2 rows expected.", rsMD.next());
		} else {
			assertTrue(sd + "one row expected.", rsMD.next());
		}
		assertEquals(sd + " -> PROCEDURE_NAME", "testBug17248345", rsMD.getString("PROCEDURE_NAME"));
		assertFalse(sd + "no more rows expected.", rsMD.next());

		// getProcedureColumns() must return 1 or 3 records, depending on if getProceduresReturnsFunctions is false or
		// true respectively. When exists a procedure and a function with same name, function is returned first.
		sd = stepDescription + " getProcedureColumns() ";
		rsMD = testDbMetaData.getProcedureColumns(null, null, "testBug17248345", "%");
		if (getProcRetFunc) {
			assertTrue(sd + "1st of 3 rows expected.", rsMD.next());
			assertEquals(sd + " -> PROCEDURE_NAME", "testBug17248345", rsMD.getString("PROCEDURE_NAME"));
			assertEquals(sd + " -> COLUMN_NAME", "", rsMD.getString("COLUMN_NAME"));
			assertTrue(sd + "2nd of 3 rows expected.", rsMD.next());
			assertEquals(sd + " -> PROCEDURE_NAME", "testBug17248345", rsMD.getString("PROCEDURE_NAME"));
			assertEquals(sd + " -> COLUMN_NAME", "funccol", rsMD.getString("COLUMN_NAME"));
			assertTrue(sd + "3rd of 3 rows expected.", rsMD.next());
		} else {
			assertTrue(sd + "one row expected.", rsMD.next());
		}
		assertEquals(sd + " -> PROCEDURE_NAME", "testBug17248345", rsMD.getString("PROCEDURE_NAME"));
		assertEquals(sd + " -> COLUMN_NAME", "proccol", rsMD.getString("COLUMN_NAME"));
		assertFalse(sd + "no more rows expected.", rsMD.next());
	}

	/*
	 * Tests DatabaseMetaData.getSQLKeywords().
	 * (Related to BUG#70701 - DatabaseMetaData.getSQLKeywords() doesn't match MySQL 5.6 reserved words)
	 * 
	 * The keywords list that this method returns depends on JDBC version.
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testReservedWords() throws Exception {
		final String mysqlKeywords = "ACCESSIBLE,ADD,ANALYZE,ASC,BEFORE,CASCADE,CHANGE,CONTINUE,DATABASE,DATABASES,DAY_HOUR,DAY_MICROSECOND,DAY_MINUTE,DAY_SECOND,DELAYED,DESC,DISTINCTROW,DIV,DUAL,ELSEIF,ENCLOSED,ESCAPED,EXIT,EXPLAIN,FLOAT4,FLOAT8,FORCE,FULLTEXT,HIGH_PRIORITY,HOUR_MICROSECOND,HOUR_MINUTE,HOUR_SECOND,IF,IGNORE,INDEX,INFILE,INT1,INT2,INT3,INT4,INT8,IO_AFTER_GTIDS,IO_BEFORE_GTIDS,ITERATE,KEY,KEYS,KILL,LEAVE,LIMIT,LINEAR,LINES,LOAD,LOCK,LONG,LONGBLOB,LONGTEXT,LOOP,LOW_PRIORITY,MASTER_BIND,MASTER_SSL_VERIFY_SERVER_CERT,MAXVALUE,MEDIUMBLOB,MEDIUMINT,MEDIUMTEXT,MIDDLEINT,MINUTE_MICROSECOND,MINUTE_SECOND,NONBLOCKING,NO_WRITE_TO_BINLOG,OPTIMIZE,OPTION,OPTIONALLY,OUTFILE,PURGE,READ,READ_WRITE,REGEXP,RENAME,REPEAT,REPLACE,REQUIRE,RESIGNAL,RESTRICT,RLIKE,SCHEMA,SCHEMAS,SECOND_MICROSECOND,SEPARATOR,SHOW,SIGNAL,SPATIAL,SQL_BIG_RESULT,SQL_CALC_FOUND_ROWS,SQL_SMALL_RESULT,SSL,STARTING,STRAIGHT_JOIN,TERMINATED,TINYBLOB,TINYINT,TINYTEXT,UNDO,UNLOCK,UNSIGNED,USAGE,USE,UTC_DATE,UTC_TIME,UTC_TIMESTAMP,VARBINARY,VARCHARACTER,WHILE,WRITE,XOR,YEAR_MONTH,ZEROFILL";
		assertEquals("MySQL keywords don't match expected.", mysqlKeywords, this.conn.getMetaData().getSQLKeywords());
	}
}
