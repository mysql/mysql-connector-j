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
		createFunction("bug68307_func", "(func_param_in INT) RETURNS INT DETERMINISTIC RETURN 1");

		createProcedure("bug68307_proc",
				"(IN proc_param_in INT, OUT proc_param_out INT, INOUT proc_param_inout INT) SELECT 1");

		// test metadata from MySQL
		DatabaseMetaData testDbMetaData = conn.getMetaData();
		checkBug68307FunctionColumntype("MySQL", testDbMetaData);
		checkBug68307ProcedureColumntype("MySQL", testDbMetaData);

		// test metadata from I__S
		Connection connUseIS = getConnectionWithProps("useInformationSchema=true");
		testDbMetaData = connUseIS.getMetaData();
		checkBug68307FunctionColumntype("I__S", testDbMetaData);
		checkBug68307ProcedureColumntype("I__S", testDbMetaData);
		connUseIS.close();
	}

	private void checkBug68307FunctionColumntype(String testAgainst, DatabaseMetaData testDbMetaData) throws Exception {
		rs = testDbMetaData.getFunctionColumns(null, null, "bug68307_%", "%");

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

	private void checkBug68307ProcedureColumntype(String testAgainst, DatabaseMetaData testDbMetaData) throws Exception {
		rs = testDbMetaData.getProcedureColumns(null, null, "bug68307_%", "%");

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
}
