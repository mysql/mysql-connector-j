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
package testsuite.regression;

import java.lang.reflect.Method;
import java.sql.ResultSet;

import javax.sql.RowSet;

import testsuite.BaseTestCase;

/**
 * Regression test cases for the ResultSet class.
 * 
 * @author Eric Herman
 */
public class CachedRowsetTest extends BaseTestCase {
	/**
	 * Creates a new CachedRowsetTest
	 * 
	 * @param name
	 *            the name of the test to run
	 */
	public CachedRowsetTest(String name) {
		super(name);
	}

	/**
	 * Runs all test cases in this test suite
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(CachedRowsetTest.class);
	}

	/**
	 * Tests fix for BUG#5188, CachedRowSet errors using PreparedStatement. Uses
	 * Sun's "com.sun.rowset.CachedRowSetImpl"
	 * 
	 * @throws Exception
	 */
	public void testBug5188() throws Exception {
		String implClass = "com.sun.rowset.CachedRowSetImpl";
		Class c;
		Method populate;
		try {
			c = Class.forName(implClass);
		} catch (ClassNotFoundException e) {
			System.out.println("skipping testBug5188. Requires: " + implClass);
			return;
		}
		populate = c.getMethod("populate", new Class[] { ResultSet.class });

		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5188");
			this.stmt.executeUpdate("CREATE TABLE testBug5188 "
					+ "(ID int NOT NULL AUTO_INCREMENT, "
					+ "datafield VARCHAR(64), " + "PRIMARY KEY(ID))");

			this.stmt.executeUpdate("INSERT INTO testBug5188(datafield) "
					+ "values('test data stuff !')");

			String sql = "SELECT * FROM testBug5188 where ID = ?";
			this.pstmt = this.conn.prepareStatement(sql);
			this.pstmt.setString(1, "1");
			this.rs = this.pstmt.executeQuery();

			// create a CachedRowSet and populate it
			RowSet cachedRowSet = (RowSet) c.newInstance();
			// cachedRowSet.populate(rs);
			populate.invoke(cachedRowSet, new Object[] { this.rs });

			// scroll through CachedRowSet ...
			assertTrue(cachedRowSet.next());
			assertEquals("1", cachedRowSet.getString("ID"));
			assertEquals("test data stuff !", cachedRowSet
					.getString("datafield"));
			assertFalse(cachedRowSet.next());
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5188");
		}
	}
}