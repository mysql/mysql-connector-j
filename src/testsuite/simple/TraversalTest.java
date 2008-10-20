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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import testsuite.BaseTestCase;

/**
 * Tests result set traversal methods.
 * 
 * @author Mark Matthews
 * @version $Id$
 */
public class TraversalTest extends BaseTestCase {

	// ~ Constructors ..........................................................

	/**
	 * Creates a new TraversalTest object.
	 * 
	 * @param name
	 *            DOCUMENT ME!
	 */
	public TraversalTest(String name) {
		super(name);
	}

	// ~ Methods ...............................................................

	/**
	 * Runs all test cases in this test suite
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(TraversalTest.class);
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
	public void testTraversal() throws SQLException {

		Statement scrollableStmt = null;

		try {
			scrollableStmt = this.conn
					.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_READ_ONLY);
			this.rs = scrollableStmt
					.executeQuery("SELECT * FROM TRAVERSAL ORDER BY pos");

			// Test isFirst()
			if (this.rs.first()) {
				assertTrue("ResultSet.isFirst() failed", this.rs.isFirst());
				this.rs.relative(-1);
				assertTrue("ResultSet.isBeforeFirst() failed", this.rs
						.isBeforeFirst());
			}

			// Test isLast()
			if (this.rs.last()) {
				assertTrue("ResultSet.isLast() failed", this.rs.isLast());
				this.rs.relative(1);
				assertTrue("ResultSet.isAfterLast() failed", this.rs
						.isAfterLast());
			}

			int count = 0;
			this.rs.beforeFirst();

			boolean forwardOk = true;

			while (this.rs.next()) {

				int pos = this.rs.getInt("POS");

				// test case-sensitive column names
				pos = this.rs.getInt("pos");
				pos = this.rs.getInt("Pos");
				pos = this.rs.getInt("POs");
				pos = this.rs.getInt("PoS");
				pos = this.rs.getInt("pOS");
				pos = this.rs.getInt("pOs");
				pos = this.rs.getInt("poS");

				if (pos != count) {
					forwardOk = false;
				}

				assertTrue("ResultSet.getRow() failed.", pos == (this.rs
						.getRow() - 1));

				count++;

			}

			assertTrue("Only traversed " + count + " / 100 rows", forwardOk);

			boolean isAfterLast = this.rs.isAfterLast();
			assertTrue("ResultSet.isAfterLast() failed", isAfterLast);
			this.rs.afterLast();

			// Scroll backwards
			count = 99;

			boolean reverseOk = true;

			while (this.rs.previous()) {

				int pos = this.rs.getInt("pos");

				if (pos != count) {
					reverseOk = false;
				}

				count--;
			}

			assertTrue("ResultSet.previous() failed", reverseOk);

			boolean isBeforeFirst = this.rs.isBeforeFirst();
			assertTrue("ResultSet.isBeforeFirst() failed", isBeforeFirst);

			this.rs.next();
			boolean isFirst = this.rs.isFirst();
			assertTrue("ResultSet.isFirst() failed", isFirst);

			// Test absolute positioning
			this.rs.absolute(50);
			int pos = this.rs.getInt("pos");
			assertTrue("ResultSet.absolute() failed", pos == 49);

			// Test relative positioning
			this.rs.relative(-1);
			pos = this.rs.getInt("pos");
			assertTrue("ResultSet.relative(-1) failed", pos == 48);

			// Test bogus absolute index
			boolean onResultSet = this.rs.absolute(200);
			assertTrue("ResultSet.absolute() to point off result set failed",
					onResultSet == false);
			onResultSet = this.rs.absolute(100);
			assertTrue(
					"ResultSet.absolute() from off this.rs to on this.rs failed",
					onResultSet);

			onResultSet = this.rs.absolute(-99);
			assertTrue("ResultSet.absolute(-99) failed", onResultSet);
			assertTrue("ResultSet absolute(-99) failed", this.rs.getInt(1) == 1);
		} finally {

			if (scrollableStmt != null) {

				try {
					scrollableStmt.close();
				} catch (SQLException sqlEx) {
					;
				}
			}
		}
	}

	private void createTestTable() throws SQLException {

		//
		// Catch the error, the table might exist
		//
		try {
			this.stmt.executeUpdate("DROP TABLE TRAVERSAL");
		} catch (SQLException SQLE) {
			;
		}

		this.stmt
				.executeUpdate("CREATE TABLE TRAVERSAL (pos int PRIMARY KEY, stringdata CHAR(32))");

		for (int i = 0; i < 100; i++) {
			this.stmt.executeUpdate("INSERT INTO TRAVERSAL VALUES (" + i
					+ ", 'StringData')");
		}
	}
}