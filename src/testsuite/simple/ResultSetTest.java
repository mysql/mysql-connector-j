/*
 Copyright  2002-2007 MySQL AB, 2008 Sun Microsystems

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
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;

import testsuite.BaseTestCase;

import com.mysql.jdbc.CharsetMapping;

public class ResultSetTest extends BaseTestCase {

	public ResultSetTest(String name) {
		super(name);
	}

	/**
	 * Runs all test cases in this test suite
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(ResultSetTest.class);
	}

	public void testPadding() throws Exception {
		if (!versionMeetsMinimum(4, 1, 0)) {
			return;
		}

		Connection paddedConn = null;

		int numChars = 32;

		Iterator charsetNames = CharsetMapping.STATIC_CHARSET_TO_NUM_BYTES_MAP
				.keySet().iterator();
		StringBuffer columns = new StringBuffer();
		StringBuffer emptyBuf = new StringBuffer();
		StringBuffer abcBuf = new StringBuffer();
		StringBuffer repeatBuf = new StringBuffer();
		StringBuffer selectBuf = new StringBuffer();

		int counter = 0;

		while (charsetNames.hasNext()) {
			String charsetName = charsetNames.next().toString();

			if (charsetName.equalsIgnoreCase("LATIN7")
					|| charsetName.equalsIgnoreCase("BINARY")) {
				continue; // no mapping in Java
			}

			if (counter != 0) {
				columns.append(",");
				emptyBuf.append(",");
				abcBuf.append(",");
				repeatBuf.append(",");
				selectBuf.append(",");
			}

			emptyBuf.append("''");
			abcBuf.append("'abc'");
			repeatBuf.append("REPEAT('b', " + numChars + ")");

			columns.append("field_");
			columns.append(charsetName);

			columns.append(" CHAR(");
			columns.append(numChars);
			columns.append(") CHARACTER SET ");
			columns.append(charsetName);

			selectBuf.append("field_");
			selectBuf.append(charsetName);

			counter++;
		}

		createTable("testPadding", "(" + columns.toString() + ", ord INT)");

		this.stmt.executeUpdate("INSERT INTO testPadding VALUES ("
				+ emptyBuf.toString() + ", 1), (" + abcBuf.toString()
				+ ", 2), (" + repeatBuf.toString() + ", 3)");

		try {
			Properties props = new Properties();
			props.setProperty("padCharsWithSpace", "true");

			paddedConn = getConnectionWithProps(props);

			testPaddingForConnection(paddedConn, numChars, selectBuf);

			props.setProperty("useDynamicCharsetInfo", "true");

			paddedConn = getConnectionWithProps(props);

			testPaddingForConnection(paddedConn, numChars, selectBuf);
		} finally {
			closeMemberJDBCResources();

			if (paddedConn != null) {
				paddedConn.close();
			}
		}
	}

	private void testPaddingForConnection(Connection paddedConn, int numChars,
			StringBuffer selectBuf) throws SQLException {

		String query = "SELECT " + selectBuf.toString()
				+ " FROM testPadding ORDER by ord";

		this.rs = paddedConn.createStatement().executeQuery(query);
		int numCols = this.rs.getMetaData().getColumnCount();

		while (this.rs.next()) {
			for (int i = 0; i < numCols; i++) {
				assertEquals("For column '"
						+ this.rs.getMetaData().getColumnName(i + 1)
						+ "' of collation "
						+ ((com.mysql.jdbc.ResultSetMetaData) this.rs
								.getMetaData()).getColumnCharacterSet(i + 1),
						numChars, this.rs.getString(i + 1).length());
			}
		}

		this.rs = ((com.mysql.jdbc.Connection) paddedConn)
				.clientPrepareStatement(query).executeQuery();

		while (this.rs.next()) {
			for (int i = 0; i < numCols; i++) {
				assertEquals("For column '"
						+ this.rs.getMetaData().getColumnName(i + 1)
						+ "' of collation "
						+ ((com.mysql.jdbc.ResultSetMetaData) this.rs
								.getMetaData()).getColumnCharacterSet(i + 1),
						numChars, this.rs.getString(i + 1).length());
			}
		}

		if (versionMeetsMinimum(4, 1)) {
			this.rs = ((com.mysql.jdbc.Connection) paddedConn).serverPrepareStatement(
					query).executeQuery();

			while (this.rs.next()) {
				for (int i = 0; i < numCols; i++) {
					assertEquals("For column '"
							+ this.rs.getMetaData().getColumnName(i + 1)
							+ "' of collation "
							+ ((com.mysql.jdbc.ResultSetMetaData) this.rs
									.getMetaData())
									.getColumnCharacterSet(i + 1), numChars,
							this.rs.getString(i + 1).length());
				}
			}
		}

		this.rs = this.stmt.executeQuery(query);

		while (this.rs.next()) {
			for (int i = 0; i < numCols; i++) {
				if (this.rs.getRow() != 3) {
					assertTrue("For column '"
							+ this.rs.getMetaData().getColumnName(i + 1)
							+ "' of collation "
							+ ((com.mysql.jdbc.ResultSetMetaData) this.rs
									.getMetaData())
									.getColumnCharacterSet(i + 1),
							numChars != this.rs.getString(i + 1).length());
				} else {
					assertEquals("For column '"
							+ this.rs.getMetaData().getColumnName(i + 1)
							+ "' of collation "
							+ ((com.mysql.jdbc.ResultSetMetaData) this.rs
									.getMetaData())
									.getColumnCharacterSet(i + 1), numChars,
							this.rs.getString(i + 1).length());
				}
			}
		}

		this.rs = ((com.mysql.jdbc.Connection) this.conn)
				.clientPrepareStatement(query).executeQuery();

		while (this.rs.next()) {
			for (int i = 0; i < numCols; i++) {
				if (this.rs.getRow() != 3) {
					assertTrue("For column '"
							+ this.rs.getMetaData().getColumnName(i + 1)
							+ "' of collation "
							+ ((com.mysql.jdbc.ResultSetMetaData) this.rs
									.getMetaData())
									.getColumnCharacterSet(i + 1),
							numChars != this.rs.getString(i + 1).length());
				} else {
					assertEquals("For column '"
							+ this.rs.getMetaData().getColumnName(i + 1)
							+ "' of collation "
							+ ((com.mysql.jdbc.ResultSetMetaData) this.rs
									.getMetaData())
									.getColumnCharacterSet(i + 1), numChars,
							this.rs.getString(i + 1).length());
				}
			}
		}

		if (versionMeetsMinimum(4, 1)) {
			this.rs = ((com.mysql.jdbc.Connection) this.conn).serverPrepareStatement(
					query).executeQuery();

			while (this.rs.next()) {
				for (int i = 0; i < numCols; i++) {
					if (this.rs.getRow() != 3) {
						assertTrue("For column '"
								+ this.rs.getMetaData().getColumnName(i + 1)
								+ "' of collation "
								+ ((com.mysql.jdbc.ResultSetMetaData) this.rs
										.getMetaData())
										.getColumnCharacterSet(i + 1),
								numChars != this.rs.getString(i + 1).length());
					} else {
						assertEquals("For column '"
								+ this.rs.getMetaData().getColumnName(i + 1)
								+ "' of collation "
								+ ((com.mysql.jdbc.ResultSetMetaData) this.rs
										.getMetaData())
										.getColumnCharacterSet(i + 1),
								numChars, this.rs.getString(i + 1).length());
					}
				}
			}
		}
	}
}
