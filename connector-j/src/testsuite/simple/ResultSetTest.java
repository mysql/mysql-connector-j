/*
 Copyright (C) 2002-2007 MySQL AB

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
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Properties;


import testsuite.BaseTestCase;

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
		Connection paddedConn = null;

		createTable("testPadding", "(field1 CHAR(255), ord INT)");
		
		this.stmt.executeUpdate("INSERT INTO testPadding VALUES ('', 1), ('abc', 2), (REPEAT('b', 255), 3)");
		
		try {
			Properties props = new Properties();
			props.setProperty("padCharsWithSpace", "true");
			
			paddedConn = getConnectionWithProps(props);
			
			String query = "SELECT field1 FROM testPadding ORDER by ord";
			
			this.rs = paddedConn.createStatement().executeQuery(query);
			
			while (this.rs.next()) {
				assertEquals(255, this.rs.getString(1).length());
			}
			
			this.rs = ((com.mysql.jdbc.Connection)paddedConn).clientPrepareStatement(query).executeQuery();
			
			while (this.rs.next()) {
				assertEquals(255, this.rs.getString(1).length());
			}
			
			if (versionMeetsMinimum(4, 1)) {
				this.rs = ((com.mysql.jdbc.Connection)paddedConn).serverPrepare(query).executeQuery();
				
				while (this.rs.next()) {
					assertEquals(255, this.rs.getString(1).length());
				}
			}	
			
			this.rs = this.stmt.executeQuery(query);

			while (this.rs.next()) {
				if (this.rs.getRow() != 3) {
					assertTrue(255 != this.rs.getString(1).length());
				} else {
					assertEquals(255, this.rs.getString(1).length());
				}
			}
			
			this.rs = ((com.mysql.jdbc.Connection)this.conn).clientPrepareStatement(query).executeQuery();
			
			while (this.rs.next()) {
				if (this.rs.getRow() != 3) {
					assertTrue(255 != this.rs.getString(1).length());
				} else {
					assertEquals(255, this.rs.getString(1).length());
				}
			}
			
			if (versionMeetsMinimum(4, 1)) {
				this.rs = ((com.mysql.jdbc.Connection)this.conn).serverPrepare(query).executeQuery();
				
				while (this.rs.next()) {
					if (this.rs.getRow() != 3) {
						assertTrue(255 != this.rs.getString(1).length());
					} else {
						assertEquals(255, this.rs.getString(1).length());
					}
				}
			}	
		} finally {
			closeMemberJDBCResources();
			
			if (paddedConn != null) {
				paddedConn.close();
			}
		}
	}
}
