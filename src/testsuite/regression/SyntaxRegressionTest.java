/*
 Copyright (c) 2013, Oracle and/or its affiliates. All rights reserved.
 

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

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import testsuite.BaseTestCase;

/**
 * Regression tests for syntax
 * 
 * @author Alexander Soklakov
 */
public class SyntaxRegressionTest extends BaseTestCase {

	public SyntaxRegressionTest(String name) {
		super(name);
	}

	/**
	 * ALTER TABLE syntax changed in 5.6GA
	 * 
	 * ALTER TABLE ... , algorithm, concurrency
	 * 
	 * algorithm:
	 *    | ALGORITHM [=] DEFAULT
	 *    | ALGORITHM [=] INPLACE
	 *    | ALGORITHM [=] COPY
	 *    
	 * concurrency:
	 *    | LOCK [=] DEFAULT
	 *    | LOCK [=] NONE
	 *    | LOCK [=] SHARED
	 *    | LOCK [=] EXCLUSIVE
	 * 
	 * @throws SQLException
	 */
	public void testAlterTableAlgorithmLock() throws SQLException {
		if (versionMeetsMinimum(5, 6, 6)) {
			
			Connection c = null;
			Properties props = new Properties();
			props.setProperty("useServerPrepStmts", "true");

			try {
				c = getConnectionWithProps(props);

				String[] algs = {
						"",
						", ALGORITHM DEFAULT", ", ALGORITHM = DEFAULT",
						", ALGORITHM INPLACE", ", ALGORITHM = INPLACE",
						", ALGORITHM COPY", ", ALGORITHM = COPY"
						};
		
				String[] lcks = {
						"",
						", LOCK DEFAULT", ", LOCK = DEFAULT",
						", LOCK NONE", ", LOCK = NONE",
						", LOCK SHARED", ", LOCK = SHARED",
						", LOCK EXCLUSIVE", ", LOCK = EXCLUSIVE"
						};
		
				createTable("testAlterTableAlgorithmLock", "(x VARCHAR(10) NOT NULL DEFAULT '') CHARSET=latin2");
				
				int i = 1;
				for (String alg : algs) {
					for (String lck : lcks) {
						i = i ^ 1;

						// TODO: 5.6.10 reports: "LOCK=NONE is not supported. Reason: COPY algorithm requires a lock. Try LOCK=SHARED."
						//       We should check if situation change in future
						if (!(lck.contains("NONE") && alg.contains("COPY"))) {

							String sql = "ALTER TABLE testAlterTableAlgorithmLock CHARSET=latin"+(i + 1) + alg + lck;
							this.stmt.executeUpdate(sql);
		
							this.pstmt = this.conn.prepareStatement("ALTER TABLE testAlterTableAlgorithmLock CHARSET=?" + alg + lck);
							assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
		
							this.pstmt = c.prepareStatement(sql);
							assertTrue(this.pstmt instanceof com.mysql.jdbc.ServerPreparedStatement);
						}
					}
				}
			
			} finally {
				if (c != null) {
					c.close();
				}
			}
		}
	}

	/**
	 * CREATE TABLE syntax changed in 5.6GA
	 * 
	 * InnoDB: Allow the location of file-per-table tablespaces to be chosen
	 *   CREATE TABLE ... DATA DIRECTORY = 'absolute/path/to/directory/'
	 *
	 * @throws SQLException
	 */
	public void testCreateTableDataDirectory() throws SQLException {

		if (versionMeetsMinimum(5, 6, 6)) {
			try {
				String tmpdir = null;
				this.rs = this.stmt.executeQuery("SHOW VARIABLES WHERE Variable_name='tmpdir' or Variable_name='innodb_file_per_table'");
				while (this.rs.next()) {
					if ("tmpdir".equals(this.rs.getString(1))) {
						tmpdir = this.rs.getString(2);
						if (tmpdir.endsWith(File.separator)) {
							tmpdir = tmpdir.substring(0, tmpdir.length()-1);
						}
					} else if ("innodb_file_per_table".equals(this.rs.getString(1))) {
						if (!this.rs.getString(2).equals("ON")) {
							fail("You need to set innodb_file_per_table to ON before running this test!");
						}
					}
				}

				createTable("testCreateTableDataDirectorya", "(x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + "'");
				createTable("testCreateTableDataDirectoryb", "(x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + File.separator + "'");
				this.stmt.executeUpdate("CREATE TEMPORARY TABLE testCreateTableDataDirectoryc (x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + "'");
				createTable("testCreateTableDataDirectoryd", "(x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + File.separator + "' INDEX DIRECTORY = '" + tmpdir + "'");
				this.stmt.executeUpdate("ALTER TABLE testCreateTableDataDirectorya DISCARD TABLESPACE");

				this.pstmt = this.conn.prepareStatement("CREATE TABLE testCreateTableDataDirectorya (x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + "'");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);

				this.pstmt = this.conn.prepareStatement("CREATE TABLE testCreateTableDataDirectorya (x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + File.separator + "'");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);

				this.pstmt = this.conn.prepareStatement("CREATE TEMPORARY TABLE testCreateTableDataDirectorya (x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + "'");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);

				this.pstmt = this.conn.prepareStatement("CREATE TABLE testCreateTableDataDirectorya (x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + "' INDEX DIRECTORY = '" + tmpdir + "'");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);

				this.pstmt = this.conn.prepareStatement("ALTER TABLE testCreateTableDataDirectorya DISCARD TABLESPACE");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);

			} finally {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS testCreateTableDataDirectoryc");
			}


		}
	}


}
