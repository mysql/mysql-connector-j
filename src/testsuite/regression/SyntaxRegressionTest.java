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

package testsuite.regression;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;

import testsuite.BaseTestCase;

import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.StringUtils;

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
				String separator = File.separatorChar == '\\' ? File.separator+File.separator : File.separator;
				this.rs = this.stmt.executeQuery("SHOW VARIABLES WHERE Variable_name='tmpdir' or Variable_name='innodb_file_per_table'");
				while (this.rs.next()) {
					if ("tmpdir".equals(this.rs.getString(1))) {
						tmpdir = this.rs.getString(2);
						if (tmpdir.endsWith(File.separator)) {
							tmpdir = tmpdir.substring(0, tmpdir.length()-1);
						}
						if (File.separatorChar == '\\') {
							tmpdir = StringUtils.escapeQuote(tmpdir, File.separator);
						}
					} else if ("innodb_file_per_table".equals(this.rs.getString(1))) {
						if (!this.rs.getString(2).equals("ON")) {
							fail("You need to set innodb_file_per_table to ON before running this test!");
						}
					}
				}

				createTable("testCreateTableDataDirectorya", "(x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + "'");
				createTable("testCreateTableDataDirectoryb", "(x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + separator + "'");
				this.stmt.executeUpdate("CREATE TEMPORARY TABLE testCreateTableDataDirectoryc (x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + "'");
				createTable("testCreateTableDataDirectoryd", "(x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + separator + "' INDEX DIRECTORY = '" + tmpdir + "'");
				this.stmt.executeUpdate("ALTER TABLE testCreateTableDataDirectorya DISCARD TABLESPACE");

				this.pstmt = this.conn.prepareStatement("CREATE TABLE testCreateTableDataDirectorya (x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + "'");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);

				this.pstmt = this.conn.prepareStatement("CREATE TABLE testCreateTableDataDirectorya (x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + separator + "'");
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

	/**
	 * Test case for transportable tablespaces syntax support:
	 * 
	 *    FLUSH TABLES ... FOR EXPORT
	 *    ALTER TABLE ... DISCARD TABLESPACE
	 *    ALTER TABLE ... IMPORT TABLESPACE
	 * 
	 * @throws SQLException
	 */
	public void testTransportableTablespaces() throws Exception {

		if (versionMeetsMinimum(5, 6, 8)) {
			String tmpdir = null;
			String uuid = null;
			this.rs = this.stmt.executeQuery("SHOW VARIABLES WHERE Variable_name='tmpdir' or Variable_name='innodb_file_per_table' or Variable_name='server_uuid'");
			while (this.rs.next()) {
				if ("tmpdir".equals(this.rs.getString(1))) {
					tmpdir = this.rs.getString(2);
					if (tmpdir.endsWith(File.separator)) {
						tmpdir = tmpdir.substring(0, tmpdir.length()-File.separator.length());
					}
				} else if ("innodb_file_per_table".equals(this.rs.getString(1))) {
					if (!this.rs.getString(2).equals("ON")) {
						fail("You need to set innodb_file_per_table to ON before running this test!");
					}
				} else if ("server_uuid".equals(this.rs.getString(1))) {
					uuid = this.rs.getString(2);
				}
			}
			
			if (uuid != null) {
				tmpdir = tmpdir + File.separator + uuid;
			}

			if (File.separatorChar == '\\') {
				tmpdir = StringUtils.escapeQuote(tmpdir, File.separator);
			}
			
			Properties props = new NonRegisteringDriver().parseURL(dbUrl, null);
			String dbname = props.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
			if (dbname == null) assertTrue("No database selected", false);

			File checkTableSpaceFile1 = new File(tmpdir + File.separator + dbname + File.separator + "testTransportableTablespaces1.ibd");
			if (checkTableSpaceFile1.exists()) {
				checkTableSpaceFile1.delete();
			}

			File checkTableSpaceFile2 = new File(tmpdir + File.separator + dbname + File.separator + "testTransportableTablespaces2.ibd");
			if (checkTableSpaceFile2.exists()) {
				checkTableSpaceFile2.delete();
			}

			createTable("testTransportableTablespaces1", "(x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + "'");
			createTable("testTransportableTablespaces2", "(x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + "'");
			this.stmt.executeUpdate("FLUSH TABLES testTransportableTablespaces1, testTransportableTablespaces2 FOR EXPORT");
			this.stmt.executeUpdate("UNLOCK TABLES");

			File tempFile = File.createTempFile("testTransportableTablespaces1", "tmp");
			tempFile.deleteOnExit();

			String tableSpacePath = tmpdir + File.separator + dbname + File.separator + "testTransportableTablespaces1.ibd";
			File tableSpaceFile = new File(tableSpacePath);

			copyFile(tableSpaceFile, tempFile);
			this.stmt.executeUpdate("ALTER TABLE testTransportableTablespaces1 DISCARD TABLESPACE");

			tableSpaceFile = new File(tableSpacePath);
			copyFile(tempFile, tableSpaceFile);
			
			this.stmt.executeUpdate("ALTER TABLE testTransportableTablespaces1 IMPORT TABLESPACE");

			this.pstmt = this.conn.prepareStatement("FLUSH TABLES testTransportableTablespaces1, testTransportableTablespaces2 FOR EXPORT");
			assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);

			this.pstmt = this.conn.prepareStatement("ALTER TABLE testTransportableTablespaces1 DISCARD TABLESPACE");
			assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);

			this.pstmt = this.conn.prepareStatement("ALTER TABLE testTransportableTablespaces1 IMPORT TABLESPACE");
			assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);

		}
	}

	private void copyFile(File source, File dest) throws IOException {
        FileInputStream is = null;
        FileOutputStream os = null;
        try {
            is = new FileInputStream(source);
            os = new FileOutputStream(dest);
            int nLength;
            byte[] buf = new byte[8000];
            while (true) {
                nLength = is.read(buf);
                if (nLength < 0) {
                    break;
                }
                os.write(buf, 0, nLength);
            }

        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (Exception ex) {
                }
            }
            if (os != null) {
                try {
                    os.close();
                } catch (Exception ex) {
                }
            }
        }
    }

	/**
	 * Test case for ALTER [IGNORE] TABLE t1 EXCHANGE PARTITION p1 WITH TABLE t2 syntax
	 * 
	 * @throws SQLException
	 */
	public void testExchangePartition() throws Exception {

		if (versionMeetsMinimum(5, 6, 6)) {
			createTable("testExchangePartition1",
				"(id int(11) NOT NULL AUTO_INCREMENT," +
				" year year(2) DEFAULT NULL," +
				" modified timestamp NOT NULL DEFAULT '0000-00-00 00:00:00'," +
				" PRIMARY KEY (id))" +
				" ENGINE=InnoDB ROW_FORMAT=COMPACT" +
				" PARTITION BY HASH (id)" +
				" PARTITIONS 2");
			createTable("testExchangePartition2", "LIKE testExchangePartition1");
			this.stmt.executeUpdate("ALTER TABLE testExchangePartition2 REMOVE PARTITIONING");
			this.stmt.executeUpdate("ALTER IGNORE TABLE testExchangePartition1 EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2");

			this.pstmt = this.conn.prepareStatement("ALTER IGNORE TABLE testExchangePartition1 EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2");
			assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
			
		}
	}

	/**
	 * Test for explicit partition selection syntax
	 * 
	 * @throws SQLException
	 */
	public void testExplicitPartitions() throws Exception {

		if (versionMeetsMinimum(5, 6, 5)) {
			Connection c = null;
			String datadir = null;
			Properties props = new NonRegisteringDriver().parseURL(dbUrl, null);
			String dbname = props.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
			
			props = new Properties();
			props.setProperty("useServerPrepStmts", "true");
			try {

				this.stmt.executeUpdate("SET @old_default_storage_engine = @@default_storage_engine");
				this.stmt.executeUpdate("SET @@default_storage_engine = 'InnoDB'");
				
				c = getConnectionWithProps(props);
				
				createTable("testExplicitPartitions",
						"(a INT NOT NULL," +
						" b varchar (64)," +
						" INDEX (b,a)," +
						" PRIMARY KEY (a))" +
						" ENGINE = InnoDB" +
						" PARTITION BY RANGE (a)" +
						" SUBPARTITION BY HASH (a) SUBPARTITIONS 2" +
						" (PARTITION pNeg VALUES LESS THAN (0) (SUBPARTITION subp0, SUBPARTITION subp1)," +
						" PARTITION `p0-9` VALUES LESS THAN (10) (SUBPARTITION subp2, SUBPARTITION subp3)," +
						" PARTITION `p10-99` VALUES LESS THAN (100) (SUBPARTITION subp4, SUBPARTITION subp5)," +
						" PARTITION `p100-99999` VALUES LESS THAN (100000) (SUBPARTITION subp6, SUBPARTITION subp7))");

				this.stmt.executeUpdate("INSERT INTO testExplicitPartitions PARTITION (pNeg, pNeg) VALUES (-1, \"pNeg(-subp1)\")");

				this.pstmt = this.conn.prepareStatement("INSERT INTO testExplicitPartitions PARTITION (pNeg, subp0) VALUES (-3, \"pNeg(-subp1)\")");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.pstmt.execute();

				this.pstmt = c.prepareStatement("INSERT INTO testExplicitPartitions PARTITION (pNeg, subp0) VALUES (-2, \"(pNeg-)subp0\")");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.ServerPreparedStatement);
				this.pstmt.execute();

				this.pstmt = c.prepareStatement("INSERT INTO testExplicitPartitions PARTITION (`p100-99999`) VALUES (100, \"`p100-99999`(-subp6)\"), (101, \"`p100-99999`(-subp7)\"), (1000, \"`p100-99999`(-subp6)\")");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.ServerPreparedStatement);
				this.pstmt.execute();

				this.stmt.executeUpdate("INSERT INTO testExplicitPartitions PARTITION(`p10-99`,subp3) VALUES (1, \"subp3\"), (10, \"p10-99\")");
				this.stmt.executeUpdate("INSERT INTO testExplicitPartitions PARTITION(subp3) VALUES (3, \"subp3\")");
				this.stmt.executeUpdate("INSERT INTO testExplicitPartitions PARTITION(`p0-9`) VALUES (5, \"p0-9:subp3\")");
				
				this.stmt.executeUpdate("FLUSH STATUS");
				this.stmt.executeQuery("SELECT * FROM testExplicitPartitions PARTITION (subp2)");

				this.pstmt = this.conn.prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (subp2,pNeg) AS TableAlias");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.pstmt = c.prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (subp2,pNeg) AS TableAlias");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.ServerPreparedStatement);

				this.pstmt = this.conn.prepareStatement("LOCK TABLE testExplicitPartitions READ, testExplicitPartitions as TableAlias READ");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.pstmt = c.prepareStatement("LOCK TABLE testExplicitPartitions READ, testExplicitPartitions as TableAlias READ");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);

				this.pstmt = this.conn.prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (subp3) AS TableAlias");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.pstmt.execute();
				this.pstmt = c.prepareStatement("SELECT COUNT(*) FROM testExplicitPartitions PARTITION (`p10-99`)");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.ServerPreparedStatement);
				this.pstmt.execute();

				this.pstmt = this.conn.prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (pNeg) WHERE a = 100");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.pstmt.execute();
				this.pstmt = c.prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (pNeg) WHERE a = 100");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.ServerPreparedStatement);
				this.pstmt.execute();

				this.stmt.executeUpdate("UNLOCK TABLES");


				// Test LOAD
				this.rs = this.stmt.executeQuery("SHOW VARIABLES WHERE Variable_name='datadir'");
				this.rs.next();
				datadir = this.rs.getString(2);

				if (dbname == null) {
					fail("No database selected");
				} else {
					File f = new File(datadir + dbname + File.separator + "loadtestExplicitPartitions.txt");
					if (f.exists()) {
						f.delete();
					}
				}

				this.pstmt = this.conn.prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (pNeg, `p10-99`) INTO OUTFILE 'loadtestExplicitPartitions.txt'");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.pstmt = c.prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (pNeg, `p10-99`) INTO OUTFILE 'loadtestExplicitPartitions.txt'");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.ServerPreparedStatement);
				this.stmt.executeQuery("SELECT * FROM testExplicitPartitions PARTITION (pNeg, `p10-99`) INTO OUTFILE 'loadtestExplicitPartitions.txt'");

				this.pstmt = this.conn.prepareStatement("ALTER TABLE testExplicitPartitions TRUNCATE PARTITION pNeg, `p10-99`");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.pstmt = c.prepareStatement("ALTER TABLE testExplicitPartitions TRUNCATE PARTITION pNeg, `p10-99`");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.ServerPreparedStatement);
				this.stmt.executeUpdate("ALTER TABLE testExplicitPartitions TRUNCATE PARTITION pNeg, `p10-99`");
				this.stmt.executeUpdate("FLUSH STATUS");

				this.pstmt = this.conn.prepareStatement("LOAD DATA INFILE 'loadtestExplicitPartitions.txt' INTO TABLE testExplicitPartitions PARTITION (pNeg, subp4, subp5)");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.pstmt = c.prepareStatement("LOAD DATA INFILE 'loadtestExplicitPartitions.txt' INTO TABLE testExplicitPartitions PARTITION (pNeg, subp4, subp5)");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.stmt.executeUpdate("LOAD DATA INFILE 'loadtestExplicitPartitions.txt' INTO TABLE testExplicitPartitions PARTITION (pNeg, subp4, subp5)");

				this.stmt.executeUpdate("ALTER TABLE testExplicitPartitions TRUNCATE PARTITION pNeg, `p10-99`");
				this.stmt.executeUpdate("FLUSH STATUS");
				this.pstmt = this.conn.prepareStatement("LOAD DATA INFILE 'loadtestExplicitPartitions.txt' INTO TABLE testExplicitPartitions PARTITION (pNeg, `p10-99`)");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.pstmt = c.prepareStatement("LOAD DATA INFILE 'loadtestExplicitPartitions.txt' INTO TABLE testExplicitPartitions PARTITION (pNeg, `p10-99`)");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.stmt.executeUpdate("LOCK TABLE testExplicitPartitions WRITE");
				this.stmt.executeUpdate("LOAD DATA INFILE 'loadtestExplicitPartitions.txt' INTO TABLE testExplicitPartitions PARTITION (pNeg, `p10-99`)");
				this.stmt.executeUpdate("UNLOCK TABLES");
				
				// Test UPDATE
				this.stmt.executeUpdate("UPDATE testExplicitPartitions PARTITION(subp0) SET b = concat(b, ', Updated')");

				this.pstmt = this.conn.prepareStatement("UPDATE testExplicitPartitions PARTITION(subp0) SET b = concat(b, ', Updated2') WHERE a = -2");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.pstmt.execute();

				this.pstmt = c.prepareStatement("UPDATE testExplicitPartitions PARTITION(subp0) SET a = -4, b = concat(b, ', Updated from a = -2') WHERE a = -2");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.ServerPreparedStatement);
				this.pstmt.execute();

				this.stmt.executeUpdate("UPDATE testExplicitPartitions PARTITION(subp0) SET b = concat(b, ', Updated2') WHERE a = 100");
				this.stmt.executeUpdate("UPDATE testExplicitPartitions PARTITION(subp0) SET a = -2, b = concat(b, ', Updated from a = 100') WHERE a = 100");

				this.pstmt = this.conn.prepareStatement("UPDATE testExplicitPartitions PARTITION(`p100-99999`, pNeg) SET a = -222, b = concat(b, ', Updated from a = 100') WHERE a = 100");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.pstmt.execute();

				this.pstmt = c.prepareStatement("UPDATE testExplicitPartitions SET b = concat(b, ', Updated2') WHERE a = 1000000");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.ServerPreparedStatement);
				this.pstmt.execute();

				// Test DELETE
				this.stmt.executeUpdate("DELETE FROM testExplicitPartitions PARTITION (pNeg) WHERE a = -1");
				this.pstmt = this.conn.prepareStatement("DELETE FROM testExplicitPartitions PARTITION (pNeg) WHERE a = -1");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.pstmt.execute();
				this.pstmt = c.prepareStatement("DELETE FROM testExplicitPartitions PARTITION (pNeg) WHERE a = -1");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.ServerPreparedStatement);
				this.pstmt.execute();

				this.stmt.executeUpdate("DELETE FROM testExplicitPartitions PARTITION (subp1) WHERE b like '%subp1%'");
				this.pstmt = this.conn.prepareStatement("DELETE FROM testExplicitPartitions PARTITION (subp1) WHERE b like '%subp1%'");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.pstmt.execute();
				this.pstmt = c.prepareStatement("DELETE FROM testExplicitPartitions PARTITION (subp1) WHERE b like '%subp1%'");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.ServerPreparedStatement);
				this.pstmt.execute();

				this.stmt.executeUpdate("FLUSH STATUS");
				this.stmt.executeUpdate("LOCK TABLE testExplicitPartitions WRITE");
				this.stmt.executeUpdate("DELETE FROM testExplicitPartitions PARTITION (subp1) WHERE b = 'p0-9:subp3'");
				this.pstmt = this.conn.prepareStatement("DELETE FROM testExplicitPartitions PARTITION (subp1) WHERE b = 'p0-9:subp3'");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.stmt.executeUpdate("DELETE FROM testExplicitPartitions PARTITION (`p0-9`) WHERE b = 'p0-9:subp3'");
				this.pstmt = this.conn.prepareStatement("DELETE FROM testExplicitPartitions PARTITION (`p0-9`) WHERE b = 'p0-9:subp3'");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.stmt.executeUpdate("UNLOCK TABLES");

				
				// Test multi-table DELETE
				this.stmt.executeUpdate("CREATE TABLE testExplicitPartitions2 LIKE testExplicitPartitions");

				this.pstmt = this.conn.prepareStatement("INSERT INTO testExplicitPartitions2 PARTITION (`p10-99`, subp3, `p100-99999`) SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.pstmt = c.prepareStatement("INSERT INTO testExplicitPartitions2 PARTITION (`p10-99`, subp3, `p100-99999`) SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.ServerPreparedStatement);
				this.stmt.executeUpdate("INSERT INTO testExplicitPartitions2 PARTITION (`p10-99`, subp3, `p100-99999`) SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");

				this.stmt.executeUpdate("ALTER TABLE testExplicitPartitions2 TRUNCATE PARTITION `p10-99`, `p0-9`, `p100-99999`");

				this.pstmt = this.conn.prepareStatement("INSERT IGNORE INTO testExplicitPartitions2 PARTITION (subp3) SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.pstmt = c.prepareStatement("INSERT IGNORE INTO testExplicitPartitions2 PARTITION (subp3) SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.ServerPreparedStatement);
				this.stmt.executeUpdate("INSERT IGNORE INTO testExplicitPartitions2 PARTITION (subp3) SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");

				this.stmt.executeUpdate("TRUNCATE TABLE testExplicitPartitions2");
				this.stmt.executeUpdate("INSERT INTO testExplicitPartitions2 SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");

				this.pstmt = this.conn.prepareStatement("CREATE TABLE testExplicitPartitions3 SELECT * FROM testExplicitPartitions PARTITION (pNeg,subp3,`p100-99999`)");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.pstmt = c.prepareStatement("CREATE TABLE testExplicitPartitions3 SELECT * FROM testExplicitPartitions PARTITION (pNeg,subp3,`p100-99999`)");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.stmt.executeUpdate("CREATE TABLE testExplicitPartitions3 SELECT * FROM testExplicitPartitions PARTITION (pNeg,subp3,`p100-99999`)");

				this.pstmt = this.conn.prepareStatement("DELETE testExplicitPartitions, testExplicitPartitions2 FROM testExplicitPartitions PARTITION (pNeg), testExplicitPartitions3, testExplicitPartitions2 PARTITION (subp3) WHERE testExplicitPartitions.a = testExplicitPartitions3.a AND testExplicitPartitions3.b = 'subp3' AND testExplicitPartitions3.a = testExplicitPartitions2.a");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.pstmt = c.prepareStatement("DELETE testExplicitPartitions, testExplicitPartitions2 FROM testExplicitPartitions PARTITION (pNeg), testExplicitPartitions3, testExplicitPartitions2 PARTITION (subp3) WHERE testExplicitPartitions.a = testExplicitPartitions3.a AND testExplicitPartitions3.b = 'subp3' AND testExplicitPartitions3.a = testExplicitPartitions2.a");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.ServerPreparedStatement);
				this.stmt.executeUpdate("DELETE testExplicitPartitions, testExplicitPartitions2 FROM testExplicitPartitions PARTITION (pNeg), testExplicitPartitions3, testExplicitPartitions2 PARTITION (subp3) WHERE testExplicitPartitions.a = testExplicitPartitions3.a AND testExplicitPartitions3.b = 'subp3' AND testExplicitPartitions3.a = testExplicitPartitions2.a");

				this.pstmt = this.conn.prepareStatement("DELETE FROM testExplicitPartitions2, testExplicitPartitions3 USING testExplicitPartitions2 PARTITION (`p0-9`), testExplicitPartitions3, testExplicitPartitions PARTITION (subp3) WHERE testExplicitPartitions.a = testExplicitPartitions3.a AND testExplicitPartitions3.b = 'subp3' AND testExplicitPartitions2.a = testExplicitPartitions.a");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);
				this.pstmt = c.prepareStatement("DELETE FROM testExplicitPartitions2, testExplicitPartitions3 USING testExplicitPartitions2 PARTITION (`p0-9`), testExplicitPartitions3, testExplicitPartitions PARTITION (subp3) WHERE testExplicitPartitions.a = testExplicitPartitions3.a AND testExplicitPartitions3.b = 'subp3' AND testExplicitPartitions2.a = testExplicitPartitions.a");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.ServerPreparedStatement);
				this.stmt.executeUpdate("DELETE FROM testExplicitPartitions2, testExplicitPartitions3 USING testExplicitPartitions2 PARTITION (`p0-9`), testExplicitPartitions3, testExplicitPartitions PARTITION (subp3) WHERE testExplicitPartitions.a = testExplicitPartitions3.a AND testExplicitPartitions3.b = 'subp3' AND testExplicitPartitions2.a = testExplicitPartitions.a");

				this.stmt.executeUpdate("SET @@default_storage_engine = @old_default_storage_engine");

			} finally {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS testExplicitPartitions, testExplicitPartitions2, testExplicitPartitions3");
				
				if (c != null) {
					c.close();
				}
				if (datadir != null) {
					File f = new File(datadir + dbname + File.separator + "loadtestExplicitPartitions.txt");
					if (f.exists()) {
						f.deleteOnExit();
					} else {
						fail("File " + datadir + dbname + File.separator + "loadtestExplicitPartitions.txt cannot be deleted." +
								"You should run server and tests on the same filesystem.");
					}
				}
			}
		}
	}

	/**
	 * WL#1326 - GIS: Precise spatial operations
	 * 
	 * GIS functions added in 5.6GA: ST_Intersection(g1 geometry, g2 geometry); ST_Difference(g1 geometry, g2 geometry);
	 * ST_Union(g1 geometry, g2 geometry); ST_SymDifference(g1 geometry, g2 geometry); ST_Buffer(g1 geometry, d
	 * numeric).
	 * 
	 * @throws SQLException
	 */
	public void testGISPreciseSpatialFunctions() throws Exception {

		if (!versionMeetsMinimum(5, 6)) {
			return;
		}

		String[] querySamples = new String[] {
				"SELECT AsText(ST_Intersection(GeomFromText('POLYGON((0 0, 8 0, 4 6, 0 0))'), GeomFromText('POLYGON((0 3, 8 3, 4 9, 0 3))')))",
				"SELECT AsText(ST_Difference(GeomFromText('POLYGON((0 0, 8 0, 4 6, 0 0))'), GeomFromText('POLYGON((0 3, 8 3, 4 9, 0 3))')))",
				"SELECT AsText(ST_Union(GeomFromText('POLYGON((0 0, 8 0, 4 6, 0 0))'), GeomFromText('POLYGON((0 3, 8 3, 4 9, 0 3))')))",
				"SELECT AsText(ST_SymDifference(GeomFromText('POLYGON((0 0, 8 0, 4 6, 0 0))'), GeomFromText('POLYGON((0 3, 8 3, 4 9, 0 3))')))",
				"SELECT AsText(ST_Buffer(GeomFromText('POLYGON((0 0, 8 0, 4 6, 0 0))'), 0.5))",
				"SELECT ST_Distance(GeomFromText('POLYGON((0 0, 8 0, 4 6, 0 0))'), GeomFromText('POLYGON((0 10, 8 10, 4 16, 0 10))'))" };

		for (String query : querySamples) {
			this.rs = this.stmt.executeQuery(query);
			assertTrue("Query should return  at least one row.", this.rs.next());
			assertFalse("Query should return only one row.", this.rs.next());
			this.rs.close();
		}

	}
	
	/**
	 * WL#5787 - IPv6-capable INET_ATON and INET_NTOA functions
	 * 
	 * IPv6 functions added in 5.6GA: INET6_ATON(ip) and INET6_NTOA(ip).
	 * 
	 * @throws SQLException
	 */
	public void testIPv6Functions() throws Exception {

		if (!versionMeetsMinimum(5, 6, 11)) {
			// MySQL 5.6.11 includes a bug fix (Bug#68454) that is required to run this test successfully.
			return;
		}

		String[][] dataSamples = new String[][] {
				{ "127.0.0.1", "172.0.0.1" },
				{ "192.168.1.1", "::ffff:192.168.1.1" },
				{ "10.1", "::ffff:10.1" },
				{ "172.16.260.4", "172.16.260.4" },
				{ "::1", "::1" },
				{ "10AA:10bb:10CC:10dd:10EE:10FF:10aa:10BB", "10aa:10bb:10cc:10dd:10ee:10ff:10aa:10bb" },
				{ "00af:0000:0000:0000:10af:000a:000b:0001", "00af:0000:0000:0000:10af:000a:000b:0001" },
				{ "48:4df1::0010:ad3:1100", "48:4df1::0010:ad3:1100" },
				{ "2000:abcd:1234:0000:efgh:1000:2000:3000", "2000:abcd:1234:0000:efgh:1000:2000:3000" },
				{ "2000:abcd:1234:0000:1000:2000:3000", "2000:abcd:1234:0000:1000:2000:3000" }
		};
		String[][] dataExpected = new String[][] {
				{ "127.0.0.1", "172.0.0.1" },
				{ "192.168.1.1", "::ffff:192.168.1.1" },
				{ "10.0.0.1", null },
				{ null, null },
				{ null, "::1" },
				{ null, "10aa:10bb:10cc:10dd:10ee:10ff:10aa:10bb" },
				{ null, "af::10af:a:b:1" },
				{ null, "48:4df1::10:ad3:1100" },
				{ null, null },
				{ null, null }
		};

		createTable("testWL5787", "(id INT AUTO_INCREMENT PRIMARY KEY, ipv4 INT UNSIGNED, ipv6 VARBINARY(16))");

		this.pstmt = this.conn.prepareStatement("INSERT INTO testWL5787 VALUES (NULL, INET_ATON(?), INET6_ATON(?))");
		
		for (String[] data : dataSamples) {
			this.pstmt.setString(1, data[0]);
			this.pstmt.setString(2, data[1]);
			this.pstmt.addBatch();
		}
		int c = 0;
		for (int r : this.pstmt.executeBatch()) {
			c += r;
		}
		assertEquals("Failed inserting data samples: wrong number of inserts.", dataSamples.length, c);
	
		this.rs = this.stmt.executeQuery("SELECT id, INET_NTOA(ipv4), INET6_NTOA(ipv6) FROM testWL5787");
		int i = 0;
		while (this.rs.next()) {
			i = this.rs.getInt(1);
			assertEquals("Wrong IPv4 data in row [" + i + "].", dataExpected[i - 1][0], this.rs.getString(2));
			assertEquals("Wrong IPv6 data in row [" + i + "].", dataExpected[i - 1][1], this.rs.getString(3));
		}
	}

	/**
	 * WL#5538 - InnoDB Full-Text Search Support
	 * 
	 * CREATE TABLE syntax changed in 5.6GA
	 * 
	 * InnoDB engine accepts FULLTEXT indexes.
	 * CREATE TABLE ... FULLTEXT(...) ... ENGINE=InnoDB
	 * 
	 * @throws SQLException
	 */
	public void testFULLTEXTSearchInnoDB() throws Exception {

		if (!versionMeetsMinimum(5, 6)) {
			return;
		}

		createTable("testFULLTEXTSearchInnoDB", "(id INT UNSIGNED AUTO_INCREMENT NOT NULL PRIMARY KEY, "
				+ "title VARCHAR(200), body TEXT, FULLTEXT (title , body)) ENGINE=InnoDB");

		this.stmt.executeUpdate("INSERT INTO testFULLTEXTSearchInnoDB (title, body) VALUES "
				+ "('MySQL Tutorial','DBMS stands for DataBase ...'), "
				+ "('How To Use MySQL Well','After you went through a ...'), "
				+ "('Optimizing MySQL','In this tutorial we will show ...'), "
				+ "('1001 MySQL Tricks','1. Never run mysqld as root. 2. ...'), "
				+ "('MySQL vs. YourSQL','In the following database comparison ...'), "
				+ "('MySQL Security','When configured properly, MySQL ...')");

		String[] querySamples = new String[] {
				"SELECT * FROM testFULLTEXTSearchInnoDB WHERE MATCH (title, body) AGAINST ('database' IN NATURAL LANGUAGE MODE)",
				"SELECT * FROM testFULLTEXTSearchInnoDB WHERE MATCH (title, body) AGAINST ('database' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION)",
				"SELECT * FROM testFULLTEXTSearchInnoDB WHERE MATCH (title, body) AGAINST ('<MySQL >YourSQL' IN BOOLEAN MODE)",
				"SELECT * FROM testFULLTEXTSearchInnoDB WHERE MATCH (title, body) AGAINST ('+MySQL -YourSQL' IN BOOLEAN MODE)",
				"SELECT MATCH (title, body) AGAINST ('database' IN NATURAL LANGUAGE MODE) FROM testFULLTEXTSearchInnoDB",
				"SELECT MATCH (title, body) AGAINST ('database' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION) FROM testFULLTEXTSearchInnoDB",
				"SELECT MATCH (title, body) AGAINST ('<MySQL >YourSQL' IN BOOLEAN MODE) FROM testFULLTEXTSearchInnoDB",
				"SELECT MATCH (title, body) AGAINST ('+MySQL -YourSQL' IN BOOLEAN MODE) FROM testFULLTEXTSearchInnoDB" };

		for (String query : querySamples) {
			this.rs = this.stmt.executeQuery(query);
			assertTrue("Query [" + query + "] should return some rows.", this.rs.next());
			this.rs.close();
		}
	}

	/**
	 * WL#6555 - Online rename index
	 * 
	 * ALTER TABLE syntax changed in 5.7.1
	 * 
	 * Alter table allows to rename indexes. ALTER TABLE ... RENAME INDEX x TO y
	 * 
	 * @throws SQLException
	 */
	public void testRenameIndex() throws Exception {

		if (!versionMeetsMinimum(5, 7, 1)) {
			return;
		}

		createTable("testRenameIndex", "(col1 INT, col2 INT, INDEX (col1)) ENGINE=InnoDB");
		this.stmt.execute("CREATE INDEX testIdx ON testRenameIndex (col2)");

		DatabaseMetaData dbmd = this.conn.getMetaData();

		this.rs = dbmd.getIndexInfo(null, null, "testRenameIndex", false, true);
		assertTrue("Expected 1 (of 2) indexes.", this.rs.next());
		assertEquals("Wrong index name for table 'testRenameIndex'.", "col1", this.rs.getString(6));
		assertTrue("Expected 2 (of 2) indexes.", this.rs.next());
		assertEquals("Wrong index name for table 'testRenameIndex'.", "testIdx", this.rs.getString(6));
		assertFalse("No more indexes expected for table 'testRenameIndex'.", this.rs.next());

		this.stmt.execute("ALTER TABLE testRenameIndex RENAME INDEX col1 TO col1Index");
		this.stmt.execute("ALTER TABLE testRenameIndex RENAME INDEX testIdx TO testIndex");

		this.rs = dbmd.getIndexInfo(null, null, "testRenameIndex", false, true);
		assertTrue("Expected 1 (of 2) indexes.", this.rs.next());
		assertEquals("Wrong index name for table 'testRenameIndex'.", "col1Index", this.rs.getString(6));
		assertTrue("Expected 2 (of 2) indexes.", this.rs.next());
		assertEquals("Wrong index name for table 'testRenameIndex'.", "testIndex", this.rs.getString(6));
		assertFalse("No more indexes expected for table 'testRenameIndex'.", this.rs.next());
	}
}
