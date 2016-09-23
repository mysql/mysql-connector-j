/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
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
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;
import java.util.concurrent.Callable;

import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.util.StringUtils;

import testsuite.BaseTestCase;

/**
 * Regression tests for syntax
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
     * | ALGORITHM [=] DEFAULT
     * | ALGORITHM [=] INPLACE
     * | ALGORITHM [=] COPY
     * 
     * concurrency:
     * | LOCK [=] DEFAULT
     * | LOCK [=] NONE
     * | LOCK [=] SHARED
     * | LOCK [=] EXCLUSIVE
     * 
     * @throws SQLException
     */
    public void testAlterTableAlgorithmLock() throws SQLException {
        if (!versionMeetsMinimum(5, 6, 6)) {
            return;
        }
        Connection c = null;
        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useServerPrepStmts, "true");

        try {
            c = getConnectionWithProps(props);

            String[] algs = { "", ", ALGORITHM DEFAULT", ", ALGORITHM = DEFAULT", ", ALGORITHM INPLACE", ", ALGORITHM = INPLACE", ", ALGORITHM COPY",
                    ", ALGORITHM = COPY" };

            String[] lcks = { "", ", LOCK DEFAULT", ", LOCK = DEFAULT", ", LOCK NONE", ", LOCK = NONE", ", LOCK SHARED", ", LOCK = SHARED", ", LOCK EXCLUSIVE",
                    ", LOCK = EXCLUSIVE" };

            createTable("testAlterTableAlgorithmLock", "(x VARCHAR(10) NOT NULL DEFAULT '') CHARSET=latin2");

            int i = 1;
            for (String alg : algs) {
                for (String lck : lcks) {
                    i = i ^ 1;

                    // TODO: 5.7.5 reports: "LOCK=NONE is not supported. Reason: COPY algorithm requires a lock. Try LOCK=SHARED."
                    //       We should check if situation change in future
                    if (!(lck.contains("NONE") && alg.contains("COPY"))) {

                        String sql = "ALTER TABLE testAlterTableAlgorithmLock CHARSET=latin" + (i + 1) + alg + lck;
                        this.stmt.executeUpdate(sql);

                        this.pstmt = this.conn.prepareStatement("ALTER TABLE testAlterTableAlgorithmLock CHARSET=?" + alg + lck);
                        assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);

                        this.pstmt = c.prepareStatement(sql);
                        assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.ServerPreparedStatement);
                    }
                }
            }

        } finally {
            if (c != null) {
                c.close();
            }
        }
    }

    /**
     * CREATE TABLE syntax changed in 5.6GA
     * 
     * InnoDB: Allow the location of file-per-table tablespaces to be chosen
     * CREATE TABLE ... DATA DIRECTORY = 'absolute/path/to/directory/'
     * 
     * Notes:
     * - DATA DIRECTORY option can't be used with temporary tables.
     * - DATA DIRECTORY and INDEX DIRECTORY can't be used together for InnoDB.
     * - Using these options result in an 'option ignored' warning for servers below MySQL 5.7.7. This syntax isn't allowed for MySQL 5.7.7 and higher.
     * 
     * @throws SQLException
     */
    public void testCreateTableDataDirectory() throws SQLException {
        if (!versionMeetsMinimum(5, 6, 6)) {
            return;
        }

        try {
            String tmpdir = null;
            String separator = File.separatorChar == '\\' ? File.separator + File.separator : File.separator;
            this.rs = this.stmt.executeQuery("SHOW VARIABLES WHERE Variable_name='tmpdir' or Variable_name='innodb_file_per_table'");
            while (this.rs.next()) {
                if ("tmpdir".equals(this.rs.getString(1))) {
                    tmpdir = this.rs.getString(2);
                    if (tmpdir.endsWith(File.separator)) {
                        tmpdir = tmpdir.substring(0, tmpdir.length() - 1);
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

            dropTable("testCreateTableDataDirectorya");
            dropTable("testCreateTableDataDirectoryb");
            dropTable("testCreateTableDataDirectoryc");
            dropTable("testCreateTableDataDirectoryd");

            createTable("testCreateTableDataDirectorya", "(x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + "'");
            createTable("testCreateTableDataDirectoryb", "(x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + separator + "'");
            this.stmt.executeUpdate("CREATE TEMPORARY TABLE testCreateTableDataDirectoryc (x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir
                    + (versionMeetsMinimum(5, 7, 7) ? "' ENGINE = MyISAM" : "'"));
            createTable("testCreateTableDataDirectoryd", "(x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + separator + "' INDEX DIRECTORY = '"
                    + tmpdir + (versionMeetsMinimum(5, 7, 7) ? "' ENGINE = MyISAM" : "'"));
            this.stmt.executeUpdate("ALTER TABLE testCreateTableDataDirectorya DISCARD TABLESPACE");

            this.pstmt = this.conn
                    .prepareStatement("CREATE TABLE testCreateTableDataDirectorya (x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + "'");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);

            this.pstmt = this.conn.prepareStatement(
                    "CREATE TABLE testCreateTableDataDirectorya (x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + separator + "'");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);

            this.pstmt = this.conn.prepareStatement(
                    "CREATE TEMPORARY TABLE testCreateTableDataDirectorya (x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + "'");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);

            this.pstmt = this.conn.prepareStatement("CREATE TABLE testCreateTableDataDirectorya (x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir
                    + "' INDEX DIRECTORY = '" + tmpdir + "'");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);

            this.pstmt = this.conn.prepareStatement("ALTER TABLE testCreateTableDataDirectorya DISCARD TABLESPACE");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);

        } finally {
            // we need to drop them even if retainArtifacts=true, otherwise temp files could be deleted by OS and DB became corrupted
            dropTable("testCreateTableDataDirectorya");
            dropTable("testCreateTableDataDirectoryb");
            dropTable("testCreateTableDataDirectoryc");
            dropTable("testCreateTableDataDirectoryd");
        }

    }

    /**
     * Test case for transportable tablespaces syntax support:
     * 
     * FLUSH TABLES ... FOR EXPORT
     * ALTER TABLE ... DISCARD TABLESPACE
     * ALTER TABLE ... IMPORT TABLESPACE
     * 
     * @throws SQLException
     */
    public void testTransportableTablespaces() throws Exception {
        if (!versionMeetsMinimum(5, 6, 8)) {
            return;
        }

        String tmpdir = null;
        String uuid = null;
        this.rs = this.stmt.executeQuery("SHOW VARIABLES WHERE Variable_name='tmpdir' or Variable_name='innodb_file_per_table' or Variable_name='server_uuid'");
        while (this.rs.next()) {
            if ("tmpdir".equals(this.rs.getString(1))) {
                tmpdir = this.rs.getString(2);
                if (tmpdir.endsWith(File.separator)) {
                    tmpdir = tmpdir.substring(0, tmpdir.length() - File.separator.length());
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

        Properties props = getPropertiesFromTestsuiteUrl();
        String dbname = props.getProperty(PropertyDefinitions.DBNAME_PROPERTY_KEY);
        if (dbname == null) {
            assertTrue("No database selected", false);
        }

        dropTable("testTransportableTablespaces1");
        dropTable("testTransportableTablespaces2");

        File checkTableSpaceFile1 = new File(tmpdir + File.separator + dbname + File.separator + "testTransportableTablespaces1.ibd");
        if (checkTableSpaceFile1.exists()) {
            checkTableSpaceFile1.delete();
        }

        File checkTableSpaceFile2 = new File(tmpdir + File.separator + dbname + File.separator + "testTransportableTablespaces2.ibd");
        if (checkTableSpaceFile2.exists()) {
            checkTableSpaceFile2.delete();
        }

        try {
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
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);

            this.pstmt = this.conn.prepareStatement("ALTER TABLE testTransportableTablespaces1 DISCARD TABLESPACE");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);

            this.pstmt = this.conn.prepareStatement("ALTER TABLE testTransportableTablespaces1 IMPORT TABLESPACE");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);

        } finally {
            // we need to drop them even if retainArtifacts=true, otherwise temp files could be deleted by OS and DB became corrupted
            dropTable("testTransportableTablespaces1");
            dropTable("testTransportableTablespaces2");
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
        if (!versionMeetsMinimum(5, 6, 6)) {
            return;
        }
        createTable("testExchangePartition1", "(id int(11) NOT NULL AUTO_INCREMENT, year year(4) DEFAULT NULL,"
                + " modified timestamp NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB ROW_FORMAT=COMPACT PARTITION BY HASH (id) PARTITIONS 2");
        createTable("testExchangePartition2", "LIKE testExchangePartition1");

        this.stmt.executeUpdate("ALTER TABLE testExchangePartition2 REMOVE PARTITIONING");
        if (versionMeetsMinimum(5, 7, 4)) {
            this.stmt.executeUpdate("ALTER TABLE testExchangePartition1 EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2");
        } else {
            this.stmt.executeUpdate("ALTER IGNORE TABLE testExchangePartition1 EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2");
        }

        if (versionMeetsMinimum(5, 7, 4)) {
            this.pstmt = this.conn.prepareStatement("ALTER TABLE testExchangePartition1 EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2");
        } else {
            this.pstmt = this.conn.prepareStatement("ALTER IGNORE TABLE testExchangePartition1 " + "EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2");
        }
        assertEquals(com.mysql.cj.jdbc.PreparedStatement.class, this.pstmt.getClass());
        this.pstmt.executeUpdate();

        Connection testConn = null;
        try {
            testConn = getConnectionWithProps("useServerPrepStmts=true,emulateUnsupportedPstmts=false");
            if (versionMeetsMinimum(5, 7, 4)) {
                this.pstmt = testConn.prepareStatement("ALTER TABLE testExchangePartition1 EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2");
            } else {
                this.pstmt = testConn
                        .prepareStatement("ALTER IGNORE TABLE testExchangePartition1 " + "EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2");

            }
            assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement.class, this.pstmt.getClass());
            this.pstmt.executeUpdate();
        } finally {
            if (testConn != null) {
                testConn.close();
            }
        }
    }

    /**
     * Test for explicit partition selection syntax
     * 
     * @throws SQLException
     */
    public void testExplicitPartitions() throws Exception {
        if (!versionMeetsMinimum(5, 6, 5)) {
            return;
        }
        Connection c = null;
        String datadir = null;
        Properties props = getPropertiesFromTestsuiteUrl();
        String dbname = props.getProperty(PropertyDefinitions.DBNAME_PROPERTY_KEY);

        props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useServerPrepStmts, "true");
        try {

            this.stmt.executeUpdate("SET @old_default_storage_engine = @@default_storage_engine");
            this.stmt.executeUpdate("SET @@default_storage_engine = 'InnoDB'");

            c = getConnectionWithProps(props);

            createTable("testExplicitPartitions",
                    "(a INT NOT NULL, b varchar (64), INDEX (b,a), PRIMARY KEY (a)) ENGINE = InnoDB"
                            + " PARTITION BY RANGE (a) SUBPARTITION BY HASH (a) SUBPARTITIONS 2"
                            + " (PARTITION pNeg VALUES LESS THAN (0) (SUBPARTITION subp0, SUBPARTITION subp1),"
                            + " PARTITION `p0-9` VALUES LESS THAN (10) (SUBPARTITION subp2, SUBPARTITION subp3),"
                            + " PARTITION `p10-99` VALUES LESS THAN (100) (SUBPARTITION subp4, SUBPARTITION subp5),"
                            + " PARTITION `p100-99999` VALUES LESS THAN (100000) (SUBPARTITION subp6, SUBPARTITION subp7))");

            this.stmt.executeUpdate("INSERT INTO testExplicitPartitions PARTITION (pNeg, pNeg) VALUES (-1, \"pNeg(-subp1)\")");

            this.pstmt = this.conn.prepareStatement("INSERT INTO testExplicitPartitions PARTITION (pNeg, subp0) VALUES (-3, \"pNeg(-subp1)\")");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.pstmt.execute();

            this.pstmt = c.prepareStatement("INSERT INTO testExplicitPartitions PARTITION (pNeg, subp0) VALUES (-2, \"(pNeg-)subp0\")");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.ServerPreparedStatement);
            this.pstmt.execute();

            this.pstmt = c.prepareStatement(
                    "INSERT INTO testExplicitPartitions PARTITION (`p100-99999`) VALUES (100, \"`p100-99999`(-subp6)\"), (101, \"`p100-99999`(-subp7)\"), (1000, \"`p100-99999`(-subp6)\")");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.ServerPreparedStatement);
            this.pstmt.execute();

            this.stmt.executeUpdate("INSERT INTO testExplicitPartitions PARTITION(`p10-99`,subp3) VALUES (1, \"subp3\"), (10, \"p10-99\")");
            this.stmt.executeUpdate("INSERT INTO testExplicitPartitions PARTITION(subp3) VALUES (3, \"subp3\")");
            this.stmt.executeUpdate("INSERT INTO testExplicitPartitions PARTITION(`p0-9`) VALUES (5, \"p0-9:subp3\")");

            this.stmt.executeUpdate("FLUSH STATUS");
            this.stmt.execute("SELECT * FROM testExplicitPartitions PARTITION (subp2)");

            this.pstmt = this.conn.prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (subp2,pNeg) AS TableAlias");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.pstmt = c.prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (subp2,pNeg) AS TableAlias");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.ServerPreparedStatement);

            this.pstmt = this.conn.prepareStatement("LOCK TABLE testExplicitPartitions READ, testExplicitPartitions as TableAlias READ");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.pstmt = c.prepareStatement("LOCK TABLE testExplicitPartitions READ, testExplicitPartitions as TableAlias READ");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);

            this.pstmt = this.conn.prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (subp3) AS TableAlias");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.pstmt.execute();
            this.pstmt = c.prepareStatement("SELECT COUNT(*) FROM testExplicitPartitions PARTITION (`p10-99`)");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.ServerPreparedStatement);
            this.pstmt.execute();

            this.pstmt = this.conn.prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (pNeg) WHERE a = 100");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.pstmt.execute();
            this.pstmt = c.prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (pNeg) WHERE a = 100");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.ServerPreparedStatement);
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

            this.pstmt = this.conn
                    .prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (pNeg, `p10-99`) INTO OUTFILE 'loadtestExplicitPartitions.txt'");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.pstmt = c.prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (pNeg, `p10-99`) INTO OUTFILE 'loadtestExplicitPartitions.txt'");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.ServerPreparedStatement);
            this.stmt.execute("SELECT * FROM testExplicitPartitions PARTITION (pNeg, `p10-99`) INTO OUTFILE 'loadtestExplicitPartitions.txt'");

            this.pstmt = this.conn.prepareStatement("ALTER TABLE testExplicitPartitions TRUNCATE PARTITION pNeg, `p10-99`");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.pstmt = c.prepareStatement("ALTER TABLE testExplicitPartitions TRUNCATE PARTITION pNeg, `p10-99`");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.ServerPreparedStatement);
            this.stmt.executeUpdate("ALTER TABLE testExplicitPartitions TRUNCATE PARTITION pNeg, `p10-99`");
            this.stmt.executeUpdate("FLUSH STATUS");

            this.pstmt = this.conn
                    .prepareStatement("LOAD DATA INFILE 'loadtestExplicitPartitions.txt' INTO TABLE testExplicitPartitions PARTITION (pNeg, subp4, subp5)");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.pstmt = c
                    .prepareStatement("LOAD DATA INFILE 'loadtestExplicitPartitions.txt' INTO TABLE testExplicitPartitions PARTITION (pNeg, subp4, subp5)");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.stmt.executeUpdate("LOAD DATA INFILE 'loadtestExplicitPartitions.txt' INTO TABLE testExplicitPartitions PARTITION (pNeg, subp4, subp5)");

            this.stmt.executeUpdate("ALTER TABLE testExplicitPartitions TRUNCATE PARTITION pNeg, `p10-99`");
            this.stmt.executeUpdate("FLUSH STATUS");
            this.pstmt = this.conn
                    .prepareStatement("LOAD DATA INFILE 'loadtestExplicitPartitions.txt' INTO TABLE testExplicitPartitions PARTITION (pNeg, `p10-99`)");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.pstmt = c.prepareStatement("LOAD DATA INFILE 'loadtestExplicitPartitions.txt' INTO TABLE testExplicitPartitions PARTITION (pNeg, `p10-99`)");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.stmt.executeUpdate("LOCK TABLE testExplicitPartitions WRITE");
            this.stmt.executeUpdate("LOAD DATA INFILE 'loadtestExplicitPartitions.txt' INTO TABLE testExplicitPartitions PARTITION (pNeg, `p10-99`)");
            this.stmt.executeUpdate("UNLOCK TABLES");

            // Test UPDATE
            this.stmt.executeUpdate("UPDATE testExplicitPartitions PARTITION(subp0) SET b = concat(b, ', Updated')");

            this.pstmt = this.conn.prepareStatement("UPDATE testExplicitPartitions PARTITION(subp0) SET b = concat(b, ', Updated2') WHERE a = -2");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.pstmt.execute();

            this.pstmt = c.prepareStatement("UPDATE testExplicitPartitions PARTITION(subp0) SET a = -4, b = concat(b, ', Updated from a = -2') WHERE a = -2");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.ServerPreparedStatement);
            this.pstmt.execute();

            this.stmt.executeUpdate("UPDATE testExplicitPartitions PARTITION(subp0) SET b = concat(b, ', Updated2') WHERE a = 100");
            this.stmt.executeUpdate("UPDATE testExplicitPartitions PARTITION(subp0) SET a = -2, b = concat(b, ', Updated from a = 100') WHERE a = 100");

            this.pstmt = this.conn.prepareStatement(
                    "UPDATE testExplicitPartitions PARTITION(`p100-99999`, pNeg) SET a = -222, b = concat(b, ', Updated from a = 100') WHERE a = 100");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.pstmt.execute();

            this.pstmt = c.prepareStatement("UPDATE testExplicitPartitions SET b = concat(b, ', Updated2') WHERE a = 1000000");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.ServerPreparedStatement);
            this.pstmt.execute();

            // Test DELETE
            this.stmt.executeUpdate("DELETE FROM testExplicitPartitions PARTITION (pNeg) WHERE a = -1");
            this.pstmt = this.conn.prepareStatement("DELETE FROM testExplicitPartitions PARTITION (pNeg) WHERE a = -1");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.pstmt.execute();
            this.pstmt = c.prepareStatement("DELETE FROM testExplicitPartitions PARTITION (pNeg) WHERE a = -1");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.ServerPreparedStatement);
            this.pstmt.execute();

            this.stmt.executeUpdate("DELETE FROM testExplicitPartitions PARTITION (subp1) WHERE b like '%subp1%'");
            this.pstmt = this.conn.prepareStatement("DELETE FROM testExplicitPartitions PARTITION (subp1) WHERE b like '%subp1%'");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.pstmt.execute();
            this.pstmt = c.prepareStatement("DELETE FROM testExplicitPartitions PARTITION (subp1) WHERE b like '%subp1%'");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.ServerPreparedStatement);
            this.pstmt.execute();

            this.stmt.executeUpdate("FLUSH STATUS");
            this.stmt.executeUpdate("LOCK TABLE testExplicitPartitions WRITE");
            this.stmt.executeUpdate("DELETE FROM testExplicitPartitions PARTITION (subp1) WHERE b = 'p0-9:subp3'");
            this.pstmt = this.conn.prepareStatement("DELETE FROM testExplicitPartitions PARTITION (subp1) WHERE b = 'p0-9:subp3'");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.stmt.executeUpdate("DELETE FROM testExplicitPartitions PARTITION (`p0-9`) WHERE b = 'p0-9:subp3'");
            this.pstmt = this.conn.prepareStatement("DELETE FROM testExplicitPartitions PARTITION (`p0-9`) WHERE b = 'p0-9:subp3'");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.stmt.executeUpdate("UNLOCK TABLES");

            // Test multi-table DELETE
            this.stmt.executeUpdate("CREATE TABLE testExplicitPartitions2 LIKE testExplicitPartitions");

            this.pstmt = this.conn.prepareStatement(
                    "INSERT INTO testExplicitPartitions2 PARTITION (`p10-99`, subp3, `p100-99999`) SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.pstmt = c.prepareStatement(
                    "INSERT INTO testExplicitPartitions2 PARTITION (`p10-99`, subp3, `p100-99999`) SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.ServerPreparedStatement);
            this.stmt.executeUpdate(
                    "INSERT INTO testExplicitPartitions2 PARTITION (`p10-99`, subp3, `p100-99999`) SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");

            this.stmt.executeUpdate("ALTER TABLE testExplicitPartitions2 TRUNCATE PARTITION `p10-99`, `p0-9`, `p100-99999`");

            this.pstmt = this.conn.prepareStatement(
                    "INSERT IGNORE INTO testExplicitPartitions2 PARTITION (subp3) SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.pstmt = c.prepareStatement(
                    "INSERT IGNORE INTO testExplicitPartitions2 PARTITION (subp3) SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.ServerPreparedStatement);
            this.stmt.executeUpdate(
                    "INSERT IGNORE INTO testExplicitPartitions2 PARTITION (subp3) SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");

            this.stmt.executeUpdate("TRUNCATE TABLE testExplicitPartitions2");
            this.stmt.executeUpdate("INSERT INTO testExplicitPartitions2 SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");

            this.pstmt = this.conn
                    .prepareStatement("CREATE TABLE testExplicitPartitions3 SELECT * FROM testExplicitPartitions PARTITION (pNeg,subp3,`p100-99999`)");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.pstmt = c.prepareStatement("CREATE TABLE testExplicitPartitions3 SELECT * FROM testExplicitPartitions PARTITION (pNeg,subp3,`p100-99999`)");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.stmt.executeUpdate("CREATE TABLE testExplicitPartitions3 SELECT * FROM testExplicitPartitions PARTITION (pNeg,subp3,`p100-99999`)");

            this.pstmt = this.conn.prepareStatement(
                    "DELETE testExplicitPartitions, testExplicitPartitions2 FROM testExplicitPartitions PARTITION (pNeg), testExplicitPartitions3, testExplicitPartitions2 PARTITION (subp3) WHERE testExplicitPartitions.a = testExplicitPartitions3.a AND testExplicitPartitions3.b = 'subp3' AND testExplicitPartitions3.a = testExplicitPartitions2.a");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.pstmt = c.prepareStatement(
                    "DELETE testExplicitPartitions, testExplicitPartitions2 FROM testExplicitPartitions PARTITION (pNeg), testExplicitPartitions3, testExplicitPartitions2 PARTITION (subp3) WHERE testExplicitPartitions.a = testExplicitPartitions3.a AND testExplicitPartitions3.b = 'subp3' AND testExplicitPartitions3.a = testExplicitPartitions2.a");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.ServerPreparedStatement);
            this.stmt.executeUpdate(
                    "DELETE testExplicitPartitions, testExplicitPartitions2 FROM testExplicitPartitions PARTITION (pNeg), testExplicitPartitions3, testExplicitPartitions2 PARTITION (subp3) WHERE testExplicitPartitions.a = testExplicitPartitions3.a AND testExplicitPartitions3.b = 'subp3' AND testExplicitPartitions3.a = testExplicitPartitions2.a");

            this.pstmt = this.conn.prepareStatement(
                    "DELETE FROM testExplicitPartitions2, testExplicitPartitions3 USING testExplicitPartitions2 PARTITION (`p0-9`), testExplicitPartitions3, testExplicitPartitions PARTITION (subp3) WHERE testExplicitPartitions.a = testExplicitPartitions3.a AND testExplicitPartitions3.b = 'subp3' AND testExplicitPartitions2.a = testExplicitPartitions.a");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.PreparedStatement);
            this.pstmt = c.prepareStatement(
                    "DELETE FROM testExplicitPartitions2, testExplicitPartitions3 USING testExplicitPartitions2 PARTITION (`p0-9`), testExplicitPartitions3, testExplicitPartitions PARTITION (subp3) WHERE testExplicitPartitions.a = testExplicitPartitions3.a AND testExplicitPartitions3.b = 'subp3' AND testExplicitPartitions2.a = testExplicitPartitions.a");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.ServerPreparedStatement);
            this.stmt.executeUpdate(
                    "DELETE FROM testExplicitPartitions2, testExplicitPartitions3 USING testExplicitPartitions2 PARTITION (`p0-9`), testExplicitPartitions3, testExplicitPartitions PARTITION (subp3) WHERE testExplicitPartitions.a = testExplicitPartitions3.a AND testExplicitPartitions3.b = 'subp3' AND testExplicitPartitions2.a = testExplicitPartitions.a");

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
                    fail("File " + datadir + dbname + File.separator + "loadtestExplicitPartitions.txt cannot be deleted."
                            + "You should run server and tests on the same filesystem.");
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

        String[][] dataSamples = new String[][] { { "127.0.0.1", "172.0.0.1" }, { "192.168.1.1", "::ffff:192.168.1.1" }, { "10.1", "::ffff:10.1" },
                { "172.16.260.4", "172.16.260.4" }, { "::1", "::1" }, { "10AA:10bb:10CC:10dd:10EE:10FF:10aa:10BB", "10aa:10bb:10cc:10dd:10ee:10ff:10aa:10bb" },
                { "00af:0000:0000:0000:10af:000a:000b:0001", "00af:0000:0000:0000:10af:000a:000b:0001" },
                { "48:4df1::0010:ad3:1100", "48:4df1::0010:ad3:1100" },
                { "2000:abcd:1234:0000:efgh:1000:2000:3000", "2000:abcd:1234:0000:efgh:1000:2000:3000" },
                { "2000:abcd:1234:0000:1000:2000:3000", "2000:abcd:1234:0000:1000:2000:3000" } };
        String[][] dataExpected = new String[][] { { "127.0.0.1", "172.0.0.1" }, { "192.168.1.1", "::ffff:192.168.1.1" }, { "10.0.0.1", null }, { null, null },
                { null, "::1" }, { null, "10aa:10bb:10cc:10dd:10ee:10ff:10aa:10bb" }, { null, "af::10af:a:b:1" }, { null, "48:4df1::10:ad3:1100" },
                { null, null }, { null, null } };

        createTable("testWL5787", "(id INT AUTO_INCREMENT PRIMARY KEY, ipv4 INT UNSIGNED, ipv6 VARBINARY(16))");

        Connection testConn = this.conn;
        if (versionMeetsMinimum(5, 7, 10)) {
            // MySQL 5.7.10+ requires non STRICT_TRANS_TABLES to use these functions with invalid data.
            Properties props = new Properties();
            props.put(PropertyDefinitions.PNAME_jdbcCompliantTruncation, "false");
            String sqlMode = getMysqlVariable("sql_mode");
            if (sqlMode.contains("STRICT_TRANS_TABLES")) {
                sqlMode = removeSqlMode("STRICT_TRANS_TABLES", sqlMode);
                props.put(PropertyDefinitions.PNAME_sessionVariables, "sql_mode='" + sqlMode + "'");
            }
            testConn = getConnectionWithProps(props);
        }
        this.pstmt = testConn.prepareStatement("INSERT INTO testWL5787 VALUES (NULL, INET_ATON(?), INET6_ATON(?))");

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

        this.pstmt.close();
        testConn.close();
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

        createTable("testFULLTEXTSearchInnoDB",
                "(id INT UNSIGNED AUTO_INCREMENT NOT NULL PRIMARY KEY, " + "title VARCHAR(200), body TEXT, FULLTEXT (title , body)) ENGINE=InnoDB");

        this.stmt.executeUpdate("INSERT INTO testFULLTEXTSearchInnoDB (title, body) VALUES ('MySQL Tutorial','DBMS stands for DataBase ...'), "
                + "('How To Use MySQL Well','After you went through a ...'), ('Optimizing MySQL','In this tutorial we will show ...'), "
                + "('1001 MySQL Tricks','1. Never run mysqld as root. 2. ...'), ('MySQL vs. YourSQL','In the following database comparison ...'), "
                + "('MySQL Security','When configured properly, MySQL ...')");

        String[] querySamples = new String[] { "SELECT * FROM testFULLTEXTSearchInnoDB WHERE MATCH (title, body) AGAINST ('database' IN NATURAL LANGUAGE MODE)",
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

        this.rs = dbmd.getIndexInfo(this.dbName, null, "testRenameIndex", false, true);
        assertTrue("Expected 1 (of 2) indexes.", this.rs.next());
        assertEquals("Wrong index name for table 'testRenameIndex'.", "col1", this.rs.getString(6));
        assertTrue("Expected 2 (of 2) indexes.", this.rs.next());
        assertEquals("Wrong index name for table 'testRenameIndex'.", "testIdx", this.rs.getString(6));
        assertFalse("No more indexes expected for table 'testRenameIndex'.", this.rs.next());

        this.stmt.execute("ALTER TABLE testRenameIndex RENAME INDEX col1 TO col1Index");
        this.stmt.execute("ALTER TABLE testRenameIndex RENAME INDEX testIdx TO testIndex");

        this.rs = dbmd.getIndexInfo(this.dbName, null, "testRenameIndex", false, true);
        assertTrue("Expected 1 (of 2) indexes.", this.rs.next());
        assertEquals("Wrong index name for table 'testRenameIndex'.", "col1Index", this.rs.getString(6));
        assertTrue("Expected 2 (of 2) indexes.", this.rs.next());
        assertEquals("Wrong index name for table 'testRenameIndex'.", "testIndex", this.rs.getString(6));
        assertFalse("No more indexes expected for table 'testRenameIndex'.", this.rs.next());
    }

    /**
     * WL#6406 - Stacked diagnostic areas
     * 
     * "STACKED" in "GET [CURRENT | STACKED] DIAGNOSTICS" syntax was added in 5.7.0. Final behavior was implemented in
     * version 5.7.2, by WL#5928 - Most statements should clear the diagnostic area.
     * 
     * @throws SQLException
     */
    public void testGetStackedDiagnostics() throws Exception {
        if (!versionMeetsMinimum(5, 7, 2)) {
            return;
        }

        // test calling GET STACKED DIAGNOSTICS outside an handler
        final Statement locallyScopedStmt = this.stmt;
        assertThrows(SQLException.class, "GET STACKED DIAGNOSTICS when handler not active", new Callable<Void>() {
            public Void call() throws Exception {
                locallyScopedStmt.execute("GET STACKED DIAGNOSTICS @num = NUMBER");
                return null;
            }
        });

        // test calling GET STACKED DIAGNOSTICS inside an handler
        // (stored procedure is based on documentation example)
        createTable("testGetStackedDiagnosticsTbl", "(c VARCHAR(8) NOT NULL)");
        createProcedure("testGetStackedDiagnosticsSP",
                "() BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN " + "GET CURRENT DIAGNOSTICS CONDITION 1 @errno = MYSQL_ERRNO, @msg = MESSAGE_TEXT; "
                        + "SELECT 'current DA before insert in handler' AS op, @errno AS errno, @msg AS msg; " // 1st result
                        + "GET STACKED DIAGNOSTICS CONDITION 1 @errno = MYSQL_ERRNO, @msg = MESSAGE_TEXT; "
                        + "SELECT 'stacked DA before insert in handler' AS op, @errno AS errno, @msg AS msg; " // 2nd result
                        + "INSERT INTO testGetStackedDiagnosticsTbl (c) VALUES('gnitset'); " + "GET CURRENT DIAGNOSTICS @num = NUMBER; "
                        + "IF @num = 0 THEN SELECT 'INSERT succeeded, current DA is empty' AS op; " // 3rd result
                        + "ELSE GET CURRENT DIAGNOSTICS CONDITION 1 @errno = MYSQL_ERRNO, @msg = MESSAGE_TEXT; "
                        + "SELECT 'current DA after insert in handler' AS op, @errno AS errno, @msg AS msg; END IF; "
                        + "GET STACKED DIAGNOSTICS CONDITION 1 @errno = MYSQL_ERRNO, @msg = MESSAGE_TEXT; "
                        + "SELECT 'stacked DA after insert in handler' AS op, @errno AS errno, @msg AS msg; END; " // 4th result
                        + "INSERT INTO testGetStackedDiagnosticsTbl (c) VALUES ('testing');INSERT INTO testGetStackedDiagnosticsTbl (c) VALUES (NULL); END");

        CallableStatement cStmt = this.conn.prepareCall("CALL testGetStackedDiagnosticsSP()");
        assertTrue(cStmt.execute());

        // test 1st ResultSet
        this.rs = cStmt.getResultSet();
        assertTrue(this.rs.next());
        assertEquals("current DA before insert in handler", this.rs.getString(1));
        assertEquals(1048, this.rs.getInt(2));
        assertEquals("Column 'c' cannot be null", this.rs.getString(3));
        assertFalse(this.rs.next());
        this.rs.close();

        // test 2nd ResultSet
        assertTrue(cStmt.getMoreResults());
        this.rs = cStmt.getResultSet();
        assertTrue(this.rs.next());
        assertEquals("stacked DA before insert in handler", this.rs.getString(1));
        assertEquals(1048, this.rs.getInt(2));
        assertEquals("Column 'c' cannot be null", this.rs.getString(3));
        assertFalse(this.rs.next());
        this.rs.close();

        // test 3rd ResultSet
        assertTrue(cStmt.getMoreResults());
        this.rs = cStmt.getResultSet();
        assertTrue(this.rs.next());
        assertEquals("INSERT succeeded, current DA is empty", this.rs.getString(1));
        assertFalse(this.rs.next());
        this.rs.close();

        // test 4th ResultSet
        assertTrue(cStmt.getMoreResults());
        this.rs = cStmt.getResultSet();
        assertTrue(this.rs.next());
        assertEquals("stacked DA after insert in handler", this.rs.getString(1));
        assertEquals(1048, this.rs.getInt(2));
        assertEquals("Column 'c' cannot be null", this.rs.getString(3));
        assertFalse(this.rs.next());
        this.rs.close();

        // no more ResultSets
        assertFalse(cStmt.getMoreResults());
        cStmt.close();

        // test table contents
        this.rs = this.stmt.executeQuery("SELECT * FROM testGetStackedDiagnosticsTbl");
        assertTrue(this.rs.next());
        assertEquals("testing", this.rs.getString(1));
        assertTrue(this.rs.next());
        assertEquals("gnitset", this.rs.getString(1));
        assertFalse(this.rs.next());
        this.rs.close();
    }

    /**
     * WL#6868 - Support transportable tablespaces for single innodb partition.
     * 
     * New syntax introduced in MySQL 5.7.4.
     * ALTER TABLE t DISCARD PARTITION {p[[,p1]..]|ALL} TABLESPACE;
     * ALTER TABLE t IMPORT PARTITION {p[[,p1]..]|ALL} TABLESPACE;
     */
    public void testDiscardImportPartitions() throws Exception {

        if (!versionMeetsMinimum(5, 7, 4)) {
            return;
        }

        createTable("testDiscardImportPartitions",
                "(id INT) ENGINE = InnoDB PARTITION BY RANGE (id) (PARTITION p1 VALUES LESS THAN (0), PARTITION p2 VALUES LESS THAN MAXVALUE)");

        this.stmt.executeUpdate("INSERT INTO testDiscardImportPartitions VALUES (-3), (-2), (-1), (0), (1), (2), (3)");

        this.rs = this.stmt.executeQuery("CHECK TABLE testDiscardImportPartitions");
        assertTrue(this.rs.next());
        assertEquals("status", this.rs.getString(3));
        assertEquals("OK", this.rs.getString(4));
        this.rs.close();

        this.stmt.executeUpdate("ALTER TABLE testDiscardImportPartitions DISCARD PARTITION p1 TABLESPACE");

        this.rs = this.stmt.executeQuery("CHECK TABLE testDiscardImportPartitions");
        assertTrue(this.rs.next());
        assertEquals("error", this.rs.getString(3));
        assertEquals("Partition p1 returned error", this.rs.getString(4));
        this.rs.close();

        assertThrows(SQLException.class, "Tablespace is missing for table .*", new Callable<Void>() {
            @SuppressWarnings("synthetic-access")
            public Void call() throws Exception {
                SyntaxRegressionTest.this.stmt.executeUpdate("ALTER TABLE testDiscardImportPartitions IMPORT PARTITION p1 TABLESPACE");
                return null;
            }
        });
    }

    /**
     * WL#7909 - Server side JSON functions
     * 
     * Test support for data type JSON.
     * 
     * New JSON functions added in MySQL 5.7.8:
     * - JSON_APPEND(), Append data to JSON document (only in 5.7.8)
     * - JSON_ARRAY_APPEND(), Append data to JSON document (added in 5.7.9+)
     * - JSON_ARRAY_INSERT(), Insert into JSON array
     * - JSON_ARRAY(), Create JSON array
     * - JSON_CONTAINS_PATH(), Whether JSON document contains any data at path
     * - JSON_CONTAINS(), Whether JSON document contains specific object at path
     * - JSON_DEPTH(), Maximum depth of JSON document
     * - JSON_EXTRACT(), Return data from JSON document
     * - JSON_INSERT(), Insert data into JSON document
     * - JSON_KEYS(), Array of keys from JSON document
     * - JSON_LENGTH(), Number of elements in JSON document
     * - JSON_MERGE(), Merge JSON documents
     * - JSON_OBJECT(), Create JSON object
     * - JSON_QUOTE(), Quote JSON document
     * - JSON_REMOVE(), Remove data from JSON document
     * - JSON_REPLACE(), Replace values in JSON document
     * - JSON_SEARCH(), Path to value within JSON document
     * - JSON_SET(), Insert data into JSON document
     * - JSON_TYPE(), Type of JSON value
     * - JSON_UNQUOTE(), Unquote JSON value
     * - JSON_VALID(), Whether JSON value is valid
     */
    public void testJsonType() throws Exception {
        if (!versionMeetsMinimum(5, 7, 8)) {
            return;
        }

        createTable("testJsonType", "(id INT PRIMARY KEY, jsonDoc JSON)");
        assertEquals(1, this.stmt.executeUpdate("INSERT INTO testJsonType VALUES (1, '{\"key1\": \"value1\"}')"));

        // Plain statement.
        this.rs = this.stmt.executeQuery("SELECT * FROM testJsonType");
        assertEquals("JSON", this.rs.getMetaData().getColumnTypeName(2));
        assertTrue(this.rs.next());
        assertEquals("{\"key1\": \"value1\"}", this.rs.getString(2));
        assertEquals("{\"key1\": \"value1\"}", this.rs.getObject(2));
        assertFalse(this.rs.next());

        // Updatable ResultSet.
        Statement testStmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        this.rs = testStmt.executeQuery("SELECT * FROM testJsonType");
        assertTrue(this.rs.next());
        this.rs.updateString(2, "{\"key1\": \"value1\", \"key2\": \"value2\"}");
        this.rs.updateRow();

        this.rs = testStmt.executeQuery("SELECT * FROM testJsonType");
        assertEquals("JSON", this.rs.getMetaData().getColumnTypeName(2));
        assertTrue(this.rs.next());
        assertEquals("{\"key1\": \"value1\", \"key2\": \"value2\"}", this.rs.getString(2));
        assertEquals("{\"key1\": \"value1\", \"key2\": \"value2\"}", this.rs.getObject(2));
        assertFalse(this.rs.next());

        // PreparedStatement.
        this.pstmt = this.conn.prepareStatement("SELECT * FROM testJsonType");
        this.rs = this.pstmt.executeQuery();
        assertEquals("JSON", this.rs.getMetaData().getColumnTypeName(2));
        assertTrue(this.rs.next());
        assertEquals("{\"key1\": \"value1\", \"key2\": \"value2\"}", this.rs.getString(2));
        assertEquals("{\"key1\": \"value1\", \"key2\": \"value2\"}", this.rs.getObject(2));
        assertFalse(this.rs.next());

        // ServerPreparedStatement.
        Connection testConn = getConnectionWithProps("useServerPrepStmts=true");
        this.pstmt = testConn.prepareStatement("SELECT * FROM testJsonType");
        this.rs = this.pstmt.executeQuery();
        assertEquals("JSON", this.rs.getMetaData().getColumnTypeName(2));
        assertTrue(this.rs.next());
        assertEquals("{\"key1\": \"value1\", \"key2\": \"value2\"}", this.rs.getString(2));
        assertEquals("{\"key1\": \"value1\", \"key2\": \"value2\"}", this.rs.getObject(2));
        assertFalse(this.rs.next());
        testConn.close();

        // CallableStatement.
        createProcedure("testJsonTypeProc", "(OUT jsonDoc JSON) SELECT t.jsonDoc INTO jsonDoc FROM testJsonType t");
        CallableStatement testCstmt = this.conn.prepareCall("{CALL testJsonTypeProc(?)}");
        testCstmt.registerOutParameter(1, Types.CHAR);
        testCstmt.execute();
        assertEquals("{\"key1\": \"value1\", \"key2\": \"value2\"}", testCstmt.getString(1));
        assertEquals("{\"key1\": \"value1\", \"key2\": \"value2\"}", testCstmt.getObject(1));

        // JSON functions.
        testJsonTypeCheckFunction(versionMeetsMinimum(5, 7, 9) ? "SELECT JSON_ARRAY_APPEND('[1]', '$', 2)" : "SELECT JSON_APPEND('[1]', '$', 2)", "[1, 2]");
        testJsonTypeCheckFunction("SELECT JSON_ARRAY_INSERT('[2]', '$[0]', 1)", "[1, 2]");
        testJsonTypeCheckFunction("SELECT JSON_ARRAY(1, 2)", "[1, 2]");
        testJsonTypeCheckFunction("SELECT JSON_CONTAINS_PATH('{\"a\": 1}', 'one', '$.a')", "1");
        testJsonTypeCheckFunction("SELECT JSON_CONTAINS('{\"a\": 1}', '1', '$.a')", "1");
        testJsonTypeCheckFunction("SELECT JSON_DEPTH('{\"a\": 1}')", "2");
        testJsonTypeCheckFunction("SELECT JSON_EXTRACT('[1, 2]', '$[0]')", "1");
        testJsonTypeCheckFunction("SELECT JSON_INSERT('[1]', '$[1]', 2)", "[1, 2]");
        testJsonTypeCheckFunction("SELECT JSON_KEYS('{\"a\": 1}')", "[\"a\"]");
        testJsonTypeCheckFunction("SELECT JSON_LENGTH('{\"a\": 1}')", "1");
        testJsonTypeCheckFunction("SELECT JSON_MERGE('[1]', '[2]')", "[1, 2]");
        testJsonTypeCheckFunction("SELECT JSON_OBJECT('a', 1)", "{\"a\": 1}");
        testJsonTypeCheckFunction("SELECT JSON_QUOTE('[1]')", "\"[1]\"");
        testJsonTypeCheckFunction("SELECT JSON_REMOVE('[1, 2]', '$[1]')", "[1]");
        testJsonTypeCheckFunction("SELECT JSON_REPLACE('[0]', '$[0]', 1)", "[1]");
        testJsonTypeCheckFunction("SELECT JSON_SEARCH('{\"a\": \"1\"}', 'one', '1')", "\"$.a\"");
        testJsonTypeCheckFunction("SELECT JSON_SET('[1, 1]', '$[1]', 2)", "[1, 2]");
        testJsonTypeCheckFunction("SELECT JSON_TYPE('[]')", "ARRAY");
        testJsonTypeCheckFunction("SELECT JSON_UNQUOTE('\"[1]\"')", "[1]");
        testJsonTypeCheckFunction("SELECT JSON_VALID('{\"a\": 1}')", "1");
    }

    private void testJsonTypeCheckFunction(String sql, String expectedResult) throws Exception {
        this.rs = this.stmt.executeQuery(sql);
        assertTrue(this.rs.next());
        assertEquals(expectedResult, this.rs.getString(1));
    }

    /**
     * WL#8016 - Parser for optimizer hints.
     * 
     * Test syntax for optimizer hints.
     * 
     * New optimizer hints feature added in MySQL 5.7.7. Hints are permitted in these contexts:
     * At the beginning of DML statements
     * - SELECT /*+ ... *&#47 ...
     * - INSERT /*+ ... *&#47 ...
     * - REPLACE /*+ ... *&#47 ...
     * - UPDATE /*+ ... *&#47 ...
     * - DELETE /*+ ... *&#47 ...
     * At the beginning of query blocks:
     * - (SELECT /*+ ... *&#47 ... )
     * - (SELECT ... ) UNION (SELECT /*+ ... *&#47 ... )
     * - (SELECT /*+ ... *&#47 ... ) UNION (SELECT /*+ ... *&#47 ... )
     * - UPDATE ... WHERE x IN (SELECT /*+ ... *&#47 ...)
     * - INSERT ... SELECT /*+ ... *&#47 ...
     * In hintable statements prefaced by EXPLAIN. For example:
     * - EXPLAIN SELECT /*+ ... *&#47 ...
     * - EXPLAIN UPDATE ... WHERE x IN (SELECT /*+ ... *&#47 ...)
     */
    public void testHints() throws Exception {
        if (!versionMeetsMinimum(5, 7, 7)) {
            return;
        }

        /*
         * Test hints syntax variations.
         */
        // Valid hints.
        testHintsSyntax("SELECT /*+ max_execution_time(100) */ SLEEP(5)", true, false);
        testHintsSyntax("SELECT/*+ max_execution_time(100) */SLEEP(5)", true, false);
        testHintsSyntax("SELECT /*+ max_execution_time(100) */ SLEEP(5) /*+ wrong location, just comments */", true, false);
        testHintsSyntax("SELECT /*+ max_execution_time(100) *//* comment */ SLEEP(5)", true, false);

        // Invalid hints.
        testHintsSyntax("SELECT /*+ max_execution_time *//*+ (100) */ SLEEP(0.5)", false, true);
        testHintsSyntax("SELECT /*+! max_execution_time (100) */ SLEEP(0.5)", false, true);

        // Valid and invalid hints.
        testHintsSyntax("SELECT /*+ max_execution_time (100) bad_hint */ SLEEP(5)", true, true);

        // No hints.
        testHintsSyntax("/*+ max_execution_time(100) */SELECT SLEEP(0.5)", false, false);
        testHintsSyntax("SELECT SLEEP(0.5) /*+ max_execution_time(100) */", false, false);
        testHintsSyntax("SELECT /* + max_execution_time(100) */ SLEEP(0.5)", false, false);
        testHintsSyntax("SELECT /* comment *//*+ max_execution_time(100) */ SLEEP(0.5)", false, false);
        testHintsSyntax("SELECT /*!+1-1, */ 1", false, false);

        /*
         * Test hints in different query types using Statements.
         */
        createTable("testHints", "(id INT PRIMARY KEY, txt CHAR(2))");

        // Hints in single query.
        assertEquals(1, this.stmt.executeUpdate("INSERT /*+ mrr(testHints) */ INTO testHints VALUES (1, 'a')"));
        assertNull(this.stmt.getWarnings());
        assertEquals(2, this.stmt.executeUpdate("REPLACE /*+ mrr(testHints) */ INTO testHints VALUES (1, 'A')"));
        assertNull(this.stmt.getWarnings());
        assertEquals(1, this.stmt.executeUpdate("UPDATE /*+ mrr(testHints) */ testHints SET txt = 'Aa'"));
        assertNull(this.stmt.getWarnings());
        this.rs = this.stmt.executeQuery("SELECT /*+ max_execution_time(100) */ * FROM testHints");
        assertNull(this.stmt.getWarnings());
        assertTrue(this.rs.next());
        assertEquals(1, this.rs.getInt(1));
        assertEquals("Aa", this.rs.getString(2));
        assertFalse(this.rs.next());
        assertEquals(1, this.stmt.executeUpdate("DELETE /*+ mrr(testHints) */ FROM testHints"));
        assertNull(this.stmt.getWarnings());

        // Hints in sub-query block.
        assertEquals(1, this.stmt.executeUpdate("INSERT INTO testHints (SELECT /*+ qb_name(dummy) */ 2, 'b')"));
        assertNull(this.stmt.getWarnings());
        assertEquals(2, this.stmt.executeUpdate("REPLACE INTO testHints (SELECT /*+ qb_name(dummy) */ 2, 'B')"));
        assertNull(this.stmt.getWarnings());
        assertEquals(1, this.stmt.executeUpdate("UPDATE testHints SET txt = 'Bb' WHERE id IN (SELECT /*+ qb_name(dummy) */ 2)"));
        assertNull(this.stmt.getWarnings());
        this.rs = this.stmt.executeQuery("SELECT /*+ max_execution_time(100) */ 1, 'Aa' UNION SELECT /*+ qb_name(dummy) */ * FROM testHints");
        assertNull(this.stmt.getWarnings());
        assertTrue(this.rs.next());
        assertEquals(1, this.rs.getInt(1));
        assertEquals("Aa", this.rs.getString(2));
        assertTrue(this.rs.next());
        assertEquals(2, this.rs.getInt(1));
        assertEquals("Bb", this.rs.getString(2));
        assertFalse(this.rs.next());
        assertEquals(1, this.stmt.executeUpdate("DELETE FROM testHints WHERE id IN (SELECT /*+ qb_name(dummy) */ 2)"));
        assertNull(this.stmt.getWarnings());

        /*
         * Test hints in different query types using PreparedStatements.
         */
        for (String connProps : new String[] { "useServerPrepStmts=false", "useServerPrepStmts=true" }) {
            Connection testConn = null;
            testConn = getConnectionWithProps(connProps);

            // Hints in single query.
            this.pstmt = testConn.prepareStatement("INSERT /*+ mrr(testHints) */ INTO testHints VALUES (?, ?)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setString(2, "a");
            assertEquals(1, this.pstmt.executeUpdate());
            assertNull(this.pstmt.getWarnings());
            this.pstmt = testConn.prepareStatement("REPLACE /*+ mrr(testHints) */ INTO testHints VALUES (?, ?)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setString(2, "A");
            assertEquals(2, this.pstmt.executeUpdate());
            assertNull(this.pstmt.getWarnings());
            this.pstmt = testConn.prepareStatement("UPDATE /*+ mrr(testHints) */ testHints SET txt = ?");
            this.pstmt.setString(1, "Aa");
            assertEquals(1, this.pstmt.executeUpdate());
            assertNull(this.pstmt.getWarnings());
            this.pstmt = testConn.prepareStatement("SELECT /*+ max_execution_time(100) */ * FROM testHints WHERE id = ?");
            this.pstmt.setInt(1, 1);
            this.rs = this.pstmt.executeQuery();
            assertNull(this.pstmt.getWarnings());
            assertTrue(this.rs.next());
            assertEquals(1, this.rs.getInt(1));
            assertEquals("Aa", this.rs.getString(2));
            assertFalse(this.rs.next());
            this.pstmt = testConn.prepareStatement("DELETE /*+ mrr(testHints) */ FROM testHints WHERE id = ?");
            this.pstmt.setInt(1, 1);
            assertEquals(1, this.pstmt.executeUpdate());
            assertNull(this.pstmt.getWarnings());

            // Hints in sub-query block.
            this.pstmt = testConn.prepareStatement("INSERT INTO testHints (SELECT /*+ qb_name(dummy) */ ?, ?)");
            this.pstmt.setInt(1, 2);
            this.pstmt.setString(2, "b");
            assertEquals(1, this.pstmt.executeUpdate());
            assertNull(this.pstmt.getWarnings());
            this.pstmt = testConn.prepareStatement("REPLACE INTO testHints (SELECT /*+ qb_name(dummy) */ ?, ?)");
            this.pstmt.setInt(1, 2);
            this.pstmt.setString(2, "B");
            assertEquals(2, this.pstmt.executeUpdate());
            assertNull(this.pstmt.getWarnings());
            this.pstmt = testConn.prepareStatement("UPDATE testHints SET txt = 'Bb' WHERE id IN (SELECT /*+ qb_name(dummy) */ ?)");
            this.pstmt.setInt(1, 2);
            assertEquals(1, this.pstmt.executeUpdate());
            assertNull(this.pstmt.getWarnings());
            this.pstmt = testConn.prepareStatement("SELECT /*+ max_execution_time(100) */ ?, ? UNION SELECT /*+ qb_name(dummy) */ * FROM testHints");
            this.pstmt.setInt(1, 1);
            this.pstmt.setString(2, "Aa");
            this.rs = this.pstmt.executeQuery();
            assertNull(this.pstmt.getWarnings());
            assertTrue(this.rs.next());
            assertEquals(1, this.rs.getInt(1));
            assertEquals("Aa", this.rs.getString(2));
            assertTrue(this.rs.next());
            assertEquals(2, this.rs.getInt(1));
            assertEquals("Bb", this.rs.getString(2));
            assertFalse(this.rs.next());
            this.pstmt = testConn.prepareStatement("DELETE FROM testHints WHERE id IN (SELECT /*+ qb_name(dummy) */ ?)");
            this.pstmt.setInt(1, 2);
            assertEquals(1, this.pstmt.executeUpdate());
            assertNull(this.pstmt.getWarnings());

            testConn.close();
        }
    }

    private void testHintsSyntax(String query, boolean processesHint, boolean warningExpected) throws Exception {
        this.stmt.clearWarnings();
        this.rs = this.stmt.executeQuery(query);
        if (warningExpected) {
            assertNotNull(this.stmt.getWarnings());
            assertTrue(this.stmt.getWarnings().getMessage().startsWith("Optimizer hint syntax error"));
        } else {
            assertNull(this.stmt.getWarnings());
        }
        assertTrue(this.rs.next());
        assertEquals(processesHint ? 1 : 0, this.rs.getInt(1));
        assertFalse(this.rs.next());
    }
}
