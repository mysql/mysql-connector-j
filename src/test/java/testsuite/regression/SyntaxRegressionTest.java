/*
 * Copyright (c) 2002, 2019, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package testsuite.regression;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import com.mysql.cj.conf.PropertyDefinitions.DatabaseTerm;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.ClientPreparedStatement;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ServerPreparedStatement;
import com.mysql.cj.util.StringUtils;

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
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");

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
                        assertTrue(this.pstmt instanceof ClientPreparedStatement);

                        this.pstmt = c.prepareStatement(sql);
                        assertTrue(this.pstmt instanceof ServerPreparedStatement);
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
            assertTrue(this.pstmt instanceof ClientPreparedStatement);

            this.pstmt = this.conn.prepareStatement(
                    "CREATE TABLE testCreateTableDataDirectorya (x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + separator + "'");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);

            this.pstmt = this.conn.prepareStatement(
                    "CREATE TEMPORARY TABLE testCreateTableDataDirectorya (x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir + "'");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);

            this.pstmt = this.conn.prepareStatement("CREATE TABLE testCreateTableDataDirectorya (x VARCHAR(10) NOT NULL DEFAULT '') DATA DIRECTORY = '" + tmpdir
                    + "' INDEX DIRECTORY = '" + tmpdir + "'");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);

            this.pstmt = this.conn.prepareStatement("ALTER TABLE testCreateTableDataDirectorya DISCARD TABLESPACE");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);

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
        String dbname = props.getProperty(PropertyKey.DBNAME.getKeyName());
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
            assertTrue(this.pstmt instanceof ClientPreparedStatement);

            this.pstmt = this.conn.prepareStatement("ALTER TABLE testTransportableTablespaces1 DISCARD TABLESPACE");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);

            this.pstmt = this.conn.prepareStatement("ALTER TABLE testTransportableTablespaces1 IMPORT TABLESPACE");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);

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

        // Using Statement, with and without validation.
        if (versionMeetsMinimum(5, 7, 5)) {
            this.stmt.executeUpdate("ALTER TABLE testExchangePartition1 EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2 WITH VALIDATION");
            this.stmt.executeUpdate("ALTER TABLE testExchangePartition1 EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2 WITHOUT VALIDATION");
        } else if (versionMeetsMinimum(5, 7, 4)) {
            this.stmt.executeUpdate("ALTER TABLE testExchangePartition1 EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2");
        } else {
            this.stmt.executeUpdate("ALTER TABLE testExchangePartition1 EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2");
            this.stmt.executeUpdate("ALTER IGNORE TABLE testExchangePartition1 EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2");
        }

        // Using Client PreparedStatement, with validation.
        if (versionMeetsMinimum(5, 7, 5)) {
            this.pstmt = this.conn
                    .prepareStatement("ALTER TABLE testExchangePartition1 EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2 WITH VALIDATION");
        } else if (versionMeetsMinimum(5, 7, 4)) {
            this.pstmt = this.conn.prepareStatement("ALTER TABLE testExchangePartition1 EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2");
        } else {
            this.pstmt = this.conn.prepareStatement("ALTER TABLE testExchangePartition1 " + "EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2");
        }
        assertEquals(ClientPreparedStatement.class, this.pstmt.getClass());
        this.pstmt.executeUpdate();

        // Using Client PreparedStatement, without validation.
        if (versionMeetsMinimum(5, 7, 5)) {
            this.pstmt = this.conn
                    .prepareStatement("ALTER TABLE testExchangePartition1 EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2 WITHOUT VALIDATION");
        } else {
            this.pstmt = this.conn.prepareStatement("ALTER IGNORE TABLE testExchangePartition1 " + "EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2");
        }
        assertEquals(ClientPreparedStatement.class, this.pstmt.getClass());
        this.pstmt.executeUpdate();

        Connection testConn = null;
        try {
            testConn = getConnectionWithProps("useServerPrepStmts=true,emulateUnsupportedPstmts=false");

            // Using Server PreparedStatement, with validation.
            if (versionMeetsMinimum(5, 7, 5)) {
                this.pstmt = testConn
                        .prepareStatement("ALTER TABLE testExchangePartition1 EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2 WITH VALIDATION");
            } else if (versionMeetsMinimum(5, 7, 4)) {
                this.pstmt = testConn.prepareStatement("ALTER TABLE testExchangePartition1 EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2");
            } else {
                this.pstmt = testConn
                        .prepareStatement("ALTER IGNORE TABLE testExchangePartition1 " + "EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2");

            }
            assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement.class, this.pstmt.getClass());
            this.pstmt.executeUpdate();

            // Using Server PreparedStatement, without validation.
            if (versionMeetsMinimum(5, 7, 5)) {
                this.pstmt = testConn
                        .prepareStatement("ALTER TABLE testExchangePartition1 EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2 WITHOUT VALIDATION");
            } else {
                this.pstmt = testConn.prepareStatement("ALTER TABLE testExchangePartition1 " + "EXCHANGE PARTITION p1 WITH TABLE testExchangePartition2");

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

        String datadir = null;
        this.rs = this.stmt.executeQuery("SHOW VARIABLES WHERE Variable_name='datadir'");
        this.rs.next();
        datadir = this.rs.getString(2);
        if (datadir != null) {
            datadir = new File(datadir).getCanonicalPath();
        }

        this.rs = this.stmt.executeQuery("SHOW VARIABLES WHERE Variable_name='secure_file_priv'");
        this.rs.next();
        String fileprivdir = this.rs.getString(2);
        if ("NULL".equalsIgnoreCase(this.rs.getString(2))) {
            fail("To run this test the server needs to be started with the option\"--secure-file-priv=\"");
        } else if (fileprivdir.length() > 0) {
            fileprivdir = new File(fileprivdir).getCanonicalPath();
            if (!datadir.equals(fileprivdir)) {
                fail("To run this test the server option\"--secure-file-priv=\" needs to be empty or to match the server's data directory.");
            }
        }

        Properties props = getPropertiesFromTestsuiteUrl();
        String dbname = props.getProperty(PropertyKey.DBNAME.getKeyName());

        props = new Properties();
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
        Connection c = null;

        boolean exceptionCaugth = false;
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
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
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
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.pstmt = c.prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (subp2,pNeg) AS TableAlias");
            assertTrue(this.pstmt instanceof ServerPreparedStatement);

            this.pstmt = this.conn.prepareStatement("LOCK TABLE testExplicitPartitions READ, testExplicitPartitions as TableAlias READ");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.pstmt = c.prepareStatement("LOCK TABLE testExplicitPartitions READ, testExplicitPartitions as TableAlias READ");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);

            this.pstmt = this.conn.prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (subp3) AS TableAlias");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.pstmt.execute();
            this.pstmt = c.prepareStatement("SELECT COUNT(*) FROM testExplicitPartitions PARTITION (`p10-99`)");
            assertTrue(this.pstmt instanceof ServerPreparedStatement);
            this.pstmt.execute();

            this.pstmt = this.conn.prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (pNeg) WHERE a = 100");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.pstmt.execute();
            this.pstmt = c.prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (pNeg) WHERE a = 100");
            assertTrue(this.pstmt instanceof ServerPreparedStatement);
            this.pstmt.execute();

            this.stmt.executeUpdate("UNLOCK TABLES");

            // Test LOAD
            if (dbname == null) {
                fail("No database selected");
            } else {
                File f = new File(datadir + File.separator + dbname + File.separator + "loadtestExplicitPartitions.txt");
                if (f.exists()) {
                    f.delete();
                }
            }

            this.pstmt = this.conn
                    .prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (pNeg, `p10-99`) INTO OUTFILE 'loadtestExplicitPartitions.txt'");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.pstmt = c.prepareStatement("SELECT * FROM testExplicitPartitions PARTITION (pNeg, `p10-99`) INTO OUTFILE 'loadtestExplicitPartitions.txt'");
            assertTrue(this.pstmt instanceof ServerPreparedStatement);
            this.stmt.execute("SELECT * FROM testExplicitPartitions PARTITION (pNeg, `p10-99`) INTO OUTFILE 'loadtestExplicitPartitions.txt'");

            this.pstmt = this.conn.prepareStatement("ALTER TABLE testExplicitPartitions TRUNCATE PARTITION pNeg, `p10-99`");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.pstmt = c.prepareStatement("ALTER TABLE testExplicitPartitions TRUNCATE PARTITION pNeg, `p10-99`");
            assertTrue(this.pstmt instanceof ServerPreparedStatement);
            this.stmt.executeUpdate("ALTER TABLE testExplicitPartitions TRUNCATE PARTITION pNeg, `p10-99`");
            this.stmt.executeUpdate("FLUSH STATUS");

            this.pstmt = this.conn
                    .prepareStatement("LOAD DATA INFILE 'loadtestExplicitPartitions.txt' INTO TABLE testExplicitPartitions PARTITION (pNeg, subp4, subp5)");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.pstmt = c
                    .prepareStatement("LOAD DATA INFILE 'loadtestExplicitPartitions.txt' INTO TABLE testExplicitPartitions PARTITION (pNeg, subp4, subp5)");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.stmt.executeUpdate("LOAD DATA INFILE 'loadtestExplicitPartitions.txt' INTO TABLE testExplicitPartitions PARTITION (pNeg, subp4, subp5)");

            this.stmt.executeUpdate("ALTER TABLE testExplicitPartitions TRUNCATE PARTITION pNeg, `p10-99`");
            this.stmt.executeUpdate("FLUSH STATUS");
            this.pstmt = this.conn
                    .prepareStatement("LOAD DATA INFILE 'loadtestExplicitPartitions.txt' INTO TABLE testExplicitPartitions PARTITION (pNeg, `p10-99`)");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.pstmt = c.prepareStatement("LOAD DATA INFILE 'loadtestExplicitPartitions.txt' INTO TABLE testExplicitPartitions PARTITION (pNeg, `p10-99`)");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.stmt.executeUpdate("LOCK TABLE testExplicitPartitions WRITE");
            this.stmt.executeUpdate("LOAD DATA INFILE 'loadtestExplicitPartitions.txt' INTO TABLE testExplicitPartitions PARTITION (pNeg, `p10-99`)");
            this.stmt.executeUpdate("UNLOCK TABLES");

            // Test UPDATE
            this.stmt.executeUpdate("UPDATE testExplicitPartitions PARTITION(subp0) SET b = concat(b, ', Updated')");

            this.pstmt = this.conn.prepareStatement("UPDATE testExplicitPartitions PARTITION(subp0) SET b = concat(b, ', Updated2') WHERE a = -2");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.pstmt.execute();

            this.pstmt = c.prepareStatement("UPDATE testExplicitPartitions PARTITION(subp0) SET a = -4, b = concat(b, ', Updated from a = -2') WHERE a = -2");
            assertTrue(this.pstmt instanceof ServerPreparedStatement);
            this.pstmt.execute();

            this.stmt.executeUpdate("UPDATE testExplicitPartitions PARTITION(subp0) SET b = concat(b, ', Updated2') WHERE a = 100");
            this.stmt.executeUpdate("UPDATE testExplicitPartitions PARTITION(subp0) SET a = -2, b = concat(b, ', Updated from a = 100') WHERE a = 100");

            this.pstmt = this.conn.prepareStatement(
                    "UPDATE testExplicitPartitions PARTITION(`p100-99999`, pNeg) SET a = -222, b = concat(b, ', Updated from a = 100') WHERE a = 100");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.pstmt.execute();

            this.pstmt = c.prepareStatement("UPDATE testExplicitPartitions SET b = concat(b, ', Updated2') WHERE a = 1000000");
            assertTrue(this.pstmt instanceof ServerPreparedStatement);
            this.pstmt.execute();

            // Test DELETE
            this.stmt.executeUpdate("DELETE FROM testExplicitPartitions PARTITION (pNeg) WHERE a = -1");
            this.pstmt = this.conn.prepareStatement("DELETE FROM testExplicitPartitions PARTITION (pNeg) WHERE a = -1");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.pstmt.execute();
            this.pstmt = c.prepareStatement("DELETE FROM testExplicitPartitions PARTITION (pNeg) WHERE a = -1");
            assertTrue(this.pstmt instanceof ServerPreparedStatement);
            this.pstmt.execute();

            this.stmt.executeUpdate("DELETE FROM testExplicitPartitions PARTITION (subp1) WHERE b like '%subp1%'");
            this.pstmt = this.conn.prepareStatement("DELETE FROM testExplicitPartitions PARTITION (subp1) WHERE b like '%subp1%'");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.pstmt.execute();
            this.pstmt = c.prepareStatement("DELETE FROM testExplicitPartitions PARTITION (subp1) WHERE b like '%subp1%'");
            assertTrue(this.pstmt instanceof ServerPreparedStatement);
            this.pstmt.execute();

            this.stmt.executeUpdate("FLUSH STATUS");
            this.stmt.executeUpdate("LOCK TABLE testExplicitPartitions WRITE");
            this.stmt.executeUpdate("DELETE FROM testExplicitPartitions PARTITION (subp1) WHERE b = 'p0-9:subp3'");
            this.pstmt = this.conn.prepareStatement("DELETE FROM testExplicitPartitions PARTITION (subp1) WHERE b = 'p0-9:subp3'");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.stmt.executeUpdate("DELETE FROM testExplicitPartitions PARTITION (`p0-9`) WHERE b = 'p0-9:subp3'");
            this.pstmt = this.conn.prepareStatement("DELETE FROM testExplicitPartitions PARTITION (`p0-9`) WHERE b = 'p0-9:subp3'");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.stmt.executeUpdate("UNLOCK TABLES");

            // Test multi-table DELETE
            this.stmt.executeUpdate("CREATE TABLE testExplicitPartitions2 LIKE testExplicitPartitions");

            this.pstmt = this.conn.prepareStatement(
                    "INSERT INTO testExplicitPartitions2 PARTITION (`p10-99`, subp3, `p100-99999`) SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.pstmt = c.prepareStatement(
                    "INSERT INTO testExplicitPartitions2 PARTITION (`p10-99`, subp3, `p100-99999`) SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");
            assertTrue(this.pstmt instanceof ServerPreparedStatement);
            this.stmt.executeUpdate(
                    "INSERT INTO testExplicitPartitions2 PARTITION (`p10-99`, subp3, `p100-99999`) SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");

            this.stmt.executeUpdate("ALTER TABLE testExplicitPartitions2 TRUNCATE PARTITION `p10-99`, `p0-9`, `p100-99999`");

            this.pstmt = this.conn.prepareStatement(
                    "INSERT IGNORE INTO testExplicitPartitions2 PARTITION (subp3) SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.pstmt = c.prepareStatement(
                    "INSERT IGNORE INTO testExplicitPartitions2 PARTITION (subp3) SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");
            assertTrue(this.pstmt instanceof ServerPreparedStatement);
            this.stmt.executeUpdate(
                    "INSERT IGNORE INTO testExplicitPartitions2 PARTITION (subp3) SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");

            this.stmt.executeUpdate("TRUNCATE TABLE testExplicitPartitions2");
            this.stmt.executeUpdate("INSERT INTO testExplicitPartitions2 SELECT * FROM testExplicitPartitions PARTITION (subp3, `p10-99`, `p100-99999`)");

            this.pstmt = this.conn
                    .prepareStatement("CREATE TABLE testExplicitPartitions3 SELECT * FROM testExplicitPartitions PARTITION (pNeg,subp3,`p100-99999`)");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.pstmt = c.prepareStatement("CREATE TABLE testExplicitPartitions3 SELECT * FROM testExplicitPartitions PARTITION (pNeg,subp3,`p100-99999`)");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.stmt.executeUpdate("CREATE TABLE testExplicitPartitions3 SELECT * FROM testExplicitPartitions PARTITION (pNeg,subp3,`p100-99999`)");

            this.pstmt = this.conn.prepareStatement(
                    "DELETE testExplicitPartitions, testExplicitPartitions2 FROM testExplicitPartitions PARTITION (pNeg), testExplicitPartitions3, testExplicitPartitions2 PARTITION (subp3) WHERE testExplicitPartitions.a = testExplicitPartitions3.a AND testExplicitPartitions3.b = 'subp3' AND testExplicitPartitions3.a = testExplicitPartitions2.a");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.pstmt = c.prepareStatement(
                    "DELETE testExplicitPartitions, testExplicitPartitions2 FROM testExplicitPartitions PARTITION (pNeg), testExplicitPartitions3, testExplicitPartitions2 PARTITION (subp3) WHERE testExplicitPartitions.a = testExplicitPartitions3.a AND testExplicitPartitions3.b = 'subp3' AND testExplicitPartitions3.a = testExplicitPartitions2.a");
            assertTrue(this.pstmt instanceof ServerPreparedStatement);
            this.stmt.executeUpdate(
                    "DELETE testExplicitPartitions, testExplicitPartitions2 FROM testExplicitPartitions PARTITION (pNeg), testExplicitPartitions3, testExplicitPartitions2 PARTITION (subp3) WHERE testExplicitPartitions.a = testExplicitPartitions3.a AND testExplicitPartitions3.b = 'subp3' AND testExplicitPartitions3.a = testExplicitPartitions2.a");

            this.pstmt = this.conn.prepareStatement(
                    "DELETE FROM testExplicitPartitions2, testExplicitPartitions3 USING testExplicitPartitions2 PARTITION (`p0-9`), testExplicitPartitions3, testExplicitPartitions PARTITION (subp3) WHERE testExplicitPartitions.a = testExplicitPartitions3.a AND testExplicitPartitions3.b = 'subp3' AND testExplicitPartitions2.a = testExplicitPartitions.a");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);
            this.pstmt = c.prepareStatement(
                    "DELETE FROM testExplicitPartitions2, testExplicitPartitions3 USING testExplicitPartitions2 PARTITION (`p0-9`), testExplicitPartitions3, testExplicitPartitions PARTITION (subp3) WHERE testExplicitPartitions.a = testExplicitPartitions3.a AND testExplicitPartitions3.b = 'subp3' AND testExplicitPartitions2.a = testExplicitPartitions.a");
            assertTrue(this.pstmt instanceof ServerPreparedStatement);
            this.stmt.executeUpdate(
                    "DELETE FROM testExplicitPartitions2, testExplicitPartitions3 USING testExplicitPartitions2 PARTITION (`p0-9`), testExplicitPartitions3, testExplicitPartitions PARTITION (subp3) WHERE testExplicitPartitions.a = testExplicitPartitions3.a AND testExplicitPartitions3.b = 'subp3' AND testExplicitPartitions2.a = testExplicitPartitions.a");

            this.stmt.executeUpdate("SET @@default_storage_engine = @old_default_storage_engine");

        } catch (SQLException e) {
            exceptionCaugth = true;
            fail(e.getMessage());

        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testExplicitPartitions, testExplicitPartitions2, testExplicitPartitions3");

            if (c != null) {
                c.close();
            }
            if (datadir != null) {
                File f = new File(datadir + File.separator + dbname + File.separator + "loadtestExplicitPartitions.txt");
                if (f.exists()) {
                    f.deleteOnExit();
                } else if (!exceptionCaugth) {
                    fail("File " + datadir + File.separator + dbname + File.separator + "loadtestExplicitPartitions.txt cannot be deleted."
                            + "You should run server and tests on the same filesystem.");
                }
            }
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
            props.put(PropertyKey.jdbcCompliantTruncation.getKeyName(), "false");
            String sqlMode = getMysqlVariable("sql_mode");
            if (sqlMode.contains("STRICT_TRANS_TABLES")) {
                sqlMode = removeSqlMode("STRICT_TRANS_TABLES", sqlMode);
                props.put(PropertyKey.sessionVariables.getKeyName(), "sql_mode='" + sqlMode + "'");
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
     * - JSON_MERGE(), Merge JSON documents (up to 8.0.2)
     * - JSON_MERGE_PRESERVE(), Merge JSON documents (since to 8.0.3)
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
        testJsonTypeCheckFunction(versionMeetsMinimum(8, 0, 3) ? "SELECT JSON_MERGE_PRESERVE('[1]', '[2]')" : "SELECT JSON_MERGE('[1]', '[2]')", "[1, 2]");
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

    /**
     * WL#6205 - InnoDB: Implement CREATE TABLESPACE for general use.
     * 
     * Tests support for new CREATE TABLESPACE syntax that extends this feature to InnoDB.
     * 
     * CREATE TABLESPACE tablespace_name ADD DATAFILE 'file_name' [FILE_BLOCK_SIZE = value] [ENGINE [=] engine_name]
     */
    public void testCreateTablespace() throws Exception {
        if (!versionMeetsMinimum(5, 7, 6)) {
            return;
        }

        try {
            this.stmt.execute("CREATE TABLESPACE testTs1 ADD DATAFILE 'testTs1.ibd'");
            this.stmt.execute("CREATE TABLESPACE testTs2 ADD DATAFILE 'testTs2.ibd'");

            testCreateTablespaceCheckTablespaces(2);

            createTable("testTs1Tbl1", "(id INT) TABLESPACE testTs1");
            createTable("testTs1Tbl2", "(id INT) TABLESPACE testTs1");
            createTable("testTs2Tbl1", "(id INT) TABLESPACE testTs2");

            testCreateTablespaceCheckTables("testTs1", 2);
            testCreateTablespaceCheckTables("testTs2", 1);

            this.stmt.execute("ALTER TABLE testTs1Tbl2 TABLESPACE testTs2");

            testCreateTablespaceCheckTables("testTs1", 1);
            testCreateTablespaceCheckTables("testTs2", 2);

            dropTable("testTs1Tbl1");
            dropTable("testTs1Tbl2");
            dropTable("testTs2Tbl1");

            testCreateTablespaceCheckTables("testTs1", 0);
            testCreateTablespaceCheckTables("testTs2", 0);

        } finally {
            // Make sure the tables are dropped before the tablespaces.
            dropTable("testTs1Tbl1");
            dropTable("testTs1Tbl2");
            dropTable("testTs2Tbl1");

            this.stmt.execute("DROP TABLESPACE testTs1");
            this.stmt.execute("DROP TABLESPACE testTs2");

            testCreateTablespaceCheckTablespaces(0);
        }
    }

    private void testCreateTablespaceCheckTablespaces(int expectedTsCount) throws Exception {
        if (versionMeetsMinimum(8, 0, 3)) {
            this.rs = this.stmt.executeQuery("SELECT COUNT(*) FROM information_schema.innodb_tablespaces WHERE name LIKE 'testTs_'");
        } else {
            this.rs = this.stmt.executeQuery("SELECT COUNT(*) FROM information_schema.innodb_sys_tablespaces WHERE name LIKE 'testTs_'");
        }
        assertTrue(this.rs.next());
        assertEquals(expectedTsCount, this.rs.getInt(1));
    }

    private void testCreateTablespaceCheckTables(String tablespace, int expectedTblCount) throws Exception {
        if (versionMeetsMinimum(8, 0, 3)) {
            this.rs = this.stmt.executeQuery("SELECT COUNT(*) FROM information_schema.innodb_tables a, information_schema.innodb_tablespaces b "
                    + "WHERE a.space = b.space AND b.name = '" + tablespace + "'");
        } else {
            this.rs = this.stmt.executeQuery("SELECT COUNT(*) FROM information_schema.innodb_sys_tables a, information_schema.innodb_sys_tablespaces b "
                    + "WHERE a.space = b.space AND b.name = '" + tablespace + "'");
        }
        assertTrue(this.rs.next());
        assertEquals(expectedTblCount, this.rs.getInt(1));
    }

    /**
     * WL#6747 - InnoDB: make fill factor settable.
     * 
     * Tests support for new syntax for setting indices MERGE_THRESHOLD on CREATE TABLE.
     * 
     * index_option:
     * COMMENT 'MERGE_THRESHOLD=n'
     */
    public void testSetMergeThreshold() throws Exception {
        if (!versionMeetsMinimum(5, 7, 6)) {
            return;
        }

        Map<String, Integer> keyMergeThresholds = new HashMap<>();
        keyMergeThresholds.put("k2", 45);
        keyMergeThresholds.put("k3", 40);
        keyMergeThresholds.put("k23", 35);
        keyMergeThresholds.put("k24", 30);
        int tableMergeThreshold = 25;

        // Create table with both table and per index merge thresholds.
        createTable("testSetMergeThreshold",
                "(c1 INT, c2 INT, c3 INT, c4 INT, KEY k1 (c1), KEY k2 (c2) COMMENT 'MERGE_THRESHOLD=" + keyMergeThresholds.get("k2")
                        + "', KEY k3 (c3) COMMENT 'MERGE_THRESHOLD=" + keyMergeThresholds.get("k3") + "', KEY k23 (c2, c3) COMMENT 'MERGE_THRESHOLD="
                        + keyMergeThresholds.get("k23") + "', KEY k24 (c2, c4) COMMENT 'MERGE_THRESHOLD=" + keyMergeThresholds.get("k24")
                        + "') COMMENT 'MERGE_THRESHOLD=" + tableMergeThreshold + "'");
        testSetMergeThresholdIndices(tableMergeThreshold, keyMergeThresholds);

        // Change table's merge threshold.
        tableMergeThreshold++;
        this.stmt.execute("ALTER TABLE testSetMergeThreshold COMMENT 'MERGE_THRESHOLD=" + tableMergeThreshold + "'");
        testSetMergeThresholdIndices(tableMergeThreshold, keyMergeThresholds);

        // Change index' merge threshold.
        keyMergeThresholds.put("k3", 41);
        this.stmt.execute("ALTER TABLE testSetMergeThreshold DROP KEY k3");
        this.stmt.execute("ALTER TABLE testSetMergeThreshold ADD KEY k3 (c3) COMMENT 'MERGE_THRESHOLD=" + keyMergeThresholds.get("k3") + "'");
        testSetMergeThresholdIndices(tableMergeThreshold, keyMergeThresholds);

        // Add new index with a non-default merge threshold value.
        keyMergeThresholds.put("k123", 15);
        this.stmt.execute("CREATE INDEX k123 ON testSetMergeThreshold (c1, c2, c3) COMMENT 'MERGE_THRESHOLD=" + keyMergeThresholds.get("k123") + "'");
        testSetMergeThresholdIndices(tableMergeThreshold, keyMergeThresholds);
    }

    private void testSetMergeThresholdIndices(int defaultMergeThreshold, Map<String, Integer> keyMergeThresholds) throws Exception {
        boolean dbMapsToSchema = ((JdbcConnection) this.conn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm)
                .getValue() == DatabaseTerm.SCHEMA;
        if (versionMeetsMinimum(8, 0, 3)) {
            this.rs = this.stmt.executeQuery("SELECT name, merge_threshold FROM information_schema.innodb_indexes WHERE table_id = "
                    + "(SELECT table_id FROM information_schema.innodb_tables WHERE name = '"
                    + (dbMapsToSchema ? this.conn.getSchema() : this.conn.getCatalog()) + "/testSetMergeThreshold')");
        } else {
            this.rs = this.stmt.executeQuery("SELECT name, merge_threshold FROM information_schema.innodb_sys_indexes WHERE table_id = "
                    + "(SELECT table_id FROM information_schema.innodb_sys_tables WHERE name = '"
                    + (dbMapsToSchema ? this.conn.getSchema() : this.conn.getCatalog()) + "/testSetMergeThreshold')");
        }

        while (this.rs.next()) {
            int expected = keyMergeThresholds.containsKey(this.rs.getString(1)) ? keyMergeThresholds.get(this.rs.getString(1)) : defaultMergeThreshold;
            assertEquals("MERGE_THRESHOLD for index " + this.rs.getString(1), expected, this.rs.getInt(2));
        }
        assertTrue(this.rs.last());
        assertTrue(this.rs.getRow() >= keyMergeThresholds.size());
    }

    /**
     * WL#7696 - InnoDB: Transparent page compression.
     * 
     * Tests COMPRESSION clause in CREATE|ALTER TABLE syntax.
     * 
     * table_option: (...) | COMPRESSION [=] {'ZLIB'|'LZ4'|'NONE'}
     */
    public void testTableCompression() throws Exception {
        if (!versionMeetsMinimum(5, 7, 8)) {
            return;
        }

        // Create table with 'zlib' compression.
        createTable("testTableCompression", "(c VARCHAR(15000)) COMPRESSION='ZLIB'");

        this.rs = this.stmt.executeQuery("show create table testTableCompression");
        assertTrue(this.rs.next());
        assertTrue(StringUtils.indexOfIgnoreCase(this.rs.getString(2), "COMPRESSION='ZLIB'") >= 0);

        // Alter table compression to 'lz4'.
        this.stmt.execute("ALTER TABLE testTableCompression COMPRESSION='LZ4'");

        this.rs = this.stmt.executeQuery("show create table testTableCompression");
        assertTrue(this.rs.next());
        assertTrue(StringUtils.indexOfIgnoreCase(this.rs.getString(2), "COMPRESSION='LZ4'") >= 0);

        // Alter table compression to 'none'.
        this.stmt.execute("ALTER TABLE testTableCompression COMPRESSION='NONE'");

        this.rs = this.stmt.executeQuery("show create table testTableCompression");
        assertTrue(this.rs.next());
        assertTrue(StringUtils.indexOfIgnoreCase(this.rs.getString(2), "COMPRESSION='NONE'") >= 0);
    }

    /**
     * WL#1326 - GIS: Precise spatial operations
     * WL#8055 - Consistent naming scheme for GIS functions - Deprecation
     * WL#8034 - More user friendly GIS functions
     * WL#7541 - GIS MBR spatial operations enhancement
     * WL#8157 - Remove deprecated GIS functions
     * WL#8055 - Consistent naming scheme for GIS functions - Deprecation
     * WL#9435 - Axis order in WKB parsing functions
     * (...)
     * 
     * Test syntax for all GIS functions.
     */
    public void testGisFunctions() throws Exception {
        final String wktPoint = "'POINT(0 0)'";
        final String wktLineString = "'LINESTRING(0 0, 8 0, 4 6, 0 0)'";
        final String wktPolygon = "'POLYGON((0 0, 8 0, 4 6, 0 0), (4 1, 6 0, 5 3, 4 1))'";
        final String wktMultiPoint = "'MULTIPOINT(0 0, 8 0, 4 6)'";
        final String wktMultiLineString = "'MULTILINESTRING((0 0, 8 0, 4 6, 0 0), (4 1, 6 0, 5 3, 4 1))'";
        final String wktMultiPolygon = "'MULTIPOLYGON(((0 0, 8 0, 4 6, 0 0), (4 1, 6 0, 5 3, 4 1)), ((0 3, 8 3, 4 9, 0 3)))'";
        final String wktGeometryCollection = "'GEOMETRYCOLLECTION(POINT(8 0), LINESTRING(0 0, 8 0, 4 6, 0 0), POLYGON((0 3, 8 3, 4 9, 0 3)))'";

        final String geoPoint1 = "Point(0, 0)";
        final String geoPoint2 = "Point(8, 0)";
        final String geoPoint3 = "Point(4, 6)";
        final String geoPoint4 = "Point(4, 1)";
        final String geoPoint5 = "Point(6, 0)";
        final String geoPoint6 = "Point(5, 3)";
        final String geoPoint7 = "Point(0, 3)";
        final String geoPoint8 = "Point(8, 3)";
        final String geoPoint9 = "Point(4, 9)";
        final String geoLineString1 = String.format("LineString(%s, %s, %s, %s)", geoPoint1, geoPoint2, geoPoint3, geoPoint1);
        final String geoLineString2 = String.format("LineString(%s, %s, %s, %s)", geoPoint4, geoPoint5, geoPoint6, geoPoint4);
        final String geoLineString3 = String.format("LineString(%s, %s, %s, %s)", geoPoint7, geoPoint8, geoPoint9, geoPoint7);
        final String geoPolygon1 = String.format("Polygon(%s, %s)", geoLineString1, geoLineString2);
        final String geoPolygon2 = String.format("Polygon(%s)", geoLineString3);
        final String geoMultiPoint = String.format("MultiPoint(%s, %s, %s)", geoPoint1, geoPoint2, geoPoint3);
        final String geoMultiLineString = String.format("MultiLineString(%s, %s)", geoLineString1, geoLineString2);
        final String geoMultiPolygon = String.format("MultiPolygon(%s, %s)", geoPolygon1, geoPolygon2);
        final String geoGeometryCollection = String.format("GeometryCollection(%s, %s, %s)", geoPoint2, geoLineString1, geoPolygon2);

        final String wkbPoint = String.format("ST_ASWKB(%s)", geoPoint1);
        final String wkbLineString = String.format("ST_ASWKB(%s)", geoLineString1);
        final String wkbPolygon = String.format("ST_ASWKB(%S)", geoPolygon1);
        final String wkbMultiPoint = String.format("ST_ASWKB(%s)", geoMultiPoint);
        final String wkbMultiLineString = String.format("ST_ASWKB(%s)", geoMultiLineString);
        final String wkbMultiPolygon = String.format("ST_ASWKB(%s)", geoMultiPolygon);
        final String wkbGeometryCollection = String.format("ST_ASWKB(%s)", geoGeometryCollection);

        final Map<String, String> args = new HashMap<>();
        args.put("gcWkt", wktGeometryCollection);
        args.put("gWkt", wktGeometryCollection);
        args.put("lsWkt", wktLineString);
        args.put("mlsWkt", wktMultiLineString);
        args.put("mptWkt", wktMultiPoint);
        args.put("mplWkt", wktMultiPolygon);
        args.put("ptWkt", wktPoint);
        args.put("plWkt", wktPolygon);
        args.put("gcGeo", geoGeometryCollection);
        args.put("gcWkb", wkbGeometryCollection);
        args.put("gGeo", geoGeometryCollection);
        args.put("gWkb", wkbGeometryCollection);
        args.put("lsGeo", geoLineString1);
        args.put("lsWkb", wkbLineString);
        args.put("mlsGeo", geoMultiLineString);
        args.put("mlsWkb", wkbMultiLineString);
        args.put("mptGeo", geoMultiPoint);
        args.put("mptWkb", wkbMultiPoint);
        args.put("mplGeo", geoMultiPolygon);
        args.put("mplWkb", wkbMultiPolygon);
        args.put("ptGeo", geoPoint1);
        args.put("ptWkb", wkbPoint);
        args.put("plGeo", geoPolygon1);
        args.put("plWkb", wkbPolygon);
        args.put("g1", geoPolygon1);
        args.put("g2", geoPolygon2);
        args.put("pt1", geoPoint1);
        args.put("pt2", geoPoint2);
        args.put("ls1", geoLineString1);
        args.put("ls2", geoLineString2);
        args.put("pl1", geoPolygon1);
        args.put("pl2", geoPolygon2);
        args.put("g", geoGeometryCollection);
        args.put("pt", geoPoint3);
        args.put("ls", geoLineString1);
        args.put("pl", geoPolygon1);
        args.put("mpl", geoMultiPolygon);
        args.put("gc", geoGeometryCollection);
        args.put("gh", "'s14f5h28wc04jsq093jd'");
        args.put("js", "'{\"type\": \"GeometryCollection\", \"geometries\": [" + //
                "{\"type\": \"Point\", \"coordinates\": [8, 0]}, " + //
                "{\"type\": \"LineString\", \"coordinates\": [[0, 0], [8, 0], [4, 6], [0, 0]]}, " + //
                "{\"type\": \"Polygon\", \"coordinates\": [[[0, 3], [8, 3], [4, 9], [0, 3]]]}]}'");

        final class GisFunction {
            String function;
            int low_version_maj;
            int low_version_min;
            int low_version_sub;
            int hi_version_maj;
            int hi_version_min;
            int hi_version_sub;
            List<String> args;

            GisFunction(String function, int low_version_maj, int low_version_min, int low_version_sub, int hi_version_maj, int hi_version_min,
                    int hi_version_sub, String... args) {
                this.function = function;
                this.low_version_maj = low_version_maj;
                this.low_version_min = low_version_min;
                this.low_version_sub = low_version_sub;
                this.hi_version_maj = hi_version_maj;
                this.hi_version_min = hi_version_min;
                this.hi_version_sub = hi_version_sub;
                this.args = Arrays.asList(args);
            }
        }
        final List<GisFunction> gisFunctions = new ArrayList<>();
        // Functions That Create Geometry Values from WKT Values
        gisFunctions.add(new GisFunction("GeomCollFromText", 5, 5, 1, 5, 7, 6, "gcWkt"));
        gisFunctions.add(new GisFunction("GeometryCollectionFromText", 5, 5, 1, 5, 7, 6, "gcWkt"));
        gisFunctions.add(new GisFunction("GeomFromText", 5, 5, 1, 5, 7, 6, "gWkt"));
        gisFunctions.add(new GisFunction("GeometryFromText", 5, 5, 1, 5, 7, 6, "gWkt"));
        gisFunctions.add(new GisFunction("LineFromText", 5, 5, 1, 5, 7, 6, "lsWkt"));
        gisFunctions.add(new GisFunction("LineStringFromText", 5, 5, 1, 5, 7, 6, "lsWkt"));
        gisFunctions.add(new GisFunction("MLineFromText", 5, 5, 1, 5, 7, 6, "mlsWkt"));
        gisFunctions.add(new GisFunction("MultiLineStringFromText", 5, 5, 1, 5, 7, 6, "mlsWkt"));
        gisFunctions.add(new GisFunction("MPointFromText", 5, 5, 1, 5, 7, 6, "mptWkt"));
        gisFunctions.add(new GisFunction("MultiPointFromText", 5, 5, 1, 5, 7, 6, "mptWkt"));
        gisFunctions.add(new GisFunction("MPolyFromText", 5, 5, 1, 5, 7, 6, "mplWkt"));
        gisFunctions.add(new GisFunction("MultiPolygonFromText", 5, 5, 1, 5, 7, 6, "mplWkt"));
        gisFunctions.add(new GisFunction("PointFromText", 5, 5, 1, 5, 7, 6, "ptWkt"));
        gisFunctions.add(new GisFunction("PolyFromText", 5, 5, 1, 5, 7, 6, "plWkt"));
        gisFunctions.add(new GisFunction("PolygonFromText", 5, 5, 1, 5, 7, 6, "plWkt"));
        gisFunctions.add(new GisFunction("ST_GeomCollFromText", 5, 6, 1, 0, 0, 0, "gcWkt"));
        gisFunctions.add(new GisFunction("ST_GeometryCollectionFromText", 5, 6, 1, 0, 0, 0, "gcWkt"));
        gisFunctions.add(new GisFunction("ST_GeomCollFromTxt", 5, 7, 6, 0, 0, 0, "gcWkt"));
        gisFunctions.add(new GisFunction("ST_GeomFromText", 5, 6, 1, 0, 0, 0, "gWkt"));
        gisFunctions.add(new GisFunction("ST_GeometryFromText", 5, 6, 1, 0, 0, 0, "gWkt"));
        gisFunctions.add(new GisFunction("ST_LineFromText", 5, 6, 1, 0, 0, 0, "lsWkt"));
        gisFunctions.add(new GisFunction("ST_LineStringFromText", 5, 6, 1, 0, 0, 0, "lsWkt"));
        gisFunctions.add(new GisFunction("ST_MLineFromText", 5, 7, 6, 0, 0, 0, "mlsWkt"));
        gisFunctions.add(new GisFunction("ST_MultiLineStringFromText", 5, 7, 6, 0, 0, 0, "mlsWkt"));
        gisFunctions.add(new GisFunction("ST_MPointFromText", 5, 7, 6, 0, 0, 0, "mptWkt"));
        gisFunctions.add(new GisFunction("ST_MultiPointFromText", 5, 7, 6, 0, 0, 0, "mptWkt"));
        gisFunctions.add(new GisFunction("ST_MPolyFromText", 5, 7, 6, 0, 0, 0, "mplWkt"));
        gisFunctions.add(new GisFunction("ST_MultiPolygonFromText", 5, 7, 6, 0, 0, 0, "mplWkt"));
        gisFunctions.add(new GisFunction("ST_PointFromText", 5, 6, 1, 0, 0, 0, "ptWkt"));
        gisFunctions.add(new GisFunction("ST_PolyFromText", 5, 6, 1, 0, 0, 0, "plWkt"));
        gisFunctions.add(new GisFunction("ST_PolygonFromText", 5, 6, 1, 0, 0, 0, "plWkt"));
        // Functions That Create Geometry Values from Geometry/WKB Values
        gisFunctions.add(new GisFunction("GeomCollFromWKB", 5, 5, 1, 5, 7, 6, "gcGeo"));
        gisFunctions.add(new GisFunction("GeometryCollectionFromWKB", 5, 5, 1, 5, 7, 6, "gcGeo"));
        gisFunctions.add(new GisFunction("GeomFromWKB", 5, 5, 1, 5, 7, 6, "gGeo"));
        gisFunctions.add(new GisFunction("GeometryFromWKB", 5, 5, 1, 5, 7, 6, "gGeo"));
        gisFunctions.add(new GisFunction("LineFromWKB", 5, 5, 1, 5, 7, 6, "lsGeo"));
        gisFunctions.add(new GisFunction("LineStringFromWKB", 5, 5, 1, 5, 7, 6, "lsGeo"));
        gisFunctions.add(new GisFunction("MLineFromWKB", 5, 5, 1, 5, 7, 6, "mlsGeo"));
        gisFunctions.add(new GisFunction("MultiLineStringFromWKB", 5, 5, 1, 5, 7, 6, "mlsGeo"));
        gisFunctions.add(new GisFunction("MPointFromWKB", 5, 5, 1, 5, 7, 6, "mptGeo"));
        gisFunctions.add(new GisFunction("MultiPointFromWKB", 5, 5, 1, 5, 7, 6, "mptGeo"));
        gisFunctions.add(new GisFunction("MPolyFromWKB", 5, 5, 1, 5, 7, 6, "mplGeo"));
        gisFunctions.add(new GisFunction("MultiPolygonFromWKB", 5, 5, 1, 5, 7, 6, "mplGeo"));
        gisFunctions.add(new GisFunction("PointFromWKB", 5, 5, 1, 5, 7, 6, "ptGeo"));
        gisFunctions.add(new GisFunction("PolyFromWKB", 5, 5, 1, 5, 7, 6, "plGeo"));
        gisFunctions.add(new GisFunction("PolygonFromWKB", 5, 5, 1, 5, 7, 6, "plGeo"));
        gisFunctions.add(new GisFunction("ST_GeomCollFromWKB", 5, 6, 1, 8, 0, 0, "gcGeo"));
        gisFunctions.add(new GisFunction("ST_GeomCollFromWKB", 5, 6, 1, 0, 0, 0, "gcWkb"));
        gisFunctions.add(new GisFunction("ST_GeomCollFromWKB", 5, 6, 1, 0, 0, 0, "gcWkb", "0"));
        gisFunctions.add(new GisFunction("ST_GeomCollFromWKB", 8, 0, 1, 0, 0, 0, "gcWkb", "0", "'axis-order=srid-defined'"));
        gisFunctions.add(new GisFunction("ST_GeometryCollectionFromWKB", 5, 6, 1, 8, 0, 0, "gcGeo"));
        gisFunctions.add(new GisFunction("ST_GeometryCollectionFromWKB", 5, 6, 1, 0, 0, 0, "gcWkb"));
        gisFunctions.add(new GisFunction("ST_GeometryCollectionFromWKB", 5, 6, 1, 0, 0, 0, "gcWkb", "0"));
        gisFunctions.add(new GisFunction("ST_GeometryCollectionFromWKB", 8, 0, 1, 0, 0, 0, "gcWkb", "0", "'axis-order=srid-defined'"));
        gisFunctions.add(new GisFunction("ST_GeomFromWKB", 5, 6, 1, 8, 0, 0, "gGeo"));
        gisFunctions.add(new GisFunction("ST_GeomFromWKB", 5, 6, 1, 0, 0, 0, "gWkb"));
        gisFunctions.add(new GisFunction("ST_GeomFromWKB", 5, 6, 1, 0, 0, 0, "gWkb", "0"));
        gisFunctions.add(new GisFunction("ST_GeomFromWKB", 8, 0, 1, 0, 0, 0, "gWkb", "0", "'axis-order=srid-defined'"));
        gisFunctions.add(new GisFunction("ST_GeometryFromWKB", 5, 6, 1, 8, 0, 0, "gGeo"));
        gisFunctions.add(new GisFunction("ST_GeometryFromWKB", 5, 6, 1, 0, 0, 0, "gWkb"));
        gisFunctions.add(new GisFunction("ST_GeometryFromWKB", 5, 6, 1, 0, 0, 0, "gWkb", "0"));
        gisFunctions.add(new GisFunction("ST_GeometryFromWKB", 8, 0, 1, 0, 0, 0, "gWkb", "0", "'axis-order=srid-defined'"));
        gisFunctions.add(new GisFunction("ST_LineFromWKB", 5, 6, 1, 8, 0, 0, "lsGeo"));
        gisFunctions.add(new GisFunction("ST_LineFromWKB", 5, 6, 1, 0, 0, 0, "lsWkb"));
        gisFunctions.add(new GisFunction("ST_LineFromWKB", 5, 6, 1, 0, 0, 0, "lsWkb", "0"));
        gisFunctions.add(new GisFunction("ST_LineFromWKB", 8, 0, 1, 0, 0, 0, "lsWkb", "0", "'axis-order=srid-defined'"));
        gisFunctions.add(new GisFunction("ST_LineStringFromWKB", 5, 6, 1, 8, 0, 0, "lsGeo"));
        gisFunctions.add(new GisFunction("ST_LineStringFromWKB", 5, 6, 1, 0, 0, 0, "lsWkb"));
        gisFunctions.add(new GisFunction("ST_LineStringFromWKB", 5, 6, 1, 0, 0, 0, "lsWkb", "0"));
        gisFunctions.add(new GisFunction("ST_LineStringFromWKB", 8, 0, 1, 0, 0, 0, "lsWkb", "0", "'axis-order=srid-defined'"));
        gisFunctions.add(new GisFunction("ST_MLineFromWKB", 5, 7, 6, 8, 0, 0, "mlsGeo"));
        gisFunctions.add(new GisFunction("ST_MLineFromWKB", 5, 7, 6, 0, 0, 0, "mlsWkb"));
        gisFunctions.add(new GisFunction("ST_MLineFromWKB", 5, 7, 6, 0, 0, 0, "mlsWkb", "0"));
        gisFunctions.add(new GisFunction("ST_MLineFromWKB", 8, 0, 1, 0, 0, 0, "mlsWkb", "0", "'axis-order=srid-defined'"));
        gisFunctions.add(new GisFunction("ST_MultiLineStringFromWKB", 5, 7, 6, 8, 0, 0, "mlsGeo"));
        gisFunctions.add(new GisFunction("ST_MultiLineStringFromWKB", 5, 7, 6, 0, 0, 0, "mlsWkb"));
        gisFunctions.add(new GisFunction("ST_MultiLineStringFromWKB", 5, 7, 6, 0, 0, 0, "mlsWkb", "0"));
        gisFunctions.add(new GisFunction("ST_MultiLineStringFromWKB", 8, 0, 1, 0, 0, 0, "mlsWkb", "0", "'axis-order=srid-defined'"));
        gisFunctions.add(new GisFunction("ST_MPointFromWKB", 5, 7, 6, 8, 0, 0, "mptGeo"));
        gisFunctions.add(new GisFunction("ST_MPointFromWKB", 5, 7, 6, 0, 0, 0, "mptWkb"));
        gisFunctions.add(new GisFunction("ST_MPointFromWKB", 5, 7, 6, 0, 0, 0, "mptWkb", "0"));
        gisFunctions.add(new GisFunction("ST_MPointFromWKB", 8, 0, 1, 0, 0, 0, "mptWkb", "0", "'axis-order=srid-defined'"));
        gisFunctions.add(new GisFunction("ST_MultiPointFromWKB", 5, 7, 6, 8, 0, 0, "mptGeo"));
        gisFunctions.add(new GisFunction("ST_MultiPointFromWKB", 5, 7, 6, 0, 0, 0, "mptWkb"));
        gisFunctions.add(new GisFunction("ST_MultiPointFromWKB", 5, 7, 6, 0, 0, 0, "mptWkb", "0"));
        gisFunctions.add(new GisFunction("ST_MultiPointFromWKB", 8, 0, 1, 0, 0, 0, "mptWkb", "0", "'axis-order=srid-defined'"));
        gisFunctions.add(new GisFunction("ST_MPolyFromWKB", 5, 7, 6, 8, 0, 0, "mplGeo"));
        gisFunctions.add(new GisFunction("ST_MPolyFromWKB", 5, 7, 6, 0, 0, 0, "mplWkb"));
        gisFunctions.add(new GisFunction("ST_MPolyFromWKB", 5, 7, 6, 0, 0, 0, "mplWkb", "0"));
        gisFunctions.add(new GisFunction("ST_MPolyFromWKB", 8, 0, 1, 0, 0, 0, "mplWkb", "0", "'axis-order=srid-defined'"));
        gisFunctions.add(new GisFunction("ST_MultiPolygonFromWKB", 5, 7, 6, 8, 0, 0, "mplGeo"));
        gisFunctions.add(new GisFunction("ST_MultiPolygonFromWKB", 5, 7, 6, 0, 0, 0, "mplWkb"));
        gisFunctions.add(new GisFunction("ST_MultiPolygonFromWKB", 5, 7, 6, 0, 0, 0, "mplWkb", "0"));
        gisFunctions.add(new GisFunction("ST_MultiPolygonFromWKB", 8, 0, 1, 0, 0, 0, "mplWkb", "0", "'axis-order=srid-defined'"));
        gisFunctions.add(new GisFunction("ST_PointFromWKB", 5, 6, 1, 8, 0, 0, "ptGeo"));
        gisFunctions.add(new GisFunction("ST_PointFromWKB", 5, 6, 1, 0, 0, 0, "ptWkb"));
        gisFunctions.add(new GisFunction("ST_PointFromWKB", 5, 6, 1, 0, 0, 0, "ptWkb", "0"));
        gisFunctions.add(new GisFunction("ST_PointFromWKB", 8, 0, 1, 0, 0, 0, "ptWkb", "0", "'axis-order=srid-defined'"));
        gisFunctions.add(new GisFunction("ST_PolyFromWKB", 5, 6, 1, 8, 0, 0, "plGeo"));
        gisFunctions.add(new GisFunction("ST_PolyFromWKB", 5, 6, 1, 0, 0, 0, "plWkb"));
        gisFunctions.add(new GisFunction("ST_PolyFromWKB", 5, 6, 1, 0, 0, 0, "plWkb", "0"));
        gisFunctions.add(new GisFunction("ST_PolyFromWKB", 8, 0, 1, 0, 0, 0, "plWkb", "0", "'axis-order=srid-defined'"));
        gisFunctions.add(new GisFunction("ST_PolygonFromWKB", 5, 6, 1, 8, 0, 0, "plGeo"));
        gisFunctions.add(new GisFunction("ST_PolygonFromWKB", 5, 6, 1, 0, 0, 0, "plWkb"));
        gisFunctions.add(new GisFunction("ST_PolygonFromWKB", 5, 6, 1, 0, 0, 0, "plWkb", "0"));
        gisFunctions.add(new GisFunction("ST_PolygonFromWKB", 8, 0, 1, 0, 0, 0, "plWkb", "0", "'axis-order=srid-defined'"));
        // MySQL-Specific Functions That Create Geometry Values
        gisFunctions.add(new GisFunction("GeometryCollection", 5, 5, 1, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("LineString", 5, 5, 1, 0, 0, 0, "pt1", "pt2"));
        gisFunctions.add(new GisFunction("MultiLineString", 5, 5, 1, 0, 0, 0, "ls1", "ls2"));
        gisFunctions.add(new GisFunction("MultiPoint", 5, 5, 1, 0, 0, 0, "pt1", "pt2"));
        gisFunctions.add(new GisFunction("MultiPolygon", 5, 5, 1, 0, 0, 0, "pl1", "pl2"));
        gisFunctions.add(new GisFunction("Point", 5, 5, 1, 0, 0, 0, "4", "6"));
        gisFunctions.add(new GisFunction("Polygon", 5, 5, 1, 0, 0, 0, "ls1", "ls2"));
        // Geometry Format Conversion Functions
        gisFunctions.add(new GisFunction("AsBinary", 5, 5, 1, 5, 7, 6, "g"));
        gisFunctions.add(new GisFunction("AsWKB", 5, 5, 1, 5, 7, 6, "g"));
        gisFunctions.add(new GisFunction("AsText", 5, 5, 1, 5, 7, 6, "g"));
        gisFunctions.add(new GisFunction("AsWKT", 5, 5, 1, 5, 7, 6, "g"));
        gisFunctions.add(new GisFunction("ST_AsBinary", 5, 6, 1, 0, 0, 0, "g"));
        gisFunctions.add(new GisFunction("ST_AsWKB", 5, 6, 1, 0, 0, 0, "g"));
        gisFunctions.add(new GisFunction("ST_AsText", 5, 6, 1, 0, 0, 0, "g"));
        gisFunctions.add(new GisFunction("ST_AsWKT", 5, 6, 1, 0, 0, 0, "g"));
        // General Geometry Property Functions
        gisFunctions.add(new GisFunction("Dimension", 5, 5, 1, 5, 7, 6, "g"));
        gisFunctions.add(new GisFunction("Envelope", 5, 5, 1, 5, 7, 6, "g"));
        gisFunctions.add(new GisFunction("GeometryType", 5, 5, 1, 5, 7, 6, "g"));
        gisFunctions.add(new GisFunction("IsEmpty", 5, 5, 1, 5, 7, 6, "g"));
        gisFunctions.add(new GisFunction("IsSimple", 5, 5, 1, 5, 7, 6, "g"));
        gisFunctions.add(new GisFunction("SRID", 5, 5, 1, 5, 7, 6, "g"));
        gisFunctions.add(new GisFunction("ST_Dimension", 5, 6, 1, 0, 0, 0, "g"));
        gisFunctions.add(new GisFunction("ST_Envelope", 5, 6, 1, 0, 0, 0, "g"));
        gisFunctions.add(new GisFunction("ST_GeometryType", 5, 6, 1, 0, 0, 0, "g"));
        gisFunctions.add(new GisFunction("ST_IsEmpty", 5, 6, 1, 0, 0, 0, "g"));
        gisFunctions.add(new GisFunction("ST_IsSimple", 5, 6, 1, 0, 0, 0, "g"));
        gisFunctions.add(new GisFunction("ST_SRID", 5, 6, 1, 0, 0, 0, "g"));
        // Point Property Functions
        gisFunctions.add(new GisFunction("X", 5, 5, 1, 5, 7, 6, "pt"));
        gisFunctions.add(new GisFunction("Y", 5, 5, 1, 5, 7, 6, "pt"));
        gisFunctions.add(new GisFunction("ST_X", 5, 6, 1, 0, 0, 0, "pt"));
        gisFunctions.add(new GisFunction("ST_Y", 5, 6, 1, 0, 0, 0, "pt"));
        // LineString and MultiLineString Property Functions
        gisFunctions.add(new GisFunction("EndPoint", 5, 5, 1, 5, 7, 6, "ls"));
        gisFunctions.add(new GisFunction("GLength", 5, 5, 1, 5, 7, 6, "ls"));
        gisFunctions.add(new GisFunction("IsClosed", 5, 5, 1, 5, 7, 6, "ls"));
        gisFunctions.add(new GisFunction("NumPoints", 5, 5, 1, 5, 7, 6, "ls"));
        gisFunctions.add(new GisFunction("PointN", 5, 5, 1, 5, 7, 6, "ls", "2"));
        gisFunctions.add(new GisFunction("StartPoint", 5, 5, 1, 5, 7, 6, "ls"));
        gisFunctions.add(new GisFunction("ST_EndPoint", 5, 6, 1, 0, 0, 0, "ls"));
        gisFunctions.add(new GisFunction("ST_IsClosed", 5, 6, 1, 0, 0, 0, "ls"));
        gisFunctions.add(new GisFunction("ST_Length", 5, 7, 6, 0, 0, 0, "ls"));
        gisFunctions.add(new GisFunction("ST_NumPoints", 5, 6, 1, 0, 0, 0, "ls"));
        gisFunctions.add(new GisFunction("ST_PointN", 5, 6, 1, 0, 0, 0, "ls", "2"));
        gisFunctions.add(new GisFunction("ST_StartPoint", 5, 6, 1, 0, 0, 0, "ls"));
        // Polygon and MultiPolygon Property Functions
        gisFunctions.add(new GisFunction("Area", 5, 5, 1, 5, 7, 6, "pl"));
        gisFunctions.add(new GisFunction("Centroid", 5, 5, 1, 5, 7, 6, "mpl"));
        gisFunctions.add(new GisFunction("ExteriorRing", 5, 5, 1, 5, 7, 6, "pl"));
        gisFunctions.add(new GisFunction("InteriorRingN", 5, 5, 1, 5, 7, 6, "pl", "1"));
        gisFunctions.add(new GisFunction("NumInteriorRings", 5, 5, 1, 5, 7, 6, "pl"));
        gisFunctions.add(new GisFunction("ST_Area", 5, 6, 1, 0, 0, 0, "pl"));
        gisFunctions.add(new GisFunction("ST_Centroid", 5, 6, 1, 0, 0, 0, "mpl"));
        gisFunctions.add(new GisFunction("ST_ExteriorRing", 5, 6, 1, 0, 0, 0, "pl"));
        gisFunctions.add(new GisFunction("ST_InteriorRingN", 5, 6, 1, 0, 0, 0, "pl", "1"));
        gisFunctions.add(new GisFunction("ST_NumInteriorRing", 5, 7, 8, 0, 0, 0, "pl"));
        gisFunctions.add(new GisFunction("ST_NumInteriorRings ", 5, 6, 1, 0, 0, 0, "pl"));
        // GeometryCollection Property Functions
        gisFunctions.add(new GisFunction("GeometryN", 5, 5, 1, 5, 7, 6, "gc", "2"));
        gisFunctions.add(new GisFunction("NumGeometries", 5, 5, 1, 5, 7, 6, "gc"));
        gisFunctions.add(new GisFunction("ST_GeometryN", 5, 6, 1, 0, 0, 0, "gc", "2"));
        gisFunctions.add(new GisFunction("ST_NumGeometries", 5, 6, 1, 0, 0, 0, "gc"));
        // Spatial Operator Functions
        gisFunctions.add(new GisFunction("Buffer", 5, 6, 1, 5, 7, 6, "g", "1"));
        gisFunctions.add(new GisFunction("ConvexHull", 5, 7, 5, 5, 7, 6, "g"));
        gisFunctions.add(new GisFunction("ST_Buffer", 5, 6, 1, 0, 0, 0, "g", "1"));
        gisFunctions.add(new GisFunction("ST_Buffer_Strategy", 5, 7, 7, 0, 0, 0, "'point_circle'", "2"));
        gisFunctions.add(new GisFunction("ST_ConvexHull", 5, 7, 5, 0, 0, 0, "g"));
        gisFunctions.add(new GisFunction("ST_Difference", 5, 6, 1, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("ST_Intersection", 5, 6, 1, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("ST_SymDifference", 5, 6, 1, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("ST_Union", 5, 6, 1, 0, 0, 0, "g1", "g2"));
        // Spatial Relation Functions That Use Object Shapes
        gisFunctions.add(new GisFunction("Crosses", 5, 5, 1, 5, 7, 6, "g1", "g2"));
        gisFunctions.add(new GisFunction("Distance", 5, 7, 5, 5, 7, 6, "g1", "g2"));
        gisFunctions.add(new GisFunction("Touches", 5, 5, 1, 5, 7, 6, "g1", "g2"));
        gisFunctions.add(new GisFunction("ST_Contains", 5, 6, 1, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("ST_Crosses", 5, 6, 1, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("ST_Disjoint", 5, 6, 1, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("ST_Distance", 5, 6, 1, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("ST_Equals", 5, 6, 1, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("ST_Intersects", 5, 6, 1, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("ST_Overlaps", 5, 6, 1, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("ST_Touches", 5, 6, 1, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("ST_Within", 5, 6, 1, 0, 0, 0, "g1", "g2"));
        // Spatial Relation Functions That Use Minimum Bounding Rectangles (MBRs)
        gisFunctions.add(new GisFunction("Contains", 5, 5, 1, 5, 7, 6, "g1", "g2"));
        gisFunctions.add(new GisFunction("Disjoint", 5, 5, 1, 5, 7, 6, "g1", "g2"));
        gisFunctions.add(new GisFunction("Equals", 5, 5, 1, 5, 7, 6, "g1", "g2"));
        gisFunctions.add(new GisFunction("Intersects", 5, 5, 1, 5, 7, 6, "g1", "g2"));
        gisFunctions.add(new GisFunction("Overlaps", 5, 5, 1, 5, 7, 6, "g1", "g2"));
        gisFunctions.add(new GisFunction("Within", 5, 5, 1, 5, 7, 6, "g1", "g2"));
        gisFunctions.add(new GisFunction("MBRContains", 5, 5, 1, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("MBRCoveredBy", 5, 7, 6, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("MBRCovers", 5, 7, 6, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("MBRDisjoint", 5, 5, 1, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("MBREqual", 5, 5, 1, 5, 7, 6, "g1", "g2"));
        gisFunctions.add(new GisFunction("MBREquals", 5, 7, 6, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("MBRIntersects", 5, 5, 1, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("MBROverlaps", 5, 5, 1, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("MBRTouches", 5, 5, 1, 0, 0, 0, "g1", "g2"));
        gisFunctions.add(new GisFunction("MBRWithin", 5, 5, 1, 0, 0, 0, "g1", "g2"));
        // Spatial Geohash Functions
        gisFunctions.add(new GisFunction("ST_GeoHash", 5, 7, 5, 0, 0, 0, "pt", "20"));
        gisFunctions.add(new GisFunction("ST_LatFromGeoHash", 5, 7, 5, 0, 0, 0, "gh"));
        gisFunctions.add(new GisFunction("ST_LongFromGeoHash", 5, 7, 5, 0, 0, 0, "gh"));
        gisFunctions.add(new GisFunction("ST_PointFromGeoHash", 5, 7, 5, 0, 0, 0, "gh", "0"));
        // Spatial GeoJSON Functions
        gisFunctions.add(new GisFunction("ST_AsGeoJSON", 5, 7, 5, 0, 0, 0, "g"));
        gisFunctions.add(new GisFunction("ST_GeomFromGeoJSON", 5, 7, 5, 0, 0, 0, "js"));
        // Spatial Convenience Functions
        gisFunctions.add(new GisFunction("ST_Distance_Sphere", 5, 7, 6, 0, 0, 0, "pt1", "pt2"));
        gisFunctions.add(new GisFunction("ST_IsValid", 5, 7, 6, 0, 0, 0, "g"));
        gisFunctions.add(new GisFunction("ST_MakeEnvelope", 5, 7, 6, 0, 0, 0, "pt1", "pt2"));
        gisFunctions.add(new GisFunction("ST_Simplify", 5, 7, 6, 0, 0, 0, "g", "1"));
        gisFunctions.add(new GisFunction("ST_Validate", 5, 7, 6, 0, 0, 0, "g"));

        for (GisFunction gf : gisFunctions) {
            if (versionMeetsMinimum(gf.low_version_maj, gf.low_version_min, gf.low_version_sub)
                    && (gf.hi_version_maj == 0 || !versionMeetsMinimum(gf.hi_version_maj, gf.hi_version_min, gf.hi_version_sub))) {
                final StringBuilder sql = new StringBuilder("SELECT ");
                sql.append(gf.function).append("(");
                String sep = "";
                for (String arg : gf.args) {
                    sql.append(sep);
                    sep = ", ";
                    if (args.containsKey(arg)) {
                        sql.append(args.get(arg));
                    } else {
                        sql.append(arg);
                    }
                }
                sql.append(")");

                this.rs = this.stmt.executeQuery(sql.toString());
                assertTrue("Query should return one row.", this.rs.next());
                assertFalse("Query should return exactly one row.", this.rs.next());

                this.pstmt = this.conn.prepareStatement(sql.toString());
                this.rs = this.pstmt.executeQuery();
                assertTrue("Query should return one row.", this.rs.next());
                assertFalse("Query should return exactly one row.", this.rs.next());
            }
        }
    }

    /**
     * WL#8252 - GCS Replication: Plugin [SERVER CHANGES]
     * 
     * Test syntax for GCS Replication commands:
     * - START GROUP_REPLICATION
     * - STOP GROUP_REPLICATION
     */
    public void testGcsReplicationCmds() throws Exception {
        if (!versionMeetsMinimum(5, 7, 6)) {
            return;
        }
        String expectedErrMsg = "The server is not configured properly to be an active member of the group\\. Please see more details on error log\\.";
        final Statement testStmt = this.stmt;
        assertThrows(SQLException.class, expectedErrMsg, new Callable<Void>() {
            public Void call() throws Exception {
                testStmt.execute("START GROUP_REPLICATION");
                return null;
            }
        });
        assertThrows(SQLException.class, expectedErrMsg, new Callable<Void>() {
            public Void call() throws Exception {
                testStmt.execute("STOP GROUP_REPLICATION");
                return null;
            }
        });

        Connection spsConn = getConnectionWithProps("useServerPrepStmts=true");
        for (Connection testConn : new Connection[] { this.conn, spsConn }) {
            final PreparedStatement testPstmt1 = testConn.prepareStatement("START GROUP_REPLICATION");
            assertThrows(SQLException.class, expectedErrMsg, new Callable<Void>() {
                public Void call() throws Exception {
                    testPstmt1.execute();
                    return null;
                }
            });
            final PreparedStatement testPstmt2 = testConn.prepareStatement("STOP GROUP_REPLICATION");
            assertThrows(SQLException.class, expectedErrMsg, new Callable<Void>() {
                public Void call() throws Exception {
                    testPstmt2.execute();
                    return null;
                }
            });
        }
        spsConn.close();
    }

    /**
     * WL#6054 - Temporarily disablement of users
     * 
     * Test user account locking syntax:
     * 
     * CREATE|ALTER USER (...)
     * - lock_option: { ACCOUNT LOCK | ACCOUNT UNLOCK }
     */
    public void testUserAccountLocking() throws Exception {
        if (!versionMeetsMinimum(5, 7, 6)) {
            return;
        }

        final String user = "testAccLck";
        final String pwd = "testAccLck";
        final Properties props = new Properties();
        props.setProperty(PropertyKey.USER.getKeyName(), user);
        props.setProperty(PropertyKey.PASSWORD.getKeyName(), pwd);

        for (String accLock : new String[] { "/* default */", "ACCOUNT UNLOCK", "ACCOUNT LOCK" }) {
            createUser("'" + user + "'@'%'", "IDENTIFIED BY '" + pwd + "' " + accLock);
            this.stmt.execute("GRANT SELECT ON *.* TO '" + user + "'@'%'");

            if (accLock.equals("ACCOUNT LOCK")) {
                assertThrows("Test case: " + accLock + ",", SQLException.class, "Access denied for user '" + user + "'@'.*'\\. Account is locked\\.",
                        new Callable<Void>() {
                            public Void call() throws Exception {
                                getConnectionWithProps(props);
                                return null;
                            }
                        });
                this.stmt.execute("ALTER USER '" + user + "'@'%' ACCOUNT UNLOCK");
            }

            final Connection testConn1 = getConnectionWithProps(props);
            assertTrue("Test case: " + accLock + ",", testConn1.createStatement().executeQuery("SELECT 1").next());

            this.stmt.execute("ALTER USER '" + user + "'@'%' ACCOUNT LOCK");
            assertTrue("Test case: " + accLock + ",", testConn1.createStatement().executeQuery("SELECT 1").next()); // Previous authentication still valid.

            assertThrows("Test case: " + accLock + ",", SQLException.class, "Access denied for user '" + user + "'@'.*'\\. Account is locked\\.",
                    new Callable<Void>() {
                        public Void call() throws Exception {
                            ((JdbcConnection) testConn1).changeUser(user, pwd);
                            return null;
                        }
                    });
            assertFalse("Test case: " + accLock + ",", testConn1.isClosed());
            assertThrows("Test case: " + accLock + ",", SQLException.class, "(?s)Communications link failure.*", new Callable<Void>() {
                public Void call() throws Exception {
                    testConn1.createStatement().executeQuery("SELECT 1");
                    return null;
                }
            });
            assertTrue("Test case: " + accLock + ",", testConn1.isClosed());

            this.stmt.execute("ALTER USER '" + user + "'@'%' ACCOUNT UNLOCK");
            Connection testConn2 = getConnectionWithProps(props);
            assertTrue("Test case: " + accLock + ",", testConn2.createStatement().executeQuery("SELECT 1").next());
            testConn2.close();

            dropUser("'" + user + "'@'%'");
        }
    }

    /**
     * WL#7131 - Add timestamp in mysql.user on the last time the password was changed
     * 
     * Test user account password expiration syntax:
     * 
     * CREATE|ALTER USER (...)
     * - password_option: { PASSWORD EXPIRE | PASSWORD EXPIRE DEFAULT | PASSWORD EXPIRE NEVER | PASSWORD EXPIRE INTERVAL N DAY }
     */
    public void testUserAccountPwdExpiration() throws Exception {
        if (!versionMeetsMinimum(5, 7, 6)) {
            return;
        }

        final String user = "testAccPwdExp";
        final String pwd = "testAccPwdExp";
        final Properties props = new Properties();
        props.setProperty(PropertyKey.USER.getKeyName(), user);
        props.setProperty(PropertyKey.PASSWORD.getKeyName(), pwd);

        // CREATE USER syntax.
        for (String accPwdExp : new String[] { "/* default */", "PASSWORD EXPIRE", "PASSWORD EXPIRE DEFAULT", "PASSWORD EXPIRE NEVER",
                "PASSWORD EXPIRE INTERVAL 365 DAY" }) {
            createUser("'" + user + "'@'%'", "IDENTIFIED BY '" + pwd + "' " + accPwdExp);
            this.stmt.execute("GRANT SELECT ON *.* TO '" + user + "'@'%'");

            if (accPwdExp.equals("PASSWORD EXPIRE")) {
                assertThrows(SQLException.class, "Your password has expired\\. To log in you must change it using a client that supports expired passwords\\.",
                        new Callable<Void>() {
                            public Void call() throws Exception {
                                getConnectionWithProps(props);
                                return null;
                            }
                        });
            } else {
                Connection testConn = getConnectionWithProps(props);
                assertTrue("Test case: " + accPwdExp + ",", testConn.createStatement().executeQuery("SELECT 1").next());
                testConn.close();
            }

            dropUser("'" + user + "'@'%'");
        }

        // ALTER USER syntax.
        for (String accPwdExp : new String[] { "PASSWORD EXPIRE", "PASSWORD EXPIRE DEFAULT", "PASSWORD EXPIRE NEVER", "PASSWORD EXPIRE INTERVAL 365 DAY" }) {
            createUser("'" + user + "'@'%'", "IDENTIFIED BY '" + pwd + "'");
            this.stmt.execute("GRANT SELECT ON *.* TO '" + user + "'@'%'");

            final Connection testConn = getConnectionWithProps(props);
            assertTrue("Test case: " + accPwdExp + ",", testConn.createStatement().executeQuery("SELECT 1").next());

            this.stmt.execute("ALTER USER '" + user + "'@'%' " + accPwdExp);
            assertTrue("Test case: " + accPwdExp + ",", testConn.createStatement().executeQuery("SELECT 1").next());

            if (accPwdExp.equals("PASSWORD EXPIRE")) {
                assertThrows(SQLException.class, "Your password has expired\\. To log in you must change it using a client that supports expired passwords\\.",
                        new Callable<Void>() {
                            public Void call() throws Exception {
                                ((JdbcConnection) testConn).changeUser(user, pwd);
                                return null;
                            }
                        });
            } else {
                ((JdbcConnection) testConn).changeUser(user, pwd);
                assertTrue("Test case: " + accPwdExp + ",", testConn.createStatement().executeQuery("SELECT 1").next());
            }

            testConn.close();
            dropUser("'" + user + "'@'%'");
        }
    }

    /**
     * WL#8548 - InnoDB: Transparent data encryption.
     * WL#8821 - Innodb tablespace encryption key rotation SQL commands.
     * 
     * Test new syntax:
     * - CREATE|ALTER TABLE (...) ENCRYPTION [=] {'Y' | 'N'}
     * - ALTER INSTANCE ROTATE INNODB MASTER KEY
     */
    public void testInnodbTablespaceEncryption() throws Exception {
        if (!versionMeetsMinimum(5, 7, 11)) {
            return;
        }

        boolean keyringPluginIsActive = false;
        this.rs = this.stmt.executeQuery("SELECT (PLUGIN_STATUS='ACTIVE') AS `TRUE` FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME LIKE 'keyring_file'");
        if (this.rs.next()) {
            keyringPluginIsActive = this.rs.getBoolean(1);
        }

        if (keyringPluginIsActive) {
            createTable("testInnodbTablespaceEncryption", "(id INT, txt VARCHAR(100)) ENCRYPTION='y'");

            this.stmt.executeUpdate("INSERT INTO testInnodbTablespaceEncryption VALUES (123, 'this is a test')");
            this.rs = this.stmt.executeQuery("SELECT * FROM testInnodbTablespaceEncryption");
            assertTrue(this.rs.next());
            assertEquals(123, this.rs.getInt(1));
            assertEquals("this is a test", this.rs.getString(2));
            assertFalse(this.rs.next());

            this.stmt.execute("ALTER INSTANCE ROTATE INNODB MASTER KEY");
            this.rs = this.stmt.executeQuery("SELECT * FROM testInnodbTablespaceEncryption");
            assertTrue(this.rs.next());
            assertEquals(123, this.rs.getInt(1));
            assertEquals("this is a test", this.rs.getString(2));
            assertFalse(this.rs.next());

            this.stmt.execute("ALTER TABLE testInnodbTablespaceEncryption ENCRYPTION='n'");
            this.rs = this.stmt.executeQuery("SELECT * FROM testInnodbTablespaceEncryption");
            assertTrue(this.rs.next());
            assertEquals(123, this.rs.getInt(1));
            assertEquals("this is a test", this.rs.getString(2));
            assertFalse(this.rs.next());

        } else { // Syntax can still be tested by with different outcome.
            System.out.println("Although not required it is recommended that the 'keyring_file' plugin is properly installed and configured to run this test.");

            String err = versionMeetsMinimum(8, 0, 4) || versionMeetsMinimum(5, 7, 22) && !versionMeetsMinimum(8, 0, 0)
                    ? "Can't find master key from keyring, please check in the server log if a keyring plugin is loaded and initialized successfully."
                    : "Can't find master key from keyring, please check keyring plugin is loaded.";

            final Statement testStmt = this.conn.createStatement();
            assertThrows(SQLException.class, err, new Callable<Void>() {
                public Void call() throws Exception {
                    testStmt.execute("CREATE TABLE testInnodbTablespaceEncryption (id INT) ENCRYPTION='y'");
                    testStmt.execute("DROP TABLE testInnodbTablespaceEncryption");
                    return null;
                }
            });
            assertThrows(SQLException.class, err, new Callable<Void>() {
                public Void call() throws Exception {
                    testStmt.execute("ALTER INSTANCE ROTATE INNODB MASTER KEY");
                    return null;
                }
            });
        }
    }
}
