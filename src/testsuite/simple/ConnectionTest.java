/*
  Copyright (c) 2002, 2017, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.simple;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Callable;

import com.mysql.jdbc.CharsetMapping;
import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.ResultSetInternalMethods;
import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.StringUtils;
import com.mysql.jdbc.Util;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import com.mysql.jdbc.log.StandardLogger;

import testsuite.BaseStatementInterceptor;
import testsuite.BaseTestCase;

/**
 * Tests java.sql.Connection functionality
 */
public class ConnectionTest extends BaseTestCase {
    /**
     * Constructor for ConnectionTest.
     * 
     * @param name
     *            the name of the test to run
     */
    public ConnectionTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(ConnectionTest.class);
    }

    /**
     * Tests catalog functionality
     * 
     * @throws Exception
     *             if an error occurs
     */
    public void testCatalog() throws Exception {
        String currentCatalog = this.conn.getCatalog();
        this.conn.setCatalog(currentCatalog);
        assertTrue(currentCatalog.equals(this.conn.getCatalog()));
    }

    /**
     * Tests a cluster connection for failover, requires a two-node cluster URL
     * specfied in com.mysql.jdbc.testsuite.ClusterUrl system proeprty.
     * 
     * @throws Exception
     */
    public void testClusterConnection() throws Exception {
        String url = System.getProperty("com.mysql.jdbc.testsuite.ClusterUrl");

        if ((url != null) && (url.length() > 0)) {
            Object versionNumObj = getSingleValueWithQuery("SHOW VARIABLES LIKE 'version'");

            if ((versionNumObj != null) && (versionNumObj.toString().indexOf("cluster") != -1)) {
                Connection clusterConn = null;
                Statement clusterStmt = null;

                try {
                    clusterConn = new NonRegisteringDriver().connect(url, null);

                    clusterStmt = clusterConn.createStatement();
                    clusterStmt.executeUpdate("DROP TABLE IF EXISTS testClusterConn");
                    clusterStmt.executeUpdate("CREATE TABLE testClusterConn (field1 INT) " + getTableTypeDecl() + " =ndbcluster");
                    clusterStmt.executeUpdate("INSERT INTO testClusterConn VALUES (1)");

                    clusterConn.setAutoCommit(false);

                    clusterStmt.execute("SELECT * FROM testClusterConn");
                    clusterStmt.executeUpdate("UPDATE testClusterConn SET field1=4");

                    // Kill the connection
                    @SuppressWarnings("unused")
                    String connectionId = getSingleValueWithQuery("SELECT CONNECTION_ID()").toString();

                    System.out.println("Please kill the MySQL server now and press return...");
                    System.in.read();

                    System.out.println("Waiting for TCP/IP timeout...");
                    Thread.sleep(10);

                    System.out.println("Attempting auto reconnect");

                    try {
                        clusterConn.setAutoCommit(true);
                        clusterConn.setAutoCommit(false);
                    } catch (SQLException sqlEx) {
                        System.out.println(sqlEx);
                    }

                    //
                    // Test that this 'new' connection is not read-only
                    //
                    clusterStmt.executeUpdate("UPDATE testClusterConn SET field1=5");

                    ResultSet rset = clusterStmt.executeQuery("SELECT * FROM testClusterConn WHERE field1=5");

                    assertTrue("One row should be returned", rset.next());
                } finally {
                    if (clusterStmt != null) {
                        clusterStmt.executeUpdate("DROP TABLE IF EXISTS testClusterConn");
                        clusterStmt.close();
                    }

                    if (clusterConn != null) {
                        clusterConn.close();
                    }
                }
            }
        }
    }

    /**
     * @throws Exception
     *             Old test was passing due to
     *             http://bugs.mysql.com/bug.php?id=989 which is fixed for 5.5+
     */
    public void testDeadlockDetection() throws Exception {
        try {
            this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_lock_wait_timeout'");
            this.rs.next();

            int timeoutSecs = this.rs.getInt(2);

            createTable("t1", "(id INTEGER, x INTEGER) ", "INNODB");
            this.stmt.executeUpdate("INSERT INTO t1 VALUES(0, 0)");
            this.conn.setAutoCommit(false);

            Properties props = new Properties();
            props.setProperty("includeInnodbStatusInDeadlockExceptions", "true");
            props.setProperty("includeThreadDumpInDeadlockExceptions", "true");

            Connection deadlockConn = getConnectionWithProps(props);
            deadlockConn.setAutoCommit(false);

            try {
                this.conn.createStatement().execute("SELECT * FROM t1 WHERE id=0 FOR UPDATE");

                // The following query should hang because con1 is locking the page
                deadlockConn.createStatement().executeUpdate("UPDATE t1 SET x=2 WHERE id=0");
            } finally {
                if (versionMeetsMinimum(5, 5)) {
                    this.conn.commit();
                    deadlockConn.commit();
                }
            }

            Thread.sleep(timeoutSecs * 2 * 1000);
        } catch (SQLException sqlEx) {
            System.out.println("Caught SQLException due to deadlock/lock timeout");
            System.out.println("SQLState: " + sqlEx.getSQLState());
            System.out.println("Vendor error: " + sqlEx.getErrorCode());
            System.out.println("Message: " + sqlEx.getMessage());

            //
            // Check whether the driver thinks it really is deadlock...
            //
            assertTrue(SQLError.SQL_STATE_ROLLBACK_SERIALIZATION_FAILURE.equals(sqlEx.getSQLState()));
            assertTrue(sqlEx.getErrorCode() == 1205);
            // Make sure INNODB Status is getting dumped into error message

            if (sqlEx.getMessage().indexOf("PROCESS privilege") != -1) {
                fail("This test requires user with process privilege");
            }

            assertTrue("Can't find INNODB MONITOR in:\n\n" + sqlEx.getMessage(), sqlEx.getMessage().indexOf("INNODB MONITOR") != -1);

            assertTrue("Can't find thread dump in:\n\n" + sqlEx.getMessage(),
                    sqlEx.getMessage().indexOf("testsuite.simple.ConnectionTest.testDeadlockDetection") != -1);

        } finally {
            this.conn.setAutoCommit(true);
        }
    }

    public void testCharsets() throws Exception {
        if (versionMeetsMinimum(4, 1)) {
            Properties props = new Properties();
            props.setProperty("useUnicode", "true");
            props.setProperty("characterEncoding", "UTF-8");

            Connection utfConn = getConnectionWithProps(props);

            this.stmt = utfConn.createStatement();

            createTable("t1", "(comment CHAR(32) ASCII NOT NULL,koi8_ru_f CHAR(32) CHARACTER SET koi8r NOT NULL) CHARSET=latin5");

            this.stmt.executeUpdate("ALTER TABLE t1 CHANGE comment comment CHAR(32) CHARACTER SET latin2 NOT NULL");
            this.stmt.executeUpdate("ALTER TABLE t1 ADD latin5_f CHAR(32) NOT NULL");
            this.stmt.executeUpdate("ALTER TABLE t1 CHARSET=latin2");
            this.stmt.executeUpdate("ALTER TABLE t1 ADD latin2_f CHAR(32) NOT NULL");
            this.stmt.executeUpdate("ALTER TABLE t1 DROP latin2_f, DROP latin5_f");

            this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment) VALUES ('a','LAT SMALL A')");
            /*
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('b','LAT SMALL B')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('c','LAT SMALL C')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('d','LAT SMALL D')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('e','LAT SMALL E')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('f','LAT SMALL F')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('g','LAT SMALL G')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('h','LAT SMALL H')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('i','LAT SMALL I')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('j','LAT SMALL J')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('k','LAT SMALL K')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('l','LAT SMALL L')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('m','LAT SMALL M')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('n','LAT SMALL N')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('o','LAT SMALL O')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('p','LAT SMALL P')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('q','LAT SMALL Q')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('r','LAT SMALL R')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('s','LAT SMALL S')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('t','LAT SMALL T')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('u','LAT SMALL U')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('v','LAT SMALL V')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('w','LAT SMALL W')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('x','LAT SMALL X')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('y','LAT SMALL Y')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('z','LAT SMALL Z')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('A','LAT CAPIT A')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('B','LAT CAPIT B')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('C','LAT CAPIT C')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('D','LAT CAPIT D')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('E','LAT CAPIT E')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('F','LAT CAPIT F')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('G','LAT CAPIT G')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('H','LAT CAPIT H')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('I','LAT CAPIT I')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('J','LAT CAPIT J')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('K','LAT CAPIT K')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('L','LAT CAPIT L')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('M','LAT CAPIT M')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('N','LAT CAPIT N')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('O','LAT CAPIT O')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('P','LAT CAPIT P')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('Q','LAT CAPIT Q')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('R','LAT CAPIT R')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('S','LAT CAPIT S')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('T','LAT CAPIT T')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('U','LAT CAPIT U')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('V','LAT CAPIT V')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('W','LAT CAPIT W')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('X','LAT CAPIT X')"); this.stmt.executeUpdate("INSERT
             * INTO t1 (koi8_ru_f,comment) VALUES ('Y','LAT CAPIT Y')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES ('Z','LAT CAPIT Z')");
             */

            String cyrillicSmallA = "\u0430";
            this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment) VALUES ('" + cyrillicSmallA + "','CYR SMALL A')");

            /*
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL BE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL VE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL GE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL DE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL IE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL IO')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL ZHE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL ZE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL I')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL KA')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL EL')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL EM')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL EN')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL O')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL PE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL ER')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL ES')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL TE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL U')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL EF')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL HA')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL TSE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL CHE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL SHA')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL SCHA')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL HARD SIGN')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL YERU')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL SOFT SIGN')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL E')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL YU')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR SMALL YA')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT A')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT BE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT VE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT GE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT DE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT IE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT IO')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT ZHE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT ZE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT I')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT KA')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT EL')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT EM')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT EN')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT O')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT PE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT ER')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT ES')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT TE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT U')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT EF')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT HA')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT TSE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT CHE')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT SHA')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT SCHA')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT HARD SIGN')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT YERU')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT SOFT SIGN')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT E')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT YU')");
             * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
             * VALUES (_koi8r'?��','CYR CAPIT YA')");
             */

            this.stmt.executeUpdate("ALTER TABLE t1 ADD utf8_f CHAR(32) CHARACTER SET utf8 NOT NULL");
            this.stmt.executeUpdate("UPDATE t1 SET utf8_f=CONVERT(koi8_ru_f USING utf8)");
            this.stmt.executeUpdate("SET CHARACTER SET koi8r");
            // this.stmt.executeUpdate("SET CHARACTER SET UTF8");
            this.rs = this.stmt.executeQuery("SELECT * FROM t1");

            ResultSetMetaData rsmd = this.rs.getMetaData();

            int numColumns = rsmd.getColumnCount();

            for (int i = 0; i < numColumns; i++) {
                System.out.print(rsmd.getColumnName(i + 1));
                System.out.print("\t\t");
            }

            System.out.println();

            while (this.rs.next()) {
                System.out.println(this.rs.getString(1) + "\t\t" + this.rs.getString(2) + "\t\t" + this.rs.getString(3));

                if (this.rs.getString(1).equals("CYR SMALL A")) {
                    this.rs.getString(2);
                }
            }

            System.out.println();

            this.stmt.executeUpdate("SET NAMES utf8");
            this.rs = this.stmt.executeQuery("SELECT _koi8r 0xC1;");

            rsmd = this.rs.getMetaData();

            numColumns = rsmd.getColumnCount();

            for (int i = 0; i < numColumns; i++) {
                System.out.print(rsmd.getColumnName(i + 1));
                System.out.print("\t\t");
            }

            System.out.println();

            while (this.rs.next()) {
                System.out.println(this.rs.getString(1).equals("\u0430") + "\t\t");
                System.out.println(new String(this.rs.getBytes(1), "KOI8_R"));

            }

            char[] c = new char[] { 0xd0b0 };

            System.out.println(new String(c));
            System.out.println("\u0430");
        }
    }

    /**
     * Tests isolation level functionality
     * 
     * @throws Exception
     *             if an error occurs
     */
    public void testIsolationLevel() throws Exception {
        if (versionMeetsMinimum(4, 0)) {
            String[] isoLevelNames = new String[] { "Connection.TRANSACTION_NONE", "Connection.TRANSACTION_READ_COMMITTED",
                    "Connection.TRANSACTION_READ_UNCOMMITTED", "Connection.TRANSACTION_REPEATABLE_READ", "Connection.TRANSACTION_SERIALIZABLE" };

            int[] isolationLevels = new int[] { Connection.TRANSACTION_NONE, Connection.TRANSACTION_READ_COMMITTED, Connection.TRANSACTION_READ_UNCOMMITTED,
                    Connection.TRANSACTION_REPEATABLE_READ, Connection.TRANSACTION_SERIALIZABLE };

            DatabaseMetaData dbmd = this.conn.getMetaData();

            for (int i = 0; i < isolationLevels.length; i++) {
                if (dbmd.supportsTransactionIsolationLevel(isolationLevels[i])) {
                    this.conn.setTransactionIsolation(isolationLevels[i]);

                    assertTrue(
                            "Transaction isolation level that was set (" + isoLevelNames[i]
                                    + ") was not returned, nor was a more restrictive isolation level used by the server",
                            this.conn.getTransactionIsolation() == isolationLevels[i] || this.conn.getTransactionIsolation() > isolationLevels[i]);
                }
            }
        }
    }

    /**
     * Tests the savepoint functionality in MySQL.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testSavepoint() throws Exception {
        DatabaseMetaData dbmd = this.conn.getMetaData();

        if (dbmd.supportsSavepoints()) {
            System.out.println("Testing SAVEPOINTs");

            try {
                this.conn.setAutoCommit(true);

                createTable("testSavepoints", "(field1 int)", "InnoDB");

                // Try with named save points
                this.conn.setAutoCommit(false);
                this.stmt.executeUpdate("INSERT INTO testSavepoints VALUES (1)");

                Savepoint afterInsert = this.conn.setSavepoint("afterInsert");
                this.stmt.executeUpdate("UPDATE testSavepoints SET field1=2");

                Savepoint afterUpdate = this.conn.setSavepoint("afterUpdate");
                this.stmt.executeUpdate("DELETE FROM testSavepoints");

                assertTrue("Row count should be 0", getRowCount("testSavepoints") == 0);
                this.conn.rollback(afterUpdate);
                assertTrue("Row count should be 1", getRowCount("testSavepoints") == 1);
                assertTrue("Value should be 2", "2".equals(getSingleValue("testSavepoints", "field1", null).toString()));
                this.conn.rollback(afterInsert);
                assertTrue("Value should be 1", "1".equals(getSingleValue("testSavepoints", "field1", null).toString()));
                this.conn.rollback();
                assertTrue("Row count should be 0", getRowCount("testSavepoints") == 0);

                // Try with 'anonymous' save points
                this.conn.rollback();

                this.stmt.executeUpdate("INSERT INTO testSavepoints VALUES (1)");
                afterInsert = this.conn.setSavepoint();
                this.stmt.executeUpdate("UPDATE testSavepoints SET field1=2");
                afterUpdate = this.conn.setSavepoint();
                this.stmt.executeUpdate("DELETE FROM testSavepoints");

                assertTrue("Row count should be 0", getRowCount("testSavepoints") == 0);
                this.conn.rollback(afterUpdate);
                assertTrue("Row count should be 1", getRowCount("testSavepoints") == 1);
                assertTrue("Value should be 2", "2".equals(getSingleValue("testSavepoints", "field1", null).toString()));
                this.conn.rollback(afterInsert);
                assertTrue("Value should be 1", "1".equals(getSingleValue("testSavepoints", "field1", null).toString()));
                this.conn.rollback();

                this.conn.releaseSavepoint(this.conn.setSavepoint());
            } finally {
                this.conn.setAutoCommit(true);
            }
        } else {
            System.out.println("MySQL version does not support SAVEPOINTs");
        }
    }

    /**
     * Tests the ability to set the connection collation via properties.
     * 
     * @throws Exception
     *             if an error occurs or the test fails
     */
    public void testNonStandardConnectionCollation() throws Exception {
        if (versionMeetsMinimum(4, 1)) {
            String collationToSet = "utf8_bin";
            String characterSet = "utf-8";

            Properties props = new Properties();
            props.setProperty("connectionCollation", collationToSet);
            props.setProperty("characterEncoding", characterSet);

            Connection collConn = null;
            Statement collStmt = null;
            ResultSet collRs = null;

            try {
                collConn = getConnectionWithProps(props);

                collStmt = collConn.createStatement();

                collRs = collStmt.executeQuery("SHOW VARIABLES LIKE 'collation_connection'");

                assertTrue(collRs.next());
                assertTrue(collationToSet.equalsIgnoreCase(collRs.getString(2)));
            } finally {
                if (collConn != null) {
                    collConn.close();
                }
            }
        }
    }

    public void testDumpQueriesOnException() throws Exception {
        Properties props = new Properties();
        props.setProperty("dumpQueriesOnException", "true");
        String bogusSQL = "SELECT 1 TO BAZ";
        Connection dumpConn = getConnectionWithProps(props);

        try {
            dumpConn.createStatement().executeQuery(bogusSQL);
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getMessage().indexOf(bogusSQL) != -1);
        }

        try {
            ((com.mysql.jdbc.Connection) dumpConn).clientPrepareStatement(bogusSQL).executeQuery();
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getMessage().indexOf(bogusSQL) != -1);
        }

        try {
            createTable("testDumpQueriesOnException", "(field1 int UNIQUE)");
            this.stmt.executeUpdate("INSERT INTO testDumpQueriesOnException VALUES (1)");

            PreparedStatement pStmt = dumpConn.prepareStatement("INSERT INTO testDumpQueriesOnException VALUES (?)");
            pStmt.setInt(1, 1);
            pStmt.executeUpdate();
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getMessage().indexOf("INSERT INTO testDumpQueriesOnException") != -1);
        }

        try {
            dumpConn.prepareStatement(bogusSQL);
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getMessage().indexOf(bogusSQL) != -1);
        }
    }

    /**
     * Tests functionality of the ConnectionPropertiesTransform interface.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testConnectionPropertiesTransform() throws Exception {
        String transformClassName = SimpleTransformer.class.getName();

        Properties props = new Properties();

        props.setProperty(NonRegisteringDriver.PROPERTIES_TRANSFORM_KEY, transformClassName);

        NonRegisteringDriver driver = new NonRegisteringDriver();

        Properties transformedProps = driver.parseURL(BaseTestCase.dbUrl, props);

        assertTrue("albequerque".equals(transformedProps.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY)));
    }

    /**
     * Tests functionality of using URLs in 'LOAD DATA LOCAL INFILE' statements.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testLocalInfileWithUrl() throws Exception {
        File infile = File.createTempFile("foo", "txt");
        infile.deleteOnExit();
        String url = infile.toURL().toExternalForm();
        FileWriter output = new FileWriter(infile);
        output.write("Test");
        output.flush();
        output.close();

        createTable("testLocalInfileWithUrl", "(field1 LONGTEXT)");

        Properties props = new Properties();
        props.setProperty("allowUrlInLocalInfile", "true");

        Connection loadConn = getConnectionWithProps(props);
        Statement loadStmt = loadConn.createStatement();

        String charset = " CHARACTER SET "
                + CharsetMapping.getMysqlCharsetForJavaEncoding(((MySQLConnection) loadConn).getEncoding(), (com.mysql.jdbc.Connection) loadConn);

        try {
            loadStmt.executeQuery("LOAD DATA LOCAL INFILE '" + url + "' INTO TABLE testLocalInfileWithUrl" + charset);
        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();

            throw sqlEx;
        }

        this.rs = this.stmt.executeQuery("SELECT * FROM testLocalInfileWithUrl");
        assertTrue(this.rs.next());
        assertTrue("Test".equals(this.rs.getString(1)));
        int count = this.stmt.executeUpdate("DELETE FROM testLocalInfileWithUrl");
        assertTrue(count == 1);

        StringBuilder escapedPath = new StringBuilder();
        String path = infile.getCanonicalPath();

        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);

            if (c == '\\') {
                escapedPath.append('\\');
            }

            escapedPath.append(c);
        }

        loadStmt.execute("LOAD DATA LOCAL INFILE '" + escapedPath.toString() + "' INTO TABLE testLocalInfileWithUrl" + charset);
        this.rs = this.stmt.executeQuery("SELECT * FROM testLocalInfileWithUrl");
        assertTrue(this.rs.next());
        assertTrue("Test".equals(this.rs.getString(1)));

        try {
            loadStmt.execute("LOAD DATA LOCAL INFILE 'foo:///' INTO TABLE testLocalInfileWithUrl" + charset);
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getMessage() != null);
            assertTrue(sqlEx.getMessage().indexOf("FileNotFoundException") != -1);
        }
    }

    public void testLocalInfileDisabled() throws Exception {
        createTable("testLocalInfileDisabled", "(field1 varchar(255))");

        File infile = File.createTempFile("foo", "txt");
        infile.deleteOnExit();
        //String url = infile.toURL().toExternalForm();
        FileWriter output = new FileWriter(infile);
        output.write("Test");
        output.flush();
        output.close();

        Connection loadConn = getConnectionWithProps(new Properties());

        try {
            // have to do this after connect, otherwise it's the server that's enforcing it
            ((com.mysql.jdbc.Connection) loadConn).setAllowLoadLocalInfile(false);
            try {
                loadConn.createStatement().execute("LOAD DATA LOCAL INFILE '" + infile.getCanonicalPath() + "' INTO TABLE testLocalInfileDisabled");
                fail("Should've thrown an exception.");
            } catch (SQLException sqlEx) {
                assertEquals(SQLError.SQL_STATE_GENERAL_ERROR, sqlEx.getSQLState());
            }

            assertFalse(loadConn.createStatement().executeQuery("SELECT * FROM testLocalInfileDisabled").next());
        } finally {
            loadConn.close();
        }
    }

    public void testServerConfigurationCache() throws Exception {
        Properties props = new Properties();

        props.setProperty("cacheServerConfiguration", "true");
        props.setProperty("profileSQL", "true");
        props.setProperty("logFactory", "com.mysql.jdbc.log.StandardLogger");

        Connection conn1 = null;
        Connection conn2 = null;
        try {
            conn1 = getConnectionWithProps(props);

            try {
                // eliminate side-effects when not run in isolation
                StandardLogger.startLoggingToBuffer();

                conn2 = getConnectionWithProps(props);

                assertTrue("Configuration wasn't cached", StandardLogger.getBuffer().toString().indexOf("SHOW VARIABLES") == -1);

                if (versionMeetsMinimum(4, 1)) {
                    assertTrue("Configuration wasn't cached", StandardLogger.getBuffer().toString().indexOf("SHOW COLLATION") == -1);

                }
            } finally {
                StandardLogger.dropBuffer();
            }
        } finally {
            if (conn1 != null) {
                conn1.close();
            }
            if (conn2 != null) {
                conn2.close();
            }
        }
    }

    /**
     * Tests whether or not the configuration 'useLocalSessionState' actually
     * prevents non-needed 'set autocommit=', 'set session transaction isolation
     * ...' and 'show variables like tx_isolation' queries.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testUseLocalSessionState() throws Exception {
        Properties props = new Properties();

        props.setProperty("useLocalSessionState", "true");
        props.setProperty("profileSQL", "true");
        props.setProperty("logFactory", "com.mysql.jdbc.log.StandardLogger");

        Connection conn1 = getConnectionWithProps(props);
        conn1.setAutoCommit(true);
        conn1.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

        StandardLogger.startLoggingToBuffer();

        conn1.setAutoCommit(true);
        conn1.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        conn1.getTransactionIsolation();

        String logAsString = StandardLogger.getBuffer().toString();

        assertTrue(logAsString.indexOf("SET SESSION") == -1 && logAsString.indexOf("SHOW VARIABLES LIKE 'tx_isolation'") == -1
                && logAsString.indexOf("SET autocommit=") == -1);
    }

    /**
     * Tests whether re-connect with non-read-only connection can happen.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testFailoverConnection() throws Exception {

        if (!isServerRunningOnWindows()) { // windows sockets don't work for this test
            Properties props = new Properties();
            props.setProperty("autoReconnect", "true");
            props.setProperty("failOverReadOnly", "false");

            Properties urlProps = new NonRegisteringDriver().parseURL(dbUrl, null);

            String host = urlProps.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);
            String port = urlProps.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);

            props.setProperty(NonRegisteringDriver.HOST_PROPERTY_KEY + ".1", host);
            props.setProperty(NonRegisteringDriver.PORT_PROPERTY_KEY + ".1", port);
            props.setProperty(NonRegisteringDriver.HOST_PROPERTY_KEY + ".2", host);
            props.setProperty(NonRegisteringDriver.PORT_PROPERTY_KEY + ".2", port);
            props.setProperty(NonRegisteringDriver.NUM_HOSTS_PROPERTY_KEY, "2");

            Connection failoverConnection = null;

            try {
                failoverConnection = getConnectionWithProps(props);

                String originalConnectionId = getSingleIndexedValueWithQuery(failoverConnection, 1, "SELECT connection_id()").toString();
                System.out.println("Original Connection Id = " + originalConnectionId);

                assertTrue("Connection should not be in READ_ONLY state", !failoverConnection.isReadOnly());

                // Kill the connection
                this.stmt.executeUpdate("KILL " + originalConnectionId);

                // This takes a bit to occur

                Thread.sleep(3000);

                try {
                    failoverConnection.createStatement().execute("SELECT 1");
                    fail("We expect an exception here, because the connection should be gone until the reconnect code picks it up again");
                } catch (SQLException sqlEx) {
                    // do-nothing
                }

                // Tickle re-connect

                failoverConnection.setAutoCommit(true);

                String newConnectionId = getSingleIndexedValueWithQuery(failoverConnection, 1, "SELECT connection_id()").toString();
                System.out.println("new Connection Id = " + newConnectionId);

                assertTrue("We should have a new connection to the server in this case", !newConnectionId.equals(originalConnectionId));
                assertTrue("Connection should not be read-only", !failoverConnection.isReadOnly());
            } finally {
                if (failoverConnection != null) {
                    failoverConnection.close();
                }
            }
        }
    }

    public void testCannedConfigs() throws Exception {
        String url = "jdbc:mysql:///?useConfigs=clusterBase";

        Properties cannedProps = new NonRegisteringDriver().parseURL(url, null);

        assertTrue("true".equals(cannedProps.getProperty("autoReconnect")));
        assertTrue("false".equals(cannedProps.getProperty("failOverReadOnly")));
        assertTrue("true".equals(cannedProps.getProperty("roundRobinLoadBalance")));

        // this will fail, but we test that too
        url = "jdbc:mysql:///?useConfigs=clusterBase,clusterBase2";

        try {
            cannedProps = new NonRegisteringDriver().parseURL(url, null);
            fail("should've bailed on that one!");
        } catch (SQLException sqlEx) {
            assertTrue(SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE.equals(sqlEx.getSQLState()));
        }
    }

    public void testUseOldUTF8Behavior() throws Exception {

        Properties props = new Properties();
        props.setProperty("useOldUTF8Behavior", "true");
        props.setProperty("useUnicode", "true");
        props.setProperty("characterEncoding", "UTF-8");
        props.setProperty("logFactory", "com.mysql.jdbc.log.StandardLogger");
        props.setProperty("profileSQL", "true");
        StandardLogger.startLoggingToBuffer();

        try {
            getConnectionWithProps(props);

            assertTrue(StringUtils.indexOfIgnoreCase(StandardLogger.getBuffer().toString(), "SET NAMES utf8") == -1);
        } finally {
            StandardLogger.dropBuffer();
        }
    }

    /**
     * Checks implementation of 'dontTrackOpenResources' property.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testDontTrackOpenResources() throws Exception {
        Properties props = new Properties();

        props.setProperty("dontTrackOpenResources", "true");
        Connection noTrackConn = null;
        Statement noTrackStatement = null;
        PreparedStatement noTrackPstmt = null;
        ResultSet rs2 = null;

        try {
            noTrackConn = getConnectionWithProps(props);
            noTrackStatement = noTrackConn.createStatement();
            noTrackPstmt = noTrackConn.prepareStatement("SELECT 1");
            rs2 = noTrackPstmt.executeQuery();
            rs2.next();

            this.rs = noTrackStatement.executeQuery("SELECT 1");
            this.rs.next();

            noTrackConn.close();

            // Under 'strict' JDBC requirements, these calls should fail
            // (and _do_ if dontTrackOpenResources == false)

            this.rs.getString(1);
            rs2.getString(1);
        } finally {
            if (rs2 != null) {
                rs2.close();
            }

            if (noTrackStatement != null) {
                noTrackStatement.close();
            }

            if (noTrackConn != null && !noTrackConn.isClosed()) {
                noTrackConn.close();
            }
        }
    }

    public void testPing() throws SQLException {
        Connection conn2 = getConnectionWithProps((String) null);

        ((com.mysql.jdbc.Connection) conn2).ping();
        conn2.close();

        try {
            ((com.mysql.jdbc.Connection) conn2).ping();
            fail("Should have failed with an exception");
        } catch (SQLException sqlEx) {
            // ignore for now
        }

        //
        // This feature caused BUG#8975, so check for that too!

        Properties props = new Properties();
        props.setProperty("autoReconnect", "true");

        getConnectionWithProps(props);
    }

    public void testSessionVariables() throws Exception {
        String getInitialWaitTimeout = getMysqlVariable("wait_timeout");

        int newWaitTimeout = Integer.parseInt(getInitialWaitTimeout) + 10000;

        Properties props = new Properties();
        props.setProperty("sessionVariables", "wait_timeout=" + newWaitTimeout);
        props.setProperty("profileSQL", "true");

        Connection varConn = getConnectionWithProps(props);

        assertTrue(!getInitialWaitTimeout.equals(getMysqlVariable(varConn, "wait_timeout")));
    }

    /**
     * Tests setting profileSql on/off in the span of one connection.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testSetProfileSql() throws Exception {
        ((com.mysql.jdbc.Connection) this.conn).setProfileSql(false);
        this.stmt.execute("SELECT 1");
        ((com.mysql.jdbc.Connection) this.conn).setProfileSql(true);
        this.stmt.execute("SELECT 1");
    }

    public void testCreateDatabaseIfNotExist() throws Exception {
        if (isAdminConnectionConfigured()) {
            Properties props = new Properties();
            props.setProperty("createDatabaseIfNotExist", "true");
            props.setProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY, "testcreatedatabaseifnotexists");

            Connection newConn = getAdminConnectionWithProps(props);
            newConn.createStatement().executeUpdate("DROP DATABASE testcreatedatabaseifnotexists");
        }
    }

    /**
     * Tests if gatherPerfMetrics works.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testGatherPerfMetrics() throws Exception {
        if (versionMeetsMinimum(4, 1)) {
            try {
                Properties props = new Properties();
                props.put("autoReconnect", "true");
                props.put("relaxAutoCommit", "true");
                props.put("logSlowQueries", "true");
                props.put("slowQueryThresholdMillis", "2000");
                // these properties were reported as the cause of NullPointerException
                props.put("gatherPerfMetrics", "true");
                props.put("reportMetricsIntervalMillis", "3000");

                Connection conn1 = getConnectionWithProps(props);
                Statement stmt1 = conn1.createStatement();
                ResultSet rs1 = stmt1.executeQuery("SELECT 1");
                rs1.next();
                conn1.close();
            } catch (NullPointerException e) {
                e.printStackTrace();
                fail();
            }
        }
    }

    /**
     * Tests if useCompress works.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testUseCompress() throws Exception {

        this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'max_allowed_packet'");
        this.rs.next();
        if (this.rs.getInt(2) < 4 + 1024 * 1024 * 16 - 1) {
            fail("You need to increase max_allowed_packet to at least " + (4 + 1024 * 1024 * 16 - 1) + " before running this test!");
        }

        if (versionMeetsMinimum(5, 6, 20) && !versionMeetsMinimum(5, 7)) {
            /*
             * The 5.6.20 patch for Bug #16963396, Bug #19030353, Bug #69477 limits the size of redo log BLOB writes
             * to 10% of the redo log file size. The 5.7.5 patch addresses the bug without imposing a limitation.
             * As a result of the redo log BLOB write limit introduced for MySQL 5.6, innodb_log_file_size should be set to a value
             * greater than 10 times the largest BLOB data size found in the rows of your tables plus the length of other variable length
             * fields (VARCHAR, VARBINARY, and TEXT type fields).
             */
            this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_log_file_size'");
            this.rs.next();
            if (this.rs.getInt(2) < 1024 * 1024 * 32 * 10) {
                fail("You need to increase innodb_log_file_size to at least " + (1024 * 1024 * 32 * 10) + " before running this test!");
            }
        }

        testCompressionWith("false", 1024 * 1024 * 16 - 2); // no split
        testCompressionWith("false", 1024 * 1024 * 16 - 1); // split with additional empty packet
        testCompressionWith("false", 1024 * 1024 * 32);   // big payload

        testCompressionWith("true", 1024 * 1024 * 16 - 2 - 3); // no split, one compressed packet
        testCompressionWith("true", 1024 * 1024 * 16 - 2 - 2); // no split, two compressed packets
        testCompressionWith("true", 1024 * 1024 * 16 - 1);   // split with additional empty packet, two compressed packets
        testCompressionWith("true", 1024 * 1024 * 32);     // big payload

    }

    /**
     * @param useCompression
     * @param maxUncompressedPacketSize
     *            mysql header + payload
     * @throws Exception
     */
    private void testCompressionWith(String useCompression, int maxPayloadSize) throws Exception {

        String sqlToSend = "INSERT INTO BLOBTEST(blobdata) VALUES (?)";
        int requiredSize = maxPayloadSize - sqlToSend.length() - "_binary''".length();

        File testBlobFile = File.createTempFile("cmj-testblob", ".dat");
        testBlobFile.deleteOnExit();

        // TODO: following cleanup doesn't work correctly during concurrent execution of testsuite 
        // cleanupTempFiles(testBlobFile, "cmj-testblob");

        BufferedOutputStream bOut = new BufferedOutputStream(new FileOutputStream(testBlobFile));

        int dataRange = Byte.MAX_VALUE - Byte.MIN_VALUE;

        for (int i = 0; i < requiredSize; i++) {
            bOut.write((byte) ((Math.random() * dataRange) + Byte.MIN_VALUE));
        }

        bOut.flush();
        bOut.close();

        Properties props = new Properties();
        props.put("useCompression", useCompression);
        Connection conn1 = getConnectionWithProps(props);
        Statement stmt1 = conn1.createStatement();

        createTable("BLOBTEST", "(pos int PRIMARY KEY auto_increment, blobdata LONGBLOB)");
        BufferedInputStream bIn = new BufferedInputStream(new FileInputStream(testBlobFile));

        this.pstmt = conn1.prepareStatement(sqlToSend);

        this.pstmt.setBinaryStream(1, bIn, (int) testBlobFile.length());
        this.pstmt.execute();
        this.pstmt.clearParameters();

        this.rs = stmt1.executeQuery("SELECT blobdata from BLOBTEST LIMIT 1");
        this.rs.next();
        InputStream is = this.rs.getBinaryStream(1);

        bIn.close();
        bIn = new BufferedInputStream(new FileInputStream(testBlobFile));
        int blobbyte = 0;
        int count = 0;
        while ((blobbyte = is.read()) > -1) {
            int filebyte = bIn.read();
            if (filebyte < 0 || filebyte != blobbyte) {
                fail("Blob is not identical to initial data.");
            }
            count++;
        }
        assertEquals(requiredSize, count);

        is.close();
        if (bIn != null) {
            bIn.close();
        }
    }

    /**
     * Tests feature of "localSocketAddress", by enumerating local IF's and
     * trying each one in turn. This test might take a long time to run, since
     * we can't set timeouts if we're using localSocketAddress. We try and keep
     * the time down on the testcase by spawning the checking of each interface
     * off into separate threads.
     * 
     * @throws Exception
     *             if the test can't use at least one of the local machine's
     *             interfaces to make an outgoing connection to the server.
     */
    public void testLocalSocketAddress() throws Exception {
        Enumeration<NetworkInterface> allInterfaces = NetworkInterface.getNetworkInterfaces();

        SpawnedWorkerCounter counter = new SpawnedWorkerCounter();

        List<LocalSocketAddressCheckThread> allChecks = new ArrayList<LocalSocketAddressCheckThread>();

        while (allInterfaces.hasMoreElements()) {
            NetworkInterface intf = allInterfaces.nextElement();

            Enumeration<InetAddress> allAddresses = intf.getInetAddresses();

            allChecks.add(new LocalSocketAddressCheckThread(allAddresses, counter));
        }

        counter.setWorkerCount(allChecks.size());

        for (LocalSocketAddressCheckThread t : allChecks) {
            t.start();
        }

        // Wait for tests to complete....
        synchronized (counter) {

            while (counter.workerCount > 0 /* safety valve */) {

                counter.wait();

                if (counter.workerCount == 0) {
                    System.out.println("Done!");
                    break;
                }
            }
        }

        boolean didOneWork = false;
        boolean didOneFail = false;

        for (LocalSocketAddressCheckThread t : allChecks) {
            if (t.atLeastOneWorked) {
                didOneWork = true;

                break;
            }
            if (!didOneFail) {
                didOneFail = true;
            }
        }

        assertTrue("At least one connection was made with the localSocketAddress set", didOneWork);

        NonRegisteringDriver d = new NonRegisteringDriver();

        String hostname = d.host(d.parseURL(dbUrl, null));

        if (!hostname.startsWith(":") && !hostname.startsWith("localhost")) {

            int indexOfColon = hostname.indexOf(":");

            if (indexOfColon != -1) {
                hostname = hostname.substring(0, indexOfColon);
            }

            boolean isLocalIf = false;

            isLocalIf = (null != NetworkInterface.getByName(hostname));

            if (!isLocalIf) {
                try {
                    isLocalIf = (null != NetworkInterface.getByInetAddress(InetAddress.getByName(hostname)));
                } catch (Throwable t) {
                    isLocalIf = false;
                }
            }

            if (!isLocalIf) {
                assertTrue("At least one connection didn't fail with localSocketAddress set", didOneFail);
            }
        }
    }

    class SpawnedWorkerCounter {
        protected int workerCount = 0;

        synchronized void setWorkerCount(int i) {
            this.workerCount = i;
        }

        synchronized void decrementWorkerCount() {
            this.workerCount--;
            notify();
        }
    }

    class LocalSocketAddressCheckThread extends Thread {
        boolean atLeastOneWorked = false;
        Enumeration<InetAddress> allAddresses = null;
        SpawnedWorkerCounter counter = null;

        LocalSocketAddressCheckThread(Enumeration<InetAddress> e, SpawnedWorkerCounter c) {
            this.allAddresses = e;
            this.counter = c;
        }

        @Override
        public void run() {

            while (this.allAddresses.hasMoreElements()) {
                InetAddress addr = this.allAddresses.nextElement();

                try {
                    Properties props = new Properties();
                    props.setProperty("localSocketAddress", addr.getHostAddress());
                    props.setProperty("connectTimeout", "2000");
                    getConnectionWithProps(props).close();

                    this.atLeastOneWorked = true;

                    break;
                } catch (SQLException sqlEx) {
                    // ignore, we're only seeing if one of these tests succeeds
                }
            }

            this.counter.decrementWorkerCount();
        }
    }

    public void testUsageAdvisorTooLargeResultSet() throws Exception {
        Connection uaConn = null;

        PrintStream stderr = System.err;

        StandardLogger.startLoggingToBuffer();

        try {
            Properties props = new Properties();
            props.setProperty("useUsageAdvisor", "true");
            props.setProperty("resultSetSizeThreshold", "4");
            props.setProperty("logger", "StandardLogger");

            uaConn = getConnectionWithProps(props);
            this.rs = uaConn.createStatement().executeQuery("SHOW VARIABLES");
            this.rs.close();

            assertTrue("Result set threshold message not present",
                    StandardLogger.getBuffer().toString().indexOf("larger than \"resultSetSizeThreshold\" of 4 rows") != -1);
        } finally {
            StandardLogger.dropBuffer();
            System.setErr(stderr);

            if (uaConn != null) {
                uaConn.close();
            }
        }
    }

    public void testUseLocalSessionStateRollback() throws Exception {
        if (!versionMeetsMinimum(5, 5, 0)) {
            return;
        }

        Properties props = new Properties();
        props.setProperty("useLocalSessionState", "true");
        props.setProperty("useLocalTransactionState", "true");
        props.setProperty("profileSQL", "true");

        StandardLogger.startLoggingToBuffer();

        createTable("testUseLocalSessionState", "(field1 varchar(32))", "InnoDB");

        Connection localStateConn = null;
        Statement localStateStmt = null;
        String searchIn = "";

        try {
            localStateConn = getConnectionWithProps(props);
            localStateStmt = localStateConn.createStatement();

            localStateConn.setAutoCommit(false);
            localStateStmt.executeUpdate("INSERT INTO testUseLocalSessionState VALUES ('abc')");
            localStateConn.rollback();
            localStateConn.rollback();
            localStateStmt.executeUpdate("INSERT INTO testUseLocalSessionState VALUES ('abc')");
            localStateConn.commit();
            localStateConn.commit();
            localStateStmt.close();
        } finally {
            searchIn = StandardLogger.getBuffer().toString();
            StandardLogger.dropBuffer();

            if (localStateStmt != null) {
                localStateStmt.close();
            }

            if (localStateConn != null) {
                localStateConn.close();
            }
        }

        int rollbackCount = 0;
        int rollbackPos = 0;

        // space is important here, we don't want to count occurrences in stack traces
        while (rollbackPos != -1) {
            rollbackPos = searchIn.indexOf(" rollback", rollbackPos);

            if (rollbackPos != -1) {
                rollbackPos += "rollback".length();
                rollbackCount++;
            }
        }

        assertEquals(1, rollbackCount);

        int commitCount = 0;
        int commitPos = 0;

        // space is important here, we don't want to count "autocommit" nor occurrences in stack traces
        while (commitPos != -1) {
            commitPos = searchIn.indexOf(" commit", commitPos);

            if (commitPos != -1) {
                commitPos += " commit".length();
                commitCount++;
            }
        }

        assertEquals(1, commitCount);
    }

    /**
     * Checks if setting useCursorFetch to "true" automatically enables
     * server-side prepared statements.
     */

    public void testCouplingOfCursorFetch() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        Connection fetchConn = null;

        try {
            Properties props = new Properties();
            props.setProperty("useServerPrepStmts", "false"); // force the issue
            props.setProperty("useCursorFetch", "true");
            fetchConn = getConnectionWithProps(props);

            String classname = "com.mysql.jdbc.ServerPreparedStatement";

            if (Util.isJdbc42()) {
                classname = "com.mysql.jdbc.JDBC42ServerPreparedStatement";
            } else if (Util.isJdbc4()) {
                classname = "com.mysql.jdbc.JDBC4ServerPreparedStatement";
            }

            assertEquals(classname, fetchConn.prepareStatement("SELECT 1").getClass().getName());
        } finally {
            if (fetchConn != null) {
                fetchConn.close();
            }
        }
    }

    public void testInterfaceImplementation() throws Exception {
        testInterfaceImplementation(getConnectionWithProps((Properties) null));
        MysqlConnectionPoolDataSource cpds = new MysqlConnectionPoolDataSource();
        cpds.setUrl(dbUrl);
        testInterfaceImplementation(cpds.getPooledConnection().getConnection());
    }

    private void testInterfaceImplementation(Connection connToCheck) throws Exception {
        Method[] dbmdMethods = java.sql.DatabaseMetaData.class.getMethods();

        // can't do this statically, as we return different
        // implementations depending on JDBC version
        DatabaseMetaData dbmd = connToCheck.getMetaData();

        checkInterfaceImplemented(dbmdMethods, dbmd.getClass(), dbmd);

        Statement stmtToCheck = connToCheck.createStatement();

        checkInterfaceImplemented(java.sql.Statement.class.getMethods(), stmtToCheck.getClass(), stmtToCheck);

        PreparedStatement pStmtToCheck = connToCheck.prepareStatement("SELECT 1");
        ParameterMetaData paramMd = pStmtToCheck.getParameterMetaData();

        checkInterfaceImplemented(java.sql.PreparedStatement.class.getMethods(), pStmtToCheck.getClass(), pStmtToCheck);
        checkInterfaceImplemented(java.sql.ParameterMetaData.class.getMethods(), paramMd.getClass(), paramMd);

        pStmtToCheck = ((com.mysql.jdbc.Connection) connToCheck).serverPrepareStatement("SELECT 1");

        checkInterfaceImplemented(java.sql.PreparedStatement.class.getMethods(), pStmtToCheck.getClass(), pStmtToCheck);
        ResultSet toCheckRs = connToCheck.createStatement().executeQuery("SELECT 1");
        checkInterfaceImplemented(java.sql.ResultSet.class.getMethods(), toCheckRs.getClass(), toCheckRs);
        toCheckRs = connToCheck.createStatement().executeQuery("SELECT 1");
        checkInterfaceImplemented(java.sql.ResultSetMetaData.class.getMethods(), toCheckRs.getMetaData().getClass(), toCheckRs.getMetaData());

        if (versionMeetsMinimum(5, 0, 0)) {
            createProcedure("interfaceImpl", "(IN p1 INT)\nBEGIN\nSELECT 1;\nEND");

            CallableStatement cstmt = connToCheck.prepareCall("{CALL interfaceImpl(?)}");

            checkInterfaceImplemented(java.sql.CallableStatement.class.getMethods(), cstmt.getClass(), cstmt);
        }
        checkInterfaceImplemented(java.sql.Connection.class.getMethods(), connToCheck.getClass(), connToCheck);
    }

    private void checkInterfaceImplemented(Method[] interfaceMethods, Class<?> implementingClass, Object invokeOn) throws NoSuchMethodException {
        for (int i = 0; i < interfaceMethods.length; i++) {
            Method toFind = interfaceMethods[i];
            Method toMatch = implementingClass.getMethod(toFind.getName(), toFind.getParameterTypes());
            assertNotNull(toFind.toString(), toMatch);
            Class<?> paramTypes[] = toFind.getParameterTypes();

            Object[] args = new Object[paramTypes.length];
            fillPrimitiveDefaults(paramTypes, args, paramTypes.length);

            try {
                toMatch.invoke(invokeOn, args);
            } catch (IllegalArgumentException e) {

            } catch (IllegalAccessException e) {

            } catch (InvocationTargetException e) {

            } catch (java.lang.AbstractMethodError e) {
                throw e;
            }
        }
    }

    public void testNonVerifyServerCert() throws Exception {
        Properties props = new Properties();
        props.setProperty("useSSL", "true");
        props.setProperty("verifyServerCertificate", "false");
        props.setProperty("requireSSL", "true");
        getConnectionWithProps(props);
    }

    public void testSelfDestruct() throws Exception {
        Connection selfDestructingConn = getConnectionWithProps("selfDestructOnPingMaxOperations=2");

        boolean failed = false;

        for (int i = 0; i < 20; i++) {
            selfDestructingConn.createStatement().execute("SELECT 1");

            try {
                selfDestructingConn.createStatement().executeQuery("/* ping */ SELECT 1");
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();

                assertEquals("08S01", sqlState);

                failed = true;

                break;
            }
        }

        if (!failed) {
            fail("Connection should've self-destructed");
        }

        failed = false;

        selfDestructingConn = getConnectionWithProps("selfDestructOnPingSecondsLifetime=1");

        for (int i = 0; i < 20; i++) {
            selfDestructingConn.createStatement().execute("SELECT SLEEP(1)");

            try {
                selfDestructingConn.createStatement().executeQuery("/* ping */ SELECT 1");
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();

                assertEquals("08S01", sqlState);

                failed = true;

                break;
            }
        }

        if (!failed) {
            fail("Connection should've self-destructed");
        }
    }

    public void testLifecyleInterceptor() throws Exception {
        createTable("testLifecycleInterceptor", "(field1 int)", "InnoDB");
        Connection liConn = null;

        try {
            liConn = getConnectionWithProps("connectionLifecycleInterceptors=testsuite.simple.TestLifecycleInterceptor");
            liConn.setAutoCommit(false);

            liConn.createStatement().executeUpdate("INSERT INTO testLifecycleInterceptor VALUES (1)");
            liConn.commit();
            assertEquals(TestLifecycleInterceptor.transactionsBegun, 1);
            assertEquals(TestLifecycleInterceptor.transactionsCompleted, 1);
            liConn.createStatement().execute("SELECT * FROM testLifecycleInterceptor");
            assertEquals(TestLifecycleInterceptor.transactionsBegun, 2);
            // implicit commit
            liConn.createStatement().executeUpdate("CREATE TABLE testLifecycleFoo (field1 int)");
            assertEquals(TestLifecycleInterceptor.transactionsCompleted, 2);
        } finally {
            if (liConn != null) {
                liConn.createStatement().executeUpdate("DROP TABLE IF EXISTS testLifecycleFoo");
                liConn.close();
            }
        }

    }

    public void testNewHostParsing() throws Exception {
        Properties parsedProps = new NonRegisteringDriver().parseURL(dbUrl, null);
        String host = parsedProps.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);
        String port = parsedProps.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);
        String user = parsedProps.getProperty(NonRegisteringDriver.USER_PROPERTY_KEY);
        String password = parsedProps.getProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY);
        String database = parsedProps.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);

        String newUrl = String.format("jdbc:mysql://address=(protocol=tcp)(host=%s)(port=%s)(user=%s)(password=%s)/%s", host, port, user != null ? user : "",
                password != null ? password : "", database);

        Properties props = getHostFreePropertiesFromTestsuiteUrl();
        props.remove(NonRegisteringDriver.USER_PROPERTY_KEY);
        props.remove(NonRegisteringDriver.PASSWORD_PROPERTY_KEY);
        props.remove(NonRegisteringDriver.DBNAME_PROPERTY_KEY);

        try {
            getConnectionWithProps(newUrl, props);
        } catch (SQLException sqlEx) {
            throw new RuntimeException("Failed to connect with URL " + newUrl, sqlEx);
        }
    }

    public void testCompression() throws Exception {
        Connection compressedConn = getConnectionWithProps("useCompression=true,maxAllowedPacket=33554432");
        Statement compressedStmt = compressedConn.createStatement();
        compressedStmt.setFetchSize(Integer.MIN_VALUE);
        this.rs = compressedStmt.executeQuery("select repeat('a', 256 * 256 * 256 - 5)");
        this.rs.next();
        String str = this.rs.getString(1);

        assertEquals((256 * 256 * 256 - 5), str.length());

        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) != 'a') {
                fail();
            }
        }
    }

    public void testIsLocal() throws Exception {
        Properties parsedProps = new NonRegisteringDriver().parseURL(dbUrl, null);
        String host = parsedProps.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY, "localhost");

        if (host.equals("localhost") || host.equals("127.0.0.1")) {
            // we can actually test this
            assertTrue(((com.mysql.jdbc.ConnectionImpl) this.conn).isServerLocal());
        }

    }

    public void testReadOnly56() throws Exception {
        if (versionMeetsMinimum(5, 6, 5)) {
            try {
                Connection notLocalState = getConnectionWithProps("profileSql=true");

                for (int i = 0; i < 2; i++) {
                    StandardLogger.startLoggingToBuffer();
                    notLocalState.setReadOnly(true);
                    assertTrue(StandardLogger.getBuffer().toString().indexOf("set session transaction read only") != -1);
                    notLocalState.createStatement().execute("set session transaction read write");
                    assertFalse(notLocalState.isReadOnly());
                }

                for (int i = 0; i < 2; i++) {
                    StandardLogger.startLoggingToBuffer();
                    notLocalState.setReadOnly(false);
                    assertTrue(StandardLogger.getBuffer().toString().indexOf("set session transaction read write") != -1);
                    notLocalState.createStatement().execute("set session transaction read only");
                    assertTrue(notLocalState.isReadOnly());
                }

                Connection localState = getConnectionWithProps("profileSql=true,useLocalSessionState=true");

                for (int i = 0; i < 2; i++) {
                    StandardLogger.startLoggingToBuffer();
                    localState.setReadOnly(true);
                    if (i == 0) {
                        assertTrue(StandardLogger.getBuffer().toString().indexOf("set session transaction read only") != -1);
                    } else {
                        assertTrue(StandardLogger.getBuffer().toString().indexOf("set session transaction read only") == -1);
                    }
                    StandardLogger.startLoggingToBuffer();
                    localState.isReadOnly();
                    assertTrue(StandardLogger.getBuffer().toString().indexOf("select @@session.tx_read_only") == -1);
                }

                Connection noOptimization = getConnectionWithProps("profileSql=true,readOnlyPropagatesToServer=false");

                for (int i = 0; i < 2; i++) {
                    StandardLogger.startLoggingToBuffer();
                    noOptimization.setReadOnly(true);
                    assertTrue(StandardLogger.getBuffer().toString().indexOf("set session transaction read only") == -1);
                    StandardLogger.startLoggingToBuffer();
                    noOptimization.isReadOnly();
                    assertTrue(StandardLogger.getBuffer().toString().indexOf("select @@session.tx_read_only") == -1);
                }
            } finally {
                StandardLogger.dropBuffer();
            }
        }
    }

    /**
     * IPv6 Connection test.
     * 
     * @throws SQLException
     */
    public void testIPv6() throws Exception {

        if (!versionMeetsMinimum(5, 6)) {
            return;
            // this test could work with MySQL 5.5 but requires specific server configuration, e.g. "--bind-address=::"
        }

        Properties connProps = getPropertiesFromTestsuiteUrl();

        String host = "::1"; // IPv6 loopback
        int port = Integer.parseInt(connProps.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY));
        String username = connProps.getProperty(NonRegisteringDriver.USER_PROPERTY_KEY);

        String ipv6Url = String.format("jdbc:mysql://address=(protocol=tcp)(host=%s)(port=%d)", host, port);

        Connection testConn = null;
        Statement testStmt = null;
        ResultSet testRS = null;

        connProps = getHostFreePropertiesFromTestsuiteUrl();

        testConn = DriverManager.getConnection(ipv6Url, connProps);
        testStmt = testConn.createStatement();
        testRS = testStmt.executeQuery("SELECT USER()");

        assertTrue(testRS.next());
        assertTrue(testRS.getString(1).startsWith(username));

        testRS.close();
        testStmt.close();
        testConn.close();
    }

    /**
     * Test connection property cacheDefaultTimezone.
     * 
     * @throws SQLException
     */
    public void testCacheDefaultTimezone() throws Exception {
        final TimeZone defaultTZ = TimeZone.getDefault();
        final TimeZone testTZ1 = TimeZone.getTimeZone("GMT-2");
        final TimeZone testTZ2 = TimeZone.getTimeZone("GMT+2");

        createTable("testCacheDefTZ", "(test TINYINT, dt DATETIME)");

        Properties connProps = new Properties();
        connProps.setProperty("useTimezone", "true");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        for (boolean cacheDefTZ : new boolean[] { true, false }) {
            try {
                String testMsg = "Test case [cacheDefaultTimezone=" + cacheDefTZ + "],";
                connProps.setProperty("cacheDefaultTimezone", Boolean.toString(cacheDefTZ));

                Connection testConn = getConnectionWithProps(connProps);
                PreparedStatement testPstmt = testConn.prepareStatement("INSERT INTO testCacheDefTZ VALUES (?, ?)");

                sdf.setTimeZone(testTZ1);
                java.sql.Timestamp tsIn = new java.sql.Timestamp(sdf.parse("1998-05-21 12:00:00").getTime());

                /*
                 * Insert same date/time instant twice using different default time zones.
                 * Same values must be stored when default time zone is cached. Different values otherwise.
                 */
                TimeZone.setDefault(testTZ1);
                testPstmt.setBoolean(1, cacheDefTZ);
                testPstmt.setTimestamp(2, tsIn);
                assertEquals(testMsg, 1, testPstmt.executeUpdate());

                TimeZone.setDefault(testTZ2);
                testPstmt.setBoolean(1, cacheDefTZ);
                testPstmt.setTimestamp(2, tsIn);
                assertEquals(testMsg, 1, testPstmt.executeUpdate());

                testPstmt.close();

                TimeZone.setDefault(defaultTZ);

                /*
                 * Verify that equal values are retrieved when default time zone is cached and different values otherwise, when default time zone doesn't
                 * change while reading data.
                 */
                Statement testStmt = testConn.createStatement();
                ResultSet testRs = testStmt.executeQuery("SELECT * FROM testCacheDefTZ WHERE test = " + cacheDefTZ);

                assertTrue(testMsg, testRs.next());
                java.sql.Timestamp timestampOut = testRs.getTimestamp(2);

                assertTrue(testMsg, testRs.next());
                assertEquals(testMsg, cacheDefTZ, timestampOut.equals(testRs.getTimestamp(2)));

                assertFalse(testMsg, testRs.next());

                /*
                 * Verify that retrieving values from the ResultSet is also affected by default time zone caching setting, allowing to "convert" them to the
                 * original date value when time zone caching is disabled.
                 * When time zone caching is enabled then the values stored in the database were the same and changing the default time zone while retrieving
                 * them doesn't affect the result.
                 */
                testRs = testStmt.executeQuery("SELECT * FROM testCacheDefTZ WHERE test = " + cacheDefTZ);

                TimeZone.setDefault(testTZ1);
                assertTrue(testMsg, testRs.next());
                timestampOut = testRs.getTimestamp(2);

                TimeZone.setDefault(testTZ2);
                assertTrue(testMsg, testRs.next());
                assertEquals(testMsg, timestampOut, testRs.getTimestamp(2));

                assertFalse(testMsg, testRs.next());

                testRs.close();
                testStmt.close();

                testConn.close();
            } finally {
                TimeZone.setDefault(defaultTZ);
            }
        }
    }

    /**
     * Test the new connection property 'enableEscapeProcessing', as well as the old connection property 'processEscapeCodesForPrepStmts' and interrelation
     * between both.
     * 
     * This test uses a StatementInterceptor to capture the query sent to the server and assert whether escape processing has been done in the client side or if
     * the query is sent untouched and escape processing will be done at server side, according to provided connection properties and type of Statement objects
     * in use.
     */
    public void testEnableEscapeProcessing() throws Exception {
        // make sure the connection string doesn't contain 'enableEscapeProcessing'
        String testUrl = BaseTestCase.dbUrl;
        int b = testUrl.indexOf("enableEscapeProcessing");
        if (b != -1) {
            int e = testUrl.indexOf('&', b);
            if (e == -1) {
                e = testUrl.length();
                b--;
            } else {
                e++;
            }
            testUrl = testUrl.substring(0, b) + testUrl.substring(e, testUrl.length());
        }
        String query = "SELECT /* testEnableEscapeProcessing: (%d) */ {fn sin(pi()/2)}, {ts '2015-08-16 11:22:33'}, {fn ucase('this is mysql')}";
        Timestamp testTimestamp = new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2015-08-16 11:22:33").getTime());

        for (int tst = 0; tst < 8; tst++) {
            boolean enableEscapeProcessing = (tst & 0x1) != 0;
            boolean processEscapeCodesForPrepStmts = (tst & 0x2) != 0;
            boolean useServerPrepStmts = (tst & 0x4) != 0;

            Properties props = new Properties();
            props.setProperty("statementInterceptors", TestEnableEscapeProcessingStatementInterceptor.class.getName());
            props.setProperty("enableEscapeProcessing", Boolean.toString(enableEscapeProcessing));
            props.setProperty("processEscapeCodesForPrepStmts", Boolean.toString(processEscapeCodesForPrepStmts));
            props.setProperty("useServerPrepStmts", Boolean.toString(useServerPrepStmts));

            Connection testConn = getConnectionWithProps(testUrl, props);
            this.stmt = testConn.createStatement();
            this.rs = this.stmt.executeQuery(String.format(query, tst));

            String testCase = String.format("Case: %d [ %s | %s | %s ]/Statement", tst, enableEscapeProcessing ? "enEscProc" : "-",
                    processEscapeCodesForPrepStmts ? "procEscProcPS" : "-", useServerPrepStmts ? "useSSPS" : "-");
            assertTrue(testCase, this.rs.next());
            assertEquals(testCase, 1d, this.rs.getDouble(1));
            assertEquals(testCase, testTimestamp, this.rs.getTimestamp(2));
            assertEquals(testCase, "THIS IS MYSQL", this.rs.getString(3));
            assertFalse(testCase, this.rs.next());

            this.pstmt = testConn.prepareStatement(String.format(query, tst));
            this.rs = this.pstmt.executeQuery();

            testCase = String.format("Case: %d [ %s | %s | %s ]/PreparedStatement", tst, enableEscapeProcessing ? "enEscProc" : "-",
                    processEscapeCodesForPrepStmts ? "procEscProcPS" : "-", useServerPrepStmts ? "useSSPS" : "-");
            assertTrue(testCase, this.rs.next());
            assertEquals(testCase, 1d, this.rs.getDouble(1));
            assertEquals(testCase, testTimestamp, this.rs.getTimestamp(2));
            assertEquals(testCase, "THIS IS MYSQL", this.rs.getString(3));
            assertFalse(testCase, this.rs.next());

            testConn.close();
        }
    }

    public static class TestEnableEscapeProcessingStatementInterceptor extends BaseStatementInterceptor {
        @Override
        public ResultSetInternalMethods preProcess(String sql, com.mysql.jdbc.Statement interceptedStatement, com.mysql.jdbc.Connection connection)
                throws SQLException {
            if (sql == null && interceptedStatement instanceof com.mysql.jdbc.PreparedStatement) {
                sql = ((com.mysql.jdbc.PreparedStatement) interceptedStatement).asSql();
            }

            int p;
            if (sql != null && (p = sql.indexOf("testEnableEscapeProcessing:")) != -1) {
                int tst = Integer.parseInt(sql.substring(sql.indexOf('(', p) + 1, sql.indexOf(')', p)));
                boolean enableEscapeProcessing = (tst & 0x1) != 0;
                boolean processEscapeCodesForPrepStmts = (tst & 0x2) != 0;
                boolean useServerPrepStmts = (tst & 0x4) != 0;
                boolean isPreparedStatement = interceptedStatement instanceof PreparedStatement;

                String testCase = String.format("Case: %d [ %s | %s | %s ]/%s", tst, enableEscapeProcessing ? "enEscProc" : "-",
                        processEscapeCodesForPrepStmts ? "procEscProcPS" : "-", useServerPrepStmts ? "useSSPS" : "-",
                        isPreparedStatement ? "PreparedStatement" : "Statement");

                boolean escapeProcessingDone = sql.indexOf('{') == -1;
                assertTrue(testCase, isPreparedStatement && processEscapeCodesForPrepStmts == escapeProcessingDone
                        || !isPreparedStatement && enableEscapeProcessing == escapeProcessingDone);
            }
            return super.preProcess(sql, interceptedStatement, connection);
        }
    }

    /**
     * Test authentication with a user that requires an SSL connection.
     * 
     * This test requires the CA truststore and the client keystore available in testsuite/ssl-test-certs.
     * The server needs to be configured with the CA and server certificates from testsuite/ssl-test-certs.
     */
    public void testUserRequireSSL() throws Exception {
        if (!versionMeetsMinimum(5, 7, 6)) {
            return;
        }

        Connection testConn;
        Statement testStmt;

        final String user = "testUserReqSSL";
        final String password = "testUserReqSSL";

        final Properties props = new Properties();
        props.setProperty(NonRegisteringDriver.USER_PROPERTY_KEY, user);
        props.setProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY, password);

        createUser("'" + user + "'@'%'", "IDENTIFIED BY '" + password + "' REQUIRE SSL");
        this.stmt.execute("GRANT SELECT ON *.* TO '" + user + "'@'%'");

        /*
         * No SSL.
         */
        props.setProperty("useSSL", "false");
        assertThrows(SQLException.class, "Access denied for user '" + user + "'@.*", new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        });

        /*
         * SSL: no server certificate validation & no client certificate.
         */
        props.setProperty("useSSL", "true");
        props.setProperty("verifyServerCertificate", "false");
        testConn = getConnectionWithProps(props);
        testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT CURRENT_USER()");
        assertTrue(this.rs.next());
        assertEquals(user, this.rs.getString(1).split("@")[0]);
        testConn.close();

        /*
         * SSL: server certificate validation & no client certificate.
         */
        props.setProperty("verifyServerCertificate", "true");
        props.setProperty("trustCertificateKeyStoreUrl", "file:src/testsuite/ssl-test-certs/ca-truststore");
        props.setProperty("trustCertificateKeyStoreType", "JKS");
        props.setProperty("trustCertificateKeyStorePassword", "password");
        testConn = getConnectionWithProps(props);
        testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT CURRENT_USER()");
        assertTrue(this.rs.next());
        assertEquals(user, this.rs.getString(1).split("@")[0]);
        testConn.close();

        /*
         * SSL: server certificate validation & client certificate.
         */
        props.setProperty("clientCertificateKeyStoreUrl", "file:src/testsuite/ssl-test-certs/client-keystore");
        props.setProperty("clientCertificateKeyStoreType", "JKS");
        props.setProperty("clientCertificateKeyStorePassword", "password");
        testConn = getConnectionWithProps(props);
        testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT CURRENT_USER()");
        assertTrue(this.rs.next());
        assertEquals(user, this.rs.getString(1).split("@")[0]);
        testConn.close();

        /*
         * SSL: no server certificate validation & client certificate.
         */
        props.setProperty("verifyServerCertificate", "false");
        props.remove("trustCertificateKeyStoreUrl");
        props.remove("trustCertificateKeyStoreType");
        props.remove("trustCertificateKeyStorePassword");
        testConn = getConnectionWithProps(props);
        testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT CURRENT_USER()");
        assertTrue(this.rs.next());
        assertEquals(user, this.rs.getString(1).split("@")[0]);
        testConn.close();
    }

    /**
     * Test authentication with a user that requires an SSL connection and an authorized client certificate.
     * 
     * This test requires the CA truststore and the client keystore available in testsuite/ssl-test-certs.
     * The server needs to be configured with the CA and server certificates from testsuite/ssl-test-certs.
     */
    public void testUserRequireX509() throws Exception {
        if (!versionMeetsMinimum(5, 7, 6)) {
            return;
        }

        Connection testConn;
        Statement testStmt;

        final String user = "testUserReqX509";
        final String password = "testUserReqX509";

        final Properties props = new Properties();
        props.setProperty(NonRegisteringDriver.USER_PROPERTY_KEY, user);
        props.setProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY, password);

        createUser("'" + user + "'@'%'", "IDENTIFIED BY '" + password + "' REQUIRE X509");
        this.stmt.execute("GRANT SELECT ON *.* TO '" + user + "'@'%'");

        /*
         * No SSL.
         */
        props.setProperty("useSSL", "false");
        assertThrows(SQLException.class, "Access denied for user '" + user + "'@.*", new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        });

        /*
         * SSL: no server certificate validation & no client certificate.
         */
        props.setProperty("useSSL", "true");
        props.setProperty("verifyServerCertificate", "false");
        assertThrows(SQLException.class, "Access denied for user '" + user + "'@.*", new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        });

        /*
         * SSL: server certificate validation & no client certificate.
         */
        props.setProperty("verifyServerCertificate", "true");
        props.setProperty("trustCertificateKeyStoreUrl", "file:src/testsuite/ssl-test-certs/ca-truststore");
        props.setProperty("trustCertificateKeyStoreType", "JKS");
        props.setProperty("trustCertificateKeyStorePassword", "password");
        assertThrows(SQLException.class, "Access denied for user '" + user + "'@.*", new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        });

        /*
         * SSL: server certificate validation & client certificate.
         */
        props.setProperty("clientCertificateKeyStoreUrl", "file:src/testsuite/ssl-test-certs/client-keystore");
        props.setProperty("clientCertificateKeyStoreType", "JKS");
        props.setProperty("clientCertificateKeyStorePassword", "password");
        testConn = getConnectionWithProps(props);
        testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT CURRENT_USER()");
        assertTrue(this.rs.next());
        assertEquals(user, this.rs.getString(1).split("@")[0]);
        testConn.close();

        /*
         * SSL: no server certificate validation & client certificate.
         */
        props.setProperty("verifyServerCertificate", "false");
        props.remove("trustCertificateKeyStoreUrl");
        props.remove("trustCertificateKeyStoreType");
        props.remove("trustCertificateKeyStorePassword");
        testConn = getConnectionWithProps(props);
        testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT CURRENT_USER()");
        assertTrue(this.rs.next());
        assertEquals(user, this.rs.getString(1).split("@")[0]);
        testConn.close();
    }
}
