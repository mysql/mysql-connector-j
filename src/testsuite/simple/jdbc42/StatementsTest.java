/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.simple.jdbc42;

import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.concurrent.Callable;

import testsuite.BaseTestCase;

public class StatementsTest extends BaseTestCase {
    // Shared test data
    private final String testDateString = "2015-08-04";
    private final String testTimeString = "12:34:56";
    private final String testDateTimeString = testDateString + " " + testTimeString + ".0";
    private final String testISODateTimeString = testDateString + "T" + testTimeString + ".0";

    private final Date testSqlDate = Date.valueOf(testDateString);
    private final Time testSqlTime = Time.valueOf(testTimeString);
    private final Timestamp testSqlTimeStamp = Timestamp.valueOf(testDateTimeString);

    private final LocalDate testLocalDate = LocalDate.parse(testDateString);
    private final LocalTime testLocalTime = LocalTime.parse(testTimeString);
    private final LocalDateTime testLocalDateTime = LocalDateTime.parse(testISODateTimeString);

    private final OffsetDateTime testOffsetDateTime = OffsetDateTime.of(2015, 8, 04, 12, 34, 56, 7890, ZoneOffset.UTC);
    private final OffsetTime testOffsetTime = OffsetTime.of(12, 34, 56, 7890, ZoneOffset.UTC);

    public StatementsTest(String name) {
        super(name);
    }

    /**
     * Test shared test data validity.
     */
    public void testSharedTestData() throws Exception {
        assertEquals(testSqlDate, Date.valueOf(testLocalDate));
        assertEquals(testSqlTime, Time.valueOf(testLocalTime));
        assertEquals(testSqlTimeStamp, Timestamp.valueOf(testLocalDateTime));

        assertEquals(testLocalDate, testSqlDate.toLocalDate());
        assertEquals(testLocalTime, testSqlTime.toLocalTime());
        assertEquals(testLocalDateTime, testSqlTimeStamp.toLocalDateTime());
    }

    /**
     * Test for Statement.executeLargeBatch(). Validate update count returned and generated keys.
     */
    public void testStmtExecuteLargeBatch() throws Exception {
        /*
         * Fully working batch
         */
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (1)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (2)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (3)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (4)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (5), (6), (7)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (8)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (9), (10)");

        long[] counts = this.stmt.executeLargeBatch();
        assertEquals(7, counts.length);
        assertEquals(1, counts[0]);
        assertEquals(1, counts[1]);
        assertEquals(1, counts[2]);
        assertEquals(1, counts[3]);
        assertEquals(3, counts[4]);
        assertEquals(1, counts[5]);
        assertEquals(2, counts[6]);

        this.rs = this.stmt.getGeneratedKeys();

        ResultSetMetaData rsmd = this.rs.getMetaData();
        assertEquals(1, rsmd.getColumnCount());
        assertEquals(JDBCType.BIGINT.getVendorTypeNumber().intValue(), rsmd.getColumnType(1));
        assertEquals(20, rsmd.getColumnDisplaySize(1));

        long generatedKey = 0;
        while (this.rs.next()) {
            assertEquals(++generatedKey, this.rs.getLong(1));
        }
        assertEquals(10, generatedKey);
        this.rs.close();

        /*
         * Batch with failing queries
         */
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (1)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (2)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch VALUES (3)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (4)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (5), (6), (7)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES ('eight')");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (9), (10)");

        try {
            this.stmt.executeLargeBatch();
            fail("BatchUpdateException expected");
        } catch (BatchUpdateException e) {
            assertEquals("Incorrect integer value: 'eight' for column 'n' at row 1", e.getMessage());
            counts = e.getLargeUpdateCounts();
            assertEquals(7, counts.length);
            assertEquals(1, counts[0]);
            assertEquals(1, counts[1]);
            assertEquals(Statement.EXECUTE_FAILED, counts[2]);
            assertEquals(1, counts[3]);
            assertEquals(3, counts[4]);
            assertEquals(Statement.EXECUTE_FAILED, counts[5]);
            assertEquals(2, counts[6]);
        } catch (Exception e) {
            fail("BatchUpdateException expected");
        }

        this.rs = this.stmt.getGeneratedKeys();
        generatedKey = 0;
        while (this.rs.next()) {
            assertEquals(++generatedKey, this.rs.getLong(1));
        }
        assertEquals(8, generatedKey);
        this.rs.close();
    }

    /**
     * Test for Statement.executeLargeUpdate(String).
     * Validate update count returned and generated keys.
     * Case: without requesting generated keys.
     */
    public void testStmtExecuteLargeUpdateNoGeneratedKeys() throws Exception {
        createTable("testExecuteLargeUpdate", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        long count = this.stmt.executeLargeUpdate("INSERT INTO testExecuteLargeUpdate (n) VALUES (1), (2), (3), (4), (5)");
        assertEquals(5, count);
        assertEquals(5, this.stmt.getLargeUpdateCount());

        final Statement stmtTmp = this.stmt;
        assertThrows(SQLException.class, "Generated keys not requested. You need to specify Statement.RETURN_GENERATED_KEYS to Statement.executeUpdate\\(\\), "
                + "Statement.executeLargeUpdate\\(\\) or Connection.prepareStatement\\(\\).", new Callable<Void>() {
            public Void call() throws Exception {
                stmtTmp.getGeneratedKeys();
                return null;
            }
        });
    }

    /**
     * Test for Statement.executeLargeUpdate(String, _).
     * Validate update count returned and generated keys.
     * Case 1: explicitly requesting generated keys.
     * Case 2: requesting generated keys by defining column indexes.
     * Case 3: requesting generated keys by defining column names.
     */
    public void testStmtExecuteLargeUpdate() throws Exception {
        createTable("testExecuteLargeUpdate", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        for (int tst = 1; tst <= 3; tst++) {
            this.stmt.execute("TRUNCATE TABLE testExecuteLargeUpdate");
            String tstCase = "Case " + tst;
            long count = 0;
            switch (tst) {
                case 1:
                    count = this.stmt.executeLargeUpdate("INSERT INTO testExecuteLargeUpdate (n) VALUES (1), (2), (3), (4), (5)",
                            Statement.RETURN_GENERATED_KEYS);
                    break;
                case 2:
                    count = this.stmt.executeLargeUpdate("INSERT INTO testExecuteLargeUpdate (n) VALUES (1), (2), (3), (4), (5)", new int[] { 1 });
                    break;
                case 3:
                    count = this.stmt.executeLargeUpdate("INSERT INTO testExecuteLargeUpdate (n) VALUES (1), (2), (3), (4), (5)", new String[] { "id" });
                    break;
            }
            assertEquals(tstCase, 5, count);
            assertEquals(tstCase, 5, this.stmt.getLargeUpdateCount());

            this.rs = this.stmt.getGeneratedKeys();

            ResultSetMetaData rsmd = this.rs.getMetaData();
            assertEquals(tstCase, 1, rsmd.getColumnCount());
            assertEquals(tstCase, JDBCType.BIGINT.getVendorTypeNumber().intValue(), rsmd.getColumnType(1));
            assertEquals(tstCase, 20, rsmd.getColumnDisplaySize(1));

            long generatedKey = 0;
            while (this.rs.next()) {
                assertEquals(tstCase, ++generatedKey, this.rs.getLong(1));
            }
            assertEquals(tstCase, 5, generatedKey);
            this.rs.close();
        }
    }

    /**
     * Test for PreparedStatement.executeLargeBatch().
     * Validate update count returned and generated keys.
     */
    public void testPrepStmtExecuteLargeBatch() throws Exception {
        /*
         * Fully working batch
         */
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        this.pstmt = this.conn.prepareStatement("INSERT INTO testExecuteLargeBatch (n) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
        this.pstmt.setInt(1, 1);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 2);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 3);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 4);
        this.pstmt.addBatch();
        this.pstmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (5), (6), (7)");
        this.pstmt.setInt(1, 8);
        this.pstmt.addBatch();
        this.pstmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (9), (10)");

        long[] counts = this.pstmt.executeLargeBatch();
        assertEquals(7, counts.length);
        assertEquals(1, counts[0]);
        assertEquals(1, counts[1]);
        assertEquals(1, counts[2]);
        assertEquals(1, counts[3]);
        assertEquals(3, counts[4]);
        assertEquals(1, counts[5]);
        assertEquals(2, counts[6]);

        this.rs = this.pstmt.getGeneratedKeys();

        ResultSetMetaData rsmd = this.rs.getMetaData();
        assertEquals(1, rsmd.getColumnCount());
        assertEquals(JDBCType.BIGINT.getVendorTypeNumber().intValue(), rsmd.getColumnType(1));
        assertEquals(20, rsmd.getColumnDisplaySize(1));

        long generatedKey = 0;
        while (this.rs.next()) {
            assertEquals(++generatedKey, this.rs.getLong(1));
        }
        assertEquals(10, generatedKey);
        this.rs.close();

        /*
         * Batch with failing queries
         */
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        this.pstmt = this.conn.prepareStatement("INSERT INTO testExecuteLargeBatch (n) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
        this.pstmt.setInt(1, 1);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 2);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 3);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 4);
        this.pstmt.addBatch();
        this.pstmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (5), (6), (7)");
        this.pstmt.setString(1, "eight");
        this.pstmt.addBatch();
        this.pstmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (9), (10)");

        try {
            this.pstmt.executeLargeBatch();
            fail("BatchUpdateException expected");
        } catch (BatchUpdateException e) {
            assertEquals("Incorrect integer value: 'eight' for column 'n' at row 1", e.getMessage());
            counts = e.getLargeUpdateCounts();
            assertEquals(7, counts.length);
            assertEquals(1, counts[0]);
            assertEquals(1, counts[1]);
            assertEquals(1, counts[2]);
            assertEquals(1, counts[3]);
            assertEquals(3, counts[4]);
            assertEquals(Statement.EXECUTE_FAILED, counts[5]);
            assertEquals(2, counts[6]);
        } catch (Exception e) {
            fail("BatchUpdateException expected");
        }

        this.rs = this.pstmt.getGeneratedKeys();
        generatedKey = 0;
        while (this.rs.next()) {
            assertEquals(++generatedKey, this.rs.getLong(1));
        }
        assertEquals(9, generatedKey);
        this.rs.close();
    }

    /**
     * Test for PreparedStatement.executeLargeUpdate().
     * Validate update count returned and generated keys.
     * Case: without requesting generated keys.
     */
    public void testPrepStmtExecuteLargeUpdateNoGeneratedKeys() throws Exception {
        createTable("testExecuteLargeUpdate", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        this.pstmt = this.conn.prepareStatement("INSERT INTO testExecuteLargeUpdate (n) VALUES (?), (?), (?), (?), (?)");
        this.pstmt.setInt(1, 1);
        this.pstmt.setInt(2, 2);
        this.pstmt.setInt(3, 3);
        this.pstmt.setInt(4, 4);
        this.pstmt.setInt(5, 5);

        long count = this.pstmt.executeLargeUpdate();
        assertEquals(5, count);
        assertEquals(5, this.pstmt.getLargeUpdateCount());

        final Statement stmtTmp = this.pstmt;
        assertThrows(SQLException.class, "Generated keys not requested. You need to specify Statement.RETURN_GENERATED_KEYS to Statement.executeUpdate\\(\\), "
                + "Statement.executeLargeUpdate\\(\\) or Connection.prepareStatement\\(\\).", new Callable<Void>() {
            public Void call() throws Exception {
                stmtTmp.getGeneratedKeys();
                return null;
            }
        });
    }

    /**
     * Test for PreparedStatement.executeLargeUpdate().
     * Validate update count returned and generated keys.
     * Case: explicitly requesting generated keys.
     */
    public void testPrepStmtExecuteLargeUpdateExplicitGeneratedKeys() throws Exception {
        createTable("testExecuteLargeUpdate", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        this.pstmt = this.conn.prepareStatement("INSERT INTO testExecuteLargeUpdate (n) VALUES (?), (?), (?), (?), (?)", Statement.RETURN_GENERATED_KEYS);
        this.pstmt.setInt(1, 1);
        this.pstmt.setInt(2, 2);
        this.pstmt.setInt(3, 3);
        this.pstmt.setInt(4, 4);
        this.pstmt.setInt(5, 5);

        long count = this.pstmt.executeLargeUpdate();
        assertEquals(5, count);
        assertEquals(5, this.pstmt.getLargeUpdateCount());

        this.rs = this.pstmt.getGeneratedKeys();

        ResultSetMetaData rsmd = this.rs.getMetaData();
        assertEquals(1, rsmd.getColumnCount());
        assertEquals(JDBCType.BIGINT.getVendorTypeNumber().intValue(), rsmd.getColumnType(1));
        assertEquals(20, rsmd.getColumnDisplaySize(1));

        long generatedKey = 0;
        while (this.rs.next()) {
            assertEquals(++generatedKey, this.rs.getLong(1));
        }
        assertEquals(5, generatedKey);
        this.rs.close();
    }

    /**
     * Test for CallableStatement.executeLargeBatch().
     * Validate update count returned and generated keys.
     */
    public void testCallStmtExecuteLargeBatch() throws Exception {
        /*
         * Fully working batch
         */
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");
        createProcedure("testExecuteLargeBatchProc", "(IN n INT) BEGIN INSERT INTO testExecuteLargeBatch (n) VALUES (n); END");

        CallableStatement testCstmt = this.conn.prepareCall("{CALL testExecuteLargeBatchProc(?)}");
        testCstmt.setInt(1, 1);
        testCstmt.addBatch();
        testCstmt.setInt(1, 2);
        testCstmt.addBatch();
        testCstmt.setInt(1, 3);
        testCstmt.addBatch();
        testCstmt.setInt(1, 4);
        testCstmt.addBatch();
        testCstmt.addBatch("{CALL testExecuteLargeBatchProc(5)}");
        testCstmt.addBatch("{CALL testExecuteLargeBatchProc(6)}");
        testCstmt.addBatch("{CALL testExecuteLargeBatchProc(7)}");
        testCstmt.setInt(1, 8);
        testCstmt.addBatch();
        testCstmt.addBatch("{CALL testExecuteLargeBatchProc(9)}");
        testCstmt.addBatch("{CALL testExecuteLargeBatchProc(10)}");

        long[] counts = testCstmt.executeLargeBatch();
        assertEquals(10, counts.length);
        assertEquals(1, counts[0]);
        assertEquals(1, counts[1]);
        assertEquals(1, counts[2]);
        assertEquals(1, counts[3]);
        assertEquals(1, counts[4]);
        assertEquals(1, counts[5]);
        assertEquals(1, counts[6]);
        assertEquals(1, counts[7]);
        assertEquals(1, counts[8]);
        assertEquals(1, counts[9]);

        this.rs = testCstmt.getGeneratedKeys();

        ResultSetMetaData rsmd = this.rs.getMetaData();
        assertEquals(1, rsmd.getColumnCount());
        assertEquals(JDBCType.BIGINT.getVendorTypeNumber().intValue(), rsmd.getColumnType(1));
        assertEquals(20, rsmd.getColumnDisplaySize(1));

        // We can't check the generated keys as they are not returned correctly in this case (last_insert_id is missing from OK_PACKET when executing inserts
        // within a stored procedure - Bug#21792359).
        //        long generatedKey = 0;
        //        while (this.rs.next()) {
        //            assertEquals(++generatedKey, this.rs.getLong(1));
        //        }
        //        assertEquals(10, generatedKey);
        this.rs.close();

        testCstmt.close();

        /*
         * Batch with failing queries
         */
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        testCstmt = this.conn.prepareCall("{call testExecuteLargeBatchProc(?)}");
        testCstmt.setInt(1, 1);
        testCstmt.addBatch();
        testCstmt.setInt(1, 2);
        testCstmt.addBatch();
        testCstmt.setInt(1, 3);
        testCstmt.addBatch();
        testCstmt.setInt(1, 4);
        testCstmt.addBatch();
        testCstmt.addBatch("{call testExecuteLargeBatchProc(5)}");
        testCstmt.addBatch("{call testExecuteLargeBatchProc('six')}");
        testCstmt.addBatch("{call testExecuteLargeBatchProc(7)}");
        testCstmt.setString(1, "eight");
        testCstmt.addBatch();
        testCstmt.addBatch("{CALL testExecuteLargeBatchProc(9)}");
        testCstmt.addBatch("{CALL testExecuteLargeBatchProc(10)}");

        try {
            testCstmt.executeLargeBatch();
            fail("BatchUpdateException expected");
        } catch (BatchUpdateException e) {
            assertEquals("Incorrect integer value: 'eight' for column 'n' at row 1", e.getMessage());
            counts = e.getLargeUpdateCounts();
            assertEquals(10, counts.length);
            assertEquals(1, counts[0]);
            assertEquals(1, counts[1]);
            assertEquals(1, counts[2]);
            assertEquals(1, counts[3]);
            assertEquals(1, counts[4]);
            assertEquals(Statement.EXECUTE_FAILED, counts[5]);
            assertEquals(1, counts[6]);
            assertEquals(Statement.EXECUTE_FAILED, counts[7]);
            assertEquals(1, counts[8]);
            assertEquals(1, counts[9]);
        } catch (Exception e) {
            fail("BatchUpdateException expected");
        }

        testCstmt.close();
    }

    /**
     * Test for CallableStatement.executeLargeUpdate().
     * Validate update count returned and generated keys.
     */
    public void testCallStmtExecuteLargeUpdate() throws Exception {
        createTable("testExecuteLargeUpdate", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");
        createProcedure("testExecuteLargeUpdateProc", "(IN n1 INT, IN n2 INT, IN n3 INT, IN n4 INT, IN n5 INT) BEGIN "
                + "INSERT INTO testExecuteLargeUpdate (n) VALUES (n1), (n2), (n3), (n4), (n5); END");

        CallableStatement testCstmt = this.conn.prepareCall("{CALL testExecuteLargeUpdateProc(?, ?, ?, ?, ?)}");
        testCstmt.setInt(1, 1);
        testCstmt.setInt(2, 2);
        testCstmt.setInt(3, 3);
        testCstmt.setInt(4, 4);
        testCstmt.setInt(5, 5);

        long count = testCstmt.executeLargeUpdate();
        assertEquals(5, count);
        assertEquals(5, testCstmt.getLargeUpdateCount());

        this.rs = testCstmt.getGeneratedKeys();

        // Although not requested, CallableStatements makes gerenated keys always available.
        ResultSetMetaData rsmd = this.rs.getMetaData();
        assertEquals(1, rsmd.getColumnCount());
        assertEquals(JDBCType.BIGINT.getVendorTypeNumber().intValue(), rsmd.getColumnType(1));
        assertEquals(20, rsmd.getColumnDisplaySize(1));

        // We can't check the generated keys as they are not returned correctly in this case (last_insert_id is missing from OK_PACKET when executing inserts
        // within a stored procedure - Bug#21792359).
        //        long generatedKey = 0;
        //        while (this.rs.next()) {
        //            assertEquals(++generatedKey, this.rs.getLong(1));
        //        }
        //        assertEquals(5, generatedKey);
        this.rs.close();
    }

    /**
     * Test for (Server)PreparedStatement.executeLargeBatch().
     * Validate update count returned and generated keys.
     */
    public void testServerPrepStmtExecuteLargeBatch() throws Exception {
        /*
         * Fully working batch
         */
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        Connection testConn = getConnectionWithProps("useServerPrepStmts=true");

        this.pstmt = testConn.prepareStatement("INSERT INTO testExecuteLargeBatch (n) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
        this.pstmt.setInt(1, 1);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 2);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 3);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 4);
        this.pstmt.addBatch();
        this.pstmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (5), (6), (7)");
        this.pstmt.setInt(1, 8);
        this.pstmt.addBatch();
        this.pstmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (9), (10)");

        long[] counts = this.pstmt.executeLargeBatch();
        assertEquals(7, counts.length);
        assertEquals(1, counts[0]);
        assertEquals(1, counts[1]);
        assertEquals(1, counts[2]);
        assertEquals(1, counts[3]);
        assertEquals(3, counts[4]);
        assertEquals(1, counts[5]);
        assertEquals(2, counts[6]);

        this.rs = this.pstmt.getGeneratedKeys();

        ResultSetMetaData rsmd = this.rs.getMetaData();
        assertEquals(1, rsmd.getColumnCount());
        assertEquals(JDBCType.BIGINT.getVendorTypeNumber().intValue(), rsmd.getColumnType(1));
        assertEquals(20, rsmd.getColumnDisplaySize(1));

        long generatedKey = 0;
        while (this.rs.next()) {
            assertEquals(++generatedKey, this.rs.getLong(1));
        }
        assertEquals(10, generatedKey);
        this.rs.close();

        /*
         * Batch with failing queries
         */
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        this.pstmt = testConn.prepareStatement("INSERT INTO testExecuteLargeBatch (n) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
        this.pstmt.setInt(1, 1);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 2);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 3);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 4);
        this.pstmt.addBatch();
        this.pstmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (5), (6), (7)");
        this.pstmt.setString(1, "eight");
        this.pstmt.addBatch();
        this.pstmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (9), (10)");

        try {
            this.pstmt.executeLargeBatch();
            fail("BatchUpdateException expected");
        } catch (BatchUpdateException e) {
            assertEquals("Incorrect integer value: 'eight' for column 'n' at row 1", e.getMessage());
            counts = e.getLargeUpdateCounts();
            assertEquals(7, counts.length);
            assertEquals(1, counts[0]);
            assertEquals(1, counts[1]);
            assertEquals(1, counts[2]);
            assertEquals(1, counts[3]);
            assertEquals(3, counts[4]);
            assertEquals(Statement.EXECUTE_FAILED, counts[5]);
            assertEquals(2, counts[6]);
        }

        this.rs = this.pstmt.getGeneratedKeys();
        generatedKey = 0;
        while (this.rs.next()) {
            assertEquals(++generatedKey, this.rs.getLong(1));
        }
        assertEquals(9, generatedKey);
        this.rs.close();

        testConn.close();
    }

    /**
     * Test for Statement.[get/set]LargeMaxRows().
     */
    public void testStmtGetSetLargeMaxRows() throws Exception {
        assertEquals(0, this.stmt.getMaxRows());
        assertEquals(0, this.stmt.getLargeMaxRows());

        this.stmt.setMaxRows(50000000);

        assertEquals(50000000, this.stmt.getMaxRows());
        assertEquals(50000000, this.stmt.getLargeMaxRows());

        final Statement stmtTmp = this.stmt;
        assertThrows(SQLException.class, "setMaxRows\\(\\) out of range. 50000001 > 50000000.", new Callable<Void>() {
            public Void call() throws Exception {
                stmtTmp.setMaxRows(50000001);
                return null;
            }
        });

        this.stmt.setLargeMaxRows(0);

        assertEquals(0, this.stmt.getMaxRows());
        assertEquals(0, this.stmt.getLargeMaxRows());

        this.stmt.setLargeMaxRows(50000000);

        assertEquals(50000000, this.stmt.getMaxRows());
        assertEquals(50000000, this.stmt.getLargeMaxRows());

        assertThrows(SQLException.class, "setMaxRows\\(\\) out of range. 50000001 > 50000000.", new Callable<Void>() {
            public Void call() throws Exception {
                stmtTmp.setLargeMaxRows(50000001L);
                return null;
            }
        });
    }

    /**
     * Test for PreparedStatement.setObject().
     * Validate new methods as well as support for the types java.time.Local[Date][Time] and java.time.Offset[Date]Time.
     */
    public void testPrepStmtSetObjectAndNewSupportedTypes() throws Exception {
        /*
         * Objects java.time.Local[Date][Time] are supported via conversion to/from java.sql.[Date|Time|Timestamp].
         */
        createTable("testSetObjectPS1", "(id INT, d DATE, t TIME, dt DATETIME, ts TIMESTAMP)");

        this.pstmt = this.conn.prepareStatement("INSERT INTO testSetObjectPS1 VALUES (?, ?, ?, ?, ?)");
        validateTestDataLocalDTTypes("testSetObjectPS1", insertTestDataLocalDTTypes(this.pstmt));

        /*
         * Objects java.time.Offset[Date]Time are supported via conversion to *CHAR or serialization.
         */
        createTable("testSetObjectPS2", "(id INT, ot1 VARCHAR(100), ot2 BLOB, odt1 VARCHAR(100), odt2 BLOB)");

        this.pstmt = this.conn.prepareStatement("INSERT INTO testSetObjectPS2 VALUES (?, ?, ?, ?, ?)");
        validateTestDataOffsetDTTypes("testSetObjectPS2", insertTestDataOffsetDTTypes(this.pstmt));
    }

    /**
     * Test for PreparedStatement.setObject(), unsupported SQL types TIME_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE and REF_CURSOR.
     */
    public void testPrepStmtSetObjectAndNewUnsupportedTypes() throws Exception {
        checkUnsupportedTypesBehavior(this.conn.prepareStatement("SELECT ?"));
    }

    /**
     * Test for CallableStatement.setObject().
     * Validate new methods as well as support for the types java.time.Local[Date][Time] and java.time.Offset[Date]Time.
     */
    public void testCallStmtSetObjectAndNewSupportedTypes() throws Exception {
        /*
         * Objects java.time.Local[Date][Time] are supported via conversion to/from java.sql.[Date|Time|Timestamp].
         */
        createTable("testSetObjectCS1", "(id INT, d DATE, t TIME, dt DATETIME, ts TIMESTAMP)");
        createProcedure("testSetObjectCS1Proc", "(IN id INT, IN d DATE, IN t TIME, IN dt DATETIME, IN ts TIMESTAMP) BEGIN "
                + "INSERT INTO testSetObjectCS1 VALUES (id, d, t, dt, ts); END");

        CallableStatement testCstmt = this.conn.prepareCall("{CALL testSetObjectCS1Proc(?, ?, ?, ?, ?)}");
        validateTestDataLocalDTTypes("testSetObjectCS1", insertTestDataLocalDTTypes(testCstmt));

        /*
         * Objects java.time.Offset[Date]Time are supported via conversion to *CHAR or serialization.
         */
        createTable("testSetObjectCS2", "(id INT, ot1 VARCHAR(100), ot2 BLOB, odt1 VARCHAR(100), odt2 BLOB)");
        createProcedure("testSetObjectCS2Proc",
                "(id INT, ot1 VARCHAR(100), ot2 BLOB, odt1 VARCHAR(100), odt2 BLOB) BEGIN INSERT INTO testSetObjectCS2 VALUES (id, ot1, ot2, odt1, odt2); END");

        testCstmt = this.conn.prepareCall("{CALL testSetObjectCS2Proc(?, ?, ?, ?, ?)}");
        validateTestDataOffsetDTTypes("testSetObjectCS2", insertTestDataOffsetDTTypes(testCstmt));
    }

    /**
     * Test for CallableStatement.setObject(), unsupported SQL types TIME_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE and REF_CURSOR.
     */
    public void testCallStmtSetObjectAndNewUnsupportedTypes() throws Exception {
        createProcedure("testUnsupportedTypesProc", "(OUT param VARCHAR(20)) BEGIN SELECT 1; END");
        checkUnsupportedTypesBehavior(this.conn.prepareCall("{CALL testUnsupportedTypesProc(?)}"));
    }

    /**
     * Test for (Server)PreparedStatement.setObject().
     * Validate new methods as well as support for the types java.time.Local[Date][Time] and java.time.Offset[Date]Time.
     */
    public void testServPrepStmtSetObjectAndNewSupportedTypes() throws Exception {
        /*
         * Objects java.time.Local[Date][Time] are supported via conversion to/from java.sql.[Date|Time|Timestamp].
         */
        createTable("testSetObjectSPS1", "(id INT, d DATE, t TIME, dt DATETIME, ts TIMESTAMP)");

        Connection testConn = getConnectionWithProps("useServerPrepStmts=true");

        this.pstmt = testConn.prepareStatement("INSERT INTO testSetObjectSPS1 VALUES (?, ?, ?, ?, ?)");
        validateTestDataLocalDTTypes("testSetObjectSPS1", insertTestDataLocalDTTypes(this.pstmt));

        /*
         * Objects java.time.Offset[Date]Time are supported via conversion to *CHAR or serialization.
         */
        createTable("testSetObjectSPS2", "(id INT, ot1 VARCHAR(100), ot2 BLOB, odt1 VARCHAR(100), odt2 BLOB)");

        this.pstmt = testConn.prepareStatement("INSERT INTO testSetObjectSPS2 VALUES (?, ?, ?, ?, ?)");
        validateTestDataOffsetDTTypes("testSetObjectSPS2", insertTestDataOffsetDTTypes(this.pstmt));
    }

    /**
     * Test for (Server)PreparedStatement.setObject(), unsupported SQL types TIME_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE and REF_CURSOR.
     */
    public void testServPrepStmtSetObjectAndNewUnsupportedTypes() throws Exception {
        Connection testConn = getConnectionWithProps("useServerPrepStmts=true");
        checkUnsupportedTypesBehavior(testConn.prepareStatement("SELECT ?"));
        testConn.close();
    }

    /**
     * Helper method for *SetObject* tests.
     * Insert data into the given PreparedStatement, or any of its subclasses, with the following structure:
     * 1 - `id` INT
     * 2 - `d` DATE (or any kind of *CHAR)
     * 3 - `t` TIME (or any kind of *CHAR)
     * 4 - `dt` DATETIME (or any kind of *CHAR)
     * 5 - `ts` TIMESTAMP (or any kind of *CHAR)
     * 
     * @param pstmt
     * @return the row count of inserted records.
     * @throws Exception
     */
    private int insertTestDataLocalDTTypes(PreparedStatement pstmt) throws Exception {
        pstmt.setInt(1, 1);
        pstmt.setDate(2, testSqlDate);
        pstmt.setTime(3, testSqlTime);
        pstmt.setTimestamp(4, testSqlTimeStamp);
        pstmt.setTimestamp(5, testSqlTimeStamp);
        assertEquals(1, pstmt.executeUpdate());

        pstmt.setInt(1, 2);
        pstmt.setObject(2, testLocalDate);
        pstmt.setObject(3, testLocalTime);
        pstmt.setObject(4, testLocalDateTime);
        pstmt.setObject(5, testLocalDateTime);
        assertEquals(1, pstmt.executeUpdate());

        pstmt.setInt(1, 3);
        pstmt.setObject(2, testLocalDate, JDBCType.DATE);
        pstmt.setObject(3, testLocalTime, JDBCType.TIME);
        pstmt.setObject(4, testLocalDateTime, JDBCType.TIMESTAMP);
        pstmt.setObject(5, testLocalDateTime, JDBCType.TIMESTAMP);
        assertEquals(1, pstmt.executeUpdate());

        pstmt.setInt(1, 4);
        pstmt.setObject(2, testLocalDate, JDBCType.DATE, 10);
        pstmt.setObject(3, testLocalTime, JDBCType.TIME, 8);
        pstmt.setObject(4, testLocalDateTime, JDBCType.TIMESTAMP, 20);
        pstmt.setObject(5, testLocalDateTime, JDBCType.TIMESTAMP, 20);
        assertEquals(1, pstmt.executeUpdate());

        pstmt.setInt(1, 5);
        pstmt.setObject(2, testLocalDate, JDBCType.VARCHAR);
        pstmt.setObject(3, testLocalTime, JDBCType.VARCHAR);
        pstmt.setObject(4, testLocalDateTime, JDBCType.VARCHAR);
        pstmt.setObject(5, testLocalDateTime, JDBCType.VARCHAR);
        assertEquals(1, pstmt.executeUpdate());

        pstmt.setInt(1, 6);
        pstmt.setObject(2, testLocalDate, JDBCType.VARCHAR, 10);
        pstmt.setObject(3, testLocalTime, JDBCType.VARCHAR, 8);
        pstmt.setObject(4, testLocalDateTime, JDBCType.VARCHAR, 20);
        pstmt.setObject(5, testLocalDateTime, JDBCType.VARCHAR, 20);
        assertEquals(1, pstmt.executeUpdate());

        if (pstmt instanceof CallableStatement) {
            CallableStatement cstmt = (CallableStatement) pstmt;

            cstmt.setInt("id", 7);
            cstmt.setDate("d", testSqlDate);
            cstmt.setTime("t", testSqlTime);
            cstmt.setTimestamp("dt", testSqlTimeStamp);
            cstmt.setTimestamp("ts", testSqlTimeStamp);
            assertEquals(1, cstmt.executeUpdate());

            cstmt.setInt("id", 8);
            cstmt.setObject("d", testLocalDate);
            cstmt.setObject("t", testLocalTime);
            cstmt.setObject("dt", testLocalDateTime);
            cstmt.setObject("ts", testLocalDateTime);
            assertEquals(1, cstmt.executeUpdate());

            cstmt.setInt("id", 9);
            cstmt.setObject("d", testLocalDate, JDBCType.DATE);
            cstmt.setObject("t", testLocalTime, JDBCType.TIME);
            cstmt.setObject("dt", testLocalDateTime, JDBCType.TIMESTAMP);
            cstmt.setObject("ts", testLocalDateTime, JDBCType.TIMESTAMP);
            assertEquals(1, cstmt.executeUpdate());

            cstmt.setInt("id", 10);
            cstmt.setObject("d", testLocalDate, JDBCType.DATE, 10);
            cstmt.setObject("t", testLocalTime, JDBCType.TIME, 8);
            cstmt.setObject("dt", testLocalDateTime, JDBCType.TIMESTAMP, 20);
            cstmt.setObject("ts", testLocalDateTime, JDBCType.TIMESTAMP, 20);
            assertEquals(1, cstmt.executeUpdate());

            cstmt.setInt("id", 11);
            cstmt.setObject("d", testLocalDate, JDBCType.VARCHAR);
            cstmt.setObject("t", testLocalTime, JDBCType.VARCHAR);
            cstmt.setObject("dt", testLocalDateTime, JDBCType.VARCHAR);
            cstmt.setObject("ts", testLocalDateTime, JDBCType.VARCHAR);
            assertEquals(1, cstmt.executeUpdate());

            cstmt.setInt("id", 12);
            cstmt.setObject("d", testLocalDate, JDBCType.VARCHAR, 10);
            cstmt.setObject("t", testLocalTime, JDBCType.VARCHAR, 8);
            cstmt.setObject("dt", testLocalDateTime, JDBCType.VARCHAR, 20);
            cstmt.setObject("ts", testLocalDateTime, JDBCType.VARCHAR, 20);
            assertEquals(1, cstmt.executeUpdate());

            return 12;
        }

        return 6;
    }

    /**
     * Helper method for *SetObject* tests.
     * Validate the test data contained in the given ResultSet with following structure:
     * 1 - `id` INT
     * 2 - `d` DATE (or any kind of *CHAR)
     * 3 - `t` TIME (or any kind of *CHAR)
     * 4 - `dt` DATETIME (or any kind of *CHAR)
     * 5 - `ts` TIMESTAMP (or any kind of *CHAR)
     * 
     * Additionally validate support for the types java.time.Local[Date][Time] in ResultSet.getObject().
     * 
     * @param tableName
     * @param expectedRowCount
     * @throws Exception
     */
    private void validateTestDataLocalDTTypes(String tableName, int expectedRowCount) throws Exception {
        this.rs = this.stmt.executeQuery("SELECT * FROM " + tableName);

        int rowCount = 0;
        while (rs.next()) {
            String row = "Row " + this.rs.getInt(1);
            assertEquals(row, ++rowCount, this.rs.getInt(1));

            assertEquals(row, testDateString, this.rs.getString(2));
            assertEquals(row, testTimeString, this.rs.getString(3));
            assertEquals(row, testDateTimeString, this.rs.getString(4));
            assertEquals(row, testDateTimeString, this.rs.getString(5));

            assertEquals(row, testSqlDate, this.rs.getDate(2));
            assertEquals(row, testSqlTime, this.rs.getTime(3));
            assertEquals(row, testSqlTimeStamp, this.rs.getTimestamp(4));
            assertEquals(row, testSqlTimeStamp, this.rs.getTimestamp(5));

            assertEquals(row, testLocalDate, this.rs.getObject(2, LocalDate.class));
            assertEquals(row, testLocalTime, this.rs.getObject(3, LocalTime.class));
            assertEquals(row, testLocalDateTime, this.rs.getObject(4, LocalDateTime.class));
            assertEquals(row, testLocalDateTime, this.rs.getObject(5, LocalDateTime.class));

            assertEquals(row, rowCount, this.rs.getInt("id"));

            assertEquals(row, testDateString, this.rs.getString("d"));
            assertEquals(row, testTimeString, this.rs.getString("t"));
            assertEquals(row, testDateTimeString, this.rs.getString("dt"));
            assertEquals(row, testDateTimeString, this.rs.getString("ts"));

            assertEquals(row, testSqlDate, this.rs.getDate("d"));
            assertEquals(row, testSqlTime, this.rs.getTime("t"));
            assertEquals(row, testSqlTimeStamp, this.rs.getTimestamp("dt"));
            assertEquals(row, testSqlTimeStamp, this.rs.getTimestamp("ts"));

            assertEquals(row, testLocalDate, this.rs.getObject("d", LocalDate.class));
            assertEquals(row, testLocalTime, this.rs.getObject("t", LocalTime.class));
            assertEquals(row, testLocalDateTime, this.rs.getObject("dt", LocalDateTime.class));
            assertEquals(row, testLocalDateTime, this.rs.getObject("ts", LocalDateTime.class));
        }
        assertEquals(expectedRowCount, rowCount);
    }

    /**
     * Helper method for *SetObject* tests.
     * Insert data into the given PreparedStatement, or any of its subclasses, with the following structure:
     * 1 - `id` INT
     * 2 - `ot1` VARCHAR
     * 3 - `ot2` BLOB
     * 4 - `odt1` VARCHAR
     * 5 - `odt2` BLOB
     * 
     * @param pstmt
     * @return the row count of inserted records.
     * @throws Exception
     */
    private int insertTestDataOffsetDTTypes(PreparedStatement pstmt) throws Exception {
        pstmt.setInt(1, 1);
        pstmt.setObject(2, testOffsetTime, JDBCType.VARCHAR);
        pstmt.setObject(3, testOffsetTime);
        pstmt.setObject(4, testOffsetDateTime, JDBCType.VARCHAR);
        pstmt.setObject(5, testOffsetDateTime);
        assertEquals(1, pstmt.executeUpdate());

        if (pstmt instanceof CallableStatement) {
            CallableStatement cstmt = (CallableStatement) pstmt;

            cstmt.setInt("id", 2);
            cstmt.setObject("ot1", testOffsetTime, JDBCType.VARCHAR);
            cstmt.setObject("ot2", testOffsetTime);
            cstmt.setObject("odt1", testOffsetDateTime, JDBCType.VARCHAR);
            cstmt.setObject("odt2", testOffsetDateTime);
            assertEquals(1, cstmt.executeUpdate());

            return 2;
        }

        return 1;
    }

    /**
     * Helper method for *SetObject* tests.
     * Validate the test data contained in the given ResultSet with following structure:
     * 1 - `id` INT
     * 2 - `ot1` VARCHAR
     * 3 - `ot2` BLOB
     * 4 - `odt1` VARCHAR
     * 5 - `odt2` BLOB
     * 
     * Additionally validate support for the types java.time.Offset[Date]Time in ResultSet.getObject().
     * 
     * @param tableName
     * @param expectedRowCount
     * @throws Exception
     */
    private void validateTestDataOffsetDTTypes(String tableName, int expectedRowCount) throws Exception {
        Connection testConn = getConnectionWithProps("autoDeserialize=true"); // Offset[Date]Time are supported via object serialization too.
        Statement testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT * FROM " + tableName);

        int rowCount = 0;
        while (rs.next()) {
            String row = "Row " + rs.getInt(1);
            assertEquals(++rowCount, rs.getInt(1));

            assertEquals(row, testOffsetTime, this.rs.getObject(2, OffsetTime.class));
            assertEquals(row, testOffsetTime, this.rs.getObject(3, OffsetTime.class));
            assertEquals(row, testOffsetDateTime, this.rs.getObject(4, OffsetDateTime.class));
            assertEquals(row, testOffsetDateTime, this.rs.getObject(5, OffsetDateTime.class));

            assertEquals(row, rowCount, this.rs.getInt("id"));

            assertEquals(row, testOffsetTime, this.rs.getObject("ot1", OffsetTime.class));
            assertEquals(row, testOffsetTime, this.rs.getObject("ot2", OffsetTime.class));
            assertEquals(row, testOffsetDateTime, this.rs.getObject("odt1", OffsetDateTime.class));
            assertEquals(row, testOffsetDateTime, this.rs.getObject("odt2", OffsetDateTime.class));
        }
        assertEquals(expectedRowCount, rowCount);
        testConn.close();
    }

    /**
     * Helper method for *SetObject* tests.
     * Check unsupported types behavior for the given PreparedStatement with a single placeholder. If this is a CallableStatement then the placeholder must
     * coincide with a parameter named `param`.
     * 
     * @param pstmt
     */
    private void checkUnsupportedTypesBehavior(final PreparedStatement pstmt) {
        final CallableStatement cstmt = pstmt instanceof CallableStatement ? (CallableStatement) pstmt : null;

        /*
         * Unsupported SQL types TIME_WITH_TIMEZONE and TIMESTAMP_WITH_TIMEZONE.
         */
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                pstmt.setObject(1, OffsetTime.now(), JDBCType.TIME_WITH_TIMEZONE);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                pstmt.setObject(1, OffsetDateTime.now(), JDBCType.TIMESTAMP_WITH_TIMEZONE);
                return null;
            }
        });
        if (cstmt != null) {
            assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    cstmt.setObject("param", OffsetTime.now(), JDBCType.TIME_WITH_TIMEZONE);
                    return null;
                }
            });
            assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    cstmt.setObject("param", OffsetDateTime.now(), JDBCType.TIMESTAMP_WITH_TIMEZONE);
                    return null;
                }
            });
        }
        /*
         * Unsupported SQL type REF_CURSOR.
         */
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                pstmt.setObject(1, new Object(), JDBCType.REF_CURSOR);
                return null;
            }
        });
        if (cstmt != null) {
            assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    cstmt.setObject("param", new Object(), JDBCType.REF_CURSOR);
                    return null;
                }
            });
        }
    }

    /**
     * Test for CallableStatement.registerOutParameter().
     */
    public void testCallStmtRegisterOutParameter() throws Exception {
        createProcedure("testRegisterOutParameterProc", "(OUT b BIT, OUT i INT, OUT c CHAR(10)) BEGIN SELECT 1, 1234, 'MySQL' INTO b, i, c; END");
        final CallableStatement testCstmt = this.conn.prepareCall("{CALL testRegisterOutParameterProc(?, ?, ?)}");

        // registerOutParameter by parameter index
        testCstmt.registerOutParameter(1, JDBCType.BOOLEAN);
        testCstmt.registerOutParameter(2, JDBCType.INTEGER);
        testCstmt.registerOutParameter(3, JDBCType.CHAR);
        testCstmt.execute();

        assertEquals(Boolean.TRUE, testCstmt.getObject(1));
        assertEquals(Integer.valueOf(1234), testCstmt.getObject(2));
        assertEquals("MySQL", testCstmt.getObject(3));

        testCstmt.registerOutParameter(1, JDBCType.BOOLEAN, 1);
        testCstmt.registerOutParameter(2, JDBCType.INTEGER, 1);
        testCstmt.registerOutParameter(3, JDBCType.CHAR, 1);
        testCstmt.execute();

        assertEquals(Boolean.TRUE, testCstmt.getObject(1));
        assertEquals(Integer.valueOf(1234), testCstmt.getObject(2));
        assertEquals("MySQL", testCstmt.getObject(3));

        testCstmt.registerOutParameter(1, JDBCType.BOOLEAN, "dummy");
        testCstmt.registerOutParameter(2, JDBCType.INTEGER, "dummy");
        testCstmt.registerOutParameter(3, JDBCType.CHAR, "dummy");
        testCstmt.execute();

        assertEquals(Boolean.TRUE, testCstmt.getObject(1));
        assertEquals(Integer.valueOf(1234), testCstmt.getObject(2));
        assertEquals("MySQL", testCstmt.getObject(3));

        // registerOutParameter by parameter name
        testCstmt.registerOutParameter("b", JDBCType.BOOLEAN);
        testCstmt.registerOutParameter("i", JDBCType.INTEGER);
        testCstmt.registerOutParameter("c", JDBCType.CHAR);
        testCstmt.execute();

        assertEquals(Boolean.TRUE, testCstmt.getObject(1));
        assertEquals(Integer.valueOf(1234), testCstmt.getObject(2));
        assertEquals("MySQL", testCstmt.getObject(3));

        testCstmt.registerOutParameter("b", JDBCType.BOOLEAN, 1);
        testCstmt.registerOutParameter("i", JDBCType.INTEGER, 1);
        testCstmt.registerOutParameter("c", JDBCType.CHAR, 1);
        testCstmt.execute();

        assertEquals(Boolean.TRUE, testCstmt.getObject(1));
        assertEquals(Integer.valueOf(1234), testCstmt.getObject(2));
        assertEquals("MySQL", testCstmt.getObject(3));

        testCstmt.registerOutParameter("b", JDBCType.BOOLEAN, "dummy");
        testCstmt.registerOutParameter("i", JDBCType.INTEGER, "dummy");
        testCstmt.registerOutParameter("c", JDBCType.CHAR, "dummy");
        testCstmt.execute();

        assertEquals(Boolean.TRUE, testCstmt.getObject(1));
        assertEquals(Integer.valueOf(1234), testCstmt.getObject(2));
        assertEquals("MySQL", testCstmt.getObject(3));
    }

    /**
     * Test for CallableStatement.registerOutParameter(), unsupported SQL types TIME_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE and REF_CURSOR.
     */
    public void testCallStmtRegisterOutParameterNewUnsupportedTypes() throws Exception {
        createProcedure("testUnsupportedTypesProc", "(OUT param VARCHAR(20)) BEGIN SELECT 1; END");
        final CallableStatement testCstmt = this.conn.prepareCall("{CALL testUnsupportedTypesProc(?)}");

        /*
         * Unsupported SQL types TIME_WITH_TIMEZONE and TIMESTAMP_WITH_TIMEZONE.
         */
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter(1, JDBCType.TIME_WITH_TIMEZONE);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter(1, JDBCType.TIME_WITH_TIMEZONE, 1);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter(1, JDBCType.TIME_WITH_TIMEZONE, "dummy");
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter("param", JDBCType.TIME_WITH_TIMEZONE);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter("param", JDBCType.TIME_WITH_TIMEZONE, 1);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter("param", JDBCType.TIME_WITH_TIMEZONE, "dummy");
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter(1, JDBCType.TIMESTAMP_WITH_TIMEZONE);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter(1, JDBCType.TIMESTAMP_WITH_TIMEZONE, 1);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter(1, JDBCType.TIMESTAMP_WITH_TIMEZONE, "dummy");
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter("param", JDBCType.TIMESTAMP_WITH_TIMEZONE);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter("param", JDBCType.TIMESTAMP_WITH_TIMEZONE, 1);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter("param", JDBCType.TIMESTAMP_WITH_TIMEZONE, "dummy");
                return null;
            }
        });

        /*
         * Unsupported SQL type REF_CURSOR.
         */
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter(1, JDBCType.REF_CURSOR);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter(1, JDBCType.REF_CURSOR, 1);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter(1, JDBCType.REF_CURSOR, "dummy");
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter("param", JDBCType.REF_CURSOR);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter("param", JDBCType.REF_CURSOR, 1);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter("param", JDBCType.REF_CURSOR, "dummy");
                return null;
            }
        });
    }
}