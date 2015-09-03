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
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

import com.mysql.jdbc.MysqlDefs;

import testsuite.BaseTestCase;

public class StatementsTest extends BaseTestCase {

    public StatementsTest(String name) {
        super(name);
    }

    /**
     * Test for Statement.executeLargeBatch()
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
     * Test for Statement.executeLargeUpdate(String)
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
     * Test for Statement.executeLargeUpdate(String, int)
     * Case: explicitly requesting generated keys.
     */
    public void testStmtExecuteLargeUpdateExplicitGeneratedKeys() throws Exception {
        createTable("testExecuteLargeUpdate", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        long count = this.stmt.executeLargeUpdate("INSERT INTO testExecuteLargeUpdate (n) VALUES (1), (2), (3), (4), (5)", Statement.RETURN_GENERATED_KEYS);
        assertEquals(5, count);
        assertEquals(5, this.stmt.getLargeUpdateCount());

        this.rs = this.stmt.getGeneratedKeys();

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
     * Test for Statement.executeLargeUpdate(String, int[])
     * Case: requesting generated keys by defining column indexes.
     */
    public void testStmtExecuteLargeUpdateGeneratedKeysByColumIndexes() throws Exception {
        createTable("testExecuteLargeUpdate", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        long count = this.stmt.executeLargeUpdate("INSERT INTO testExecuteLargeUpdate (n) VALUES (1), (2), (3), (4), (5)", new int[] { 1 });
        assertEquals(5, count);
        assertEquals(5, this.stmt.getLargeUpdateCount());

        this.rs = this.stmt.getGeneratedKeys();

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
     * Test for Statement.executeLargeUpdate(String, String[])
     * Case: requesting generated keys by defining column names.
     */
    public void testStmtExecuteLargeUpdateGeneratedKeysByColumNames() throws Exception {
        createTable("testExecuteLargeUpdate", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        long count = this.stmt.executeLargeUpdate("INSERT INTO testExecuteLargeUpdate (n) VALUES (1), (2), (3), (4), (5)", new String[] { "id" });
        assertEquals(5, count);
        assertEquals(5, this.stmt.getLargeUpdateCount());

        this.rs = this.stmt.getGeneratedKeys();

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
     * Test for Statement.[get/set]LargeMaxRows()
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
     * Test for PreparedStatement.executeLargeBatch()
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
     * Test for PreparedStatement.executeLargeUpdate()
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
     * Test for PreparedStatement.executeLargeUpdate()
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
     * Test for CallableStatement.executeLargeBatch()
     */
    public void testCallStmtExecuteLargeBatch() throws Exception {
        /*
         * Fully working batch
         */
        createProcedure("testExecuteLargeBatch_insert", "(IN n INT) BEGIN INSERT INTO testExecuteLargeBatch (n) VALUES (n); END");
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        CallableStatement cstmt = this.conn.prepareCall("{CALL testExecuteLargeBatch_insert(?)}");
        cstmt.setInt(1, 1);
        cstmt.addBatch();
        cstmt.setInt(1, 2);
        cstmt.addBatch();
        cstmt.setInt(1, 3);
        cstmt.addBatch();
        cstmt.setInt(1, 4);
        cstmt.addBatch();
        cstmt.addBatch("{CALL testExecuteLargeBatch_insert(5)}");
        cstmt.addBatch("{CALL testExecuteLargeBatch_insert(6)}");
        cstmt.addBatch("{CALL testExecuteLargeBatch_insert(7)}");
        cstmt.setInt(1, 8);
        cstmt.addBatch();
        cstmt.addBatch("{CALL testExecuteLargeBatch_insert(9)}");
        cstmt.addBatch("{CALL testExecuteLargeBatch_insert(10)}");

        long[] counts = cstmt.executeLargeBatch();
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

        this.rs = cstmt.getGeneratedKeys();

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

        cstmt.close();

        /*
         * Batch with failing queries
         */
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        cstmt = this.conn.prepareCall("{call testExecuteLargeBatch_insert(?)}");
        cstmt.setInt(1, 1);
        cstmt.addBatch();
        cstmt.setInt(1, 2);
        cstmt.addBatch();
        cstmt.setInt(1, 3);
        cstmt.addBatch();
        cstmt.setInt(1, 4);
        cstmt.addBatch();
        cstmt.addBatch("{call testExecuteLargeBatch_insert(5)}");
        cstmt.addBatch("{call testExecuteLargeBatch_insert('six')}");
        cstmt.addBatch("{call testExecuteLargeBatch_insert(7)}");
        cstmt.setString(1, "eight");
        cstmt.addBatch();
        cstmt.addBatch("{CALL testExecuteLargeBatch_insert(9)}");
        cstmt.addBatch("{CALL testExecuteLargeBatch_insert(10)}");

        try {
            cstmt.executeLargeBatch();
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

        cstmt.close();
    }

    /**
     * Test for CallableStatement.executeLargeUpdate()
     */
    public void testCallStmtExecuteLargeUpdate() throws Exception {
        createProcedure("testExecuteLargeUpdate_insert", "(IN n1 INT, IN n2 INT, IN n3 INT, IN n4 INT, IN n5 INT) BEGIN "
                + "INSERT INTO testExecuteLargeUpdate (n) VALUES (n1), (n2), (n3), (n4), (n5); END");
        createTable("testExecuteLargeUpdate", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        CallableStatement cstmt = this.conn.prepareCall("{CALL testExecuteLargeUpdate_insert(?, ?, ?, ?, ?)}");
        cstmt.setInt(1, 1);
        cstmt.setInt(2, 2);
        cstmt.setInt(3, 3);
        cstmt.setInt(4, 4);
        cstmt.setInt(5, 5);

        long count = cstmt.executeLargeUpdate();
        assertEquals(5, count);
        assertEquals(5, cstmt.getLargeUpdateCount());

        this.rs = cstmt.getGeneratedKeys();

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
     * Test for ServerPreparedStatement.executeLargeBatch()
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
    }
}