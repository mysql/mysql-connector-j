/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates. All rights reserved.
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

package testsuite.simple;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import testsuite.BaseTestCase;

/**
 * Tests result set traversal methods.
 */
public class TraversalTest extends BaseTestCase {
    @BeforeEach
    public void setUp() throws Exception {
        createTestTable();
    }

    @AfterEach
    public void tearDown() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE TRAVERSAL");
        } catch (SQLException SQLE) {
        }
    }

    @Test
    public void testTraversal() throws SQLException {
        Statement scrollableStmt = null;

        try {
            scrollableStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            this.rs = scrollableStmt.executeQuery("SELECT * FROM TRAVERSAL ORDER BY pos");

            // Test isFirst()
            if (this.rs.first()) {
                assertTrue(this.rs.isFirst(), "ResultSet.isFirst() failed");
                this.rs.relative(-1);
                assertTrue(this.rs.isBeforeFirst(), "ResultSet.isBeforeFirst() failed");
            }

            // Test isLast()
            if (this.rs.last()) {
                assertTrue(this.rs.isLast(), "ResultSet.isLast() failed");
                this.rs.relative(1);
                assertTrue(this.rs.isAfterLast(), "ResultSet.isAfterLast() failed");
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

                assertTrue(pos == (this.rs.getRow() - 1), "ResultSet.getRow() failed.");

                count++;

            }

            assertTrue(forwardOk, "Only traversed " + count + " / 100 rows");

            boolean isAfterLast = this.rs.isAfterLast();
            assertTrue(isAfterLast, "ResultSet.isAfterLast() failed");
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

            assertTrue(reverseOk, "ResultSet.previous() failed");

            boolean isBeforeFirst = this.rs.isBeforeFirst();
            assertTrue(isBeforeFirst, "ResultSet.isBeforeFirst() failed");

            this.rs.next();
            boolean isFirst = this.rs.isFirst();
            assertTrue(isFirst, "ResultSet.isFirst() failed");

            // Test absolute positioning
            this.rs.absolute(50);
            int pos = this.rs.getInt("pos");
            assertTrue(pos == 49, "ResultSet.absolute() failed");

            // Test relative positioning
            this.rs.relative(-1);
            pos = this.rs.getInt("pos");
            assertTrue(pos == 48, "ResultSet.relative(-1) failed");

            // Test bogus absolute index
            boolean onResultSet = this.rs.absolute(200);
            assertTrue(onResultSet == false, "ResultSet.absolute() to point off result set failed");
            onResultSet = this.rs.absolute(100);
            assertTrue(onResultSet, "ResultSet.absolute() from off this.rs to on this.rs failed");

            onResultSet = this.rs.absolute(-99);
            assertTrue(onResultSet, "ResultSet.absolute(-99) failed");
            assertTrue(this.rs.getInt(1) == 1, "ResultSet absolute(-99) failed");

        } finally {
            if (scrollableStmt != null) {

                try {
                    scrollableStmt.close();
                } catch (SQLException sqlEx) {
                    // ignore
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
            // ignore
        }

        this.stmt.executeUpdate("CREATE TABLE TRAVERSAL (pos int PRIMARY KEY, stringdata CHAR(32))");

        for (int i = 0; i < 100; i++) {
            this.stmt.executeUpdate("INSERT INTO TRAVERSAL VALUES (" + i + ", 'StringData')");
        }
    }
}
