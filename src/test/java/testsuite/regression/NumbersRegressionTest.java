/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSetMetaData;

import org.junit.jupiter.api.Test;

import testsuite.BaseTestCase;

/**
 * Tests various number-handling issues that have arisen in the JDBC driver at one time or another.
 */
public class NumbersRegressionTest extends BaseTestCase {
    /**
     * Tests that BIGINT retrieval works correctly
     * 
     * @throws Exception
     */
    @Test
    public void testBigInt() throws Exception {
        createTable("bigIntRegression", "(val BIGINT NOT NULL)");
        this.stmt.executeUpdate("INSERT INTO bigIntRegression VALUES (6692730313872877584)");
        this.rs = this.stmt.executeQuery("SELECT val FROM bigIntRegression");

        while (this.rs.next()) {
            // check retrieval
            long retrieveAsLong = this.rs.getLong(1);
            assertEquals(6692730313872877584L, retrieveAsLong);
        }

        this.rs.close();
        this.stmt.executeUpdate("DROP TABLE IF EXISTS bigIntRegression");

        String bigIntAsString = "6692730313872877584";

        long parsedBigIntAsLong = Long.parseLong(bigIntAsString);

        // check JDK parsing
        assertTrue(bigIntAsString.equals(String.valueOf(parsedBigIntAsLong)));
    }

    /**
     * Tests correct type assignment for MySQL FLOAT and REAL datatypes.
     * 
     * @throws Exception
     */
    @Test
    public void testFloatsAndReals() throws Exception {
        createTable("floatsAndReals", "(floatCol FLOAT, realCol REAL, doubleCol DOUBLE)");
        this.stmt.executeUpdate("INSERT INTO floatsAndReals VALUES (0, 0, 0)");

        this.rs = this.stmt.executeQuery("SELECT floatCol, realCol, doubleCol FROM floatsAndReals");

        ResultSetMetaData rsmd = this.rs.getMetaData();

        this.rs.next();

        assertTrue(rsmd.getColumnClassName(1).equals("java.lang.Float"));
        assertTrue(this.rs.getObject(1).getClass().getName().equals("java.lang.Float"));

        assertTrue(rsmd.getColumnClassName(2).equals("java.lang.Double"));
        assertTrue(this.rs.getObject(2).getClass().getName().equals("java.lang.Double"));

        assertTrue(rsmd.getColumnClassName(3).equals("java.lang.Double"));
        assertTrue(this.rs.getObject(3).getClass().getName().equals("java.lang.Double"));
    }

    /**
     * Tests that ResultSetMetaData precision and scale methods work correctly for all numeric types.
     * 
     * @throws Exception
     */
    @Test
    public void testPrecisionAndScale() throws Exception {
        testPrecisionForType("TINYINT", 3, -1, false);
        testPrecisionForType("TINYINT", 3, -1, true);
        testPrecisionForType("SMALLINT", 5, -1, false);
        testPrecisionForType("SMALLINT", 5, -1, true);
        testPrecisionForType("MEDIUMINT", 7, -1, false);
        testPrecisionForType("MEDIUMINT", 8, -1, true);
        testPrecisionForType("INT", 10, -1, false);
        testPrecisionForType("INT", 10, -1, true);
        testPrecisionForType("BIGINT", 19, -1, false);
        testPrecisionForType("BIGINT", 20, -1, true);

        testPrecisionForType("FLOAT", 8, 4, false);
        testPrecisionForType("FLOAT", 8, 4, true);
        testPrecisionForType("DOUBLE", 8, 4, false);
        testPrecisionForType("DOUBLE", 8, 4, true);

        testPrecisionForType("DECIMAL", 8, 4, false);
        testPrecisionForType("DECIMAL", 8, 0, false);
        testPrecisionForType("DECIMAL", 10, 4, true);
        testPrecisionForType("DECIMAL", 10, 0, true);

        testPrecisionForType("DECIMAL", 9, 0, false);
        testPrecisionForType("DECIMAL", 9, 0, true);
    }

    private void testPrecisionForType(String typeName, int m, int d, boolean unsigned) throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS precisionAndScaleRegression");

            StringBuilder createStatement = new StringBuilder("CREATE TABLE precisionAndScaleRegression ( val ");
            createStatement.append(typeName);
            createStatement.append("(");
            createStatement.append(m);

            if (d != -1) {
                createStatement.append(",");
                createStatement.append(d);
            }

            createStatement.append(")");

            if (unsigned) {
                createStatement.append(" UNSIGNED ");
            }

            createStatement.append(" NOT NULL)");

            this.stmt.executeUpdate(createStatement.toString());

            this.rs = this.stmt.executeQuery("SELECT val FROM precisionAndScaleRegression");

            ResultSetMetaData rsmd = this.rs.getMetaData();
            assertTrue(rsmd.getPrecision(1) == m,
                    "Precision returned incorrectly for type " + typeName + ", " + m + " != rsmd.getPrecision() = " + rsmd.getPrecision(1));

            if (d != -1) {
                assertTrue(rsmd.getScale(1) == d, "Scale returned incorrectly for type " + typeName + ", " + d + " != rsmd.getScale() = " + rsmd.getScale(1));
            }
        } finally {
            if (this.rs != null) {
                try {
                    this.rs.close();
                } catch (Exception ex) {
                    // ignore
                }
            }

            this.stmt.executeUpdate("DROP TABLE IF EXISTS precisionAndScaleRegression");
        }
    }

    @Test
    public void testIntShouldReturnLong() throws Exception {
        createTable("testIntRetLong", "(field1 INT)");
        this.stmt.executeUpdate("INSERT INTO testIntRetLong VALUES (1)");

        this.rs = this.stmt.executeQuery("SELECT * FROM testIntRetLong");
        this.rs.next();

        assertTrue(this.rs.getObject(1).getClass().equals(java.lang.Integer.class));
    }

    /**
     * Tests fix for BUG#5729, UNSIGNED BIGINT returned incorrectly
     * 
     * @throws Exception
     */
    @Test
    public void testBug5729() throws Exception {
        String valueAsString = "1095923280000";

        createTable("testBug5729", "(field1 BIGINT UNSIGNED)");
        this.stmt.executeUpdate("INSERT INTO testBug5729 VALUES (" + valueAsString + ")");

        this.rs = this.conn.prepareStatement("SELECT * FROM testBug5729").executeQuery();
        this.rs.next();

        assertTrue(this.rs.getObject(1).toString().equals(valueAsString));
    }

    /**
     * Tests fix for BUG#8484 - ResultSet.getBigDecimal() throws exception when rounding would need to occur to set scale.
     * 
     * @throws Exception
     * @deprecated
     */
    @Deprecated
    @Test
    public void testBug8484() throws Exception {
        createTable("testBug8484", "(field1 DECIMAL(16, 8), field2 varchar(32))");
        this.stmt.executeUpdate("INSERT INTO testBug8484 VALUES (12345678.12345678, '')");
        this.rs = this.stmt.executeQuery("SELECT field1, field2 FROM testBug8484");
        this.rs.next();
        assertEquals("12345678.123", this.rs.getBigDecimal(1, 3).toString());
        assertEquals("0.000", this.rs.getBigDecimal(2, 3).toString());

        this.pstmt = this.conn.prepareStatement("SELECT field1, field2 FROM testBug8484");
        this.rs = this.pstmt.executeQuery();
        this.rs.next();
        assertEquals("12345678.123", this.rs.getBigDecimal(1, 3).toString());
        assertEquals("0.000", this.rs.getBigDecimal(2, 3).toString());
    }
}
