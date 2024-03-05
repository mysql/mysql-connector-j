/*
 * Copyright (c) 2002, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package testsuite.simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import com.mysql.cj.conf.PropertyKey;

import testsuite.BaseTestCase;

public class NumbersTest extends BaseTestCase {

    private static final long TEST_BIGINT_VALUE = 6147483647L;

    @Test
    public void testNumbers() throws SQLException {
        createTable("number_test", "(minBigInt bigint, maxBigInt bigint, testBigInt bigint)");
        this.stmt.executeUpdate(
                "INSERT INTO number_test (minBigInt,maxBigInt,testBigInt) values (" + Long.MIN_VALUE + "," + Long.MAX_VALUE + "," + TEST_BIGINT_VALUE + ")");

        this.rs = this.stmt.executeQuery("SELECT * from number_test");

        while (this.rs.next()) {
            long minBigInt = this.rs.getLong(1);
            long maxBigInt = this.rs.getLong(2);
            long testBigInt = this.rs.getLong(3);
            assertTrue(minBigInt == Long.MIN_VALUE, "Minimum bigint not stored correctly");
            assertTrue(maxBigInt == Long.MAX_VALUE, "Maximum bigint not stored correctly");
            assertTrue(TEST_BIGINT_VALUE == testBigInt, "Test bigint not stored correctly");
        }
    }

    @Test
    public void testFloatSetters() throws Exception {
        createTable("testFloatSetters", "(f1 FLOAT(8,4), f2 FLOAT(8,4), f3 FLOAT(8,4))");

        Properties props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

        boolean useSPS = false;
        boolean rewriteBS = false;
        do {
            this.stmt.executeUpdate("truncate table testFloatSetters");
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSPS);
            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "" + rewriteBS);

            Connection con = getConnectionWithProps(props);
            con.setAutoCommit(false);
            PreparedStatement ps = con.prepareStatement("insert into testFloatSetters values(?,?,?)");
            for (int i = 1; i < 10000; i++) {
                ps.setFloat(1, 3.0f);
                ps.setFloat(2, 3.12f);
                ps.setFloat(3, 3.12345f);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
            con.commit();

            int cnt = 0;
            this.rs = this.stmt.executeQuery("select * from testFloatSetters");
            while (this.rs.next()) {
                cnt++;
                assertEquals(3.0f, this.rs.getFloat(1));
                assertEquals(3.12f, this.rs.getFloat(2));
                assertEquals(3.1235f, this.rs.getFloat(3));
            }
            assertEquals(9999, cnt);

        } while ((useSPS = !useSPS) || (rewriteBS = !rewriteBS));
    }

    @Test
    public void testDoubleSetters() throws Exception {
        createTable("testDoubleSetters", "(f1 DOUBLE(8,4), f2 DOUBLE(8,4), f3 DOUBLE(8,4))");

        Properties props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

        boolean useSPS = false;
        boolean rewriteBS = false;
        do {
            this.stmt.executeUpdate("truncate table testDoubleSetters");
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSPS);
            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "" + rewriteBS);

            Connection con = getConnectionWithProps(props);
            con.setAutoCommit(false);
            PreparedStatement ps = con.prepareStatement("insert into testDoubleSetters values(?,?,?)");
            for (int i = 1; i < 10000; i++) {
                ps.setDouble(1, 3.0d);
                ps.setDouble(2, 3.12d);
                ps.setDouble(3, 3.12345d);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
            con.commit();

            int cnt = 0;
            this.rs = this.stmt.executeQuery("select * from testDoubleSetters");
            while (this.rs.next()) {
                cnt++;
                assertEquals(3.0d, this.rs.getDouble(1));
                assertEquals(3.12d, this.rs.getDouble(2));
                assertEquals(3.1235d, this.rs.getDouble(3));
            }
            assertEquals(9999, cnt);

        } while ((useSPS = !useSPS) || (rewriteBS = !rewriteBS));
    }

}
