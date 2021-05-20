/*
 * Copyright (c) 2014, 2021, Oracle and/or its affiliates.
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import com.mysql.cj.CharsetMappingWrapper;
import com.mysql.cj.MysqlConnection;
import com.mysql.cj.Query;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.util.StringUtils;

import testsuite.BaseQueryInterceptor;
import testsuite.BaseTestCase;

public class CharsetRegressionTest extends BaseTestCase {
    /**
     * Tests fix for Bug#73663 (19479242), utf8mb4 does not work for connector/j >=5.1.13
     * 
     * This test is only run when character_set_server=utf8mb4 and collation-server set to one of utf8mb4 collations (it's better to test two configurations:
     * with default utf8mb4_general_ci and one of non-default, say utf8mb4_bin)
     * 
     * @throws Exception
     */
    @Test
    public void testBug73663() throws Exception {
        this.rs = this.stmt.executeQuery("show variables like 'collation_server'");
        this.rs.next();
        String collation = this.rs.getString(2);

        assumeTrue(
                collation != null && collation.startsWith("utf8mb4")
                        && "utf8mb4".equals(((MysqlConnection) this.conn).getSession().getServerSession().getServerVariable("character_set_server")),
                "This test requires server configured with character_set_server=utf8mb4 and collation-server set to one of utf8mb4 collations.");

        Properties p = new Properties();
        p.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");
        p.setProperty(PropertyKey.queryInterceptors.getKeyName(), Bug73663QueryInterceptor.class.getName());

        getConnectionWithProps(p);
        // failure will be thrown from the statement interceptor if any "SET NAMES utf8" statement is issued instead of "SET NAMES utf8mb4"
    }

    /**
     * Statement interceptor used to implement preceding test.
     */
    public static class Bug73663QueryInterceptor extends BaseQueryInterceptor {
        @Override
        public <M extends Message> M preProcess(M queryPacket) {
            String sql = StringUtils.toString(queryPacket.getByteBuffer(), 1, (queryPacket.getPosition() - 1));
            if (sql.contains("SET NAMES utf8") && !sql.contains("utf8mb4")) {
                fail("Character set statement issued: " + sql);
            }
            return null;
        }

        @Override
        public <T extends Resultset> T preProcess(Supplier<String> str, Query interceptedQuery) {
            String sql = str.get();
            if (sql.contains("SET NAMES utf8") && !sql.contains("utf8mb4")) {
                fail("Character set statement issued: " + sql);
            }
            return null;
        }
    }

    /**
     * Tests fix for Bug#72630 (18758686), NullPointerException during handshake in some situations
     * 
     * @throws Exception
     */
    @Test
    public void testBug72630() throws Exception {
        // bug is related to authentication plugins, available only in 5.5.7+ 
        if (versionMeetsMinimum(5, 5, 7)) {
            try {
                createUser("'Bug72630User'@'%'", "IDENTIFIED WITH mysql_native_password");
                this.stmt.execute("GRANT ALL ON *.* TO 'Bug72630User'@'%'");
                this.stmt.executeUpdate(
                        ((MysqlConnection) this.conn).getSession().versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'Bug72630User'@'%' IDENTIFIED BY 'pwd'"
                                : "set password for 'Bug72630User'@'%' = PASSWORD('pwd')");

                final Properties props = new Properties();
                props.setProperty(PropertyKey.USER.getKeyName(), "Bug72630User");
                props.setProperty(PropertyKey.PASSWORD.getKeyName(), "pwd");
                props.setProperty(PropertyKey.characterEncoding.getKeyName(), "NonexistentEncoding");

                assertThrows(SQLException.class, "Unsupported character encoding 'NonexistentEncoding'", new Callable<Void>() {
                    public Void call() throws Exception {
                        try {
                            getConnectionWithProps(props);
                            return null;
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            throw ex;
                        }
                    }
                });

                props.remove(PropertyKey.characterEncoding.getKeyName());
                props.setProperty(PropertyKey.passwordCharacterEncoding.getKeyName(), "NonexistentEncoding");
                assertThrows(SQLException.class, "Unsupported character encoding 'NonexistentEncoding'", new Callable<Void>() {
                    public Void call() throws Exception {
                        getConnectionWithProps(props);
                        return null;
                    }
                });
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Tests fix for Bug#25504578, CONNECT FAILS WHEN CONNECTIONCOLLATION=ISO-8859-13
     * 
     * @throws Exception
     */
    @Test
    public void testBug25504578() throws Exception {

        Properties p = new Properties();
        String cjCharset = CharsetMappingWrapper.getStaticJavaEncodingForMysqlCharset("latin7");
        p.setProperty(PropertyKey.characterEncoding.getKeyName(), cjCharset);

        getConnectionWithProps(p);
    }

    /**
     * Tests fix for Bug#81196 (23227334), CONNECTOR/J NOT FOLLOWING DATABASE CHARACTER SET.
     * 
     * @throws Exception
     */
    @Test
    public void testBug81196() throws Exception {
        // utf8mb4 was added in MySQL 5.5.2
        if (versionMeetsMinimum(5, 5, 2)) {
            final String fourBytesValue = "\ud841\udf0e";

            createTable("testBug81196", //"TestDb.TestTable",
                    "(`id` int AUTO_INCREMENT NOT NULL, `name` varchar(50)  NULL," + "CONSTRAINT `PK_LastViewedMatch_id` PRIMARY KEY  (`id`))"
                            + " ENGINE=InnoDB AUTO_INCREMENT=1 CHARSET=utf8mb4 DEFAULT COLLATE utf8mb4_unicode_ci");

            Properties p = new Properties();

            /* With a single-byte encoding */
            p.setProperty(PropertyKey.characterEncoding.getKeyName(), CharsetMappingWrapper.getStaticJavaEncodingForMysqlCharset("latin1"));
            Connection conn1 = getConnectionWithProps(p);
            Statement st1 = conn1.createStatement();
            st1.executeUpdate("INSERT INTO testBug81196(name) VALUES ('" + fourBytesValue + "')");
            ResultSet rs1 = st1.executeQuery("SELECT * from testBug81196");
            assertTrue(rs1.next());
            assertFalse(fourBytesValue.equals(rs1.getString(2)));

            /* With a UTF-8 encoding */
            st1.executeUpdate("TRUNCATE TABLE testBug81196");
            p.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");
            Connection conn2 = getConnectionWithProps(p);
            Statement st2 = conn2.createStatement();
            st2.executeUpdate("INSERT INTO testBug81196(name) VALUES ('" + fourBytesValue + "')");
            ResultSet rs2 = st2.executeQuery("SELECT * from testBug81196");
            assertTrue(rs2.next());
            assertEquals(fourBytesValue, rs2.getString(2));

            /* With a UTF-8 encoding and connectionCollation=utf8mb4_unicode_ci */
            st1.executeUpdate("TRUNCATE TABLE testBug81196");
            p.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");
            p.setProperty(PropertyKey.connectionCollation.getKeyName(), "utf8mb4_unicode_ci");
            Connection conn2_1 = getConnectionWithProps(p);
            Statement st2_1 = conn2_1.createStatement();
            st2_1.executeUpdate("INSERT INTO testBug81196(name) VALUES ('" + fourBytesValue + "')");
            ResultSet rs2_1 = st2_1.executeQuery("SELECT * from testBug81196");
            assertTrue(rs2_1.next());
            assertEquals(fourBytesValue, rs2_1.getString(2));

            /* With connectionCollation=utf8_bin, SET NAMES utf8 is expected */
            st1.executeUpdate("TRUNCATE TABLE testBug81196");
            p.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");
            p.setProperty(PropertyKey.connectionCollation.getKeyName(), "utf8_bin");
            Connection conn3 = getConnectionWithProps(p);
            final Statement st3 = conn3.createStatement();

            assertThrows(SQLException.class, "Incorrect string value: '\\\\xF0\\\\xA0\\\\x9C\\\\x8E' for column 'name' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    st3.executeUpdate("INSERT INTO testBug81196(name) VALUES ('" + fourBytesValue + "')");
                    return null;
                }
            });
        }
    }

    /**
     * Tests fix for Bug#100606 (31818423), UNECESARY CALL TO "SET NAMES 'UTF8' COLLATE 'UTF8_GENERAL_CI'".
     * 
     * @throws Exception
     */
    @Test
    public void testBug100606() throws Exception {
        this.rs = this.stmt.executeQuery("show variables like 'collation_server'");
        this.rs.next();
        String collation = this.rs.getString(2);

        assumeTrue(
                collation != null && collation.startsWith("utf8_general_ci")
                        && ((MysqlConnection) this.conn).getSession().getServerSession().getServerVariable("character_set_server").startsWith("utf8"),
                "This test requires server configured with character_set_server=utf8 and collation-server=utf8_general_ci.");

        String fallbackCollation = versionMeetsMinimum(8, 0, 1) ? "utf8mb4_0900_ai_ci" : "utf8mb4_general_ci";

        Properties p = new Properties();
        p.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");
        p.setProperty(PropertyKey.queryInterceptors.getKeyName(), TestSetNamesQueryInterceptor.class.getName());
        checkCollationConnection(p, "SET NAMES", false, fallbackCollation);

        p.setProperty(PropertyKey.connectionCollation.getKeyName(), "utf8_general_ci");
        checkCollationConnection(p, "SET NAMES", false, "utf8_general_ci");
    }

    /**
     * Tests fix for Bug#25554464, CONNECT FAILS WITH NPE WHEN THE SERVER STARTED WITH CUSTOM COLLATION.
     * 
     * This test requires a special server configuration with:
     * <ul>
     * <li>character-set-server = custom
     * <li>collation-server = custom_bin
     * </ul>
     * where 'custom_bin' is not a primary collation for 'custom' character set and has an index == 1024 on MySQL 8.0+ or index == 253 for older servers.
     * 
     * @throws Exception
     */
    @Test
    public void testBug25554464_1() throws Exception {
        this.rs = this.stmt.executeQuery("show variables like 'collation_server'");
        this.rs.next();
        String collation = this.rs.getString(2);
        this.rs = this.stmt.executeQuery("select ID from INFORMATION_SCHEMA.COLLATIONS where COLLATION_NAME='custom_bin'");

        assumeTrue(
                collation != null && collation.startsWith("custom_bin") && this.rs.next() && this.rs.getInt(1) == (versionMeetsMinimum(8, 0, 1) ? 1024 : 253),
                "This test requires server configured with custom character set and custom_bin collation.");

        String fallbackCollation = versionMeetsMinimum(8, 0, 1) ? "utf8mb4_0900_ai_ci" : "utf8mb4_general_ci";

        Properties p = new Properties();
        p.setProperty(PropertyKey.queryInterceptors.getKeyName(), TestSetNamesQueryInterceptor.class.getName());

        // With no specific properties c/J is using a predefined fallback collation 'utf8mb4_0900_ai_ci' or 'utf8mb4_general_ci' depending on server version.
        // Post-handshake does not issue a SET NAMES because the predefined collation should still be used.
        checkCollationConnection(p, "SET NAMES", false, fallbackCollation);

        // 'detectCustomCollations' itself doesn't change the expected collation, the predefined one should still be used.
        p.setProperty(PropertyKey.detectCustomCollations.getKeyName(), "true");
        checkCollationConnection(p, "SET NAMES", false, fallbackCollation);

        // The predefined collation should still be used
        p.setProperty(PropertyKey.customCharsetMapping.getKeyName(), "custom:Cp1252");
        checkCollationConnection(p, "SET NAMES", false, fallbackCollation);

        // Handshake collation is still 'utf8mb4_0900_ai_ci' because 'custom_bin' index > 255, then c/J sends the SET NAMES for the requested collation in a post-handshake.
        p.setProperty(PropertyKey.connectionCollation.getKeyName(), "custom_bin");
        checkCollationConnection(p, "SET NAMES custom COLLATE custom_bin", true, "custom_bin");

        p.setProperty(PropertyKey.connectionCollation.getKeyName(), "custom_general_ci");
        checkCollationConnection(p, "SET NAMES custom COLLATE custom_general_ci", true, "custom_general_ci");

        // Handshake collation is still 'utf8mb4_0900_ai_ci' because 'Cp1252' is mapped to 'custom' charset via the 'customCharsetMapping' property.
        // C/J sends the SET NAMES for the requested collation in a post-handshake.
        p.setProperty(PropertyKey.characterEncoding.getKeyName(), "Cp1252");
        p.remove(PropertyKey.connectionCollation.getKeyName());
        checkCollationConnection(p, "SET NAMES custom", true, "custom_general_ci");
    }

    /**
     * Tests fix for Bug#25554464, CONNECT FAILS WITH NPE WHEN THE SERVER STARTED WITH CUSTOM COLLATION.
     * 
     * This test requires a special server configuration with:
     * <ul>
     * <li>character-set-server = custom
     * <li>collation-server = custom_general_ci
     * </ul>
     * where 'custom_general_ci' is a primary collation for 'custom' character set and has an index == 1025 on MySQL 8.0+ or index == 254 for older servers.
     * 
     * @throws Exception
     */
    @Test
    public void testBug25554464_2() throws Exception {
        this.rs = this.stmt.executeQuery("show variables like 'collation_server'");
        this.rs.next();
        String collation = this.rs.getString(2);

        this.rs = this.stmt.executeQuery("select ID from INFORMATION_SCHEMA.COLLATIONS where COLLATION_NAME='custom_general_ci'");

        assumeTrue(
                collation != null && collation.startsWith("custom_general_ci") && this.rs.next()
                        && this.rs.getInt(1) == (versionMeetsMinimum(8, 0, 1) ? 1025 : 254),
                "This test requires server configured with custom character set and custom_general_ci collation.");

        String fallbackCollation = versionMeetsMinimum(8, 0, 1) ? "utf8mb4_0900_ai_ci" : "utf8mb4_general_ci";

        Properties p = new Properties();
        p.setProperty(PropertyKey.queryInterceptors.getKeyName(), TestSetNamesQueryInterceptor.class.getName());

        // With no specific properties c/J is using a predefined fallback collation 'utf8mb4_0900_ai_ci' or 'utf8mb4_general_ci' depending on server version.
        // Post-handshake does not issue a SET NAMES because the predefined collation should still be used.
        checkCollationConnection(p, "SET NAMES", false, fallbackCollation);

        // The predefined one should still be used.
        p.setProperty(PropertyKey.detectCustomCollations.getKeyName(), "true");
        p.setProperty(PropertyKey.customCharsetMapping.getKeyName(), "custom:Cp1252");
        checkCollationConnection(p, "SET NAMES", false, fallbackCollation);

        // Sets the predefined collation via the handshake response, but, in a post-handshake, issues the SET NAMES setting the required 'connectionCollation'.
        p.setProperty(PropertyKey.connectionCollation.getKeyName(), "custom_general_ci");
        checkCollationConnection(p, "SET NAMES custom COLLATE custom_general_ci", true, "custom_general_ci");

        // Sets the predefined collation via the handshake response, but, in a post-handshake, issues the SET NAMES setting the required 'characterEncoding'.
        // The chosen collation then is 'custom_general_ci' because it's a primary one for 'custom' character set.
        p.setProperty(PropertyKey.characterEncoding.getKeyName(), "Cp1252");
        p.remove(PropertyKey.connectionCollation.getKeyName());
        checkCollationConnection(p, "SET NAMES custom", true, "custom_general_ci");
    }

    public static class TestSetNamesQueryInterceptor extends BaseQueryInterceptor {
        public static String query = "";
        public static boolean usedSetNames = false;

        @Override
        public QueryInterceptor init(MysqlConnection conn, Properties props, Log log) {
            usedSetNames = false;
            return super.init(conn, props, log);
        }

        @Override
        public <M extends Message> M preProcess(M queryPacket) {
            String sql = StringUtils.toString(queryPacket.getByteBuffer(), 1, (queryPacket.getPosition() - 1));
            if (sql.contains(query)) {
                usedSetNames = true;
            }
            return null;
        }

        @Override
        public <T extends Resultset> T preProcess(Supplier<String> str, Query interceptedQuery) {
            String sql = str.get();
            if (sql.contains(query)) {
                usedSetNames = true;
            }
            return null;
        }
    }

    @Test
    public void testPasswordCharacterEncoding() throws Exception {
        this.rs = this.stmt.executeQuery("show variables like 'collation_server'");
        this.rs.next();
        String collation = this.rs.getString(2);
        assumeTrue(collation != null && collation.startsWith("latin1"), "This test requires a server configured with latin1 character set.");

        Properties p = new Properties();
        p.setProperty(PropertyKey.queryInterceptors.getKeyName(), TestSetNamesQueryInterceptor.class.getName());

        String requestedCollation = "latin1_swedish_ci";
        String fallbackCollation = versionMeetsMinimum(8, 0, 1) ? "utf8mb4_0900_ai_ci" : "utf8mb4_general_ci";

        checkCollationConnection(p, "SET NAMES", false, fallbackCollation);

        p.setProperty(PropertyKey.passwordCharacterEncoding.getKeyName(), "UTF-8");
        checkCollationConnection(p, "SET NAMES", false, fallbackCollation);

        p.setProperty(PropertyKey.connectionCollation.getKeyName(), requestedCollation);
        checkCollationConnection(p, "SET NAMES latin1 COLLATE " + requestedCollation, true, requestedCollation);

        p.setProperty(PropertyKey.characterEncoding.getKeyName(), "Cp1252");
        p.remove(PropertyKey.connectionCollation.getKeyName());
        checkCollationConnection(p, "SET NAMES latin1", true, requestedCollation);

        requestedCollation = versionMeetsMinimum(8, 0, 1) ? "utf8mb4_0900_ai_ci" : "utf8mb4_general_ci";

        p.setProperty(PropertyKey.connectionCollation.getKeyName(), requestedCollation);
        checkCollationConnection(p, "SET NAMES", false, requestedCollation);

        p.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");
        p.remove(PropertyKey.connectionCollation.getKeyName());
        checkCollationConnection(p, "SET NAMES", false, requestedCollation);
    }

    private void checkCollationConnection(Properties props, String query, boolean expectQueryIsIssued, String expectedCollation) throws Exception {
        TestSetNamesQueryInterceptor.query = query;
        Connection c = getConnectionWithProps(props);
        if (expectQueryIsIssued) {
            assertTrue(TestSetNamesQueryInterceptor.usedSetNames);
        } else {
            assertFalse(TestSetNamesQueryInterceptor.usedSetNames);
        }
        this.rs = c.createStatement().executeQuery("show variables like 'collation_connection'");
        this.rs.next();
        assertEquals(expectedCollation, this.rs.getString(2));
        c.close();
    }
}
