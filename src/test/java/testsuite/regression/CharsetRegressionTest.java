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
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import com.mysql.cj.CacheAdapter;
import com.mysql.cj.CharsetMappingWrapper;
import com.mysql.cj.CharsetSettings;
import com.mysql.cj.MysqlConnection;
import com.mysql.cj.NativeSession;
import com.mysql.cj.Query;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.PropertyDefinitions.DatabaseTerm;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.util.StringUtils;

import testsuite.BaseQueryInterceptor;
import testsuite.BaseTestCase;

public class CharsetRegressionTest extends BaseTestCase {

    /**
     * Tests fix for BUG#7607 - MS932, SHIFT_JIS and Windows_31J not recog. as aliases for sjis.
     * 
     * @throws Exception
     */
    @Test
    public void testBug7607() throws Exception {
        Connection ms932Conn = null, cp943Conn = null, shiftJisConn = null, windows31JConn = null;

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "MS932");

            ms932Conn = getConnectionWithProps(props);

            this.rs = ms932Conn.createStatement().executeQuery("SHOW VARIABLES LIKE 'character_set_client'");
            assertTrue(this.rs.next());
            String encoding = this.rs.getString(2);
            assertEquals("cp932", encoding.toLowerCase(Locale.ENGLISH));

            this.rs = ms932Conn.createStatement().executeQuery("SELECT 'abc'");
            assertTrue(this.rs.next());

            String charsetToCheck = "ms932";

            assertEquals(charsetToCheck,
                    ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterEncoding(1).toLowerCase(Locale.ENGLISH));

            try {
                ms932Conn.createStatement().executeUpdate("drop table if exists testBug7607");
                ms932Conn.createStatement().executeUpdate("create table testBug7607 (sortCol int, col1 varchar(100) ) character set sjis");
                ms932Conn.createStatement().executeUpdate("insert into testBug7607 values(1, 0x835C)"); // standard
                // sjis
                ms932Conn.createStatement().executeUpdate("insert into testBug7607 values(2, 0x878A)"); // NEC
                // kanji

                this.rs = ms932Conn.createStatement().executeQuery("SELECT col1 FROM testBug7607 ORDER BY sortCol ASC");
                assertTrue(this.rs.next());
                String asString = this.rs.getString(1);
                assertTrue("\u30bd".equals(asString));

                assertTrue(this.rs.next());
                asString = this.rs.getString(1);
                assertEquals("\u3231", asString);
            } finally {
                ms932Conn.createStatement().executeUpdate("drop table if exists testBug7607");
            }

            props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "SHIFT_JIS");

            shiftJisConn = getConnectionWithProps(props);

            this.rs = shiftJisConn.createStatement().executeQuery("SHOW VARIABLES LIKE 'character_set_client'");
            assertTrue(this.rs.next());
            encoding = this.rs.getString(2);
            assertTrue("sjis".equalsIgnoreCase(encoding));

            this.rs = shiftJisConn.createStatement().executeQuery("SELECT 'abc'");
            assertTrue(this.rs.next());

            String charSetUC = ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterEncoding(1).toUpperCase(Locale.US);

            props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "WINDOWS-31J");

            windows31JConn = getConnectionWithProps(props);

            this.rs = windows31JConn.createStatement().executeQuery("SHOW VARIABLES LIKE 'character_set_client'");
            assertTrue(this.rs.next());
            encoding = this.rs.getString(2);

            assertEquals("cp932", encoding.toLowerCase(Locale.ENGLISH));

            this.rs = windows31JConn.createStatement().executeQuery("SELECT 'abc'");
            assertTrue(this.rs.next());

            assertEquals("windows-31j".toLowerCase(Locale.ENGLISH),
                    ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterEncoding(1).toLowerCase(Locale.ENGLISH));

            props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "CP943");

            cp943Conn = getConnectionWithProps(props);

            this.rs = cp943Conn.createStatement().executeQuery("SHOW VARIABLES LIKE 'character_set_client'");
            assertTrue(this.rs.next());
            encoding = this.rs.getString(2);
            assertTrue("sjis".equalsIgnoreCase(encoding));

            this.rs = cp943Conn.createStatement().executeQuery("SELECT 'abc'");
            assertTrue(this.rs.next());

            charSetUC = ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterEncoding(1).toUpperCase(Locale.US);

            assertEquals("CP943", charSetUC);

        } finally {
            if (ms932Conn != null) {
                ms932Conn.close();
            }

            if (shiftJisConn != null) {
                shiftJisConn.close();
            }

            if (windows31JConn != null) {
                windows31JConn.close();
            }

            if (cp943Conn != null) {
                cp943Conn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#9206, can not use 'UTF-8' for characterSetResults configuration property.
     * 
     * @throws Exception
     */
    @Test
    public void testBug9206() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.characterSetResults.getKeyName(), "UTF-8");
        getConnectionWithProps(props).close();
    }

    /**
     * Tests fix for BUG#10496 - SQLException is thrown when using property "characterSetResults"
     * 
     * @throws Exception
     */
    @Test
    public void testBug10496() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "WINDOWS-31J");
        props.setProperty(PropertyKey.characterSetResults.getKeyName(), "WINDOWS-31J");
        getConnectionWithProps(props).close();

        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "EUC_JP");
        props.setProperty(PropertyKey.characterSetResults.getKeyName(), "EUC_JP");
        getConnectionWithProps(props).close();
    }

    /**
     * Tests fix for BUG#12752 - Cp1251 incorrectly mapped to win1251 for servers newer than 4.0.x.
     * 
     * @throws Exception
     */
    @Test
    public void testBug12752() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "Cp1251");
        getConnectionWithProps(props).close();
    }

    /**
     * Tests fix for BUG#15544, no "dos" character set in MySQL > 4.1.0
     * 
     * @throws Exception
     */
    @Test
    public void testBug15544() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "Cp437");
        Connection dosConn = null;

        try {
            dosConn = getConnectionWithProps(props);
        } finally {
            if (dosConn != null) {
                dosConn.close();
            }
        }
    }

    @Test
    public void testBug37931() throws Exception {
        Connection _conn = null;
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.characterSetResults.getKeyName(), "ISO88591");

        try {
            _conn = getConnectionWithProps(props);
            assertTrue(false, "This point should not be reached.");
        } catch (Exception e) {
            assertEquals("Unsupported character encoding 'ISO88591'", e.getMessage());
        } finally {
            if (_conn != null) {
                _conn.close();
            }
        }

        props.setProperty(PropertyKey.characterSetResults.getKeyName(), "null");

        try {
            _conn = getConnectionWithProps(props);

            Statement _stmt = _conn.createStatement();
            ResultSet _rs = _stmt.executeQuery("show variables where variable_name='character_set_results'");
            if (_rs.next()) {
                String res = _rs.getString(2);
                if (res == null || "NULL".equalsIgnoreCase(res) || res.length() == 0) {
                    assertTrue(true);
                } else {
                    assertTrue(false);
                }
            }
        } finally {
            if (_conn != null) {
                _conn.close();
            }
        }
    }

    /**
     * Tests fix for Bug#64205 (13702427), Connected through Connector/J 5.1 to MySQL 5.5, the error message is garbled.
     * 
     * @throws Exception
     */
    @Test
    public void testBug64205() throws Exception {
        Properties props = getPropertiesFromTestsuiteUrl();
        String dbname = props.getProperty(PropertyKey.DBNAME.getKeyName());
        if (dbname == null) {
            assertTrue(false, "No database selected");
        }

        props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "EUC_JP");

        Connection testConn = null;
        Statement testSt = null;
        ResultSet testRs = null;
        try {
            testConn = getConnectionWithProps(props);
            testSt = testConn.createStatement();
            testRs = testSt.executeQuery("SELECT * FROM `" + dbname + "`.`\u307b\u3052\u307b\u3052`");
        } catch (SQLException e1) {
            if (e1.getClass().getName().endsWith("SQLSyntaxErrorException")) {
                assertEquals("Table '" + dbname + ".\u307B\u3052\u307b\u3052' doesn't exist", e1.getMessage());
            } else if (e1.getErrorCode() == MysqlErrorNumbers.ER_FILE_NOT_FOUND) {
                // this could happen on Windows with 5.5 and 5.6 servers where BUG#14642248 exists
                assertTrue(e1.getMessage().contains("Can't find file"));
            } else {
                throw e1;
            }

            testSt.close();
            testConn.close();

            try {
                props.setProperty(PropertyKey.characterSetResults.getKeyName(), "SJIS");
                testConn = getConnectionWithProps(props);
                testSt = testConn.createStatement();
                testSt.execute("SET lc_messages = 'ru_RU'");
                testRs = testSt.executeQuery("SELECT * FROM `" + dbname + "`.`\u307b\u3052\u307b\u3052`");
            } catch (SQLException e2) {
                if (e2.getClass().getName().endsWith("SQLSyntaxErrorException")) {
                    assertEquals("\u0422\u0430\u0431\u043b\u0438\u0446\u0430 '" + dbname
                            + ".\u307b\u3052\u307b\u3052' \u043d\u0435 \u0441\u0443\u0449\u0435\u0441\u0442\u0432\u0443\u0435\u0442", e2.getMessage());
                } else if (e2.getErrorCode() == MysqlErrorNumbers.ER_FILE_NOT_FOUND) {
                    // this could happen on Windows with 5.5 and 5.6 servers where BUG#14642248 exists
                    assertTrue(e2.getMessage().indexOf("\u0444\u0430\u0439\u043b") > -1,
                            "File not found error message should be russian but is this one: " + e2.getMessage());
                } else {
                    throw e2;
                }
            }

        } finally {
            if (testRs != null) {
                testRs.close();
            }
            if (testSt != null) {
                testSt.close();
            }
            if (testConn != null) {
                testConn.close();
            }
        }

        // also test with explicit characterSetResults and cacheServerConfiguration
        try {
            props.setProperty(PropertyKey.characterSetResults.getKeyName(), "EUC_JP");
            props.setProperty(PropertyKey.cacheServerConfiguration.getKeyName(), "true");
            testConn = getConnectionWithProps(props);
            testSt = testConn.createStatement();
            testRs = testSt.executeQuery("SELECT * FROM `" + dbname + "`.`\u307b\u3052\u307b\u3052`");
            fail("Exception should be thrown for attemping to query non-existing table");
        } catch (SQLException e1) {
            if (e1.getClass().getName().endsWith("SQLSyntaxErrorException")) {
                assertEquals("Table '" + dbname + ".\u307B\u3052\u307b\u3052' doesn't exist", e1.getMessage());
            } else if (e1.getErrorCode() == MysqlErrorNumbers.ER_FILE_NOT_FOUND) {
                // this could happen on Windows with 5.5 and 5.6 servers where BUG#14642248 exists
                assertTrue(e1.getMessage().contains("Can't find file"));
            } else {
                throw e1;
            }
        } finally {
            testConn.close();
        }
        props.remove(PropertyKey.cacheServerConfiguration.getKeyName());

        // Error messages may also be received after the handshake but before connection initialization is complete. This tests the interpretation of
        // errors thrown during this time window using a SatementInterceptor that throws an Exception while setting the session variables.
        // Start by getting the Latin1 version of the error to compare later.
        String latin1ErrorMsg = "";
        int latin1ErrorLen = 0;
        try {
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "Latin1");
            props.setProperty(PropertyKey.characterSetResults.getKeyName(), "Latin1");
            props.setProperty(PropertyKey.sessionVariables.getKeyName(), "lc_messages=ru_RU");
            props.setProperty(PropertyKey.queryInterceptors.getKeyName(), TestBug64205QueryInterceptor.class.getName());
            testConn = getConnectionWithProps(props);
            fail("Exception should be trown for syntax error, caused by the exception interceptor");
        } catch (Exception e) {
            latin1ErrorMsg = e.getMessage();
            latin1ErrorLen = latin1ErrorMsg.length();
        }
        // Now compare with results when using a proper encoding.
        try {
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "EUC_JP");
            props.setProperty(PropertyKey.characterSetResults.getKeyName(), "EUC_JP");
            props.setProperty(PropertyKey.sessionVariables.getKeyName(), "lc_messages=ru_RU");
            props.setProperty(PropertyKey.queryInterceptors.getKeyName(), TestBug64205QueryInterceptor.class.getName());
            testConn = getConnectionWithProps(props);
            fail("Exception should be trown for syntax error, caused by the exception interceptor");
        } catch (SQLException e) {
            // There should be the Russian version of this error message, correctly encoded. A mis-interpretation, e.g. decoding as latin1, would return a
            // wrong message with the wrong size.
            assertEquals(29 + dbname.length(), e.getMessage().length());
            assertFalse(latin1ErrorMsg.equals(e.getMessage()));
            assertFalse(latin1ErrorLen == e.getMessage().length());
        } finally {
            testConn.close();
        }
    }

    public static class TestBug64205QueryInterceptor extends BaseQueryInterceptor {
        private JdbcConnection connection;

        @Override
        public QueryInterceptor init(MysqlConnection conn, Properties props, Log log) {
            this.connection = (JdbcConnection) conn;
            return super.init(conn, props, log);
        }

        @Override
        public <M extends Message> M postProcess(M queryPacket, M originalResponsePacket) {
            String sql = StringUtils.toString(queryPacket.getByteBuffer(), 1, (queryPacket.getPosition() - 1));
            if (sql.contains("lc_messages=ru_RU")) {
                try {
                    this.connection.createStatement()
                            .executeQuery("SELECT * FROM `"
                                    + (this.connection.getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm)
                                            .getValue() == DatabaseTerm.SCHEMA ? this.connection.getSchema() : this.connection.getCatalog())
                                    + "`.`\u307b\u3052\u307b\u3052`");
                } catch (Exception e) {
                    throw ExceptionFactory.createException(e.getMessage(), e);
                }
            }
            return originalResponsePacket;
        }
    }

    /**
     * Bug #41730 - SQL Injection when using U+00A5 and SJIS/Windows-31J
     * 
     * @throws Exception
     */
    @Test
    public void testBug41730() throws Exception {
        try {
            "".getBytes("sjis");
        } catch (UnsupportedEncodingException ex) {
            assumeFalse(true, "Test requires JVM with sjis support.");
        }

        Connection conn2 = null;
        PreparedStatement pstmt2 = null;
        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "sjis");
            conn2 = getConnectionWithProps(props);
            pstmt2 = conn2.prepareStatement("select ?");
            pstmt2.setString(1, "\u00A5'");
            // this will throw an exception with a syntax error if it fails
            this.rs = pstmt2.executeQuery();
        } finally {
            try {
                if (pstmt2 != null) {
                    pstmt2.close();
                }
            } catch (SQLException ex) {
            }
            try {
                if (conn2 != null) {
                    conn2.close();
                }
            } catch (SQLException ex) {
            }
        }
    }

    /**
     * Tests character conversion bug.
     * 
     * @throws Exception
     */
    @Test
    public void testAsciiCharConversion() throws Exception {
        byte[] buf = new byte[10];
        buf[0] = (byte) '?';
        buf[1] = (byte) 'S';
        buf[2] = (byte) 't';
        buf[3] = (byte) 'a';
        buf[4] = (byte) 't';
        buf[5] = (byte) 'e';
        buf[6] = (byte) '-';
        buf[7] = (byte) 'b';
        buf[8] = (byte) 'o';
        buf[9] = (byte) 't';

        String testString = "?State-bot";
        String convertedString = StringUtils.toAsciiString(buf);

        for (int i = 0; i < convertedString.length(); i++) {
            System.out.println((byte) convertedString.charAt(i));
        }

        assertTrue(testString.equals(convertedString), "Converted string != test string");
    }

    /**
     * Tests for regression of encoding forced by user, reported by Jive Software
     * 
     * @throws Exception
     */
    @Test
    public void testEncodingRegression() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");
        DriverManager.getConnection(dbUrl, props).close();
    }

    /**
     * Tests fix for BUG#879
     * 
     * @throws Exception
     */
    @Test
    public void testEscapeSJISDoubleEscapeBug() throws Exception {
        String testString = "'It\\'s a boy!'";

        //byte[] testStringAsBytes = testString.getBytes("SJIS");

        byte[] origByteStream = new byte[] { (byte) 0x95, (byte) 0x5c, (byte) 0x8e, (byte) 0x96, (byte) 0x5c, (byte) 0x62, (byte) 0x5c };

        //String origString = "\u955c\u8e96\u5c62\\";

        origByteStream = new byte[] { (byte) 0x8d, (byte) 0xb2, (byte) 0x93, (byte) 0x91, (byte) 0x81, (byte) 0x40, (byte) 0x8c, (byte) 0x5c };

        testString = new String(origByteStream, "SJIS");

        Properties connProps = new Properties();
        connProps.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        connProps.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        connProps.setProperty(PropertyKey.characterEncoding.getKeyName(), "sjis");

        Connection sjisConn = getConnectionWithProps(connProps);
        Statement sjisStmt = sjisConn.createStatement();

        try {
            sjisStmt.executeUpdate("DROP TABLE IF EXISTS doubleEscapeSJISTest");
            sjisStmt.executeUpdate("CREATE TABLE doubleEscapeSJISTest (field1 BLOB)");

            PreparedStatement sjisPStmt = sjisConn.prepareStatement("INSERT INTO doubleEscapeSJISTest VALUES (?)");
            sjisPStmt.setString(1, testString);
            sjisPStmt.executeUpdate();

            this.rs = sjisStmt.executeQuery("SELECT * FROM doubleEscapeSJISTest");

            this.rs.next();

            String retrString = this.rs.getString(1);

            System.out.println(retrString.equals(testString));
        } finally {
            sjisStmt.executeUpdate("DROP TABLE IF EXISTS doubleEscapeSJISTest");
        }
    }

    @Test
    public void testGreekUtf8411() throws Exception {
        Properties newProps = new Properties();
        newProps.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        newProps.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        newProps.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");

        Connection utf8Conn = this.getConnectionWithProps(newProps);

        Statement utfStmt = utf8Conn.createStatement();

        createTable("greekunicode", "(ID INTEGER NOT NULL  AUTO_INCREMENT,UpperCase VARCHAR (30),LowerCase VARCHAR (30),Accented "
                + " VARCHAR (30),Special VARCHAR (30),PRIMARY KEY(ID)) DEFAULT CHARACTER SET utf8", "InnoDB");

        String upper = "\u0394\u930F\u039A\u0399\u039C\u0397";
        String lower = "\u03B4\u03BF\u03BA\u03B9\u03BC\u03B7";
        String accented = "\u03B4\u03CC\u03BA\u03AF\u03BC\u03AE";
        String special = "\u037E\u03C2\u03B0";

        utfStmt.executeUpdate("INSERT INTO greekunicode VALUES ('1','" + upper + "','" + lower + "','" + accented + "','" + special + "')");

        this.rs = utfStmt.executeQuery("SELECT UpperCase, LowerCase, Accented, Special from greekunicode");

        this.rs.next();

        assertTrue(upper.equals(this.rs.getString(1)));
        assertTrue(lower.equals(this.rs.getString(2)));
        assertTrue(accented.equals(this.rs.getString(3)));
        assertTrue(special.equals(this.rs.getString(4)));
    }

    /**
     * Tests fix for BUG#24840 - character encoding of "US-ASCII" doesn't map correctly for 4.1 or newer
     * 
     * @throws Exception
     */
    @Test
    public void testBug24840() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "US-ASCII");

        getConnectionWithProps(props).close();
    }

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
            assertFalse(sql.contains("SET NAMES utf8") && !sql.contains("utf8mb4"), "Character set statement issued: " + sql);
            return null;
        }

        @Override
        public <T extends Resultset> T preProcess(Supplier<String> str, Query interceptedQuery) {
            String sql = str.get();
            assertFalse(sql.contains("SET NAMES utf8") && !sql.contains("utf8mb4"), "Character set statement issued: " + sql);
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
        String cjCharset = CharsetMappingWrapper.getStaticJavaEncodingForMysqlCharset("latin7");
        Properties props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), cjCharset);
        getConnectionWithProps(props);
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

            Properties props = new Properties();
            props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

            /* With a single-byte encoding */
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), CharsetMappingWrapper.getStaticJavaEncodingForMysqlCharset("latin1"));
            Connection conn1 = getConnectionWithProps(props);
            Statement st1 = conn1.createStatement();
            st1.executeUpdate("INSERT INTO testBug81196(name) VALUES ('" + fourBytesValue + "')");
            ResultSet rs1 = st1.executeQuery("SELECT * from testBug81196");
            assertTrue(rs1.next());
            assertFalse(fourBytesValue.equals(rs1.getString(2)));

            /* With a UTF-8 encoding */
            st1.executeUpdate("TRUNCATE TABLE testBug81196");
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");
            Connection conn2 = getConnectionWithProps(props);
            Statement st2 = conn2.createStatement();
            st2.executeUpdate("INSERT INTO testBug81196(name) VALUES ('" + fourBytesValue + "')");
            ResultSet rs2 = st2.executeQuery("SELECT * from testBug81196");
            assertTrue(rs2.next());
            assertEquals(fourBytesValue, rs2.getString(2));

            /* With a UTF-8 encoding and connectionCollation=utf8mb4_unicode_ci */
            st1.executeUpdate("TRUNCATE TABLE testBug81196");
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");
            props.setProperty(PropertyKey.connectionCollation.getKeyName(), "utf8mb4_unicode_ci");
            Connection conn2_1 = getConnectionWithProps(props);
            Statement st2_1 = conn2_1.createStatement();
            st2_1.executeUpdate("INSERT INTO testBug81196(name) VALUES ('" + fourBytesValue + "')");
            ResultSet rs2_1 = st2_1.executeQuery("SELECT * from testBug81196");
            assertTrue(rs2_1.next());
            assertEquals(fourBytesValue, rs2_1.getString(2));

            /* With connectionCollation=utf8_bin, SET NAMES utf8 is expected */
            st1.executeUpdate("TRUNCATE TABLE testBug81196");
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");
            props.setProperty(PropertyKey.connectionCollation.getKeyName(), "utf8_bin");
            Connection conn3 = getConnectionWithProps(props);
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

        Properties props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.queryInterceptors.getKeyName(), TestSetNamesQueryInterceptor.class.getName());

        String requestedCollation = "latin1_swedish_ci";
        String fallbackCollation = versionMeetsMinimum(8, 0, 1) ? "utf8mb4_0900_ai_ci" : "utf8mb4_general_ci";

        checkCollationConnection(props, "SET NAMES", false, fallbackCollation);

        props.setProperty(PropertyKey.passwordCharacterEncoding.getKeyName(), "UTF-8");
        checkCollationConnection(props, "SET NAMES", false, fallbackCollation);

        props.setProperty(PropertyKey.connectionCollation.getKeyName(), requestedCollation);
        checkCollationConnection(props, "SET NAMES latin1 COLLATE " + requestedCollation, true, requestedCollation);

        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "Cp1252");
        props.remove(PropertyKey.connectionCollation.getKeyName());
        checkCollationConnection(props, "SET NAMES latin1", true, requestedCollation);

        requestedCollation = versionMeetsMinimum(8, 0, 1) ? "utf8mb4_0900_ai_ci" : "utf8mb4_general_ci";

        props.setProperty(PropertyKey.connectionCollation.getKeyName(), requestedCollation);
        checkCollationConnection(props, "SET NAMES", false, requestedCollation);

        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");
        props.remove(PropertyKey.connectionCollation.getKeyName());
        checkCollationConnection(props, "SET NAMES", false, requestedCollation);
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

    /**
     * Tests fix for Bug#71038, Add an option for custom collations detection
     * 
     * @throws Exception
     */
    @Test
    public void testBug71038() throws Exception {
        Properties p = new Properties();
        p.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        p.setProperty(PropertyKey.detectCustomCollations.getKeyName(), "false");
        p.setProperty(PropertyKey.queryInterceptors.getKeyName(), Bug71038QueryInterceptor.class.getName());

        JdbcConnection c = (JdbcConnection) getConnectionWithProps(p);
        Bug71038QueryInterceptor si = (Bug71038QueryInterceptor) c.getQueryInterceptorsInstances().get(0);
        assertTrue(si.cnt == 0, "SELECT from INFORMATION_SCHEMA.COLLATIONS was issued when detectCustomCollations=false");
        c.close();

        p.setProperty(PropertyKey.detectCustomCollations.getKeyName(), "true");
        p.setProperty(PropertyKey.queryInterceptors.getKeyName(), Bug71038QueryInterceptor.class.getName());

        c = (JdbcConnection) getConnectionWithProps(p);
        si = (Bug71038QueryInterceptor) c.getQueryInterceptorsInstances().get(0);
        assertTrue(si.cnt > 0, "SELECT from INFORMATION_SCHEMA.COLLATIONS wasn't issued when detectCustomCollations=true");
        c.close();
    }

    /**
     * Counts the number of issued "SHOW COLLATION" statements.
     */
    public static class Bug71038QueryInterceptor extends BaseQueryInterceptor {
        int cnt = 0;

        @Override
        public <M extends Message> M preProcess(M queryPacket) {
            String sql = StringUtils.toString(queryPacket.getByteBuffer(), 1, (queryPacket.getPosition() - 1));
            if (sql.contains("from INFORMATION_SCHEMA.COLLATIONS")) {
                this.cnt++;
            }
            return null;
        }
    }

    /**
     * Tests fix for Bug#91317 (28207422), Wrong defaults on collation mappings.
     * 
     * @throws Exception
     */
    @Test
    public void testBug91317() throws Exception {
        Map<String, String> defaultCollations = new HashMap<>();

        Properties p = new Properties();
        p.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        p.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        p.setProperty(PropertyKey.detectCustomCollations.getKeyName(), "true");
        p.setProperty(PropertyKey.customCharsetMapping.getKeyName(), "custom:Cp1252");
        Connection c = getConnectionWithProps(p);

        // Compare server-side and client-side collation defaults.
        this.rs = this.stmt.executeQuery("SELECT COLLATION_NAME, CHARACTER_SET_NAME, ID FROM INFORMATION_SCHEMA.COLLATIONS WHERE IS_DEFAULT = 'Yes'");
        while (this.rs.next()) {
            String collationName = this.rs.getString(1);
            String charsetName = this.rs.getString(2);
            int collationId = this.rs.getInt(3);

            int mappedCollationId = ((MysqlConnection) c).getSession().getServerSession().getCharsetSettings()
                    .getCollationIndexForMysqlCharsetName(charsetName);

            defaultCollations.put(charsetName, collationName);

            // Default collation for 'utf8mb4' is 'utf8mb4_0900_ai_ci' in MySQL 8.0.1 and above, 'utf8mb4_general_ci' in the others.
            if ("utf8mb4".equalsIgnoreCase(charsetName) && !versionMeetsMinimum(8, 0, 1)) {
                mappedCollationId = 45;
            }

            assertEquals(collationId, mappedCollationId);
            assertEquals(collationName,
                    ((MysqlConnection) c).getSession().getServerSession().getCharsetSettings().getCollationNameForCollationIndex(mappedCollationId));
        }

        ServerVersion sv = ((JdbcConnection) this.conn).getServerVersion();

        // Check `collation_connection` for each one of the known character sets.
        this.rs = this.stmt.executeQuery("SELECT character_set_name FROM information_schema.character_sets");
        int csCount = 0;
        while (this.rs.next()) {
            csCount++;
            String cs = this.rs.getString(1);

            // The following cannot be set as client_character_set
            // (https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html#charset-connection-impermissible-client-charset)
            if (cs.equalsIgnoreCase("ucs2") || cs.equalsIgnoreCase("utf16") || cs.equalsIgnoreCase("utf16le") || cs.equalsIgnoreCase("utf32")) {
                continue;
            }

            String javaEnc = ((MysqlConnection) c).getSession().getServerSession().getCharsetSettings().getJavaEncodingForMysqlCharset(cs);
            System.out.println(cs + "->" + javaEnc);
            String charsetForJavaEnc = ((MysqlConnection) c).getSession().getServerSession().getCharsetSettings().getMysqlCharsetForJavaEncoding(javaEnc, sv);
            String expectedCollation = defaultCollations.get(charsetForJavaEnc);

            if ("UTF-8".equalsIgnoreCase(javaEnc)) {
                // UTF-8 is the exception. This encoding is converted to MySQL charset 'utf8mb4' instead of 'utf8', and its corresponding collation.
                expectedCollation = versionMeetsMinimum(8, 0, 1) ? "utf8mb4_0900_ai_ci" : "utf8mb4_general_ci";
            }

            Properties p2 = new Properties();
            p2.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
            p2.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            p2.setProperty(PropertyKey.detectCustomCollations.getKeyName(), "true");
            p2.setProperty(PropertyKey.customCharsetMapping.getKeyName(), "custom:Cp1252");
            p2.setProperty(PropertyKey.characterEncoding.getKeyName(), javaEnc);

            Connection testConn = getConnectionWithProps(p2);
            ResultSet testRs = testConn.createStatement().executeQuery("SHOW VARIABLES LIKE 'collation_connection'");
            assertTrue(testRs.next());
            assertEquals(expectedCollation, testRs.getString(2));
            testConn.close();
        }
        // Assert that some charsets were tested.
        assertTrue(csCount > 35); // There are 39 charsets in MySQL 5.5.61, 40 in MySQL 5.6.41 and 41 in MySQL 5.7.23 and above, but these numbers can vary.
    }

    /**
     * Test for Bug#72712 - SET NAMES issued unnecessarily.
     * 
     * Using a statement interceptor, ensure that SET NAMES is not called if the encoding requested by the client application matches that of
     * character_set_server.
     * 
     * Also test that character_set_results is not set unnecessarily.
     * 
     * @throws Exception
     */
    @Test
    public void testBug72712() throws Exception {
        assumeTrue(((MysqlConnection) this.conn).getSession().getServerSession().getServerVariable("character_set_server").equals("latin1"),
                "This test only run when character_set_server=latin1");

        Properties p = new Properties();
        p.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        p.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        p.setProperty(PropertyKey.characterEncoding.getKeyName(), "cp1252");
        p.setProperty(PropertyKey.characterSetResults.getKeyName(), "cp1252");
        p.setProperty(PropertyKey.queryInterceptors.getKeyName(), Bug72712QueryInterceptor.class.getName());

        getConnectionWithProps(p);
        // exception will be thrown from the statement interceptor if any SET statements are issued
    }

    /**
     * Statement interceptor used to implement preceding test.
     */
    public static class Bug72712QueryInterceptor extends BaseQueryInterceptor {
        @Override
        public <T extends Resultset> T preProcess(Supplier<String> str, Query interceptedQuery) {
            String sql = str.get();
            if (sql.contains("SET NAMES")
                    || sql.contains(CharsetSettings.CHARACTER_SET_RESULTS) && !(sql.contains("SHOW VARIABLES") || sql.contains("SELECT  @@"))) {
                throw ExceptionFactory.createException("Wrongt statement issued: " + sql);
            }
            return null;
        }
    }

    /**
     * Tests fix for Bug#95139 (29807572), CACHESERVERCONFIGURATION APPEARS TO THWART CHARSET DETECTION.
     * 
     * @throws Exception
     */
    @Test
    public void testBug95139() throws Exception {

        Properties p = new Properties();
        p.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        p.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        p.setProperty(PropertyKey.queryInterceptors.getKeyName(), Bug95139QueryInterceptor.class.getName());
        testBug95139CheckVariables(p, 1, null, "SET " + CharsetSettings.CHARACTER_SET_RESULTS + " = NULL");

        p.setProperty(PropertyKey.cacheServerConfiguration.getKeyName(), "true");
        p.setProperty(PropertyKey.detectCustomCollations.getKeyName(), "true");

        // Empty the cache possibly created by other tests to get a correct queryVarsCnt on the next step
        Connection c = getConnectionWithProps(p);
        Field f = NativeSession.class.getDeclaredField("serverConfigCache");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        CacheAdapter<String, Map<String, String>> cache = (CacheAdapter<String, Map<String, String>>) f.get(((MysqlConnection) c).getSession());
        if (cache != null) {
            cache.invalidateAll();
        }

        p.setProperty(PropertyKey.characterEncoding.getKeyName(), "cp1252");
        p.setProperty(PropertyKey.characterSetResults.getKeyName(), "cp1252");
        testBug95139CheckVariables(p, 1, null, null);

        p.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");
        p.setProperty(PropertyKey.characterSetResults.getKeyName(), "cp1252");
        testBug95139CheckVariables(p, 0, null, "SET " + CharsetSettings.CHARACTER_SET_RESULTS + " = latin1");

        p.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");
        p.remove(PropertyKey.characterSetResults.getKeyName());
        testBug95139CheckVariables(p, 0, null, "SET " + CharsetSettings.CHARACTER_SET_RESULTS + " = NULL");

        p.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");
        p.setProperty(PropertyKey.passwordCharacterEncoding.getKeyName(), "latin1");
        testBug95139CheckVariables(p, 0, "SET NAMES utf8mb4", "SET " + CharsetSettings.CHARACTER_SET_RESULTS + " = NULL");

        p.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");
        p.setProperty(PropertyKey.passwordCharacterEncoding.getKeyName(), "latin1");
        p.setProperty(PropertyKey.connectionCollation.getKeyName(), "utf8mb4_bin");
        testBug95139CheckVariables(p, 0, "SET NAMES utf8mb4 COLLATE utf8mb4_bin", "SET " + CharsetSettings.CHARACTER_SET_RESULTS + " = NULL");
    }

    private void testBug95139CheckVariables(Properties p, int queryVarsCnt, String expSetNamesQuery, String expSetCharacterSetResultsquery) throws Exception {
        Connection con = getConnectionWithProps(p);

        Bug95139QueryInterceptor si = (Bug95139QueryInterceptor) ((JdbcConnection) con).getQueryInterceptorsInstances().get(0);
        assertEquals(queryVarsCnt, si.queryVarsCnt);
        assertEquals(expSetNamesQuery == null ? 0 : 1, si.setNamesCnt);
        assertEquals(expSetCharacterSetResultsquery == null ? 0 : 1, si.setCharacterSetResultsCnt);
        if (expSetNamesQuery != null) {
            assertEquals(expSetNamesQuery, si.setNamesQuery);
        }
        if (expSetCharacterSetResultsquery != null) {
            assertEquals(expSetCharacterSetResultsquery, si.setCharacterSetResultsQuery);
        }

        Map<String, String> svs = ((MysqlConnection) con).getSession().getServerSession().getServerVariables();
        System.out.println(svs);
        Map<String, String> exp = new HashMap<>();
        exp.put("character_set_client", svs.get("character_set_client"));
        exp.put("character_set_connection", svs.get("character_set_connection"));
        exp.put("character_set_results", svs.get("character_set_results") == null ? "" : svs.get("character_set_results"));
        exp.put("character_set_server", svs.get("character_set_server"));
        exp.put("collation_server", svs.get("collation_server"));
        exp.put("collation_connection", svs.get("collation_connection"));

        ResultSet rset = con.createStatement()
                .executeQuery("show variables where variable_name='character_set_client' or variable_name='character_set_connection'"
                        + " or variable_name='character_set_results' or variable_name='character_set_server' or variable_name='collation_server'"
                        + " or variable_name='collation_connection'");
        while (rset.next()) {
            System.out.println(rset.getString(1) + "=" + rset.getString(2));
            assertEquals(exp.get(rset.getString(1)), rset.getString(2), rset.getString(1));
        }

        con.close();
    }

    public static class Bug95139QueryInterceptor extends BaseQueryInterceptor {
        int queryVarsCnt = 0;
        int setNamesCnt = 0;
        String setNamesQuery = null;
        int setCharacterSetResultsCnt = 0;
        String setCharacterSetResultsQuery = null;

        @Override
        public <M extends Message> M preProcess(M queryPacket) {
            String sql = StringUtils.toString(queryPacket.getByteBuffer(), 0, queryPacket.getPosition());
            if (sql.contains("SET NAMES")) {
                this.setNamesCnt++;
                this.setNamesQuery = sql.substring(sql.indexOf("SET"));
            } else if (sql.contains("SET " + CharsetSettings.CHARACTER_SET_RESULTS)) {
                this.setCharacterSetResultsCnt++;
                this.setCharacterSetResultsQuery = sql.substring(sql.indexOf("SET"));
            } else if (sql.contains("SHOW VARIABLES") || sql.contains("SELECT  @@")) {
                System.out.println(sql.substring(sql.indexOf("S")));
                this.queryVarsCnt++;
            }
            return null;
        }

        @Override
        public <T extends Resultset> T preProcess(Supplier<String> str, Query interceptedQuery) {
            String sql = str.get();
            if (sql.contains("SET NAMES")) {
                this.setNamesCnt++;
            } else if (sql.contains("SET " + CharsetSettings.CHARACTER_SET_RESULTS)) {
                this.setCharacterSetResultsCnt++;
            } else if (sql.contains("SHOW VARIABLES") || sql.contains("SELECT  @@")) {
                System.out.println(sql.substring(sql.indexOf("S")));
                this.queryVarsCnt++;
            }
            return null;
        }
    }

}
