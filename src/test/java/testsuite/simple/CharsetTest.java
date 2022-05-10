/*
 * Copyright (c) 2005, 2022, Oracle and/or its affiliates.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;

import org.junit.jupiter.api.Test;

import com.mysql.cj.CharsetMapping;
import com.mysql.cj.CharsetMappingWrapper;
import com.mysql.cj.MysqlConnection;
import com.mysql.cj.conf.PropertyDefinitions.SslMode;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.util.StringUtils;

import testsuite.BaseTestCase;

public class CharsetTest extends BaseTestCase {
    @Test
    public void testCP932Backport() throws Exception {
        try {
            "".getBytes("WINDOWS-31J");
        } catch (UnsupportedEncodingException uee) {
            assumeFalse(true, "Test requires JVM with WINDOWS-31J support.");
        }

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "WINDOWS-31J");
        getConnectionWithProps(props).close();
    }

    @Test
    public void testNECExtendedCharsByEUCJPSolaris() throws Exception {
        try {
            "".getBytes("EUC_JP_Solaris");
        } catch (UnsupportedEncodingException uee) {
            assumeFalse(true, "Test requires JVM with EUC_JP_Solaris support.");
        }

        char necExtendedChar = 0x3231; // 0x878A of WINDOWS-31J, NEC
        // special(row13).
        String necExtendedCharString = String.valueOf(necExtendedChar);

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "EUC_JP_Solaris");

        Connection conn2 = getConnectionWithProps(props);
        Statement stmt2 = conn2.createStatement();

        createTable("t_eucjpms", "(c1 char(1)) default character set = eucjpms");
        stmt2.executeUpdate("INSERT INTO t_eucjpms VALUES ('" + necExtendedCharString + "')");
        this.rs = stmt2.executeQuery("SELECT c1 FROM t_eucjpms");
        this.rs.next();
        assertEquals(necExtendedCharString, this.rs.getString("c1"));

        this.rs.close();
        stmt2.close();
        conn2.close();

        props.setProperty(PropertyKey.characterSetResults.getKeyName(), "EUC_JP_Solaris");
        conn2 = getConnectionWithProps(props);
        stmt2 = this.conn.createStatement();

        this.rs = stmt2.executeQuery("SELECT c1 FROM t_eucjpms");
        this.rs.next();
        assertEquals(necExtendedCharString, this.rs.getString("c1"));

        this.rs.close();
        stmt2.close();
        conn2.close();
    }

    /**
     * Test data of sjis. sjis consists of ASCII, JIS-Roman, JISX0201 and JISX0208.
     */
    public static final char[] SJIS_CHARS = new char[] { 0xFF71, // halfwidth katakana letter A, 0xB100 of SJIS, one of JISX0201.
            0x65E5, // CJK unified ideograph, 0x93FA of SJIS, one of JISX0208.
            0x8868, // CJK unified ideograph, 0x955C of SJIS, one of '5c' character.
            0x2016 // 0x8161 of SJIS/WINDOWS-31J, converted to differently to/from ucs2
    };

    /**
     * Test data of cp932. WINDOWS-31J consists of ASCII, JIS-Roman, JISX0201, JISX0208, NEC special characters(row13), NEC selected IBM special characters, and
     * IBM special characters.
     */
    private static final char[] CP932_CHARS = new char[] { 0xFF71, // halfwidth katakana letter A, 0xB100 of WINDOWS-31J, one of JISX0201.
            0x65E5, // CJK unified ideograph, 0x93FA of WINDOWS-31J, one of JISX0208.
            0x3231, // parenthesized ideograph stok, 0x878B of WINDOWS-31J, one of NEC special characters(row13).
            0x67BB, // CJK unified ideograph, 0xEDC6 of WINDOWS-31J, one of NEC selected IBM special characters.
            0x6D6F, // CJK unified ideograph, 0xFAFC of WINDOWS-31J, one of IBM special characters.
            0x8868, // one of CJK unified ideograph, 0x955C of WINDOWS-31J, one of '5c' characters.
            0x2225 // 0x8161 of SJIS/WINDOWS-31J, converted to differently to/from ucs2
    };

    /**
     * Test data of ujis. ujis consists of ASCII, JIS-Roman, JISX0201, JISX0208, JISX0212.
     */
    public static final char[] UJIS_CHARS = new char[] { 0xFF71, // halfwidth katakana letter A, 0x8EB1 of ujis, one of JISX0201.
            0x65E5, // CJK unified ideograph, 0xC6FC of ujis, one of JISX0208.
            0x7B5D, // CJK unified ideograph, 0xE4B882 of ujis, one of JISX0212
            0x301C // wave dash, 0xA1C1 of ujis, convertion rule is different from ujis
    };

    /**
     * Test data of eucjpms. ujis consists of ASCII, JIS-Roman, JISX0201, JISX0208, JISX0212, NEC special characters(row13)
     */
    public static final char[] EUCJPMS_CHARS = new char[] { 0xFF71, // halfwidth katakana letter A, 0x8EB1 of ujis, one of JISX0201.
            0x65E5, // CJK unified ideograph, 0xC6FC of ujis, one of JISX0208.
            0x7B5D, // CJK unified ideograph, 0xE4B882 of ujis, one of JISX0212
            0x3231, // parenthesized ideograph stok, 0x878A of WINDOWS-31J, one of NEC special characters(row13).
            0xFF5E // wave dash, 0xA1C1 of eucjpms, convertion rule is different from ujis
    };

    @Test
    public void testInsertCharStatement() throws Exception {
        try {
            "".getBytes("SJIS");
        } catch (UnsupportedEncodingException uee) {
            assumeFalse(true, "Test requires JVM with SJIS support.");
        }

        Map<String, char[]> testDataMap = new HashMap<>();

        List<String> charsetList = new ArrayList<>();

        Map<String, Connection> connectionMap = new HashMap<>();

        Map<String, Connection> connectionWithResultMap = new HashMap<>();

        Map<String, Statement> statementMap = new HashMap<>();

        Map<String, Statement> statementWithResultMap = new HashMap<>();

        Map<String, String> javaToMysqlCharsetMap = new HashMap<>();

        charsetList.add("SJIS");
        testDataMap.put("SJIS", SJIS_CHARS);
        javaToMysqlCharsetMap.put("SJIS", "sjis");

        charsetList.add("Shift_JIS");
        testDataMap.put("Shift_JIS", SJIS_CHARS);
        javaToMysqlCharsetMap.put("Shift_JIS", "sjis");

        charsetList.add("CP943");
        testDataMap.put("CP943", SJIS_CHARS);
        javaToMysqlCharsetMap.put("CP943", "sjis");

        charsetList.add("WINDOWS-31J");
        testDataMap.put("WINDOWS-31J", CP932_CHARS);
        javaToMysqlCharsetMap.put("WINDOWS-31J", "cp932");

        charsetList.add("MS932");
        testDataMap.put("MS932", CP932_CHARS);
        javaToMysqlCharsetMap.put("MS932", "cp932");

        charsetList.add("EUC_JP");
        testDataMap.put("EUC_JP", UJIS_CHARS);
        // testDataHexMap.put("EUC_JP", UJIS_CHARS_HEX);
        javaToMysqlCharsetMap.put("EUC_JP", "ujis");

        charsetList.add("EUC_JP_Solaris");
        testDataMap.put("EUC_JP_Solaris", EUCJPMS_CHARS);
        // testDataHexMap.put("EUC_JP_Solaris", EUCJPMS_CHARS_HEX);
        javaToMysqlCharsetMap.put("EUC_JP_Solaris", "eucjpms");

        for (String charset : charsetList) {
            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

            props.setProperty(PropertyKey.characterEncoding.getKeyName(), charset);
            Connection conn2 = getConnectionWithProps(props);
            connectionMap.put(charset.toLowerCase(Locale.ENGLISH), conn2);
            statementMap.put(charset.toLowerCase(Locale.ENGLISH), conn2.createStatement());

            props.setProperty(PropertyKey.characterSetResults.getKeyName(), charset);
            Connection connWithResult = getConnectionWithProps(props);
            connectionWithResultMap.put(charset, connWithResult);
            statementWithResultMap.put(charset, connWithResult.createStatement());
        }

        for (String charset : charsetList) {
            String mysqlCharset = javaToMysqlCharsetMap.get(charset);
            Statement stmt2 = statementMap.get(charset.toLowerCase(Locale.ENGLISH));
            String query1 = "DROP TABLE IF EXISTS t1";
            String query2 = "CREATE TABLE t1 (c1 int, c2 char(1)) DEFAULT CHARACTER SET = " + mysqlCharset;
            stmt2.executeUpdate(query1);
            stmt2.executeUpdate(query2);
            char[] testData = testDataMap.get(charset);
            for (int i = 0; i < testData.length; i++) {
                String query3 = "INSERT INTO t1 values(" + i + ", '" + testData[i] + "')";
                stmt2.executeUpdate(query3);
                String query4 = "SELECT c2 FROM t1 WHERE c1 = " + i;
                this.rs = stmt2.executeQuery(query4);
                this.rs.next();
                String value = this.rs.getString(1);

                assertEquals(String.valueOf(testData[i]), value, "For character set " + charset + "/ " + mysqlCharset);
            }
            String query5 = "DROP TABLE t1";
            stmt2.executeUpdate(query5);
        }
    }

    /**
     * Prints static mappings for analysis.
     * 
     * @throws Exception
     */
    @Test
    public void testCharsetMapping() throws Exception {
        SortedMap<String, Charset> availableCharsets = Charset.availableCharsets();
        Set<String> k = availableCharsets.keySet();
        System.out.println("Java encoding --> Initial encoding (Can encode), Encoding by index, Index by encoding, collation by index, charset by index...");
        System.out.println("===================================");
        Iterator<String> i1 = k.iterator();
        while (i1.hasNext()) {
            String canonicalName = i1.next();
            java.nio.charset.Charset cs = availableCharsets.get(canonicalName);
            canonicalName = cs.name();

            int index = CharsetMappingWrapper.getStaticCollationIndexForJavaEncoding(canonicalName, this.serverVersion);
            String csname = CharsetMappingWrapper.getStaticMysqlCharsetNameForCollationIndex(index);

            System.out.println((canonicalName + "                              ").substring(0, 26) + " (" + cs.canEncode() + ") --> "
                    + CharsetMappingWrapper.getStaticJavaEncodingForCollationIndex(index) + "  :  " + index + "  :  "
                    + CharsetMappingWrapper.getStaticCollationNameForCollationIndex(index) + "  :  "
                    + CharsetMappingWrapper.getStaticMysqlCharsetNameForCollationIndex(index) + "  :  "
                    + CharsetMappingWrapper.getStaticMysqlCharsetByName(csname) + "  :  " + CharsetMappingWrapper.getStaticJavaEncodingForMysqlCharset(csname)
                    + "  :  " + CharsetMappingWrapper.getStaticMysqlCharsetForJavaEncoding(canonicalName, this.serverVersion) + "  :  "
                    + CharsetMappingWrapper.getStaticCollationIndexForJavaEncoding(canonicalName, this.serverVersion) + "  :  "
                    + CharsetMappingWrapper.isStaticMultibyteCharset(canonicalName));

            Set<String> s = cs.aliases();
            Iterator<String> j = s.iterator();
            while (j.hasNext()) {
                String alias = j.next();
                index = CharsetMappingWrapper.getStaticCollationIndexForJavaEncoding(alias, this.serverVersion);
                csname = CharsetMappingWrapper.getStaticMysqlCharsetNameForCollationIndex(index);
                System.out.println("   " + (alias + "                              ").substring(0, 30) + " --> "
                        + CharsetMappingWrapper.getStaticJavaEncodingForCollationIndex(index) + "  :  " + index + "  :  "
                        + CharsetMappingWrapper.getStaticCollationNameForCollationIndex(index) + "  :  "
                        + CharsetMappingWrapper.getStaticMysqlCharsetNameForCollationIndex(index) + "  :  "
                        + CharsetMappingWrapper.getStaticMysqlCharsetByName(csname) + "  :  "
                        + CharsetMappingWrapper.getStaticJavaEncodingForMysqlCharset(csname) + "  :  "
                        + CharsetMappingWrapper.getStaticMysqlCharsetForJavaEncoding(alias, this.serverVersion) + "  :  "
                        + CharsetMappingWrapper.getStaticCollationIndexForJavaEncoding(alias, this.serverVersion) + "  :  "
                        + CharsetMappingWrapper.isStaticMultibyteCharset(alias));
            }
            System.out.println("===================================");
        }
        for (int i = 1; i < CharsetMapping.MAP_SIZE; i++) {
            String csname = CharsetMappingWrapper.getStaticMysqlCharsetNameForCollationIndex(i);
            if (csname != null) {
                String enc = CharsetMappingWrapper.getStaticJavaEncodingForCollationIndex(i);
                System.out.println((i + "   ").substring(0, 4) + " by index--> "
                        + (CharsetMappingWrapper.getStaticCollationNameForCollationIndex(i) + "                    ").substring(0, 20) + "  :  "
                        + (csname + "          ").substring(0, 10) + "  :  " + (enc + "                    ").substring(0, 20)

                        + " by charset--> " + (CharsetMappingWrapper.getStaticJavaEncodingForMysqlCharset(csname) + "                  ").substring(0, 20)
                        + " by encoding--> " + (CharsetMappingWrapper.getStaticCollationIndexForJavaEncoding(enc, this.serverVersion) + "   ").substring(0, 4)
                        + "  :  " + (CharsetMappingWrapper.getStaticMysqlCharsetForJavaEncoding(enc, this.serverVersion) + "               ").substring(0, 15));
            }
        }
    }

    /**
     * Test for the gb18030 character set
     * 
     * @throws Exception
     */
    @Test
    public void testGB18030() throws Exception {
        // check that server supports this character set
        this.rs = this.stmt.executeQuery("show collation like 'gb18030_chinese_ci'");
        assumeTrue(this.rs.next(), "This test requires the server suporting gb18030 character set.");

        // phrases to check
        String[][] str = new String[][] {
                { "C4EEC5ABBDBFA1A4B3E0B1DABBB3B9C520A1A4CBD5B6ABC6C2", "\u5FF5\u5974\u5A07\u00B7\u8D64\u58C1\u6000\u53E4 \u00B7\u82CF\u4E1C\u5761" },
                { "B4F3BDADB6ABC8A5A3ACC0CBCCD4BEA1A1A2C7A7B9C5B7E7C1F7C8CBCEEFA1A3",
                        "\u5927\u6C5F\u4E1C\u53BB\uFF0C\u6D6A\u6DD8\u5C3D\u3001\u5343\u53E4\u98CE\u6D41\u4EBA\u7269\u3002" },
                { "B9CAC0DDCEF7B1DFA3ACC8CBB5C0CAC7A1A2C8FDB9FAD6DCC0C9B3E0B1DAA1A3",
                        "\u6545\u5792\u897F\u8FB9\uFF0C\u4EBA\u9053\u662F\u3001\u4E09\u56FD\u5468\u90CE\u8D64\u58C1\u3002" },
                { "C2D2CAAFB1C0D4C6A3ACBEAACCCEC1D1B0B6A3ACBEEDC6F0C7A7B6D1D1A9A1A3",
                        "\u4E71\u77F3\u5D29\u4E91\uFF0C\u60CA\u6D9B\u88C2\u5CB8\uFF0C\u5377\u8D77\u5343\u5806\u96EA\u3002" },
                { "BDADC9BDC8E7BBADA3ACD2BBCAB1B6E0C9D9BAC0BDDCA3A1", "\u6C5F\u5C71\u5982\u753B\uFF0C\u4E00\u65F6\u591A\u5C11\u8C6A\u6770\uFF01" },
                { "D2A3CFEBB9ABE8AAB5B1C4EAA3ACD0A1C7C7B3F5BCDEC1CBA3ACD0DBD7CBD3A2B7A2A1A3",
                        "\u9065\u60F3\u516C\u747E\u5F53\u5E74\uFF0C\u5C0F\u4E54\u521D\u5AC1\u4E86\uFF0C\u96C4\u59FF\u82F1\u53D1\u3002" },
                { "D3F0C9C8C2DABDEDA3ACCCB8D0A6BCE4A1A2E9C9E9D6BBD2B7C9D1CCC3F0A1A3",
                        "\u7FBD\u6247\u7EB6\u5DFE\uFF0C\u8C08\u7B11\u95F4\u3001\u6A2F\u6A79\u7070\u98DE\u70DF\u706D\u3002" },
                { "B9CAB9FAC9F1D3CEA3ACB6E0C7E9D3A6D0A6CED2A1A2D4E7C9FABBAAB7A2A1A3",
                        "\u6545\u56FD\u795E\u6E38\uFF0C\u591A\u60C5\u5E94\u7B11\u6211\u3001\u65E9\u751F\u534E\u53D1\u3002" },
                { "C8CBBCE4C8E7C3CEA3ACD2BBE9D7BBB9F5AABDADD4C2A1A3", "\u4EBA\u95F4\u5982\u68A6\uFF0C\u4E00\u6A3D\u8FD8\u9179\u6C5F\u6708\u3002" },
                { "5373547483329330", "SsTt\uC23F" }, { "8239AB318239AB358239AF3583308132833087348335EB39", "\uB46C\uB470\uB498\uB7B5\uB7F3\uD47C" },
                { "97339631973396339733A6359831C0359831C536", "\uD85A\uDC1F\uD85A\uDC21\uD85A\uDCC3\uD864\uDD27\uD864\uDD5A" },
                { "9835CF329835CE359835F336", "\uD869\uDD6A\uD869\uDD63\uD869\uDED6" }, { "833988318339883283398539", "\uF45A\uF45B\uF444" },
                { "823398318233973582339A3882348A32", "\u4460\u445A\u447B\u48C8" }, { "8134D5318134D6328134D832", "\u1817\u1822\u1836" },
                { "4A7320204B82339A35646566", "Js  K\u4478def" }, { "8130883281308833", "\u00CE\u00CF" }, { "E05FE06A777682339230", "\u90F7\u9107wv\u4423" },
                { "814081418139FE30", "\u4E02\u4E04\u3499" }, { "81308130FEFE", "\u0080\uE4C5" }, { "E3329A35E3329A34", "\uDBFF\uDFFF\uDBFF\uDFFE" } };
        HashMap<String, String> expected = new HashMap<>();

        // check variables
        Connection con = getConnectionWithProps("characterEncoding=GB18030");
        Statement st = con.createStatement();
        ResultSet rset = st.executeQuery("show variables like 'character_set_client'");
        rset.next();
        assertEquals("gb18030", rset.getString(2));
        rset = st.executeQuery("show variables like 'character_set_connection'");
        rset.next();
        assertEquals("gb18030", rset.getString(2));
        rset = st.executeQuery("show variables like 'collation_connection'");
        rset.next();
        assertEquals("gb18030_chinese_ci", rset.getString(2));

        st.executeUpdate("DROP TABLE IF EXISTS testGB18030");
        st.executeUpdate("CREATE TABLE testGB18030(C VARCHAR(100) CHARACTER SET gb18030)");

        // insert phrases
        PreparedStatement pst = null;
        pst = con.prepareStatement("INSERT INTO testGB18030 VALUES(?)");
        for (int i = 0; i < str.length; i++) {
            expected.put(str[i][0], str[i][1]);
            pst.setString(1, str[i][1]);
            pst.addBatch();
        }
        pst.executeBatch();

        // read phrases
        rset = st.executeQuery("SELECT c, HEX(c), CONVERT(c USING utf8mb4) FROM testGB18030");
        int resCount = 0;
        while (rset.next()) {
            resCount++;
            String hex = rset.getString(2);
            assertTrue(expected.containsKey(hex), "HEX value " + hex + " for char " + rset.getString(1) + " is unexpected");
            assertEquals(expected.get(hex), rset.getString(1));
            assertEquals(expected.get(hex), rset.getString(3));
        }
        assertEquals(str.length, resCount);

        // chars that can't be converted to utf8/utf16
        st.executeUpdate("TRUNCATE TABLE testGB18030");
        st.executeUpdate("INSERT INTO testGB18030 VALUES(0xFE39FE39FE38FE38),(0xFE39FE38A976)");
        rset = st.executeQuery("SELECT c, HEX(c), CONVERT(c USING utf8mb4) FROM testGB18030");
        while (rset.next()) {
            String hex = rset.getString(2);
            if ("FE39FE39FE38FE38".equals(hex)) {
                assertEquals("\uFFFD\uFFFD", rset.getString(1));
                assertEquals("??", rset.getString(3));
            } else if ("FE39FE38A976".equals(hex)) {
                assertEquals("\uFFFD\uFE59", rset.getString(1));
                assertEquals("?\uFE59", rset.getString(3));
            } else {
                fail("HEX value " + hex + " unexpected");
            }
        }

        st.executeUpdate("DROP TABLE IF EXISTS testGB18030");
        con.close();
    }

    /**
     * Tests the ability to set the connection collation via properties.
     * 
     * @throws Exception
     */
    @Test
    public void testNonStandardConnectionCollation() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.connectionCollation.getKeyName(), "utf8_bin");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "utf-8");

        try (Connection collConn = getConnectionWithProps(props)) {
            Statement collStmt = collConn.createStatement();
            ResultSet collRs = collStmt.executeQuery("SHOW VARIABLES LIKE 'collation_connection'");

            assertTrue(collRs.next());
            assertEquals(versionMeetsMinimum(8, 0, 30) ? "utf8mb3_bin" : "utf8_bin", collRs.getString(2));
        }
    }

    @Test
    public void testCharsets() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");

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

    /**
     * Tests if the driver configures character sets correctly for 4.1.x servers. Requires that the 'admin connection' is configured, as this test needs to
     * create/drop databases.
     * 
     * @throws Exception
     */
    @Test
    public void testCollation41() throws Exception {
        Map<String, String> charsetsAndCollations = getCharacterSetsAndCollations();
        charsetsAndCollations.remove("latin7"); // Maps to multiple Java charsets
        charsetsAndCollations.remove("ucs2"); // can't be used as a connection charset

        for (String charsetName : charsetsAndCollations.keySet()) {
            String enc = ((MysqlConnection) this.conn).getSession().getServerSession().getCharsetSettings().getJavaEncodingForMysqlCharset(charsetName);
            if (enc == null) {
                continue;
            }
            Connection charsetConn = null;
            Statement charsetStmt = null;

            try {
                System.out.print("Testing character set " + charsetName);

                Properties props = new Properties();
                props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
                props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
                props.setProperty(PropertyKey.characterEncoding.getKeyName(), enc);

                System.out.println(", Java encoding " + enc);

                charsetConn = getConnectionWithProps(props);

                charsetStmt = charsetConn.createStatement();

                charsetStmt.executeUpdate("DROP DATABASE IF EXISTS testCollation41");
                charsetStmt.executeUpdate("DROP TABLE IF EXISTS testCollation41");
                charsetStmt.executeUpdate("CREATE DATABASE testCollation41 DEFAULT CHARACTER SET " + charsetName);
                charsetStmt.close();

                charsetConn.setCatalog("testCollation41");

                // We've switched catalogs, so we need to recreate the statement to pick this up...
                charsetStmt = charsetConn.createStatement();

                StringBuilder createTableCommand = new StringBuilder("CREATE TABLE testCollation41(field1 VARCHAR(255), field2 INT)");

                charsetStmt.executeUpdate(createTableCommand.toString());

                charsetStmt.executeUpdate("INSERT INTO testCollation41 VALUES ('abc', 0)");

                int updateCount = charsetStmt.executeUpdate("UPDATE testCollation41 SET field2=1 WHERE field1='abc'");
                assertEquals(1, updateCount);
            } finally {
                if (charsetStmt != null) {
                    charsetStmt.executeUpdate("DROP TABLE IF EXISTS testCollation41");
                    charsetStmt.executeUpdate("DROP DATABASE IF EXISTS testCollation41");
                    charsetStmt.close();
                }

                if (charsetConn != null) {
                    charsetConn.close();
                }
            }
        }
    }

    private Map<String, String> getCharacterSetsAndCollations() throws Exception {
        Map<String, String> charsetsToLoad = new HashMap<>();

        try {
            this.rs = this.stmt.executeQuery("SHOW character set");

            while (this.rs.next()) {
                charsetsToLoad.put(this.rs.getString("Charset"), this.rs.getString("Default collation"));
            }

            //
            // These don't have mappings in Java...
            //
            charsetsToLoad.remove("swe7");
            charsetsToLoad.remove("hp8");
            charsetsToLoad.remove("dec8");
            charsetsToLoad.remove("koi8u");
            charsetsToLoad.remove("keybcs2");
            charsetsToLoad.remove("geostd8");
            charsetsToLoad.remove("armscii8");
        } finally {
            if (this.rs != null) {
                this.rs.close();
            }
        }

        return charsetsToLoad;
    }

    @Test
    public void testCSC5765() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "utf8");
        props.setProperty(PropertyKey.characterSetResults.getKeyName(), "utf8");
        props.setProperty(PropertyKey.connectionCollation.getKeyName(), "utf8_bin");

        Connection utf8Conn = null;

        try {
            utf8Conn = getConnectionWithProps(props);
            this.rs = utf8Conn.createStatement().executeQuery("SHOW VARIABLES LIKE 'character_%'");
            while (this.rs.next()) {
                System.out.println(this.rs.getString(1) + " = " + this.rs.getString(2));
            }

            this.rs = utf8Conn.createStatement().executeQuery("SHOW VARIABLES LIKE 'collation_%'");
            while (this.rs.next()) {
                System.out.println(this.rs.getString(1) + " = " + this.rs.getString(2));
            }
        } finally {
            if (utf8Conn != null) {
                utf8Conn.close();
            }
        }
    }

    /**
     * These two charsets have different names depending on version of MySQL server.
     * 
     * @throws Exception
     */
    @Test
    public void testNewCharsetsConfiguration() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "EUC_KR");
        getConnectionWithProps(props).close();

        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "KOI8_R");
        getConnectionWithProps(props).close();
    }

    /**
     * Tests that 'latin1' character conversion works correctly.
     * 
     * @throws Exception
     */
    @Test
    public void testLatin1Encoding() throws Exception {
        char[] latin1Charset = { 0x0000, 0x0001, 0x0002, 0x0003, 0x0004, 0x0005, 0x0006, 0x0007, 0x0008, 0x0009, 0x000A, 0x000B, 0x000C, 0x000D, 0x000E, 0x000F,
                0x0010, 0x0011, 0x0012, 0x0013, 0x0014, 0x0015, 0x0016, 0x0017, 0x0018, 0x0019, 0x001A, 0x001B, 0x001C, 0x001D, 0x001E, 0x001F, 0x0020, 0x0021,
                0x0022, 0x0023, 0x0024, 0x0025, 0x0026, 0x0027, 0x0028, 0x0029, 0x002A, 0x002B, 0x002C, 0x002D, 0x002E, 0x002F, 0x0030, 0x0031, 0x0032, 0x0033,
                0x0034, 0x0035, 0x0036, 0x0037, 0x0038, 0x0039, 0x003A, 0x003B, 0x003C, 0x003D, 0x003E, 0x003F, 0x0040, 0x0041, 0x0042, 0x0043, 0x0044, 0x0045,
                0x0046, 0x0047, 0x0048, 0x0049, 0x004A, 0x004B, 0x004C, 0x004D, 0x004E, 0x004F, 0x0050, 0x0051, 0x0052, 0x0053, 0x0054, 0x0055, 0x0056, 0x0057,
                0x0058, 0x0059, 0x005A, 0x005B, 0x005C, 0x005D, 0x005E, 0x005F, 0x0060, 0x0061, 0x0062, 0x0063, 0x0064, 0x0065, 0x0066, 0x0067, 0x0068, 0x0069,
                0x006A, 0x006B, 0x006C, 0x006D, 0x006E, 0x006F, 0x0070, 0x0071, 0x0072, 0x0073, 0x0074, 0x0075, 0x0076, 0x0077, 0x0078, 0x0079, 0x007A, 0x007B,
                0x007C, 0x007D, 0x007E, 0x007F, 0x0080, 0x0081, 0x0082, 0x0083, 0x0084, 0x0085, 0x0086, 0x0087, 0x0088, 0x0089, 0x008A, 0x008B, 0x008C, 0x008D,
                0x008E, 0x008F, 0x0090, 0x0091, 0x0092, 0x0093, 0x0094, 0x0095, 0x0096, 0x0097, 0x0098, 0x0099, 0x009A, 0x009B, 0x009C, 0x009D, 0x009E, 0x009F,
                0x00A0, 0x00A1, 0x00A2, 0x00A3, 0x00A4, 0x00A5, 0x00A6, 0x00A7, 0x00A8, 0x00A9, 0x00AA, 0x00AB, 0x00AC, 0x00AD, 0x00AE, 0x00AF, 0x00B0, 0x00B1,
                0x00B2, 0x00B3, 0x00B4, 0x00B5, 0x00B6, 0x00B7, 0x00B8, 0x00B9, 0x00BA, 0x00BB, 0x00BC, 0x00BD, 0x00BE, 0x00BF, 0x00C0, 0x00C1, 0x00C2, 0x00C3,
                0x00C4, 0x00C5, 0x00C6, 0x00C7, 0x00C8, 0x00C9, 0x00CA, 0x00CB, 0x00CC, 0x00CD, 0x00CE, 0x00CF, 0x00D0, 0x00D1, 0x00D2, 0x00D3, 0x00D4, 0x00D5,
                0x00D6, 0x00D7, 0x00D8, 0x00D9, 0x00DA, 0x00DB, 0x00DC, 0x00DD, 0x00DE, 0x00DF, 0x00E0, 0x00E1, 0x00E2, 0x00E3, 0x00E4, 0x00E5, 0x00E6, 0x00E7,
                0x00E8, 0x00E9, 0x00EA, 0x00EB, 0x00EC, 0x00ED, 0x00EE, 0x00EF, 0x00F0, 0x00F1, 0x00F2, 0x00F3, 0x00F4, 0x00F5, 0x00F6, 0x00F7, 0x00F8, 0x00F9,
                0x00FA, 0x00FB, 0x00FC, 0x00FD, 0x00FE, 0x00FF };

        String latin1String = new String(latin1Charset);
        Connection latin1Conn = null;
        PreparedStatement pStmt = null;

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "cp1252");
            latin1Conn = getConnectionWithProps(props);

            createTable("latin1RegressTest", "(stringField TEXT)");

            pStmt = latin1Conn.prepareStatement("INSERT INTO latin1RegressTest VALUES (?)");
            pStmt.setString(1, latin1String);
            pStmt.executeUpdate();

            ((com.mysql.cj.jdbc.JdbcConnection) latin1Conn).getPropertySet().getProperty(PropertyKey.traceProtocol).setValue(true);

            this.rs = latin1Conn.createStatement().executeQuery("SELECT * FROM latin1RegressTest");
            ((com.mysql.cj.jdbc.JdbcConnection) latin1Conn).getPropertySet().getProperty(PropertyKey.traceProtocol).setValue(false);

            this.rs.next();

            String retrievedString = this.rs.getString(1);

            System.out.println(latin1String);
            System.out.println(retrievedString);

            if (!retrievedString.equals(latin1String)) {
                int stringLength = Math.min(retrievedString.length(), latin1String.length());

                for (int i = 0; i < stringLength; i++) {
                    char rChar = retrievedString.charAt(i);
                    char origChar = latin1String.charAt(i);

                    assertFalse((rChar != '?') && (rChar != origChar),
                            "characters differ at position " + i + "'" + rChar + "' retrieved from database, original char was '" + origChar + "'");
                }
            }
        } finally {
            if (latin1Conn != null) {
                latin1Conn.close();
            }
        }
    }

    /**
     * Tests that the 0x5c escaping works (we didn't use to have this).
     * 
     * @throws Exception
     */
    @Test
    public void testSjis5c() throws Exception {
        byte[] origByteStream = new byte[] { (byte) 0x95, (byte) 0x5c, (byte) 0x8e, (byte) 0x96 };

        //
        // Print the hex values of the string
        //
        StringBuilder bytesOut = new StringBuilder();

        for (int i = 0; i < origByteStream.length; i++) {
            bytesOut.append(Integer.toHexString(origByteStream[i] & 255));
            bytesOut.append(" ");
        }

        System.out.println(bytesOut.toString());

        String origString = new String(origByteStream, "SJIS");
        byte[] newByteStream = StringUtils.getBytes(origString, "SJIS");

        //
        // Print the hex values of the string (should have an extra 0x5c)
        //
        bytesOut = new StringBuilder();

        for (int i = 0; i < newByteStream.length; i++) {
            bytesOut.append(Integer.toHexString(newByteStream[i] & 255));
            bytesOut.append(" ");
        }

        System.out.println(bytesOut.toString());

        //
        // Now, insert and retrieve the value from the database
        //
        Connection sjisConn = null;
        Statement sjisStmt = null;

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "SJIS");
            sjisConn = getConnectionWithProps(props);

            sjisStmt = sjisConn.createStatement();

            this.rs = sjisStmt.executeQuery("SHOW VARIABLES LIKE 'character_set%'");

            while (this.rs.next()) {
                System.out.println(this.rs.getString(1) + " = " + this.rs.getString(2));
            }

            sjisStmt.executeUpdate("DROP TABLE IF EXISTS sjisTest");

            sjisStmt.executeUpdate("CREATE TABLE sjisTest (field1 char(50)) DEFAULT CHARACTER SET SJIS");

            this.pstmt = sjisConn.prepareStatement("INSERT INTO sjisTest VALUES (?)");
            this.pstmt.setString(1, origString);
            this.pstmt.executeUpdate();

            this.rs = sjisStmt.executeQuery("SELECT * FROM sjisTest");

            while (this.rs.next()) {
                byte[] testValueAsBytes = this.rs.getBytes(1);

                bytesOut = new StringBuilder();

                for (int i = 0; i < testValueAsBytes.length; i++) {
                    bytesOut.append(Integer.toHexString(testValueAsBytes[i] & 255));
                    bytesOut.append(" ");
                }

                System.out.println("Value retrieved from database: " + bytesOut.toString());

                String testValue = this.rs.getString(1);

                assertTrue(testValue.equals(origString));
            }
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS sjisTest");
        }
    }

    /**
     * Tests that UTF-8 character conversion works correctly.
     * 
     * @throws Exception
     */
    @Test
    public void testUtf8Encoding() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF8");
        props.setProperty(PropertyKey.jdbcCompliantTruncation.getKeyName(), "false");

        Connection utfConn = DriverManager.getConnection(dbUrl, props);
        testConversionForString("UTF8", utfConn, "\u043c\u0438\u0445\u0438");
    }

    @Test
    public void testUtf8Encoding2() throws Exception {
        String field1 = "K��sel";
        String field2 = "B�b";
        byte[] field1AsBytes = field1.getBytes("utf-8");
        byte[] field2AsBytes = field2.getBytes("utf-8");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF8");

        Connection utfConn = DriverManager.getConnection(dbUrl, props);
        Statement utfStmt = utfConn.createStatement();

        try {
            utfStmt.executeUpdate("DROP TABLE IF EXISTS testUtf8");
            utfStmt.executeUpdate("CREATE TABLE testUtf8 (field1 varchar(32), field2 varchar(32)) CHARACTER SET UTF8");
            utfStmt.executeUpdate("INSERT INTO testUtf8 VALUES ('" + field1 + "','" + field2 + "')");

            PreparedStatement pStmt = utfConn.prepareStatement("INSERT INTO testUtf8 VALUES (?, ?)");
            pStmt.setString(1, field1);
            pStmt.setString(2, field2);
            pStmt.executeUpdate();

            this.rs = utfStmt.executeQuery("SELECT * FROM testUtf8");
            assertTrue(this.rs.next());

            // Compare results stored using direct statement
            // Compare to original string
            assertTrue(field1.equals(this.rs.getString(1)));
            assertTrue(field2.equals(this.rs.getString(2)));

            // Compare byte-for-byte, ignoring encoding
            assertTrue(bytesAreSame(field1AsBytes, this.rs.getBytes(1)));
            assertTrue(bytesAreSame(field2AsBytes, this.rs.getBytes(2)));

            assertTrue(this.rs.next());

            // Compare to original string
            assertTrue(field1.equals(this.rs.getString(1)));
            assertTrue(field2.equals(this.rs.getString(2)));

            // Compare byte-for-byte, ignoring encoding
            assertTrue(bytesAreSame(field1AsBytes, this.rs.getBytes(1)));
            assertTrue(bytesAreSame(field2AsBytes, this.rs.getBytes(2)));
        } finally {
            utfStmt.executeUpdate("DROP TABLE IF EXISTS testUtf8");
        }
    }

    private boolean bytesAreSame(byte[] byte1, byte[] byte2) {
        if (byte1.length != byte2.length) {
            return false;
        }

        for (int i = 0; i < byte1.length; i++) {
            if (byte1[i] != byte2[i]) {
                return false;
            }
        }

        return true;
    }

    private void testConversionForString(String charsetName, Connection convConn, String charsToTest) throws Exception {
        PreparedStatement pStmt = null;

        this.stmt = convConn.createStatement();

        createTable("charConvTest_" + charsetName, "(field1 CHAR(50) CHARACTER SET " + charsetName + ")");

        this.stmt.executeUpdate("INSERT INTO charConvTest_" + charsetName + " VALUES ('" + charsToTest + "')");
        pStmt = convConn.prepareStatement("INSERT INTO charConvTest_" + charsetName + " VALUES (?)");
        pStmt.setString(1, charsToTest);
        pStmt.executeUpdate();
        this.rs = this.stmt.executeQuery("SELECT * FROM charConvTest_" + charsetName);

        assertTrue(this.rs.next());

        String testValue = this.rs.getString(1);
        System.out.println(testValue);
        assertTrue(testValue.equals(charsToTest));

    }

    @Test
    public void testCsc4194() throws Exception {
        try {
            "".getBytes("Windows-31J");
        } catch (UnsupportedEncodingException ex) {
            assumeFalse(true, "Test requires JVM with Windows-31J support.");
        }

        Connection sjisConn = null;
        Connection windows31JConn = null;

        try {
            String tableNameText = "testCsc4194Text";
            String tableNameBlob = "testCsc4194Blob";

            createTable(tableNameBlob, "(field1 BLOB)");
            String charset = "";

            charset = " CHARACTER SET cp932";

            createTable(tableNameText, "(field1 TEXT)" + charset);

            Properties windows31JProps = new Properties();
            windows31JProps.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            windows31JProps.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            windows31JProps.setProperty(PropertyKey.characterEncoding.getKeyName(), "Windows-31J");

            windows31JConn = getConnectionWithProps(windows31JProps);
            testCsc4194InsertCheckBlob(windows31JConn, tableNameBlob);

            testCsc4194InsertCheckText(windows31JConn, tableNameText, "Windows-31J");

            Properties sjisProps = new Properties();
            sjisProps.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            sjisProps.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            sjisProps.setProperty(PropertyKey.characterEncoding.getKeyName(), "sjis");

            sjisConn = getConnectionWithProps(sjisProps);
            testCsc4194InsertCheckBlob(sjisConn, tableNameBlob);
            testCsc4194InsertCheckText(sjisConn, tableNameText, "Windows-31J");

        } finally {

            if (windows31JConn != null) {
                windows31JConn.close();
            }

            if (sjisConn != null) {
                sjisConn.close();
            }
        }
    }

    private void testCsc4194InsertCheckBlob(Connection c, String tableName) throws Exception {
        byte[] bArray = new byte[] { (byte) 0xac, (byte) 0xed, (byte) 0x00, (byte) 0x05 };

        PreparedStatement testStmt = c.prepareStatement("INSERT INTO " + tableName + " VALUES (?)");
        testStmt.setBytes(1, bArray);
        testStmt.executeUpdate();

        this.rs = c.createStatement().executeQuery("SELECT field1 FROM " + tableName);
        assertTrue(this.rs.next());
        assertEquals(getByteArrayString(bArray), getByteArrayString(this.rs.getBytes(1)));
        this.rs.close();
    }

    private void testCsc4194InsertCheckText(Connection c, String tableName, String encoding) throws Exception {
        byte[] kabuInShiftJIS = { (byte) 0x87, // a double-byte charater("kabu") in Shift JIS
                (byte) 0x8a, };

        String expected = new String(kabuInShiftJIS, encoding);
        PreparedStatement testStmt = c.prepareStatement("INSERT INTO " + tableName + " VALUES (?)");
        testStmt.setString(1, expected);
        testStmt.executeUpdate();

        this.rs = c.createStatement().executeQuery("SELECT field1 FROM " + tableName);
        assertTrue(this.rs.next());
        assertEquals(expected, this.rs.getString(1));
        this.rs.close();
    }

    private String getByteArrayString(byte[] ba) {
        StringBuilder buffer = new StringBuilder();
        if (ba != null) {
            for (int i = 0; i < ba.length; i++) {
                buffer.append("0x" + Integer.toHexString(ba[i] & 0xff) + " ");
            }
        } else {
            buffer.append("null");
        }
        return buffer.toString();
    }

    @Test
    public void testCodePage1252() throws Exception {
        /*
         * from
         * ftp://ftp.unicode.org/Public/MAPPINGS/VENDORS/MICSFT/WINDOWS/
         * CP1252.TXT
         * 
         * 0x80 0x20AC #EURO SIGN 0x81 #UNDEFINED 0x82 0x201A #SINGLE LOW-9
         * QUOTATION MARK 0x83 0x0192 #LATIN SMALL LETTER F WITH HOOK 0x84
         * 0x201E #DOUBLE LOW-9 QUOTATION MARK 0x85 0x2026 #HORIZONTAL
         * ELLIPSIS 0x86 0x2020 #DAGGER 0x87 0x2021 #DOUBLE DAGGER 0x88
         * 0x02C6 #MODIFIER LETTER CIRCUMFLEX ACCENT 0x89 0x2030 #PER MILLE
         * SIGN 0x8A 0x0160 #LATIN CAPITAL LETTER S WITH CARON 0x8B 0x2039
         * #SINGLE LEFT-POINTING ANGLE QUOTATION MARK 0x8C 0x0152 #LATIN
         * CAPITAL LIGATURE OE 0x8D #UNDEFINED 0x8E 0x017D #LATIN CAPITAL
         * LETTER Z WITH CARON 0x8F #UNDEFINED 0x90 #UNDEFINED
         */
        String codePage1252 = new String(new byte[] { (byte) 0x80, (byte) 0x82, (byte) 0x83, (byte) 0x84, (byte) 0x85, (byte) 0x86, (byte) 0x87, (byte) 0x88,
                (byte) 0x89, (byte) 0x8a, (byte) 0x8b, (byte) 0x8c, (byte) 0x8e }, "Cp1252");

        System.out.println(codePage1252);

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "Cp1252");
        Connection cp1252Conn = getConnectionWithProps(props);
        createTable("testCp1252", "(field1 varchar(32) CHARACTER SET latin1)");
        cp1252Conn.createStatement().executeUpdate("INSERT INTO testCp1252 VALUES ('" + codePage1252 + "')");
        this.rs = cp1252Conn.createStatement().executeQuery("SELECT field1 FROM testCp1252");
        this.rs.next();
        assertEquals(this.rs.getString(1), codePage1252);
    }
}
