/*
  Copyright (c) 2005, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;

import testsuite.BaseTestCase;

import com.mysql.jdbc.CharsetMapping;

public class CharsetTest extends BaseTestCase {

    public CharsetTest(String name) {
        super(name);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(CharsetTest.class);
    }

    public void testCP932Backport() throws Exception {
        if (versionMeetsMinimum(4, 1, 12)) {
            if (versionMeetsMinimum(5, 0)) {
                if (!versionMeetsMinimum(5, 0, 3)) {
                    return;
                }
            }

            try {
                "".getBytes("WINDOWS-31J");
            } catch (UnsupportedEncodingException uee) {
                return;
            }

            Properties props = new Properties();
            props.put("useUnicode", "true");
            props.put("characterEncoding", "WINDOWS-31J");
            getConnectionWithProps(props).close();
        }
    }

    public void testNECExtendedCharsByEUCJPSolaris() throws Exception {
        if (!isRunningOnJdk131()) {
            try {
                "".getBytes("EUC_JP_Solaris");
            } catch (UnsupportedEncodingException uee) {
                return;
            }

            if (versionMeetsMinimum(5, 0, 5)) {
                char necExtendedChar = 0x3231; // 0x878A of WINDOWS-31J, NEC
                // special(row13).
                String necExtendedCharString = String.valueOf(necExtendedChar);

                Properties props = new Properties();

                props.put("useUnicode", "true");
                props.put("characterEncoding", "EUC_JP_Solaris");

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

                props.put("characterSetResults", "EUC_JP_Solaris");
                conn2 = getConnectionWithProps(props);
                stmt2 = this.conn.createStatement();

                this.rs = stmt2.executeQuery("SELECT c1 FROM t_eucjpms");
                this.rs.next();
                assertEquals(necExtendedCharString, this.rs.getString("c1"));

                this.rs.close();
                stmt2.close();
                conn2.close();
            }
        }
    }

    /**
     * Test data of sjis. sjis consists of ASCII, JIS-Roman, JISX0201 and
     * JISX0208.
     */
    public static final char[] SJIS_CHARS = new char[] { 0xFF71, // halfwidth katakana letter A, 0xB100 of SJIS, one of JISX0201.
            0x65E5, // CJK unified ideograph, 0x93FA of SJIS, one of JISX0208.
            0x8868, // CJK unified ideograph, 0x955C of SJIS, one of '5c' character.
            0x2016 // 0x8161 of SJIS/WINDOWS-31J, converted to differently to/from ucs2
    };

    /**
     * Test data of cp932. WINDOWS-31J consists of ASCII, JIS-Roman, JISX0201,
     * JISX0208, NEC special characters(row13), NEC selected IBM special
     * characters, and IBM special characters.
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
     * Test data of ujis. ujis consists of ASCII, JIS-Roman, JISX0201, JISX0208,
     * JISX0212.
     */
    public static final char[] UJIS_CHARS = new char[] { 0xFF71, // halfwidth katakana letter A, 0x8EB1 of ujis, one of JISX0201.
            0x65E5, // CJK unified ideograph, 0xC6FC of ujis, one of JISX0208.
            0x7B5D, // CJK unified ideograph, 0xE4B882 of ujis, one of JISX0212
            0x301C // wave dash, 0xA1C1 of ujis, convertion rule is different from ujis
    };

    /**
     * Test data of eucjpms. ujis consists of ASCII, JIS-Roman, JISX0201,
     * JISX0208, JISX0212, NEC special characters(row13)
     */
    public static final char[] EUCJPMS_CHARS = new char[] { 0xFF71, // halfwidth katakana letter A, 0x8EB1 of ujis, one of JISX0201.
            0x65E5, // CJK unified ideograph, 0xC6FC of ujis, one of JISX0208.
            0x7B5D, // CJK unified ideograph, 0xE4B882 of ujis, one of JISX0212
            0x3231, // parenthesized ideograph stok, 0x878A of WINDOWS-31J, one of NEC special characters(row13).
            0xFF5E // wave dash, 0xA1C1 of eucjpms, convertion rule is different from ujis
    };

    public void testInsertCharStatement() throws Exception {
        if (!isRunningOnJdk131()) {
            try {
                "".getBytes("SJIS");
            } catch (UnsupportedEncodingException uee) {
                return;
            }

            if (versionMeetsMinimum(4, 1, 12)) {
                Map<String, char[]> testDataMap = new HashMap<String, char[]>();

                List<String> charsetList = new ArrayList<String>();

                Map<String, Connection> connectionMap = new HashMap<String, Connection>();

                Map<String, Connection> connectionWithResultMap = new HashMap<String, Connection>();

                Map<String, Statement> statementMap = new HashMap<String, Statement>();

                Map<String, Statement> statementWithResultMap = new HashMap<String, Statement>();

                Map<String, String> javaToMysqlCharsetMap = new HashMap<String, String>();

                charsetList.add("SJIS");
                testDataMap.put("SJIS", SJIS_CHARS);
                javaToMysqlCharsetMap.put("SJIS", "sjis");

                charsetList.add("Shift_JIS");
                testDataMap.put("Shift_JIS", SJIS_CHARS);
                javaToMysqlCharsetMap.put("Shift_JIS", "sjis");

                charsetList.add("CP943");
                testDataMap.put("CP943", SJIS_CHARS);
                javaToMysqlCharsetMap.put("CP943", "sjis");

                if (versionMeetsMinimum(5, 0, 3)) {
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

                } else {
                    charsetList.add("EUC_JP");
                    testDataMap.put("EUC_JP", UJIS_CHARS);
                    javaToMysqlCharsetMap.put("EUC_JP", "ujis");
                }

                for (String charset : charsetList) {
                    Properties props = new Properties();

                    props.put("useUnicode", "true");
                    props.put("characterEncoding", charset);
                    Connection conn2 = getConnectionWithProps(props);
                    connectionMap.put(charset.toLowerCase(Locale.ENGLISH), conn2);
                    statementMap.put(charset.toLowerCase(Locale.ENGLISH), conn2.createStatement());

                    props.put("characterSetResult", charset);
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

                        assertEquals("For character set " + charset + "/ " + mysqlCharset, String.valueOf(testData[i]), value);
                    }
                    String query5 = "DROP TABLE t1";
                    stmt2.executeUpdate(query5);
                }
            }
        }
    }

    public void testUtf8OutsideBMPInBlob() throws Exception {
        createTable("utf8Test",
                "(include_blob BLOB, include_tinyblob TINYBLOB, include_longblob LONGBLOB, exclude_tinyblob TINYBLOB, exclude_blob BLOB, exclude_longblob LONGBLOB)");

        // We know this gets truncated in MySQL currently, even though it's valid UTF-8, it's just 4 bytes encoded
        String outsideBmp = new String(new byte[] { (byte) 0xF0, (byte) 0x90, (byte) 0x80, (byte) 0x80 }, "UTF-8");
        byte[] outsideBmpBytes = outsideBmp.getBytes("UTF-8");
        System.out.println(outsideBmpBytes.length);

        Connection utf8Conn = getConnectionWithProps("useBlobToStoreUTF8OutsideBMP=true, characterEncoding=UTF-8");

        String insertStatement = "INSERT INTO utf8Test VALUES (?, ?, ?, ?, ?, ?)";

        this.pstmt = utf8Conn.prepareStatement(insertStatement);

        this.pstmt.setString(1, outsideBmp);
        this.pstmt.setString(2, outsideBmp);
        this.pstmt.setString(3, outsideBmp);
        this.pstmt.setString(4, outsideBmp);
        this.pstmt.setString(5, outsideBmp);
        this.pstmt.setString(6, outsideBmp);
        this.pstmt.executeUpdate();

        String query = "SELECT include_blob, include_tinyblob, include_longblob, exclude_tinyblob, exclude_blob, exclude_longblob FROM utf8Test";
        this.rs = utf8Conn.createStatement().executeQuery(query);
        this.rs.next();

        assertEquals(this.rs.getObject(1).toString(), outsideBmp);
        assertEquals(this.rs.getObject(2).toString(), outsideBmp);
        assertEquals(this.rs.getObject(3).toString(), outsideBmp);
        assertEquals(this.rs.getObject(4).toString(), outsideBmp);
        assertEquals(this.rs.getObject(5).toString(), outsideBmp);
        assertEquals(this.rs.getObject(6).toString(), outsideBmp);

        assertEquals("java.lang.String", this.rs.getObject(1).getClass().getName());
        assertEquals("java.lang.String", this.rs.getMetaData().getColumnClassName(1));
        assertEquals(Types.VARCHAR, this.rs.getMetaData().getColumnType(1));

        assertEquals("java.lang.String", this.rs.getObject(2).getClass().getName());
        assertEquals("java.lang.String", this.rs.getMetaData().getColumnClassName(2));
        assertEquals(Types.VARCHAR, this.rs.getMetaData().getColumnType(2));

        assertEquals("java.lang.String", this.rs.getObject(3).getClass().getName());
        assertEquals("java.lang.String", this.rs.getMetaData().getColumnClassName(3));
        assertEquals(Types.LONGVARCHAR, this.rs.getMetaData().getColumnType(3));

        assertEquals("java.lang.String", this.rs.getObject(4).getClass().getName());
        assertEquals("java.lang.String", this.rs.getMetaData().getColumnClassName(4));
        assertEquals(Types.VARCHAR, this.rs.getMetaData().getColumnType(4));

        assertEquals("java.lang.String", this.rs.getObject(5).getClass().getName());
        assertEquals("java.lang.String", this.rs.getMetaData().getColumnClassName(5));
        assertEquals(Types.VARCHAR, this.rs.getMetaData().getColumnType(5));

        assertEquals("java.lang.String", this.rs.getObject(6).getClass().getName());
        assertEquals("java.lang.String", this.rs.getMetaData().getColumnClassName(6));
        assertEquals(Types.LONGVARCHAR, this.rs.getMetaData().getColumnType(6));

        utf8Conn = getConnectionWithProps("useBlobToStoreUTF8OutsideBMP=true, characterEncoding=UTF-8,utf8OutsideBmpIncludedColumnNamePattern=.*include.*,utf8OutsideBmpExcludedColumnNamePattern=.*blob");

        this.rs = utf8Conn.createStatement().executeQuery(query);
        this.rs.next();

        // Should walk/talk like a string, encoded in utf-8 on the server (4-byte)
        assertEquals(this.rs.getObject(1).toString(), outsideBmp);
        assertEquals(this.rs.getObject(2).toString(), outsideBmp);
        assertEquals(this.rs.getObject(3).toString(), outsideBmp);

        assertEquals("java.lang.String", this.rs.getObject(1).getClass().getName());
        assertEquals("java.lang.String", this.rs.getMetaData().getColumnClassName(1));
        assertEquals(Types.VARCHAR, this.rs.getMetaData().getColumnType(1));

        assertEquals("java.lang.String", this.rs.getObject(2).getClass().getName());
        assertEquals("java.lang.String", this.rs.getMetaData().getColumnClassName(2));
        assertEquals(Types.VARCHAR, this.rs.getMetaData().getColumnType(2));

        assertEquals("java.lang.String", this.rs.getObject(3).getClass().getName());
        assertEquals("java.lang.String", this.rs.getMetaData().getColumnClassName(3));
        assertEquals(Types.LONGVARCHAR, this.rs.getMetaData().getColumnType(3));

        // These should be left as a blob, since it matches the exclusion regex
        assertTrue(bytesAreSame(this.rs.getBytes(4), outsideBmpBytes));
        assertEquals("[B", this.rs.getObject(4).getClass().getName());
        assertEquals("[B", this.rs.getMetaData().getColumnClassName(4));
        assertEquals(Types.VARBINARY, this.rs.getMetaData().getColumnType(4));

        // Should behave types-wise just like BLOB, including LONGVARBINARY type mapping
        assertTrue(bytesAreSame(this.rs.getBytes(5), outsideBmpBytes));
        assertEquals("[B", this.rs.getObject(5).getClass().getName());
        assertEquals("[B", this.rs.getMetaData().getColumnClassName(5));
        assertEquals(Types.LONGVARBINARY, this.rs.getMetaData().getColumnType(5));

        assertTrue(bytesAreSame(this.rs.getBytes(6), outsideBmpBytes));
        assertEquals("[B", this.rs.getObject(6).getClass().getName());
        assertEquals("[B", this.rs.getMetaData().getColumnClassName(6));
        assertEquals(Types.LONGVARBINARY, this.rs.getMetaData().getColumnType(6));

        //
        // Check error handling
        //

        utf8Conn = getConnectionWithProps("useBlobToStoreUTF8OutsideBMP=true, characterEncoding=UTF-8,utf8OutsideBmpIncludedColumnNamePattern={{,utf8OutsideBmpExcludedColumnNamePattern={{");

        try {
            utf8Conn.createStatement().executeQuery(query);
            fail("Expected an exception");
        } catch (SQLException sqlEx) {
            assertNotNull(sqlEx.getCause());
            assertEquals("java.util.regex.PatternSyntaxException", sqlEx.getCause().getClass().getName());
        }

        utf8Conn = getConnectionWithProps("useBlobToStoreUTF8OutsideBMP=true, characterEncoding=UTF-8,utf8OutsideBmpIncludedColumnNamePattern={{,utf8OutsideBmpExcludedColumnNamePattern=.*");

        try {
            utf8Conn.createStatement().executeQuery(query);
            fail("Expected an exception");
        } catch (SQLException sqlEx) {
            assertNotNull(sqlEx.getCause());
            assertEquals("java.util.regex.PatternSyntaxException", sqlEx.getCause().getClass().getName());
        }

        utf8Conn = getConnectionWithProps("useBlobToStoreUTF8OutsideBMP=true, characterEncoding=UTF-8,utf8OutsideBmpIncludedColumnNamePattern={{,utf8OutsideBmpExcludedColumnNamePattern={{,paranoid=true");

        try {
            utf8Conn.createStatement().executeQuery(query);
            fail("Expected an exception");
        } catch (SQLException sqlEx) {
            assertNull(sqlEx.getCause());
        }

        utf8Conn = getConnectionWithProps("useBlobToStoreUTF8OutsideBMP=true, characterEncoding=UTF-8,utf8OutsideBmpIncludedColumnNamePattern={{,utf8OutsideBmpExcludedColumnNamePattern=.*,paranoid=true");

        try {
            utf8Conn.createStatement().executeQuery(query);
            fail("Expected an exception");
        } catch (SQLException sqlEx) {
            assertNull(sqlEx.getCause());
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

    /**
     * Prints static mappings for analysis.
     * 
     * @throws Exception
     */
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

            int index = CharsetMapping.getCollationIndexForJavaEncoding(canonicalName, this.conn);
            String csname = CharsetMapping.getMysqlCharsetNameForCollationIndex(index);

            System.out.println((canonicalName + "                              ").substring(0, 26) + " (" + cs.canEncode() + ") --> "
                    + CharsetMapping.getJavaEncodingForCollationIndex(index) + "  :  " + index + "  :  "
                    + CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME[index] + "  :  " + CharsetMapping.getMysqlCharsetNameForCollationIndex(index) + "  :  "
                    + CharsetMapping.CHARSET_NAME_TO_CHARSET.get(csname) + "  :  " + CharsetMapping.getJavaEncodingForMysqlCharset(csname) + "  :  "
                    + CharsetMapping.getMysqlCharsetForJavaEncoding(canonicalName, (com.mysql.jdbc.Connection) this.conn) + "  :  "
                    + CharsetMapping.getCollationIndexForJavaEncoding(canonicalName, this.conn) + "  :  " + CharsetMapping.isMultibyteCharset(canonicalName));

            Set<String> s = cs.aliases();
            Iterator<String> j = s.iterator();
            while (j.hasNext()) {
                String alias = j.next();
                index = CharsetMapping.getCollationIndexForJavaEncoding(alias, this.conn);
                csname = CharsetMapping.getMysqlCharsetNameForCollationIndex(index);
                System.out.println("   " + (alias + "                              ").substring(0, 30) + " --> "
                        + CharsetMapping.getJavaEncodingForCollationIndex(index) + "  :  " + index + "  :  "
                        + CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME[index] + "  :  " + CharsetMapping.getMysqlCharsetNameForCollationIndex(index)
                        + "  :  " + CharsetMapping.CHARSET_NAME_TO_CHARSET.get(csname) + "  :  " + CharsetMapping.getJavaEncodingForMysqlCharset(csname)
                        + "  :  " + CharsetMapping.getMysqlCharsetForJavaEncoding(alias, (com.mysql.jdbc.Connection) this.conn) + "  :  "
                        + CharsetMapping.getCollationIndexForJavaEncoding(alias, this.conn) + "  :  " + CharsetMapping.isMultibyteCharset(alias));
            }
            System.out.println("===================================");
        }
        for (int i = 1; i < CharsetMapping.MAP_SIZE; i++) {
            String csname = CharsetMapping.getMysqlCharsetNameForCollationIndex(i);
            String enc = CharsetMapping.getJavaEncodingForCollationIndex(i);
            System.out.println((i + "   ").substring(0, 4) + " by index--> "
                    + (CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME[i] + "                    ").substring(0, 20) + "  :  "
                    + (csname + "          ").substring(0, 10) + "  :  " + (enc + "                    ").substring(0, 20)

                    + " by charset--> " + (CharsetMapping.getJavaEncodingForMysqlCharset(csname) + "                  ").substring(0, 20)

                    + " by encoding--> " + (CharsetMapping.getCollationIndexForJavaEncoding(enc, this.conn) + "   ").substring(0, 4) + "  :  "
                    + (CharsetMapping.getMysqlCharsetForJavaEncoding(enc, (com.mysql.jdbc.Connection) this.conn) + "               ").substring(0, 15));
        }
    }

    /**
     * Test for the gb18030 character set
     * 
     * @throws Exception
     */
    public void testGB18030() throws Exception {
        // check that server supports this character set
        this.rs = this.stmt.executeQuery("show collation like 'gb18030_chinese_ci'");
        if (!this.rs.next()) {
            return;
        }

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
        HashMap<String, String> expected = new HashMap<String, String>();

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
            assertTrue("HEX value " + hex + " for char " + rset.getString(1) + " is unexpected", expected.containsKey(hex));
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
}
