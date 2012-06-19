/*
    Copyright (c) 2005, 2012, Oracle and/or its affiliates. All rights reserved.

    This program is free software; you can redistribute it and/or modify
    it under the terms of version 2 of the GNU General Public License as 
    published by the Free Software Foundation.

    There are special exceptions to the terms and conditions of the GPL 
    as it is applied to this software. View the full text of the 
    exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
    software distribution.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package testsuite.simple;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import testsuite.BaseTestCase;

public class CharsetTests extends BaseTestCase {

	public CharsetTests(String name) {
		super(name);
	}

	public static void main(String[] args) {
		junit.textui.TestRunner.run(CharsetTests.class);
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
	
				createTable("t_eucjpms", "(c1 char(1))"
						+ " default character set = eucjpms");
				stmt2.executeUpdate("INSERT INTO t_eucjpms VALUES ('"
						+ necExtendedCharString + "')");
				this.rs = stmt2.executeQuery("SELECT c1 FROM t_eucjpms");
				this.rs.next();
				assertEquals(necExtendedCharString, this.rs.getString("c1"));
	
				this.rs.close();
				stmt2.close();
				conn2.close();
	
				props.put("characterSetResults", "EUC_JP_Solaris");
				conn2 = getConnectionWithProps(props);
				stmt2 = conn.createStatement();
	
				this.rs = stmt2.executeQuery("SELECT c1 FROM t_eucjpms");
				this.rs.next();
				assertEquals(necExtendedCharString, rs.getString("c1"));
	
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
	public static final char[] SJIS_CHARS = new char[] { 0xFF71, // halfwidth
			// katakana
			// letter A,
			// 0xB100 of
			// SJIS, one
			// of
			// JISX0201.
			0x65E5, // CJK unified ideograph, 0x93FA of SJIS, one of JISX0208.
			0x8868, // CJK unified ideograph, 0x955C of SJIS, one of '5c'
			// character.
			0x2016 // 0x8161 of SJIS/WINDOWS-31J, converted to differently
	// to/from ucs2
	};

	/**
	 * Test data of cp932. WINDOWS-31J consists of ASCII, JIS-Roman, JISX0201,
	 * JISX0208, NEC special characters(row13), NEC selected IBM special
	 * characters, and IBM special characters.
	 */
	private static final char[] CP932_CHARS = new char[] { 0xFF71, // halfwidth
			// katakana
			// letter A,
			// 0xB100 of
			// WINDOWS-31J,
			// one of
			// JISX0201.
			0x65E5, // CJK unified ideograph, 0x93FA of WINDOWS-31J, one of
			// JISX0208.
			0x3231, // parenthesized ideograph stok, 0x878B of WINDOWS-31J, one
			// of NEC special characters(row13).
			0x67BB, // CJK unified ideograph, 0xEDC6 of WINDOWS-31J, one of NEC
			// selected IBM special characters.
			0x6D6F, // CJK unified ideograph, 0xFAFC of WINDOWS-31J, one of IBM
			// special characters.
			0x8868, // one of CJK unified ideograph, 0x955C of WINDOWS-31J, one
			// of '5c' characters.
			0x2225 // 0x8161 of SJIS/WINDOWS-31J, converted to differently
	// to/from ucs2
	};

	/**
	 * Test data of ujis. ujis consists of ASCII, JIS-Roman, JISX0201, JISX0208,
	 * JISX0212.
	 */
	public static final char[] UJIS_CHARS = new char[] { 0xFF71, // halfwidth
			// katakana
			// letter A,
			// 0x8EB1 of
			// ujis, one
			// of
			// JISX0201.
			0x65E5, // CJK unified ideograph, 0xC6FC of ujis, one of JISX0208.
			0x7B5D, // CJK unified ideograph, 0xE4B882 of ujis, one of JISX0212
			0x301C // wave dash, 0xA1C1 of ujis, convertion rule is different
	// from ujis
	};

	/**
	 * Test data of eucjpms. ujis consists of ASCII, JIS-Roman, JISX0201,
	 * JISX0208, JISX0212, NEC special characters(row13)
	 */
	public static final char[] EUCJPMS_CHARS = new char[] { 0xFF71, // halfwidth
			// katakana
			// letter A,
			// 0x8EB1 of
			// ujis, one
			// of
			// JISX0201.
			0x65E5, // CJK unified ideograph, 0xC6FC of ujis, one of JISX0208.
			0x7B5D, // CJK unified ideograph, 0xE4B882 of ujis, one of JISX0212
			0x3231, // parenthesized ideograph stok, 0x878A of WINDOWS-31J, one
			// of NEC special characters(row13).
			0xFF5E // wave dash, 0xA1C1 of eucjpms, convertion rule is
	// different from ujis
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
					statementMap.put(charset.toLowerCase(Locale.ENGLISH), conn2
							.createStatement());
	
					props.put("characterSetResult", charset);
					Connection connWithResult = getConnectionWithProps(props);
					connectionWithResultMap.put(charset, connWithResult);
					statementWithResultMap.put(charset, connWithResult
							.createStatement());
				}
	
				for (String charset : charsetList) {
					String mysqlCharset = javaToMysqlCharsetMap.get(charset);
					Statement stmt2 = statementMap.get(charset.toLowerCase(Locale.ENGLISH));
					String query1 = "DROP TABLE IF EXISTS t1";
					String query2 = "CREATE TABLE t1 (c1 int, c2 char(1)) "
							+ "DEFAULT CHARACTER SET = " + mysqlCharset;
					stmt2.executeUpdate(query1);
					stmt2.executeUpdate(query2);
					char[] testData = testDataMap.get(charset);
					for (int i = 0; i < testData.length; i++) {
						String query3 = "INSERT INTO t1 values(" + i + ", '"
								+ testData[i] + "')";
						stmt2.executeUpdate(query3);
						String query4 = "SELECT c2 FROM t1 WHERE c1 = " + i;
						this.rs = stmt2.executeQuery(query4);
						this.rs.next();
						String value = rs.getString(1);
	
						assertEquals("For character set " + charset + "/ "
								+ mysqlCharset, String.valueOf(testData[i]), value);
					}
					String query5 = "DROP TABLE t1";
					stmt2.executeUpdate(query5);
				}
			}
		}
	}
	
	public void testUtf8OutsideBMPInBlob() throws Exception {
		createTable("utf8Test", "(include_blob BLOB, include_tinyblob TINYBLOB, include_longblob LONGBLOB, exclude_tinyblob TINYBLOB, exclude_blob BLOB, exclude_longblob LONGBLOB)");
		
		// We know this gets truncated in MySQL currently, even though it's valid UTF-8, it's just 4 bytes encoded
		String outsideBmp = new String(new byte[] {(byte) 0xF0, (byte) 0x90, (byte) 0x80, (byte) 0x80}, "UTF-8");
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
}
