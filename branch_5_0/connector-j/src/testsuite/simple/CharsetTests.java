/*
    Copyright (C) 2005 MySQL AB

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

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import testsuite.BaseTestCase;

public class CharsetTests extends BaseTestCase {

	public CharsetTests(String name) {
		super(name);
		// TODO Auto-generated constructor stub
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

			Properties props = new Properties();
			props.put("useUnicode", "true");
			props.put("characterEncoding", "WINDOWS-31J");
			getConnectionWithProps(props).close();
		}
	}

	public void testNECExtendedCharsByEUCJPSolaris() throws Exception {
		if (!isRunningOnJdk131()) {
			if (versionMeetsMinimum(5, 0, 5)) {
				char necExtendedChar = 0x3231; // 0x878A of WINDOWS-31J, NEC
				// special(row13).
				String necExtendedCharString = String.valueOf(necExtendedChar);
	
				Properties props = new Properties();
				
				props.put("useUnicode", "true");
				props.put("characterEncoding", "EUC_JP_Solaris");
	
				Connection conn2 = getConnectionWithProps(props);
				Statement stmt2 = conn2.createStatement();
	
				stmt2.executeUpdate("DROP TABLE IF EXISTS t_eucjpms");
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
	
				stmt2.executeUpdate("DROP TABLE t_eucjpms");
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
			if (versionMeetsMinimum(4, 1, 12)) {
				Map testDataMap = new HashMap();
	
				List charsetList = new ArrayList();
	
				Map connectionMap = new HashMap();
	
				Map connectionWithResultMap = new HashMap();
	
				Map statementMap = new HashMap();
	
				Map statementWithResultMap = new HashMap();
	
				Map javaToMysqlCharsetMap = new HashMap();
	
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
	
				Iterator charsetIterator = charsetList.iterator();
	
				while (charsetIterator.hasNext()) {
					String charset = (String) charsetIterator.next();
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
	
				charsetIterator = charsetList.iterator();
				while (charsetIterator.hasNext()) {
					String charset = (String) charsetIterator.next();
	
					String mysqlCharset = (String) javaToMysqlCharsetMap
							.get(charset);
					Statement stmt2 = (Statement) statementMap.get(charset
							.toLowerCase(Locale.ENGLISH));
					String query1 = "DROP TABLE IF EXISTS t1";
					String query2 = "CREATE TABLE t1 (c1 int, c2 char(1)) "
							+ "DEFAULT CHARACTER SET = " + mysqlCharset;
					stmt2.executeUpdate(query1);
					stmt2.executeUpdate(query2);
					char[] testData = (char[]) testDataMap.get(charset);
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
}
