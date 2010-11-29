/*
 Copyright (c) 2002, 2010, Oracle and/or its affiliates. All rights reserved.
 

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
package testsuite.regression;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import testsuite.BaseTestCase;

import com.mysql.jdbc.CharsetMapping;
import com.mysql.jdbc.StringUtils;

/**
 * Tests for regressions of bugs in String handling in the driver.
 * 
 * @author Mark Matthews
 * @version StringRegressionTest.java,v 1.1 2002/11/04 14:58:25 mark_matthews
 *          Exp
 */
public class StringRegressionTest extends BaseTestCase {
	/**
	 * Creates a new StringTest object.
	 * 
	 * @param name
	 *            DOCUMENT ME!
	 */
	public StringRegressionTest(String name) {
		super(name);
	}

	/**
	 * Runs all test cases in this test suite
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(StringRegressionTest.class);
	}

	/**
	 * Tests character conversion bug.
	 * 
	 * @throws Exception
	 *             if there is an internal error (which is a bug).
	 */
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

		assertTrue("Converted string != test string",
				testString.equals(convertedString));
	}

	/**
	 * Tests fix for BUG#4010 -- GBK encoding getting escaped doubly when
	 * database default character set is GBK. Requires version older than 4.1.0
	 * and server set to default character set of 'gbk' to run.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug4010() throws Exception {
		if (!versionMeetsMinimum(4, 1)) {
			if ("GBK".equalsIgnoreCase(getMysqlVariable("character_set"))) {
				String origString = "\u603d";
				Properties props = new Properties();
				props.put("useUnicode", "true");
				props.put("characterEncoding", "GBK");

				Connection unicodeConn = getConnectionWithProps(props);
				Statement unicodeStmt = unicodeConn.createStatement();
				PreparedStatement unicodePstmt = unicodeConn
						.prepareStatement("INSERT INTO testBug4010 VALUES (?)");

				try {
					unicodeStmt
							.executeUpdate("DROP TABLE IF EXISTS testBug4010");
					unicodeStmt
							.executeUpdate("CREATE TABLE testBug4010 (field1 varchar(10))");

					unicodePstmt.setString(1, origString);
					unicodePstmt.executeUpdate();

					this.rs = unicodeStmt
							.executeQuery("SELECT * FROM testBug4010");
					assertTrue(this.rs.next());

					String stringFromDb = this.rs.getString(1);
					assertTrue("Retrieved string != sent string",
							origString.equals(stringFromDb));
				} finally {
					unicodeStmt
							.executeUpdate("DROP TABLE IF EXISTS testBug4010");
					unicodeStmt.close();
					unicodePstmt.close();
					unicodeConn.close();
				}
			} else {
				System.err
						.println("WARN: Test not valid for servers not running GBK encoding");
			}
		} else {
			System.err
					.println("WARN: Test not valid for MySQL version > 4.1.0, skipping");
		}
	}

	/**
	 * Tests for regression of encoding forced by user, reported by Jive
	 * Software
	 * 
	 * @throws Exception
	 *             when encoding is not supported (which is a bug)
	 */
	public void testEncodingRegression() throws Exception {
		Properties props = new Properties();
		props.put("characterEncoding", "UTF-8");
		props.put("useUnicode", "true");
		DriverManager.getConnection(dbUrl, props).close();
	}

	/**
	 * Tests fix for BUG#879
	 * 
	 * @throws Exception
	 *             if the bug resurfaces.
	 */
	public void testEscapeSJISDoubleEscapeBug() throws Exception {
		if (!isRunningOnJdk131()) {
			String testString = "'It\\'s a boy!'";

			byte[] testStringAsBytes = testString.getBytes("SJIS");

			byte[] escapedStringBytes = StringUtils
					.escapeEasternUnicodeByteStream(testStringAsBytes,
							testString, 0, testString.length());

			String escapedString = new String(escapedStringBytes, "SJIS");

			assertTrue(testString.equals(escapedString));

			byte[] origByteStream = new byte[] { (byte) 0x95, (byte) 0x5c,
					(byte) 0x8e, (byte) 0x96, (byte) 0x5c, (byte) 0x62,
					(byte) 0x5c };

			String origString = "\u955c\u8e96\u5c62\\";

			byte[] newByteStream = StringUtils.escapeEasternUnicodeByteStream(
					origByteStream, origString, 0, origString.length());

			assertTrue((newByteStream.length == (origByteStream.length + 2))
					&& (newByteStream[1] == 0x5c) && (newByteStream[2] == 0x5c)
					&& (newByteStream[5] == 0x5c) && (newByteStream[6] == 0x5c));

			origByteStream = new byte[] { (byte) 0x8d, (byte) 0xb2,
					(byte) 0x93, (byte) 0x91, (byte) 0x81, (byte) 0x40,
					(byte) 0x8c, (byte) 0x5c };

			testString = new String(origByteStream, "SJIS");

			Properties connProps = new Properties();
			connProps.put("useUnicode", "true");
			connProps.put("characterEncoding", "sjis");

			Connection sjisConn = getConnectionWithProps(connProps);
			Statement sjisStmt = sjisConn.createStatement();

			try {
				sjisStmt.executeUpdate("DROP TABLE IF EXISTS doubleEscapeSJISTest");
				sjisStmt.executeUpdate("CREATE TABLE doubleEscapeSJISTest (field1 BLOB)");

				PreparedStatement sjisPStmt = sjisConn
						.prepareStatement("INSERT INTO doubleEscapeSJISTest VALUES (?)");
				sjisPStmt.setString(1, testString);
				sjisPStmt.executeUpdate();

				this.rs = sjisStmt
						.executeQuery("SELECT * FROM doubleEscapeSJISTest");

				this.rs.next();

				String retrString = this.rs.getString(1);

				System.out.println(retrString.equals(testString));
			} finally {
				sjisStmt.executeUpdate("DROP TABLE IF EXISTS doubleEscapeSJISTest");
			}
		}
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @throws Exception
	 *             DOCUMENT ME!
	 */
	public void testGreekUtf8411() throws Exception {
		if (versionMeetsMinimum(4, 1)) {
			Properties newProps = new Properties();
			newProps.put("useUnicode", "true");
			newProps.put("characterEncoding", "UTF-8");

			Connection utf8Conn = this.getConnectionWithProps(newProps);

			Statement utfStmt = utf8Conn.createStatement();

			createTable(
					"greekunicode",
					"(ID INTEGER NOT NULL "
							+ " AUTO_INCREMENT,UpperCase VARCHAR (30),LowerCase VARCHAR (30),Accented "
							+ " VARCHAR (30),Special VARCHAR (30),PRIMARY KEY(ID)) "
							+ "DEFAULT CHARACTER SET utf8", "InnoDB");

			String upper = "\u0394\u930F\u039A\u0399\u039C\u0397";
			String lower = "\u03B4\u03BF\u03BA\u03B9\u03BC\u03B7";
			String accented = "\u03B4\u03CC\u03BA\u03AF\u03BC\u03AE";
			String special = "\u037E\u03C2\u03B0";

			utfStmt.executeUpdate("INSERT INTO greekunicode VALUES " + "('1','"
					+ upper + "','" + lower + "','" + accented + "','"
					+ special + "')");

			this.rs = utfStmt
					.executeQuery("SELECT UpperCase, LowerCase, Accented, Special from greekunicode");

			this.rs.next();

			assertTrue(upper.equals(this.rs.getString(1)));
			assertTrue(lower.equals(this.rs.getString(2)));
			assertTrue(accented.equals(this.rs.getString(3)));
			assertTrue(special.equals(this.rs.getString(4)));
		}
	}

	/**
	 * Tests that 'latin1' character conversion works correctly.
	 * 
	 * @throws Exception
	 *             if any errors occur
	 */
	public void testLatin1Encoding() throws Exception {
		char[] latin1Charset = { 0x0000, 0x0001, 0x0002, 0x0003, 0x0004,
				0x0005, 0x0006, 0x0007, 0x0008, 0x0009, 0x000A, 0x000B, 0x000C,
				0x000D, 0x000E, 0x000F, 0x0010, 0x0011, 0x0012, 0x0013, 0x0014,
				0x0015, 0x0016, 0x0017, 0x0018, 0x0019, 0x001A, 0x001B, 0x001C,
				0x001D, 0x001E, 0x001F, 0x0020, 0x0021, 0x0022, 0x0023, 0x0024,
				0x0025, 0x0026, 0x0027, 0x0028, 0x0029, 0x002A, 0x002B, 0x002C,
				0x002D, 0x002E, 0x002F, 0x0030, 0x0031, 0x0032, 0x0033, 0x0034,
				0x0035, 0x0036, 0x0037, 0x0038, 0x0039, 0x003A, 0x003B, 0x003C,
				0x003D, 0x003E, 0x003F, 0x0040, 0x0041, 0x0042, 0x0043, 0x0044,
				0x0045, 0x0046, 0x0047, 0x0048, 0x0049, 0x004A, 0x004B, 0x004C,
				0x004D, 0x004E, 0x004F, 0x0050, 0x0051, 0x0052, 0x0053, 0x0054,
				0x0055, 0x0056, 0x0057, 0x0058, 0x0059, 0x005A, 0x005B, 0x005C,
				0x005D, 0x005E, 0x005F, 0x0060, 0x0061, 0x0062, 0x0063, 0x0064,
				0x0065, 0x0066, 0x0067, 0x0068, 0x0069, 0x006A, 0x006B, 0x006C,
				0x006D, 0x006E, 0x006F, 0x0070, 0x0071, 0x0072, 0x0073, 0x0074,
				0x0075, 0x0076, 0x0077, 0x0078, 0x0079, 0x007A, 0x007B, 0x007C,
				0x007D, 0x007E, 0x007F, 0x0080, 0x0081, 0x0082, 0x0083, 0x0084,
				0x0085, 0x0086, 0x0087, 0x0088, 0x0089, 0x008A, 0x008B, 0x008C,
				0x008D, 0x008E, 0x008F, 0x0090, 0x0091, 0x0092, 0x0093, 0x0094,
				0x0095, 0x0096, 0x0097, 0x0098, 0x0099, 0x009A, 0x009B, 0x009C,
				0x009D, 0x009E, 0x009F, 0x00A0, 0x00A1, 0x00A2, 0x00A3, 0x00A4,
				0x00A5, 0x00A6, 0x00A7, 0x00A8, 0x00A9, 0x00AA, 0x00AB, 0x00AC,
				0x00AD, 0x00AE, 0x00AF, 0x00B0, 0x00B1, 0x00B2, 0x00B3, 0x00B4,
				0x00B5, 0x00B6, 0x00B7, 0x00B8, 0x00B9, 0x00BA, 0x00BB, 0x00BC,
				0x00BD, 0x00BE, 0x00BF, 0x00C0, 0x00C1, 0x00C2, 0x00C3, 0x00C4,
				0x00C5, 0x00C6, 0x00C7, 0x00C8, 0x00C9, 0x00CA, 0x00CB, 0x00CC,
				0x00CD, 0x00CE, 0x00CF, 0x00D0, 0x00D1, 0x00D2, 0x00D3, 0x00D4,
				0x00D5, 0x00D6, 0x00D7, 0x00D8, 0x00D9, 0x00DA, 0x00DB, 0x00DC,
				0x00DD, 0x00DE, 0x00DF, 0x00E0, 0x00E1, 0x00E2, 0x00E3, 0x00E4,
				0x00E5, 0x00E6, 0x00E7, 0x00E8, 0x00E9, 0x00EA, 0x00EB, 0x00EC,
				0x00ED, 0x00EE, 0x00EF, 0x00F0, 0x00F1, 0x00F2, 0x00F3, 0x00F4,
				0x00F5, 0x00F6, 0x00F7, 0x00F8, 0x00F9, 0x00FA, 0x00FB, 0x00FC,
				0x00FD, 0x00FE, 0x00FF };

		String latin1String = new String(latin1Charset);
		PreparedStatement pStmt = null;

		try {
			createTable("latin1RegressTest", "(stringField TEXT)");

			pStmt = this.conn
					.prepareStatement("INSERT INTO latin1RegressTest VALUES (?)");
			pStmt.setString(1, latin1String);
			pStmt.executeUpdate();

			((com.mysql.jdbc.Connection) this.conn).setTraceProtocol(true);

			this.rs = this.stmt.executeQuery("SELECT * FROM latin1RegressTest");
			((com.mysql.jdbc.Connection) this.conn).setTraceProtocol(false);

			this.rs.next();

			String retrievedString = this.rs.getString(1);

			System.out.println(latin1String);
			System.out.println(retrievedString);

			if (!retrievedString.equals(latin1String)) {
				int stringLength = Math.min(retrievedString.length(),
						latin1String.length());

				for (int i = 0; i < stringLength; i++) {
					char rChar = retrievedString.charAt(i);
					char origChar = latin1String.charAt(i);

					if ((rChar != '?') && (rChar != origChar)) {
						fail("characters differ at position "
								+ i
								+ "'"
								+ rChar
								+ "' retrieved from database, original char was '"
								+ origChar + "'");
					}
				}
			}
		} finally {
			if (this.rs != null) {
				try {
					this.rs.close();
				} catch (Exception ex) {
					// ignore
				}
			}

			if (pStmt != null) {
				try {
					pStmt.close();
				} catch (Exception ex) {
					// ignore
				}
			}
		}
	}

	/**
	 * Tests newline being treated correctly.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	public void testNewlines() throws Exception {
		String newlineStr = "Foo\nBar\n\rBaz";

		createTable("newlineRegressTest", "(field1 MEDIUMTEXT)");

		this.stmt.executeUpdate("INSERT INTO newlineRegressTest VALUES ('"
				+ newlineStr + "')");
		this.pstmt = this.conn
				.prepareStatement("INSERT INTO newlineRegressTest VALUES (?)");
		this.pstmt.setString(1, newlineStr);
		this.pstmt.executeUpdate();

		this.rs = this.stmt.executeQuery("SELECT * FROM newlineRegressTest");

		while (this.rs.next()) {
			assertTrue(this.rs.getString(1).equals(newlineStr));
		}

	}

	/**
	 * Tests that single-byte character conversion works correctly.
	 * 
	 * @throws Exception
	 *             if any errors occur
	 */
	// TODO: Use Unicode Literal escapes for this, for now, this test is
	// broken :(
	/*
	 * public void testSingleByteConversion() throws Exception {
	 * testConversionForString("latin1", "��� ����");
	 * testConversionForString("latin1", "Kaarle ��nis Ilmari");
	 * testConversionForString("latin1", "������������������"); }
	 */

	/**
	 * Tests that the 0x5c escaping works (we didn't use to have this).
	 * 
	 * @throws Exception
	 *             if an error occurs.
	 */
	public void testSjis5c() throws Exception {
		byte[] origByteStream = new byte[] { (byte) 0x95, (byte) 0x5c,
				(byte) 0x8e, (byte) 0x96 };

		//
		// Print the hex values of the string
		//
		StringBuffer bytesOut = new StringBuffer();

		for (int i = 0; i < origByteStream.length; i++) {
			bytesOut.append(Integer.toHexString(origByteStream[i] & 255));
			bytesOut.append(" ");
		}

		System.out.println(bytesOut.toString());

		String origString = new String(origByteStream, "SJIS");
		byte[] newByteStream = StringUtils.getBytes(origString, "SJIS",
				"ISO8859_1              ", false, null, null);

		//
		// Print the hex values of the string (should have an extra 0x5c)
		//
		bytesOut = new StringBuffer();

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
			props.put("useUnicode", "true");
			props.put("characterEncoding", "SJIS");
			sjisConn = getConnectionWithProps(props);

			sjisStmt = sjisConn.createStatement();

			this.rs = sjisStmt
					.executeQuery("SHOW VARIABLES LIKE 'character_set%'");

			while (this.rs.next()) {
				System.out.println(this.rs.getString(1) + " = "
						+ this.rs.getString(2));
			}

			sjisStmt.executeUpdate("DROP TABLE IF EXISTS sjisTest");

			if (versionMeetsMinimum(4, 1)) {
				sjisStmt.executeUpdate("CREATE TABLE sjisTest (field1 char(50)) DEFAULT CHARACTER SET SJIS");
			} else {
				sjisStmt.executeUpdate("CREATE TABLE sjisTest (field1 char(50))");
			}

			this.pstmt = sjisConn
					.prepareStatement("INSERT INTO sjisTest VALUES (?)");
			this.pstmt.setString(1, origString);
			this.pstmt.executeUpdate();

			this.rs = sjisStmt.executeQuery("SELECT * FROM sjisTest");

			while (this.rs.next()) {
				byte[] testValueAsBytes = this.rs.getBytes(1);

				bytesOut = new StringBuffer();

				for (int i = 0; i < testValueAsBytes.length; i++) {
					bytesOut.append(Integer
							.toHexString(testValueAsBytes[i] & 255));
					bytesOut.append(" ");
				}

				System.out.println("Value retrieved from database: "
						+ bytesOut.toString());

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
	 *             if any errors occur
	 */
	public void testUtf8Encoding() throws Exception {
		Properties props = new Properties();
		props.put("characterEncoding", "UTF8");
		props.put("useUnicode", "true");
		props.put("jdbcCompliantTruncation", "false");

		Connection utfConn = DriverManager.getConnection(dbUrl, props);
		testConversionForString("UTF8", utfConn, "\u043c\u0438\u0445\u0438");
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @throws Exception
	 *             ...
	 */
	public void testUtf8Encoding2() throws Exception {
		String field1 = "K��sel";
		String field2 = "B�b";
		byte[] field1AsBytes = field1.getBytes("utf-8");
		byte[] field2AsBytes = field2.getBytes("utf-8");

		Properties props = new Properties();
		props.put("characterEncoding", "UTF8");
		props.put("useUnicode", "true");

		Connection utfConn = DriverManager.getConnection(dbUrl, props);
		Statement utfStmt = utfConn.createStatement();

		try {
			utfStmt.executeUpdate("DROP TABLE IF EXISTS testUtf8");
			utfStmt.executeUpdate("CREATE TABLE testUtf8 (field1 varchar(32), field2 varchar(32)) CHARACTER SET UTF8");
			utfStmt.executeUpdate("INSERT INTO testUtf8 VALUES ('" + field1
					+ "','" + field2 + "')");

			PreparedStatement pStmt = utfConn
					.prepareStatement("INSERT INTO testUtf8 VALUES (?, ?)");
			pStmt.setString(1, field1);
			pStmt.setString(2, field2);
			pStmt.executeUpdate();

			ResultSet rs = utfStmt.executeQuery("SELECT * FROM testUtf8");
			assertTrue(rs.next());

			// Compare results stored using direct statement
			// Compare to original string
			assertTrue(field1.equals(rs.getString(1)));
			assertTrue(field2.equals(rs.getString(2)));

			// Compare byte-for-byte, ignoring encoding
			assertTrue(bytesAreSame(field1AsBytes, rs.getBytes(1)));
			assertTrue(bytesAreSame(field2AsBytes, rs.getBytes(2)));

			assertTrue(rs.next());

			// Compare to original string
			assertTrue(field1.equals(rs.getString(1)));
			assertTrue(field2.equals(rs.getString(2)));

			// Compare byte-for-byte, ignoring encoding
			assertTrue(bytesAreSame(field1AsBytes, rs.getBytes(1)));
			assertTrue(bytesAreSame(field2AsBytes, rs.getBytes(2)));
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

	private void testConversionForString(String charsetName,
			Connection convConn, String charsToTest) throws Exception {
		PreparedStatement pStmt = null;

		this.stmt = convConn.createStatement();
		createTable("charConvTest", "(field1 varchar(255))");
		this.stmt.executeUpdate("INSERT INTO charConvTest VALUES ('"
				+ charsToTest + "')");

		if (!versionMeetsMinimum(4, 1)) {
			createTable("CREATE TABLE charConvTest_" + charsetName,
					"(field1 CHAR(50))");
		} else {
			createTable("charConvTest_" + charsetName,
					"(field1 CHAR(50) CHARACTER SET " + charsetName + ")");
		}

		this.stmt.executeUpdate("INSERT INTO charConvTest_" + charsetName
				+ " VALUES ('" + charsToTest + "')");
		pStmt = convConn.prepareStatement("INSERT INTO charConvTest_"
				+ charsetName + " VALUES (?)");
		pStmt.setString(1, charsToTest);
		pStmt.executeUpdate();
		this.rs = this.stmt.executeQuery("SELECT * FROM charConvTest_"
				+ charsetName);

		assertTrue(this.rs.next());

		String testValue = this.rs.getString(1);
		System.out.println(testValue);
		assertTrue(testValue.equals(charsToTest));

	}

	private void testConversionForString(String charsetName, String charsToTest)
			throws Exception {
		testConversionForString(charsetName, this.conn, charsToTest);
	}

	/**
	 * Tests fix for BUG#7601, '+' duplicated in fixDecimalExponent().
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testBug7601() throws Exception {
		assertTrue("1.5E+7".equals(StringUtils.fixDecimalExponent("1.5E+7")));
		assertTrue("1.5E-7".equals(StringUtils.fixDecimalExponent("1.5E-7")));
		assertTrue("1.5E+7".equals(StringUtils.fixDecimalExponent("1.5E7")));
	}

	public void testBug11629() throws Exception {
		if (isRunningOnJdk131()) {
			return;
		}

		PrintStream oldOut = System.out;
		PrintStream oldError = System.err;

		try {
			ByteArrayOutputStream bOut = new ByteArrayOutputStream();
			PrintStream newOut = new PrintStream(bOut);
			System.setOut(newOut);

			ByteArrayOutputStream bErr = new ByteArrayOutputStream();
			PrintStream newErr = new PrintStream(bErr);
			System.setErr(newErr);

			Properties props = new Properties();
			props.setProperty("characterEncoding", "utf8");
			getConnectionWithProps(props).close();
			String withExclaims = new String(bOut.toByteArray());
			assertTrue(withExclaims.indexOf("!") == -1);
			assertTrue(withExclaims.length() == 0); // to catch any other
			// System.out.printlns()

			withExclaims = new String(bErr.toByteArray());
			assertTrue(withExclaims.indexOf("!") == -1);
			assertTrue(withExclaims.length() == 0); // to catch any other
			// System.err.printlns()
		} finally {
			System.setOut(oldOut);
			System.setErr(oldError);
		}
	}

	/**
	 * Tests fix for BUG#11614 - StringUtils.getBytes() doesn't work when using
	 * multibyte character encodings and a length in _characters_ is specified.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug11614() throws Exception {
		if (isRunningOnJdk131()) {
			return; // test not valid on JDK-1.3.1
		}

		if (versionMeetsMinimum(4, 1)) {
			createTable(
					"testBug11614",
					"(`id` INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,"
							+ "`text` TEXT NOT NULL,"
							+ "PRIMARY KEY(`id`)) CHARACTER SET utf8 COLLATE utf8_general_ci");

			Properties props = new Properties();
			props.setProperty("characterEncoding", "utf8");

			Connection utf8Conn = null;

			try {
				utf8Conn = getConnectionWithProps(props);

				utf8Conn.createStatement()
						.executeUpdate(
								"INSERT INTO testBug11614  (`id`,`text`) values (1,'')");
				this.rs = utf8Conn.createStatement().executeQuery(
						"SELECT `text` FROM testBug11614 WHERE id=1");
				assertTrue(this.rs.next());

				Clob c = this.rs.getClob(1);
				c.truncate(0);
				int blockSize = 8192;
				int sizeToTest = blockSize + 100;

				StringBuffer blockBuf = new StringBuffer(sizeToTest);

				for (int i = 0; i < sizeToTest; i++) {
					blockBuf.append('\u00f6');
				}

				String valueToTest = blockBuf.toString();

				c.setString(1, valueToTest);
				this.pstmt = utf8Conn
						.prepareStatement("UPDATE testBug11614 SET `text` = ? WHERE id=1");
				this.pstmt.setClob(1, c);
				this.pstmt.executeUpdate();
				this.pstmt.close();

				String fromDatabase = getSingleIndexedValueWithQuery(utf8Conn,
						1, "SELECT `text` FROM testBug11614").toString();
				assertEquals(valueToTest, fromDatabase);
			} finally {
				if (utf8Conn != null) {
					utf8Conn.close();
				}

			}
		}
	}

	public void testCodePage1252() throws Exception {
		if (versionMeetsMinimum(4, 1, 0)) {
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
			String codePage1252 = new String(new byte[] { (byte) 0x80,
					(byte) 0x82, (byte) 0x83, (byte) 0x84, (byte) 0x85,
					(byte) 0x86, (byte) 0x87, (byte) 0x88, (byte) 0x89,
					(byte) 0x8a, (byte) 0x8b, (byte) 0x8c, (byte) 0x8e },
					"Cp1252");

			System.out.println(codePage1252);

			Properties props = new Properties();
			props.setProperty("characterEncoding", "Cp1252");
			Connection cp1252Conn = getConnectionWithProps(props);
			createTable("testCp1252",
					"(field1 varchar(32) CHARACTER SET latin1)");
			cp1252Conn.createStatement().executeUpdate(
					"INSERT INTO testCp1252 VALUES ('" + codePage1252 + "')");
			this.rs = cp1252Conn.createStatement().executeQuery(
					"SELECT field1 FROM testCp1252");
			this.rs.next();
			assertEquals(this.rs.getString(1), codePage1252);
		}
	}

	/**
	 * Tests fix for BUG#23645 - Some collations/character sets reported as
	 * "unknown" (specifically cias variants of existing character sets), and
	 * inability to override the detected server character set.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug23645() throws Exception {
		if (versionMeetsMinimum(4, 1)) {
			// Part of this isn't easily testable, hence the assertion in
			// CharsetMapping
			// that checks for mappings existing in both directions...

			// What we test here is the ability to override the character
			// mapping
			// when the server returns an "unknown" character encoding.

			String currentlyConfiguredCharacterSet = getSingleIndexedValueWithQuery(
					2, "SHOW VARIABLES LIKE 'character_set_connection'")
					.toString();
			System.out.println(currentlyConfiguredCharacterSet);

			String javaNameForMysqlName = CharsetMapping
					.getJavaEncodingForMysqlEncoding(
							currentlyConfiguredCharacterSet, null);
			System.out.println(javaNameForMysqlName);

			for (int i = 1; i < CharsetMapping.INDEX_TO_CHARSET.length; i++) {
				String possibleCharset = CharsetMapping.INDEX_TO_CHARSET[i];

				if (!javaNameForMysqlName.equals(possibleCharset)) {
					System.out.println(possibleCharset);

					Properties props = new Properties();
					props.setProperty("characterEncoding", possibleCharset);
					props.setProperty(
							"com.mysql.jdbc.faultInjection.serverCharsetIndex",
							"65535");

					Connection forcedCharConn = null;

					forcedCharConn = getConnectionWithProps(props);

					String forcedCharset = getSingleIndexedValueWithQuery(
							forcedCharConn, 2,
							"SHOW VARIABLES LIKE 'character_set_connection'")
							.toString();

					System.out.println(forcedCharset);

					break;
				}
			}

		}
	}

	/**
	 * Tests fix for BUG#24840 - character encoding of "US-ASCII" doesn't map
	 * correctly for 4.1 or newer
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug24840() throws Exception {
		Properties props = new Properties();
		props.setProperty("characterEncoding", "US-ASCII");

		getConnectionWithProps(props).close();
	}

	/**
	 * Tests fix for BUG#25047 - StringUtils.indexOfIgnoreCaseRespectQuotes()
	 * isn't case-insensitive on the first character of the target.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug25047() throws Exception {
		assertEquals(26, StringUtils.indexOfIgnoreCaseRespectQuotes(0,
				"insert into Test (TestID) values (?)", "VALUES", '`', false));
		assertEquals(26, StringUtils.indexOfIgnoreCaseRespectQuotes(0,
				"insert into Test (TestID) VALUES (?)", "values", '`', false));

		assertEquals(StringUtils.indexOfIgnoreCaseRespectQuotes(0,
				"insert into Test (TestID) values (?)", "VALUES", '`', false),
				StringUtils.indexOfIgnoreCaseRespectQuotes(0,
						"insert into Test (TestID) VALUES (?)", "VALUES", '`',
						false));
		assertEquals(StringUtils.indexOfIgnoreCaseRespectQuotes(0,
				"insert into Test (TestID) values (?)", "values", '`', false),
				StringUtils.indexOfIgnoreCaseRespectQuotes(0,
						"insert into Test (TestID) VALUES (?)", "values", '`',
						false));
	}
}
