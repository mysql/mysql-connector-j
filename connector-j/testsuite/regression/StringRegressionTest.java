/*
   Copyright (C) 2002 MySQL AB
   
      This program is free software; you can redistribute it and/or modify
      it under the terms of the GNU General Public License as published by
      the Free Software Foundation; either version 2 of the License, or
      (at your option) any later version.
   
      This program is distributed in the hope that it will be useful,
      but WITHOUT ANY WARRANTY; without even the implied warranty of
      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
      GNU General Public License for more details.
   
      You should have received a copy of the GNU General Public License
      along with this program; if not, write to the Free Software
      Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
      
 */
package testsuite.regression;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

import testsuite.BaseTestCase;

import com.mysql.jdbc.StringUtils;


/**
 * Tests for regressions of bugs in String handling
 * in the driver.
 * 
 * @author  Mark Matthews
 * @version StringRegressionTest.java,v 1.1 2002/11/04 14:58:25 mark_matthews Exp
 */
public class StringRegressionTest
    extends BaseTestCase {

    //~ Constructors ..........................................................

    /**
     * Creates a new StringTest object.
     * 
     * @param name DOCUMENT ME!
     */
    public StringRegressionTest(String name) {
        super(name);
    }

    //~ Methods ...............................................................

    /**
     * Runs this testsuite.
     * 
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        new StringRegressionTest("testAsciiCharConversion").run();
        new StringRegressionTest("testEncodingRegression").run();
    }

    /**
     * Tests character conversion bug.
     * 
     * @throws Exception if there is an internal error (which
     * is a bug).
     */
    public void testAsciiCharConversion()
                                 throws Exception {

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
     * Tests for regression of encoding forced by user, reported
     * by Jive Software
     * 
     * @throws Exception when encoding is not supported (which
     * is a bug)
     */
    public void testEncodingRegression()
                                throws Exception {

        Properties props = new Properties();
        props.put("characterEncoding", "UTF-8");
        props.put("useUnicode", "true");
        DriverManager.getConnection(dbUrl, props).close();
    }

    /**
     * Tests that the 0x5c escaping works (we didn't use to have
     * this).
     * 
     * @throws Exception if an error occurs.
     */
    public void testSjis5c()
                    throws Exception {

        byte[] origByteStream = new byte[] {
            (byte) 0x95, (byte) 0x5c, (byte) 0x8e, (byte) 0x96
        };

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
        byte[] newByteStream = StringUtils.getBytes(origString, "SJIS");

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
        try {

            Properties props = new Properties();
            props.put("useUnicode", "true");
            props.put("characterEncoding", "SJIS");
            conn = DriverManager.getConnection(dbUrl, props);
            stmt = conn.createStatement();
            stmt.executeUpdate("DROP TABLE IF EXISTS sjisTest");
            stmt.executeUpdate("CREATE TABLE sjisTest (field1 char(50))");
            stmt.executeUpdate(
                    "INSERT INTO sjisTest VALUES ('" + origString + "')");
            rs = stmt.executeQuery("SELECT * FROM sjisTest");
            rs.next();

            String testValue = rs.getString(1);
            assertTrue(testValue.equals(origString));
            
            pstmt = conn.prepareStatement("INSERT INTO sjisTest VALUES (?)");
            pstmt.setString(1, origString);
            pstmt.executeUpdate();
            
            rs = stmt.executeQuery("SELECT * FROM sjisTest");

            while (rs.next()) {
                testValue = rs.getString(1);
                assertTrue(testValue.equals(origString));
            }
            
        } finally {
            stmt.executeUpdate("DROP TABLE IF EXISTS sjisTest");
        }
    }

    /**
     * Tests that single-byte character conversion
     * works correctly.
     * 
     * @throws Exception if any errors occur
     */
    public void testSingleByteConversion()
                                  throws Exception {
        testConversionForString("æøå èéòù");
        testConversionForString("Kaarle Äänis Ilmari");
        testConversionForString("áàâãéèêíìîóòôõúùûç");
    }

    /**
     * Tests that UTF-8 character conversion
     * works correctly.
     * 
     * @throws Exception if any errors occur
     */
    public void testUtf8Encoding()
                          throws Exception {

        Properties props = new Properties();
        props.put("characterEncoding", "UTF-8");
        props.put("useUnicode", "true");

        Connection utfConn = DriverManager.getConnection(dbUrl, props);
        testConversionForString(utfConn, "\u043c\u0438\u0445\u0438");
    }
    
    /**
     * Tests newline being treated correctly.
     * 
     * @throws Exception if an error occurs
     */
    public void testNewlines() throws Exception {
        String newlineStr = "Foo\nBar\n\rBaz";
        
        stmt.executeUpdate("DROP TABLE IF EXISTS newlineRegressTest");
        stmt.executeUpdate("CREATE TABLE newlineRegressTest (field1 MEDIUMTEXT)");
        
        try {
            stmt.executeUpdate("INSERT INTO newlineRegressTest VALUES ('" + newlineStr + "')");
            pstmt = conn.prepareStatement("INSERT INTO newlineRegressTest VALUES (?)");
            pstmt.setString(1, newlineStr);
            pstmt.executeUpdate();
            
            rs = stmt.executeQuery("SELECT * FROM newlineRegressTest");
            
            while (rs.next()) {
                assertTrue(rs.getString(1).equals(newlineStr));
            }
        } finally {
            stmt.executeUpdate("DROP TABLE IF EXISTS newlineRegressTest");
        }
    }
    
    /**
     * Tests that 'latin1' character conversion
     * works correctly.
     * 
     * @throws Exception if any errors occur
     */
    public void testLatin1Encoding()
                            throws Exception {

        char[] latin1Charset = {
            0x0000, 0x0001, 0x0002, 0x0003, 0x0004, 0x0005, 0x0006, 0x0007, 
            0x0008, 0x0009, 0x000A, 0x000B, 0x000C, 0x000D, 0x000E, 0x000F, 
            0x0010, 0x0011, 0x0012, 0x0013, 0x0014, 0x0015, 0x0016, 0x0017, 
            0x0018, 0x0019, 0x001A, 0x001B, 0x001C, 0x001D, 0x001E, 0x001F, 
            0x0020, 0x0021, 0x0022, 0x0023, 0x0024, 0x0025, 0x0026, 0x0027, 
            0x0028, 0x0029, 0x002A, 0x002B, 0x002C, 0x002D, 0x002E, 0x002F, 
            0x0030, 0x0031, 0x0032, 0x0033, 0x0034, 0x0035, 0x0036, 0x0037, 
            0x0038, 0x0039, 0x003A, 0x003B, 0x003C, 0x003D, 0x003E, 0x003F, 
            0x0040, 0x0041, 0x0042, 0x0043, 0x0044, 0x0045, 0x0046, 0x0047, 
            0x0048, 0x0049, 0x004A, 0x004B, 0x004C, 0x004D, 0x004E, 0x004F, 
            0x0050, 0x0051, 0x0052, 0x0053, 0x0054, 0x0055, 0x0056, 0x0057, 
            0x0058, 0x0059, 0x005A, 0x005B, 0x005C, 0x005D, 0x005E, 0x005F, 
            0x0060, 0x0061, 0x0062, 0x0063, 0x0064, 0x0065, 0x0066, 0x0067, 
            0x0068, 0x0069, 0x006A, 0x006B, 0x006C, 0x006D, 0x006E, 0x006F, 
            0x0070, 0x0071, 0x0072, 0x0073, 0x0074, 0x0075, 0x0076, 0x0077, 
            0x0078, 0x0079, 0x007A, 0x007B, 0x007C, 0x007D, 0x007E, 0x007F, 
            0x0080, 0x0081, 0x0082, 0x0083, 0x0084, 0x0085, 0x0086, 0x0087, 
            0x0088, 0x0089, 0x008A, 0x008B, 0x008C, 0x008D, 0x008E, 0x008F, 
            0x0090, 0x0091, 0x0092, 0x0093, 0x0094, 0x0095, 0x0096, 0x0097, 
            0x0098, 0x0099, 0x009A, 0x009B, 0x009C, 0x009D, 0x009E, 0x009F, 
            0x00A0, 0x00A1, 0x00A2, 0x00A3, 0x00A4, 0x00A5, 0x00A6, 0x00A7, 
            0x00A8, 0x00A9, 0x00AA, 0x00AB, 0x00AC, 0x00AD, 0x00AE, 0x00AF, 
            0x00B0, 0x00B1, 0x00B2, 0x00B3, 0x00B4, 0x00B5, 0x00B6, 0x00B7, 
            0x00B8, 0x00B9, 0x00BA, 0x00BB, 0x00BC, 0x00BD, 0x00BE, 0x00BF, 
            0x00C0, 0x00C1, 0x00C2, 0x00C3, 0x00C4, 0x00C5, 0x00C6, 0x00C7, 
            0x00C8, 0x00C9, 0x00CA, 0x00CB, 0x00CC, 0x00CD, 0x00CE, 0x00CF, 
            0x00D0, 0x00D1, 0x00D2, 0x00D3, 0x00D4, 0x00D5, 0x00D6, 0x00D7, 
            0x00D8, 0x00D9, 0x00DA, 0x00DB, 0x00DC, 0x00DD, 0x00DE, 0x00DF, 
            0x00E0, 0x00E1, 0x00E2, 0x00E3, 0x00E4, 0x00E5, 0x00E6, 0x00E7, 
            0x00E8, 0x00E9, 0x00EA, 0x00EB, 0x00EC, 0x00ED, 0x00EE, 0x00EF, 
            0x00F0, 0x00F1, 0x00F2, 0x00F3, 0x00F4, 0x00F5, 0x00F6, 0x00F7, 
            0x00F8, 0x00F9, 0x00FA, 0x00FB, 0x00FC, 0x00FD, 0x00FE, 0x00FF
        };
        
        String latin1String = new String(latin1Charset);
        PreparedStatement pStmt = null;
        
        try {
            stmt.executeUpdate("DROP TABLE IF EXISTS latin1RegressTest");
            stmt.executeUpdate("CREATE TABLE latin1RegressTest (stringField TEXT)");
        
            pStmt = conn.prepareStatement("INSERT INTO latin1RegressTest VALUES (?)");
            pStmt.setString(1, latin1String);
            pStmt.executeUpdate();
        
        
            rs = stmt.executeQuery("SELECT * FROM latin1RegressTest");
        
            rs.next();
        
            String retrievedString = rs.getString(1);
            
            System.out.println(latin1String);
            System.out.println(retrievedString);
        
            if (!retrievedString.equals(latin1String)) {
                int stringLength = Math.min(retrievedString.length(), latin1String.length());
                
                for (int i = 0; i < stringLength; i++) {
                    char rChar = retrievedString.charAt(i);
                    char origChar = latin1String.charAt(i);
                    
                    if (rChar != '?' && (rChar != origChar)) {
                        fail("characters differ at position " + i + "'" + rChar + "' retrieved from database, original char was '" + origChar + "'");
                    }
                }
            }
        } finally {
            if (rs != null) {
                try {
                    rs.close();
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
                
            stmt.executeUpdate("DROP TABLE IF EXISTS latin1RegressTest");
        }    
    }

    private void testConversionForString(Connection convConn, 
                                         String charsToTest)
                                  throws Exception {

        PreparedStatement pStmt = null;

        try {
            stmt = convConn.createStatement();
            stmt.executeUpdate("DROP TABLE IF EXISTS charConvTest");
            stmt.executeUpdate("CREATE TABLE charConvTest (field1 char(50))");
            stmt.executeUpdate(
                    "INSERT INTO charConvTest VALUES ('" + charsToTest + "')");
            pStmt = convConn.prepareStatement(
                            "INSERT INTO charConvTest VALUES (?)");
            pStmt.setString(1, charsToTest);
            pStmt.executeUpdate();
            rs = stmt.executeQuery("SELECT * FROM charConvTest");

            boolean hadRows = false;

            while (rs.next()) {
                hadRows = true;

                String testValue = rs.getString(1);
                System.out.println(testValue);
                assertTrue(testValue.equals(charsToTest));
            }

            assertTrue(hadRows);
        } finally {
            stmt.executeUpdate("DROP TABLE IF EXISTS charConvTest");
        }
    }

    private void testConversionForString(String charsToTest)
                                  throws Exception {
        testConversionForString(this.conn, charsToTest);
    }
}