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

import com.mysql.jdbc.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import java.util.Properties;

import testsuite.BaseTestCase;


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
        } finally {
            stmt.executeUpdate("DROP TABLE IF EXISTS sjisTest");
        }
    }
    
    public void testSingleByteConversion() throws Exception {
         testConversionForString("æøå èéòù");
         testConversionForString("Kaarle Äänis Ilmari");
    }
    
    public void testUtf8Encoding() throws Exception {
        Properties props = new Properties();
        props.put("characterEncoding", "UTF-8");
        props.put("useUnicode", "true");
        Connection utfConn = DriverManager.getConnection(dbUrl, props);
        
        testConversionForString(utfConn, "\u043c\u0438\u0445\u0438");  
    }
    
    private void testConversionForString(Connection convConn, String charsToTest)
        throws Exception {
    
        PreparedStatement pStmt = null;
        
        try {
            stmt = convConn.createStatement();
            
            stmt.executeUpdate("DROP TABLE IF EXISTS charConvTest");
            stmt.executeUpdate("CREATE TABLE charConvTest (field1 char(50))");
            stmt.executeUpdate(
                    "INSERT INTO charConvTest VALUES ('" + charsToTest + "')");
           
            pStmt = convConn.prepareStatement("INSERT INTO charConvTest VALUES (?)");
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