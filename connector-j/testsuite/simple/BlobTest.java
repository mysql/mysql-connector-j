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
package testsuite.simple;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import java.sql.ResultSet;
import java.sql.SQLException;

import testsuite.BaseTestCase;


/** 
 * Tests BLOB functionality in the driver.
 * 
 * @author  Mark Matthews
 * @version $Id$
 */
public class BlobTest
    extends BaseTestCase {

    //~ Instance/static variables .............................................

    private static final byte[] TESTBLOB = new byte[1024 * 1024];

    //~ Initializers ..........................................................

    static {

        int dataRange = Byte.MAX_VALUE - Byte.MIN_VALUE;

        for (int i = 0; i < TESTBLOB.length; i++) {
            TESTBLOB[i] = (byte) ((Math.random() * dataRange) + Byte.MIN_VALUE);
        }  
    }

    //~ Constructors ..........................................................

    /**
     * Creates a new BlobTest object.
     * 
     * @param name the test to run
     */
    public BlobTest(String name) {
        super(name);
    }

    //~ Methods ...............................................................

    /**
     * Runs all test cases
     * 
     * @param args ignored
     */
    public static void main(String[] args) {
        new BlobTest("testBytesInsert").run();
        new BlobTest("testByteStreamInsert").run();
    }

    /**
     * Setup the test case
     * 
     * @throws Exception if an error occurs
     */
    public void setUp()
               throws Exception {
        super.setUp();
        createTestTable();
    }

    /**
     * Destroy resources created by test case
     * 
     * @throws Exception if an error occurs
     */
    public void tearDown()
                  throws Exception {

        try {
            stmt.executeUpdate("DROP TABLE IF EXISTS BLOBTEST");
        } finally {
            super.tearDown();
        }
    }

    private void createTestTable()
                          throws Exception {

        //
        // Catch the error, the table might exist
        //
        try {
            stmt.executeUpdate("DROP TABLE BLOBTEST");
        } catch (SQLException SQLE) {
            ;
        }

        stmt.executeUpdate(
                "CREATE TABLE BLOBTEST (pos int PRIMARY KEY auto_increment, "
                + "blobdata LONGBLOB)");
    }

    /**
     * Tests inserting BLOB data as byte[]
     * 
     * @throws Exception if an error occurs
     */
    public void testBytesInsert()
                         throws Exception {
        pstmt = conn.prepareStatement(
                        "INSERT INTO BLOBTEST(blobdata) VALUES (?)");
        pstmt.setBytes(1, TESTBLOB);
        pstmt.execute();

        int rowsUpdated = pstmt.getUpdateCount();
        pstmt.clearParameters();
        doRetrieval();
    }

    /**
     * Tests inserting blob data as a stream
     * 
     * @throws Exception if an error occurs
     */
    public void testByteStreamInsert()
                              throws Exception {

        java.io.ByteArrayInputStream bIn = new java.io.ByteArrayInputStream(
                                                   TESTBLOB);
        pstmt = conn.prepareStatement(
                        "INSERT INTO BLOBTEST(blobdata) VALUES (?)");
        pstmt.setBinaryStream(1, bIn, TESTBLOB.length);
        pstmt.execute();

        int rowsUpdated = pstmt.getUpdateCount();
        pstmt.clearParameters();
        doRetrieval();
    }

    private void doRetrieval()
                      throws Exception {

        boolean passed = false;
        ResultSet rs = stmt.executeQuery(
                               "SELECT blobdata from BLOBTEST LIMIT 1");
        rs.next();

        byte[] retrBytes = rs.getBytes(1);
        passed = checkBlob(retrBytes);
        assertTrue("Inserted BLOB data did not match retrieved BLOB data for getBytes().", 
                   passed);
        retrBytes = rs.getBlob(1).getBytes(1L, (int) rs.getBlob(1).length());
        passed = checkBlob(retrBytes);
        assertTrue("Inserted BLOB data did not match retrieved BLOB data for getBlob().", 
                   passed);

        InputStream inStr = rs.getBinaryStream(1);
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        int b;

        while ((b = inStr.read()) != -1) {
            bOut.write((byte) b);
        }

        retrBytes = bOut.toByteArray();
        passed = checkBlob(retrBytes);
        assertTrue("Inserted BLOB data did not match retrieved BLOB data for getBinaryStream().", 
                   passed);
        inStr = rs.getAsciiStream(1);
        bOut = new ByteArrayOutputStream();

        while ((b = inStr.read()) != -1) {
            bOut.write((byte) b);
        }

        retrBytes = bOut.toByteArray();
        passed = checkBlob(retrBytes);
        assertTrue("Inserted BLOB data did not match retrieved BLOB data for getAsciiStream().", 
                   passed);
        inStr = rs.getUnicodeStream(1);
        bOut = new ByteArrayOutputStream();

        while ((b = inStr.read()) != -1) {
            bOut.write((byte) b);
        }

        retrBytes = bOut.toByteArray();
        passed = checkBlob(retrBytes);
        assertTrue("Inserted BLOB data did not match retrieved BLOB data for getUnicodeStream().", 
                   passed);
    }

    private boolean checkBlob(byte[] retrBytes) {

        boolean passed = false;

        if (retrBytes.length == TESTBLOB.length) {

            for (int i = 0; i < TESTBLOB.length; i++) {

                if (retrBytes[i] != TESTBLOB[i]) {

                    
                    passed = false;
                    System.out.println(
                            "Byte pattern differed at position " + i + " , "
                            + retrBytes[i] + " != " + TESTBLOB[i]);
                            
                    for (int j = 0; j < i + 10 && j < i; j++) {
                        System.out.print(Integer.toHexString(retrBytes[j]) + " ");
                        
                    }
                    
                    for (int j = 0; j < i + 10 && j < i; j++) {
                        System.out.print(Integer.toHexString(TESTBLOB[j]) + " ");
                        
                    }

                    break;
                }

                passed = true;
            }
        } else {
            passed = false;
            System.out.println(
                    "retrBytes.length(" + retrBytes.length
                    + ") != testBlob.length(" + TESTBLOB.length + ")");
        }

        return passed;
    }
}