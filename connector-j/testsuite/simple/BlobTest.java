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

    private static final byte[] TESTBLOB = new byte[512 * 1024];

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
     * @param name DOCUMENT ME!
     */
    public BlobTest(String name) {
        super(name);
    }

    //~ Methods ...............................................................

    /**
     * DOCUMENT ME!
     * 
     * @param args DOCUMENT ME!
     */
    public static void main(String[] args) {
        new BlobTest("testBytesInsert").run();
        new BlobTest("testByteStreamInsert").run();
    }

    /**
     * DOCUMENT ME!
     * 
     * @throws Exception DOCUMENT ME!
     */
    public void setUp()
               throws Exception {
        super.setUp();
        createTestTable();
    }

    /**
     * DOCUMENT ME!
     * 
     * @throws Exception DOCUMENT ME!
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
                          throws SQLException {

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
     * DOCUMENT ME!
     * 
     * @throws SQLException DOCUMENT ME!
     */
    public void testBytesInsert()
                         throws SQLException {
        pstmt = conn.prepareStatement(
                        "INSERT INTO BLOBTEST(blobdata) VALUES (?)");
        pstmt.setBytes(1, TESTBLOB);
        pstmt.execute();

        int rowsUpdated = pstmt.getUpdateCount();
        pstmt.clearParameters();
        doRetrieval();
    }

    /**
     * DOCUMENT ME!
     * 
     * @throws SQLException DOCUMENT ME!
     */
    public void testByteStreamInsert()
                              throws SQLException {

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
                      throws SQLException {

        boolean passed = false;
        passed = false;

        String message = "";

        try {

            ResultSet rs = stmt.executeQuery(
                                   "SELECT blobdata from BLOBTEST LIMIT 1");
            rs.next();

            byte[] retrBytes = rs.getBytes(1);

            if (retrBytes.length == TESTBLOB.length) {

                /*
                   for (int i = 0; i < 20; i++) {
                       System.out.print(retrBytes[i] + " ");
                   }
                   System.out.println();
                   
                   for (int i = 0; i < 20; i++) {
                       System.out.print(testBlob[i] + " ");
                   }
                   System.out.println();
                 */
                for (int i = 0; i < TESTBLOB.length; i++) {

                    if (retrBytes[i] != TESTBLOB[i]) {

                        for (int j = i - 10; j < i + 10; j++) {
                            System.out.print(retrBytes[j] + " ");
                        }

                        System.out.println();

                        for (int j = i - 10; j < i + 10; j++) {
                            System.out.print(TESTBLOB[j] + " ");
                        }

                        System.out.println();
                        passed = false;
                        message = "Byte pattern differed at position " + i
                                  + " , " + retrBytes[i] + " != "
                                  + TESTBLOB[i];

                        break;
                    }

                    passed = true;
                }
            } else {
                passed = false;
                message = "retrBytes.length(" + retrBytes.length
                          + ") != testBlob.length(" + TESTBLOB.length + ")";
            }

            assertTrue("Inserted BLOB data did not match retrieved BLOB data."
                       + message, passed);
        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
        }
    }
}