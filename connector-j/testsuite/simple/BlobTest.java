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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.DriverManager;

import testsuite.BaseTestCase;

/** 
 *
 * @author  Administrator
 * @version 
 */
public class BlobTest extends BaseTestCase {

	static byte[] testBlob = new byte[2 * 1024 * 1024]; // 2 meg blob

	static {
		int dataRange = Byte.MAX_VALUE - Byte.MIN_VALUE;

		for (int i = 0; i < testBlob.length; i++) {
			testBlob[i] = (byte) ((Math.random() * dataRange) + Byte.MIN_VALUE);
		}
	}

	public BlobTest(String name)
	{
		super(name);
	}
	
	public static void main(String[] args)
	{
		new BlobTest("testBytesInsert").run();
		new BlobTest("testByteStreamInsert").run();
	}
	
	public void setUp() throws Exception {
		super.setUp();
		createTestTable();		
	}
	
	public void tearDown() throws Exception {
		try
		{
			stmt.executeUpdate("DROP TABLE IF EXISTS BLOBTEST");
		}
		finally 
		{
			super.tearDown();
		}
	}

	

	private void createTestTable() throws SQLException {

		//
		// Catch the error, the table might exist
		//

		try {
			stmt.executeUpdate("DROP TABLE BLOBTEST");
		} catch (SQLException SQLE) {
		}

		stmt.executeUpdate(
			"CREATE TABLE BLOBTEST (pos int PRIMARY KEY auto_increment, blobdata LONGBLOB)");
	}

	public void testBytesInsert() throws SQLException {
		pstmt =
			conn.prepareStatement("INSERT INTO BLOBTEST(blobdata) VALUES (?)");

		pstmt.setBytes(1, testBlob);

		pstmt.execute();

		int rowsUpdated = pstmt.getUpdateCount();

		pstmt.clearParameters();
		
		doRetrieval();
	}

	public void testByteStreamInsert() throws SQLException {
		java.io.ByteArrayInputStream bIn =
			new java.io.ByteArrayInputStream(testBlob);
			
		pstmt =
			conn.prepareStatement("INSERT INTO BLOBTEST(blobdata) VALUES (?)");

		pstmt.setBinaryStream(1, bIn, 0);

		pstmt.execute();

		int rowsUpdated = pstmt.getUpdateCount();

		pstmt.clearParameters();
		
		doRetrieval();
	}

	private void doRetrieval() throws SQLException {

		boolean passed = false;

		passed = false;

		String message = "";
		
		try {
			ResultSet rs =
				stmt.executeQuery("SELECT blobdata from BLOBTEST LIMIT 1");

			rs.next();

			byte[] retrBytes = rs.getBytes(1);

			if (retrBytes.length == testBlob.length) {
				
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
				
				for (int i = 0; i < testBlob.length; i++) {
					if (retrBytes[i] != testBlob[i]) {
						
						for (int j = i-10; j < i + 10; j++)
						{
							System.out.print(retrBytes[j] + " ");
						}
						System.out.println();
						
						for (int j = i-10; j < i + 10; j++)
						{
							System.out.print(testBlob[j] + " ");
						}
						System.out.println();
							
						passed = false;
						message = "Byte pattern differed at position " + i + " , " + retrBytes[i] + " != " + testBlob[i];
						break;
					}

					passed = true;
				}
			}
			else
			{
				passed = false;
				message = "retrBytes.length(" + retrBytes.length + ") != testBlob.length(" + testBlob.length + ")";
			}
			
			assertTrue("Inserted BLOB data did not match retrieved BLOB data." + message, passed);
			
		} catch (SQLException sqlEx) {
			sqlEx.printStackTrace();
		}
	}

}