/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.sql.Connection;

import testsuite.BaseTestCase;

/**
 * Tests BLOB functionality in the driver.
 * 
 * @author Mark Matthews
 * @version $Id$
 */
public class BlobTest extends BaseTestCase {

	protected static File testBlobFile;

	static {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				for (int i = 0; i < 5; i++) {
					try {
						if (testBlobFile.delete()) {
							break;
						}
					} catch (Throwable t) {
					}
				}
			}
		});
	}
	/**
	 * Creates a new BlobTest object.
	 * 
	 * @param name
	 *            the test to run
	 */
	public BlobTest(String name) {
		super(name);
	}

	/**
	 * Runs all test cases in this test suite
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(BlobTest.class);
	}

	/**
	 * Setup the test case
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	public void setUp() throws Exception {
		super.setUp();

		if (versionMeetsMinimum(4, 0)) {
			int requiredSize = 32 * 1024 * 1024;

			if (testBlobFile == null || testBlobFile.length() != requiredSize) {
				createBlobFile(requiredSize);
			}

		} else {
			int requiredSize = 8 * 1024 * 1024;

			if (testBlobFile == null || testBlobFile.length() != requiredSize) {
				createBlobFile(requiredSize);
			}
		}

		createTestTable();
	}

	public void testByteStreamInsert() throws Exception {
		testByteStreamInsert(this.conn);
	}
	
	/**
	 * Tests inserting blob data as a stream
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	private void testByteStreamInsert(Connection c) throws Exception {
		BufferedInputStream bIn = new BufferedInputStream(new FileInputStream(
				testBlobFile));
		this.pstmt = c
				.prepareStatement("INSERT INTO BLOBTEST(blobdata) VALUES (?)");
		this.pstmt.setBinaryStream(1, bIn, (int) testBlobFile.length());
		this.pstmt.execute();

		this.pstmt.clearParameters();
		doRetrieval();
	}

	private boolean checkBlob(byte[] retrBytes) throws Exception {
		boolean passed = false;
		BufferedInputStream bIn = new BufferedInputStream(new FileInputStream(
				testBlobFile));

		try {
			int fileLength = (int) testBlobFile.length();
			if (retrBytes.length == fileLength) {
				for (int i = 0; i < fileLength; i++) {
					byte fromFile = (byte) (bIn.read() & 0xff);

					if (retrBytes[i] != fromFile) {
						passed = false;
						System.out.println("Byte pattern differed at position "
								+ i + " , " + retrBytes[i] + " != " + fromFile);

						for (int j = 0; (j < (i + 10)) /* && (j < i) */; j++) {
							System.out.print(Integer
									.toHexString(retrBytes[j] & 0xff)
									+ " ");
						}

						break;
					}

					passed = true;
				}
			} else {
				passed = false;
				System.out.println("retrBytes.length(" + retrBytes.length
						+ ") != testBlob.length(" + fileLength + ")");
			}

			return passed;
		} finally {
			if (bIn != null) {
				bIn.close();
			}
		}
	}

	private void createTestTable() throws Exception {
		createTable("BLOBTEST", "(pos int PRIMARY KEY auto_increment, "
						+ "blobdata LONGBLOB)");
	}

	/**
	 * Mark this as deprecated to avoid warnings from compiler...
	 * 
	 * @deprecated
	 * 
	 * @throws Exception
	 *             if an error occurs retrieving the value
	 */
	private void doRetrieval() throws Exception {
		boolean passed = false;
		this.rs = this.stmt
				.executeQuery("SELECT blobdata from BLOBTEST LIMIT 1");
		this.rs.next();

		byte[] retrBytes = this.rs.getBytes(1);
		passed = checkBlob(retrBytes);
		assertTrue(
				"Inserted BLOB data did not match retrieved BLOB data for getBytes().",
				passed);
		retrBytes = this.rs.getBlob(1).getBytes(1L,
				(int) this.rs.getBlob(1).length());
		passed = checkBlob(retrBytes);
		assertTrue(
				"Inserted BLOB data did not match retrieved BLOB data for getBlob().",
				passed);

		InputStream inStr = this.rs.getBinaryStream(1);
		ByteArrayOutputStream bOut = new ByteArrayOutputStream();
		int b;

		while ((b = inStr.read()) != -1) {
			bOut.write((byte) b);
		}

		retrBytes = bOut.toByteArray();
		passed = checkBlob(retrBytes);
		assertTrue(
				"Inserted BLOB data did not match retrieved BLOB data for getBinaryStream().",
				passed);
		inStr = this.rs.getAsciiStream(1);
		bOut = new ByteArrayOutputStream();

		while ((b = inStr.read()) != -1) {
			bOut.write((byte) b);
		}

		retrBytes = bOut.toByteArray();
		passed = checkBlob(retrBytes);
		assertTrue(
				"Inserted BLOB data did not match retrieved BLOB data for getAsciiStream().",
				passed);
		inStr = this.rs.getUnicodeStream(1);
		bOut = new ByteArrayOutputStream();

		while ((b = inStr.read()) != -1) {
			bOut.write((byte) b);
		}

		retrBytes = bOut.toByteArray();
		passed = checkBlob(retrBytes);
		assertTrue(
				"Inserted BLOB data did not match retrieved BLOB data for getUnicodeStream().",
				passed);
	}

	private final static String TEST_BLOB_FILE_PREFIX = "cmj-testblob";
	
	private void createBlobFile(int size) throws Exception {
		if (testBlobFile != null && testBlobFile.length() != size) {
			testBlobFile.delete();
		}

		testBlobFile = File.createTempFile(TEST_BLOB_FILE_PREFIX, ".dat");
		testBlobFile.deleteOnExit();
		
		cleanupTempFiles(testBlobFile, TEST_BLOB_FILE_PREFIX);

		BufferedOutputStream bOut = new BufferedOutputStream(
				new FileOutputStream(testBlobFile));

		int dataRange = Byte.MAX_VALUE - Byte.MIN_VALUE;

		for (int i = 0; i < size; i++) {
			bOut.write((byte) ((Math.random() * dataRange) + Byte.MIN_VALUE));
		}

		bOut.flush();
		bOut.close();
	}
}
