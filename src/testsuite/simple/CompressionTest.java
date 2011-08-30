package testsuite.simple;

/*
Copyright (c) 2011, Oracle and/or its affiliates. All rights reserved.


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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;

import testsuite.BaseTestCase;

public class CompressionTest extends BaseTestCase {

	public CompressionTest(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Tests if useCompress works.
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testUseCompress() throws Exception {

		// Original test
		// Properties props = new Properties();
		// props.put("useCompression", "true");
		// props.put("traceProtocol", "true");
		// Connection conn1 = getConnectionWithProps(props);
		// Statement stmt1 = conn1.createStatement();
		// ResultSet rs1 = stmt1.executeQuery("SELECT VERSION()");
		// rs1.next();
		// rs1.getString(1);
		// stmt1.close();
		// conn1.close();

		File testBlobFile = null;
		int requiredSize = 0;

		Properties props = new Properties();
		props.put("useCompression", "true");
		Connection conn1 = getConnectionWithProps(props);

		Statement stmt1 = conn1.createStatement();
		// Get real value
		this.rs = stmt1
				.executeQuery("SHOW VARIABLES LIKE 'max_allowed_packet'");
		this.rs.next();
		// Create smaller than maximum allowed BLOB for testing
		requiredSize = this.rs.getInt(2) / 8;
		System.out.println("Required size: " + requiredSize);
		this.rs.close();

		// http://dev.mysql.com/doc/refman/5.1/en/server-system-variables.html#sysvar_max_allowed_packet
		// setting GLOBAL variable during test is not ok
		// The protocol limit for max_allowed_packet is 1GB.
		if (testBlobFile == null || testBlobFile.length() != requiredSize) {
			if (testBlobFile != null && testBlobFile.length() != requiredSize) {
				testBlobFile.delete();
			}

			testBlobFile = File.createTempFile("cmj-testblob", ".dat");
			testBlobFile.deleteOnExit();

			cleanupTempFiles(testBlobFile, "cmj-testblob");

			BufferedOutputStream bOut = new BufferedOutputStream(
					new FileOutputStream(testBlobFile));

			int dataRange = Byte.MAX_VALUE - Byte.MIN_VALUE;

			for (int i = 0; i < requiredSize; i++) {
				bOut.write((byte) ((Math.random() * dataRange) + Byte.MIN_VALUE));
			}

			bOut.flush();
			bOut.close();
		}

		createTable("BLOBTEST",
				"(pos int PRIMARY KEY auto_increment, blobdata LONGBLOB)");
		BufferedInputStream bIn = new BufferedInputStream(new FileInputStream(
				testBlobFile));

		this.pstmt = conn1
				.prepareStatement("INSERT INTO BLOBTEST(blobdata) VALUES (?)");
		this.pstmt.setBinaryStream(1, bIn, (int) testBlobFile.length());
		this.pstmt.execute();
		this.pstmt.clearParameters();

		this.rs = stmt1.executeQuery("SELECT blobdata from BLOBTEST LIMIT 1");
		this.rs.next();

		if (bIn != null) {
			bIn.close();
		}
	}
}
