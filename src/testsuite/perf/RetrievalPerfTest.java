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
package testsuite.perf;

import testsuite.BaseTestCase;

/**
 * Simplistic test for performance regression.
 * 
 * @author Mark Matthews
 */
public class RetrievalPerfTest extends BaseTestCase {
	// ~ Static fields/initializers
	// ---------------------------------------------

	private static final int NUM_TESTS = 10000;

	private static final int NUM_ROWS = 80;

	// ~ Constructors
	// -----------------------------------------------------------

	/**
	 * Constructor for RetrievalPerfTest.
	 * 
	 * @param name
	 *            name of the test to run
	 */
	public RetrievalPerfTest(String name) {
		super(name);
	}

	// ~ Methods
	// ----------------------------------------------------------------

	/**
	 * Runs all tests.
	 * 
	 * @param args
	 *            ignored
	 */
	public static void main(String[] args) {
		new RetrievalPerfTest("testRetrievalMyIsam").run();
		new RetrievalPerfTest("testRetrievalHeap").run();
		new RetrievalPerfTest("testRetrievalCached").run();
	}

	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	public void setUp() throws Exception {
		super.setUp();
		createTable("retrievalPerfTestHeap",  "(priKey INT NOT NULL PRIMARY KEY,"
						+ "charField VARCHAR(80)) ", "HEAP");
		createTable("retrievalPerfTestMyIsam", "(priKey INT NOT NULL PRIMARY KEY,"
				+ "charField VARCHAR(80)) ", "MyISAM");

		for (int i = 0; i < NUM_ROWS; i++) {
			this.stmt
					.executeUpdate("INSERT INTO retrievalPerfTestHeap (priKey, charField) VALUES ("
							+ i
							+ ",'abcdefghijklmnopqrstuvqxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')");
			this.stmt
					.executeUpdate("INSERT INTO retrievalPerfTestMyIsam (priKey, charField) VALUES ("
							+ i
							+ ",'abcdefghijklmnopqrstuvqxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')");
		}
	}

	/**
	 * Tests retrieval from the query cache
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	public void testRetrievalCached() throws Exception {
		this.stmt.executeUpdate("SET QUERY_CACHE_TYPE = DEMAND");

		double fullBegin = System.currentTimeMillis();
		double averageQueryTimeMs = 0;
		double averageTraversalTimeMs = 0;

		for (int i = 0; i < NUM_TESTS; i++) {
			long queryBegin = System.currentTimeMillis();
			this.rs = this.stmt
					.executeQuery("SELECT SQL_CACHE * FROM retrievalPerfTestHeap");

			long queryEnd = System.currentTimeMillis();
			averageQueryTimeMs += ((double) (queryEnd - queryBegin) / NUM_TESTS);

			long traverseBegin = System.currentTimeMillis();

			while (this.rs.next()) {
				this.rs.getInt(1);
				this.rs.getString(2);
			}

			long traverseEnd = System.currentTimeMillis();
			averageTraversalTimeMs += ((double) (traverseEnd - traverseBegin) / NUM_TESTS);
		}

		double fullEnd = System.currentTimeMillis();
		double fullTime = (fullEnd - fullBegin) / 1000;
		double queriesPerSec = NUM_TESTS / fullTime;
		double rowsPerSec = (NUM_ROWS * NUM_TESTS) / fullTime;
		System.out.println("\nQuery Cache From Heap Retrieval\n");
		System.out.println("Full test took: " + fullTime + " seconds.");
		System.out.println("Queries/second: " + queriesPerSec);
		System.out.println("Rows/second: " + rowsPerSec);
		System.out.println("Avg. Query Exec Time: " + averageQueryTimeMs
				+ " ms");
		System.out.println("Avg. Traversal Time: " + averageTraversalTimeMs
				+ " ms");

		// We're doing something wrong if we can't beat 45 seconds :(
		assertTrue(fullTime < 45);
	}

	/**
	 * Tests retrieval from HEAP tables
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	public void testRetrievalHeap() throws Exception {
		double fullBegin = System.currentTimeMillis();
		double averageQueryTimeMs = 0;
		double averageTraversalTimeMs = 0;

		for (int i = 0; i < NUM_TESTS; i++) {
			long queryBegin = System.currentTimeMillis();
			this.rs = this.stmt
					.executeQuery("SELECT * FROM retrievalPerfTestHeap");

			long queryEnd = System.currentTimeMillis();
			averageQueryTimeMs += ((double) (queryEnd - queryBegin) / NUM_TESTS);

			long traverseBegin = System.currentTimeMillis();

			while (this.rs.next()) {
				this.rs.getInt(1);
				this.rs.getString(2);
			}

			long traverseEnd = System.currentTimeMillis();
			averageTraversalTimeMs += ((double) (traverseEnd - traverseBegin) / NUM_TESTS);
		}

		double fullEnd = System.currentTimeMillis();
		double fullTime = (fullEnd - fullBegin) / 1000;
		double queriesPerSec = NUM_TESTS / fullTime;
		double rowsPerSec = (NUM_ROWS * NUM_TESTS) / fullTime;
		System.out.println("\nHEAP Table Retrieval\n");
		System.out.println("Full test took: " + fullTime + " seconds.");
		System.out.println("Queries/second: " + queriesPerSec);
		System.out.println("Rows/second: " + rowsPerSec);
		System.out.println("Avg. Query Exec Time: " + averageQueryTimeMs
				+ " ms");
		System.out.println("Avg. Traversal Time: " + averageTraversalTimeMs
				+ " ms");

		// We're doing something wrong if we can't beat 45 seconds :(
		assertTrue(fullTime < 45);
	}

	/**
	 * Tests retrieval speed from MyISAM type tables
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	public void testRetrievalMyIsam() throws Exception {
		double fullBegin = System.currentTimeMillis();
		double averageQueryTimeMs = 0;
		double averageTraversalTimeMs = 0;

		for (int i = 0; i < NUM_TESTS; i++) {
			long queryBegin = System.currentTimeMillis();
			this.rs = this.stmt
					.executeQuery("SELECT * FROM retrievalPerfTestMyIsam");

			long queryEnd = System.currentTimeMillis();
			averageQueryTimeMs += ((double) (queryEnd - queryBegin) / NUM_TESTS);

			long traverseBegin = System.currentTimeMillis();

			while (this.rs.next()) {
				this.rs.getInt(1);
				this.rs.getString(2);
			}

			long traverseEnd = System.currentTimeMillis();
			averageTraversalTimeMs += ((double) (traverseEnd - traverseBegin) / NUM_TESTS);
		}

		double fullEnd = System.currentTimeMillis();
		double fullTime = (fullEnd - fullBegin) / 1000;
		double queriesPerSec = NUM_TESTS / fullTime;
		double rowsPerSec = (NUM_ROWS * NUM_TESTS) / fullTime;
		System.out.println("\nMyIsam Retrieval\n");
		System.out.println("Full test took: " + fullTime + " seconds.");
		System.out.println("Queries/second: " + queriesPerSec);
		System.out.println("Rows/second: " + rowsPerSec);
		System.out.println("Avg. Query Exec Time: " + averageQueryTimeMs
				+ " ms");
		System.out.println("Avg. Traversal Time: " + averageTraversalTimeMs
				+ " ms");

		// We're doing something wrong if we can't beat 45 seconds :(
		assertTrue(fullTime < 45);
	}
}
