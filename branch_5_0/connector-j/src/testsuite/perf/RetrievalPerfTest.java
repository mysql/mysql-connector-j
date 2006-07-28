/*
 Copyright (C) 2002-2004 MySQL AB

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
		this.stmt.executeUpdate("DROP TABLE IF EXISTS retrievalPerfTestHeap");
		this.stmt.executeUpdate("DROP TABLE IF EXISTS retrievalPerfTestMyIsam");
		this.stmt
				.executeUpdate("CREATE TABLE retrievalPerfTestHeap (priKey INT NOT NULL PRIMARY KEY,"
						+ "charField VARCHAR(80)) TYPE=HEAP");
		this.stmt
				.executeUpdate("CREATE TABLE retrievalPerfTestMyIsam (priKey INT NOT NULL PRIMARY KEY,"
						+ "charField VARCHAR(80)) TYPE=MyISAM");

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
	 * @see junit.framework.TestCase#tearDown()
	 */
	public void tearDown() throws Exception {
		this.stmt.executeUpdate("DROP TABLE IF EXISTS retrievalPerfTestHeap");
		this.stmt.executeUpdate("DROP TABLE IF EXISTS retrievalPerfTestMyIsam");
		super.tearDown();
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
