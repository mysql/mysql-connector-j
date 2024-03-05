/*
 * Copyright (c) 2002, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package testsuite.perf;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mysql.cj.MysqlConnection;

import testsuite.BaseTestCase;

/**
 * Simplistic test for performance regression.
 */
public class RetrievalPerfTest extends BaseTestCase {

    private static final int NUM_TESTS = 10000;

    private static final int NUM_ROWS = 80;

    @BeforeEach
    public void setUp() throws Exception {
        createTable("retrievalPerfTestHeap", "(priKey INT NOT NULL PRIMARY KEY, charField VARCHAR(80)) ", "HEAP");
        createTable("retrievalPerfTestMyIsam", "(priKey INT NOT NULL PRIMARY KEY, charField VARCHAR(80)) ", "MyISAM");

        for (int i = 0; i < NUM_ROWS; i++) {
            this.stmt.executeUpdate(
                    "INSERT INTO retrievalPerfTestHeap (priKey, charField) VALUES (" + i + ",'abcdefghijklmnopqrstuvqxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')");
            this.stmt.executeUpdate(
                    "INSERT INTO retrievalPerfTestMyIsam (priKey, charField) VALUES (" + i + ",'abcdefghijklmnopqrstuvqxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')");
        }
    }

    /**
     * Tests retrieval from the query cache
     *
     * @throws Exception
     */
    @Test
    public void testRetrievalCached() throws Exception {
        assumeTrue(((MysqlConnection) this.conn).getSession().getServerSession().isQueryCacheEnabled(),
                "This test requires the server with enabled query cache.");

        this.stmt.executeUpdate("SET QUERY_CACHE_TYPE = DEMAND");

        double fullBegin = System.currentTimeMillis();
        double averageQueryTimeMs = 0;
        double averageTraversalTimeMs = 0;

        for (int i = 0; i < NUM_TESTS; i++) {
            long queryBegin = System.currentTimeMillis();
            this.rs = this.stmt.executeQuery("SELECT SQL_CACHE * FROM retrievalPerfTestHeap");

            long queryEnd = System.currentTimeMillis();
            averageQueryTimeMs += (double) (queryEnd - queryBegin) / NUM_TESTS;

            long traverseBegin = System.currentTimeMillis();

            while (this.rs.next()) {
                this.rs.getInt(1);
                this.rs.getString(2);
            }

            long traverseEnd = System.currentTimeMillis();
            averageTraversalTimeMs += (double) (traverseEnd - traverseBegin) / NUM_TESTS;
        }

        double fullEnd = System.currentTimeMillis();
        double fullTime = (fullEnd - fullBegin) / 1000;
        double queriesPerSec = NUM_TESTS / fullTime;
        double rowsPerSec = NUM_ROWS * NUM_TESTS / fullTime;
        System.out.println("\nQuery Cache From Heap Retrieval\n");
        System.out.println("Full test took: " + fullTime + " seconds.");
        System.out.println("Queries/second: " + queriesPerSec);
        System.out.println("Rows/second: " + rowsPerSec);
        System.out.println("Avg. Query Exec Time: " + averageQueryTimeMs + " ms");
        System.out.println("Avg. Traversal Time: " + averageTraversalTimeMs + " ms");

        // We're doing something wrong if we can't beat 45 seconds :(
        assertTrue(fullTime < 45);
    }

    /**
     * Tests retrieval from HEAP tables
     *
     * @throws Exception
     */
    @Test
    public void testRetrievalHeap() throws Exception {
        double fullBegin = System.currentTimeMillis();
        double averageQueryTimeMs = 0;
        double averageTraversalTimeMs = 0;

        for (int i = 0; i < NUM_TESTS; i++) {
            long queryBegin = System.currentTimeMillis();
            this.rs = this.stmt.executeQuery("SELECT * FROM retrievalPerfTestHeap");

            long queryEnd = System.currentTimeMillis();
            averageQueryTimeMs += (double) (queryEnd - queryBegin) / NUM_TESTS;

            long traverseBegin = System.currentTimeMillis();

            while (this.rs.next()) {
                this.rs.getInt(1);
                this.rs.getString(2);
            }

            long traverseEnd = System.currentTimeMillis();
            averageTraversalTimeMs += (double) (traverseEnd - traverseBegin) / NUM_TESTS;
        }

        double fullEnd = System.currentTimeMillis();
        double fullTime = (fullEnd - fullBegin) / 1000;
        double queriesPerSec = NUM_TESTS / fullTime;
        double rowsPerSec = NUM_ROWS * NUM_TESTS / fullTime;
        System.out.println("\nHEAP Table Retrieval\n");
        System.out.println("Full test took: " + fullTime + " seconds.");
        System.out.println("Queries/second: " + queriesPerSec);
        System.out.println("Rows/second: " + rowsPerSec);
        System.out.println("Avg. Query Exec Time: " + averageQueryTimeMs + " ms");
        System.out.println("Avg. Traversal Time: " + averageTraversalTimeMs + " ms");

        // We're doing something wrong if we can't beat 45 seconds :(
        assertTrue(fullTime < 45);
    }

    /**
     * Tests retrieval speed from MyISAM type tables
     *
     * @throws Exception
     */
    @Test
    public void testRetrievalMyIsam() throws Exception {
        double fullBegin = System.currentTimeMillis();
        double averageQueryTimeMs = 0;
        double averageTraversalTimeMs = 0;

        for (int i = 0; i < NUM_TESTS; i++) {
            long queryBegin = System.currentTimeMillis();
            this.rs = this.stmt.executeQuery("SELECT * FROM retrievalPerfTestMyIsam");

            long queryEnd = System.currentTimeMillis();
            averageQueryTimeMs += (double) (queryEnd - queryBegin) / NUM_TESTS;

            long traverseBegin = System.currentTimeMillis();

            while (this.rs.next()) {
                this.rs.getInt(1);
                this.rs.getString(2);
            }

            long traverseEnd = System.currentTimeMillis();
            averageTraversalTimeMs += (double) (traverseEnd - traverseBegin) / NUM_TESTS;
        }

        double fullEnd = System.currentTimeMillis();
        double fullTime = (fullEnd - fullBegin) / 1000;
        double queriesPerSec = NUM_TESTS / fullTime;
        double rowsPerSec = NUM_ROWS * NUM_TESTS / fullTime;
        System.out.println("\nMyIsam Retrieval\n");
        System.out.println("Full test took: " + fullTime + " seconds.");
        System.out.println("Queries/second: " + queriesPerSec);
        System.out.println("Rows/second: " + rowsPerSec);
        System.out.println("Avg. Query Exec Time: " + averageQueryTimeMs + " ms");
        System.out.println("Avg. Traversal Time: " + averageTraversalTimeMs + " ms");

        // We're doing something wrong if we can't beat 45 seconds :(
        assertTrue(fullTime < 45);
    }

}
