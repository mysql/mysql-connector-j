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
package testsuite.perf;

import testsuite.BaseTestCase;

/**
 * Simplistic test for performance regression.
 * 
 * @author Mark Matthews
 */
public class RetrievalPerfTest extends BaseTestCase {

    private final static int NUM_TESTS = 10000;
    private final static int NUM_ROWS = 80;
    
	/**
	 * Constructor for RetrievalPerfTest.
	 * @param name
	 */
	public RetrievalPerfTest(String name) {
		super(name);
	}

	public static void main(String[] args) {
        new RetrievalPerfTest("testRetrievalMyIsam").run();
        new RetrievalPerfTest("testRetrievalHeap").run();
        new RetrievalPerfTest("testRetrievalCached").run();
	}

    /**
     * Tests retrieval from HEAP tables
     */    
    public void testRetrievalHeap() throws Exception {
        double fullBegin = System.currentTimeMillis();
        double averageQueryTimeMs = 0;
        double averageTraversalTimeMs = 0;
        
        for (int i = 0; i < NUM_TESTS; i++) {
            long queryBegin = System.currentTimeMillis();
            
            rs = stmt.executeQuery("SELECT * FROM retrievalPerfTestHeap");
            
            long queryEnd = System.currentTimeMillis();
            
            averageQueryTimeMs += (double)(queryEnd - queryBegin) / NUM_TESTS;
            
            long traverseBegin = System.currentTimeMillis();
            
            while (rs.next()) {
                rs.getInt(1);
                rs.getString(2);
            }
            
            long traverseEnd = System.currentTimeMillis();
            
            averageTraversalTimeMs += (double)(traverseEnd - traverseBegin) / NUM_TESTS;
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
     * Tests retrieval from the query cache
     */
    public void testRetrievalCached() throws Exception {
        stmt.executeUpdate("SET QUERY_CACHE_TYPE = DEMAND");
        
        double fullBegin = System.currentTimeMillis();
        double averageQueryTimeMs = 0;
        double averageTraversalTimeMs = 0;
        
        for (int i = 0; i < NUM_TESTS; i++) {
            long queryBegin = System.currentTimeMillis();
            
            rs = stmt.executeQuery("SELECT SQL_CACHE * FROM retrievalPerfTestHeap");
            
            long queryEnd = System.currentTimeMillis();
            
            averageQueryTimeMs += (double)(queryEnd - queryBegin) / NUM_TESTS;
            
            long traverseBegin = System.currentTimeMillis();
            
            while (rs.next()) {
                rs.getInt(1);
                rs.getString(2);
            }
            
            long traverseEnd = System.currentTimeMillis();
            
            averageTraversalTimeMs += (double)(traverseEnd - traverseBegin) / NUM_TESTS;
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
     * Tests retrieval speed from MyISAM type tables
     */
    public void testRetrievalMyIsam() throws Exception {
        double fullBegin = System.currentTimeMillis();
        double averageQueryTimeMs = 0;
        double averageTraversalTimeMs = 0;
        
        for (int i = 0; i < NUM_TESTS; i++) {
            long queryBegin = System.currentTimeMillis();
            
            rs = stmt.executeQuery("SELECT * FROM retrievalPerfTestMyIsam");
            
            long queryEnd = System.currentTimeMillis();
            
            averageQueryTimeMs += (double)(queryEnd - queryBegin) / NUM_TESTS;
            
            long traverseBegin = System.currentTimeMillis();
            
            while (rs.next()) {
                rs.getInt(1);
                rs.getString(2);
            }
            
            long traverseEnd = System.currentTimeMillis();
            
            averageTraversalTimeMs += (double)(traverseEnd - traverseBegin) / NUM_TESTS;
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
    
	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	public void setUp() throws Exception {
		super.setUp();
        
        stmt.executeUpdate("DROP TABLE IF EXISTS retrievalPerfTestHeap");
        stmt.executeUpdate("DROP TABLE IF EXISTS retrievalPerfTestMyIsam");
        
        stmt.executeUpdate("CREATE TABLE retrievalPerfTestHeap (priKey INT NOT NULL PRIMARY KEY,"
         + "charField VARCHAR(80)) TYPE=HEAP");
        stmt.executeUpdate("CREATE TABLE retrievalPerfTestMyIsam (priKey INT NOT NULL PRIMARY KEY,"
         + "charField VARCHAR(80)) TYPE=MyISAM");
         
        for (int i = 0; i < NUM_ROWS; i++) {
            stmt.executeUpdate("INSERT INTO retrievalPerfTestHeap (priKey, charField) VALUES (" + i + ",'abcdefghijklmnopqrstuvqxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')");
            stmt.executeUpdate("INSERT INTO retrievalPerfTestMyIsam (priKey, charField) VALUES (" + i + ",'abcdefghijklmnopqrstuvqxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')");
        }
	}

	/**
	 * @see junit.framework.TestCase#tearDown()
	 */
	public void tearDown() throws Exception {
        
        stmt.executeUpdate("DROP TABLE IF EXISTS retrievalPerfTestHeap");
        stmt.executeUpdate("DROP TABLE IF EXISTS retrievalPerfTestMyIsam");
        
		super.tearDown();
           
	}

}
