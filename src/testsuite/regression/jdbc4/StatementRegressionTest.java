/*
  Copyright (c) 2013, 2016, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
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

package testsuite.regression.jdbc4;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.mysql.jdbc.CallableStatement;
import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.ReplicationConnection;
import com.mysql.jdbc.StatementImpl;
import com.mysql.jdbc.Util;

import testsuite.BaseTestCase;

public class StatementRegressionTest extends BaseTestCase {
    /**
     * Creates a new StatementRegressionTest.
     * 
     * @param name
     *            the name of the test
     */
    public StatementRegressionTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(StatementRegressionTest.class);
    }

    /**
     * Tests fix for BUG#68916 - closeOnCompletion doesn't work.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug68916() throws Exception {
        // Prepare common test objects
        createProcedure("testBug68916_proc", "() BEGIN SELECT 1; SELECT 2; SELECT 3; END");
        createTable("testBug68916_tbl", "(fld1 INT NOT NULL AUTO_INCREMENT, fld2 INT, PRIMARY KEY(fld1))");

        // STEP 1: Test using standard connection (no properties)
        subTestBug68916ForStandardConnection();

        // STEP 2: Test using connection property holdResultsOpenOverStatementClose=true
        subTestBug68916ForHoldResultsOpenOverStatementClose();

        // STEP 3: Test using connection property dontTrackOpenResources=true
        subTestBug68916ForDontTrackOpenResources();

        // STEP 4: Test using connection property allowMultiQueries=true
        subTestBug68916ForAllowMultiQueries();

        // STEP 5: Test concurrent Statement/ResultSet sharing same Connection
        subTestBug68916ForConcurrency();
    }

    private void subTestBug68916ForStandardConnection() throws Exception {
        Connection testConnection = this.conn;
        String testStep;
        ResultSet testResultSet1, testResultSet2, testResultSet3;

        // We are testing against code that was compiled with Java 6, so methods isCloseOnCompletion() and
        // closeOnCompletion() aren't available in the Statement interface. We need to test directly our implementations.
        StatementImpl testStatement = null;
        PreparedStatement testPrepStatement = null;
        CallableStatement testCallStatement = null;

        /*
         * Testing with standard connection (no properties)
         */
        testStep = "Standard Connection";

        /*
         * SUB-STEP 0: The basics (connection without properties)
         */
        // **testing Statement**
        // ResultSets should be closed when owning Statement is closed
        testStatement = (StatementImpl) testConnection.createStatement();

        assertFalse(testStep + ".ST:0. Statement.isCloseOnCompletion(): false by default.", testStatement.isCloseOnCompletion());
        assertFalse(testStep + ".ST:0. Statement.isClosed(): false.", testStatement.isClosed());

        testStatement.closeOnCompletion();

        assertTrue(testStep + ".ST:0. Statement.isCloseOnCompletion(): true after Statement.closeOnCompletion().", testStatement.isCloseOnCompletion());
        assertFalse(testStep + ".ST:0. Statement.isClosed(): false.", testStatement.isClosed());

        testStatement.closeOnCompletion();

        assertTrue(testStep + ".ST:0. Statement.isCloseOnCompletion(): true after 2nd Statement.closeOnCompletion().", testStatement.isCloseOnCompletion());

        // test Statement.close()
        testResultSet1 = testStatement.executeQuery("SELECT 1");

        assertFalse(testStep + ".ST:0. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:0. Statement.isClosed(): false.", testStatement.isClosed());

        testStatement.close();

        assertTrue(testStep + ".ST:0. ResultSet.isClosed(): true after Statement.Close().", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:0. Statement.isClosed(): true after Statement.Close().", testStatement.isClosed());

        // **testing PreparedStatement**
        // ResultSets should be closed when owning PreparedStatement is closed
        testPrepStatement = (com.mysql.jdbc.PreparedStatement) testConnection.prepareStatement("SELECT 1");

        assertFalse(testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): false by default.", testPrepStatement.isCloseOnCompletion());
        assertFalse(testStep + ".PS:0. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        testPrepStatement.closeOnCompletion();

        assertTrue(testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): true after Statement.closeOnCompletion().",
                testPrepStatement.isCloseOnCompletion());
        assertFalse(testStep + ".PS:0. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        testPrepStatement.closeOnCompletion();

        assertTrue(testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): true after 2nd Statement.closeOnCompletion().",
                testPrepStatement.isCloseOnCompletion());

        // test PreparedStatement.close()
        testPrepStatement.execute();
        testResultSet1 = testPrepStatement.getResultSet();

        assertFalse(testStep + ".PS:0. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".PS:0. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        testPrepStatement.close();

        assertTrue(testStep + ".PS:0. ResultSet.isClosed(): true after PreparedStatement.close().", testResultSet1.isClosed());
        assertTrue(testStep + ".PS:0. PreparedStatement.isClosed(): true after PreparedStatement.close().", testPrepStatement.isClosed());

        /*
         * SUB-STEP 1: One ResultSet (connection without properties)
         */
        // **testing Statement**
        // Statement using closeOnCompletion should be closed when last ResultSet is closed
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");

        assertFalse(testStep + ".ST:1. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false.", testStatement.isClosed());

        while (testResultSet1.next()) {
        }

        assertFalse(testStep + ".ST:1. ResultSet.isClosed(): false after ResultSet have reached the end.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".ST:1. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:1. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        // test implicit resultset close, keeping statement open, when following with an executeBatch()
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testStatement.addBatch("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)");
        testStatement.executeBatch();

        assertTrue(testStep + ".ST:1. ResultSet.isClosed(): true after executeBatch() in same Statement.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet1 = testStatement.getGeneratedKeys();
        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".ST:1. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:1. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        // test implicit resultset close keeping statement open, when following with an executeUpdate()
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)", com.mysql.jdbc.Statement.RETURN_GENERATED_KEYS);

        assertTrue(testStep + ".ST:1. ResultSet.isClosed(): true after executeUpdate() in same Statement.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet1 = testStatement.getGeneratedKeys();
        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".ST:1. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:1. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        // **testing PreparedStatement**
        // PreparedStatement using closeOnCompletion should be closed when last ResultSet is closed
        testPrepStatement = (PreparedStatement) testConnection.prepareStatement("SELECT 1");
        testPrepStatement.closeOnCompletion();

        testResultSet1 = testPrepStatement.executeQuery();

        assertFalse(testStep + ".PS:1. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".PS:1. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        while (testResultSet1.next()) {
        }

        assertFalse(testStep + ".PS:1. ResultSet.isClosed(): false after ResultSet have reached the end.", testResultSet1.isClosed());
        assertFalse(testStep + ".PS:1. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".PS:1. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".PS:1. PreparedStatement.isClosed(): true when last ResultSet is closed.", testPrepStatement.isClosed());

        /*
         * SUB-STEP 2: Multiple ResultSets, sequentially (connection without properties)
         */
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testResultSet2 = testStatement.executeQuery("SELECT 2"); // closes testResultSet1

        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true after 2nd Statement.executeQuery().", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:2. Statement.isClosed(): false.", testStatement.isClosed());

        while (testResultSet2.next()) {
        }

        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false after ResultSet have reached the end.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:2. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet3 = testStatement.executeQuery("SELECT 3"); // closes testResultSet2

        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true after 3rd Statement.executeQuery().", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false.", testResultSet3.isClosed());
        assertFalse(testStep + ".ST:2. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet3.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true.", testResultSet3.isClosed());
        assertTrue(testStep + ".ST:2. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        /*
         * SUB-STEP 3: Multiple ResultSets, returned at once (connection without properties)
         */
        // **testing Statement**
        // Statement using closeOnCompletion should be closed when last ResultSet is closed
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        assertTrue(testStep + ".ST:3. There should be some ResultSets.", testStatement.execute("CALL testBug68916_proc"));
        testResultSet1 = testStatement.getResultSet();

        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:3. Statement.isClosed(): false.", testStatement.isClosed());

        assertTrue(testStep + ".ST:3. There should be more ResultSets.", testStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        testResultSet2 = testStatement.getResultSet();

        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.KEEP_CURRENT_RESULT).", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:3. Statement.isClosed(): false.", testStatement.isClosed());

        assertTrue(testStep + ".ST:3. There should be more ResultSets.", testStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS));
        testResultSet3 = testStatement.getResultSet();

        assertTrue(testStep + ".ST:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false.", testResultSet3.isClosed());
        assertFalse(testStep + ".ST:3. Statement.isClosed(): false.", testStatement.isClosed());

        // no more ResultSets, must close Statement
        assertFalse(testStep + ".ST:3. There should be no more ResultSets.", testStatement.getMoreResults());

        assertTrue(testStep + ".ST:3. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:3. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertTrue(testStep + ".ST:3. ResultSet.isClosed(): true after last Satement.getMoreResults().", testResultSet3.isClosed());
        assertTrue(testStep + ".ST:3. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        // **testing CallableStatement**
        // CallableStatement using closeOnCompletion should be closed when last ResultSet is closed
        testCallStatement = (CallableStatement) testConnection.prepareCall("CALL testBug68916_proc");
        testCallStatement.closeOnCompletion();

        assertTrue(testStep + ".CS:3. There should be some ResultSets.", testCallStatement.execute());
        testResultSet1 = testCallStatement.getResultSet();

        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".CS:3. CallableStatement.isClosed(): false.", testCallStatement.isClosed());

        assertTrue(testStep + ".CS:3. There should be more ResultSets.", testCallStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        testResultSet2 = testCallStatement.getResultSet();

        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.KEEP_CURRENT_RESULT).", testResultSet1.isClosed());
        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false.", testResultSet2.isClosed());
        assertFalse(testStep + ".CS:3. CallableStatement.isClosed(): false.", testCallStatement.isClosed());

        assertTrue(testStep + ".CS:3. There should be more ResultSets.", testCallStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS));
        testResultSet3 = testCallStatement.getResultSet();

        assertTrue(testStep + ".CS:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).", testResultSet1.isClosed());
        assertTrue(testStep + ".CS:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).", testResultSet2.isClosed());
        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false.", testResultSet3.isClosed());
        assertFalse(testStep + ".CS:3. CallableStatement.isClosed(): false.", testCallStatement.isClosed());

        // no more ResultSets, must close Statement
        assertFalse(testStep + ".CS:3. There should be no more ResultSets.", testCallStatement.getMoreResults());

        assertTrue(testStep + ".CS:3. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".CS:3. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertTrue(testStep + ".CS:3. ResultSet.isClosed(): true after last Satement.getMoreResults().", testResultSet3.isClosed());
        assertTrue(testStep + ".CS:3. CallableStatement.isClosed(): true when last ResultSet is closed.", testCallStatement.isClosed());

        /*
         * SUB-STEP 4: Generated Keys ResultSet (connection without properties)
         */
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)", Statement.RETURN_GENERATED_KEYS);

        testResultSet1 = testStatement.getGeneratedKeys();
        assertTrue(testStep + ".ST:4. Statement.getGeneratedKeys(): should return some values.", testResultSet1.next());

        assertFalse(testStep + ".ST:4. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:4. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".ST:4. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:4. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        // test again and combine with simple query
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (4), (5), (6)", Statement.RETURN_GENERATED_KEYS);

        testResultSet1 = testStatement.getGeneratedKeys();
        assertTrue(testStep + ".ST:4. Statement.getGeneratedKeys(): should return some values.", testResultSet1.next());

        testResultSet2 = testStatement.executeQuery("SELECT 2");

        assertTrue(testStep + ".ST:4. ResultSet.isClosed(): true after executeQuery() in same Statement.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:4. ResultSet.isClosed(): false.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:4. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet2.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".ST:4. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:4. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertTrue(testStep + ".ST:4. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());
    }

    private void subTestBug68916ForHoldResultsOpenOverStatementClose() throws Exception {
        Connection testConnection;
        String testStep;
        ResultSet testResultSet1, testResultSet2, testResultSet3;

        // We are testing against code that was compiled with Java 6, so methods isCloseOnCompletion() and
        // closeOnCompletion() aren't available in the Statement interface. We need to test directly our
        // implementations.
        StatementImpl testStatement = null;
        PreparedStatement testPrepStatement = null;
        CallableStatement testCallStatement = null;

        /*
         * Testing with connection property holdResultsOpenOverStatementClose=true
         */
        testStep = "Conn. Prop. 'holdResultsOpenOverStatementClose'";
        testConnection = getConnectionWithProps("holdResultsOpenOverStatementClose=true");

        /*
         * SUB-STEP 0: The basics (holdResultsOpenOverStatementClose=true)
         */
        // **testing Statement**
        // ResultSets should stay open when owning Statement is closed
        testStatement = (StatementImpl) testConnection.createStatement();

        assertFalse(testStep + ".ST:0. Statement.isCloseOnCompletion(): false dy default.", testStatement.isCloseOnCompletion());
        assertFalse(testStep + ".ST:0. Statement.isClosed(): false.", testStatement.isClosed());

        testStatement.closeOnCompletion();

        assertTrue(testStep + ".ST:0. Statement.isCloseOnCompletion(): true after Statement.closeOnCompletion().", testStatement.isCloseOnCompletion());
        assertFalse(testStep + ".ST:0. Statement.isClosed(): false.", testStatement.isClosed());

        testStatement.closeOnCompletion();

        assertTrue(testStep + ".ST:0. Statement.isCloseOnCompletion(): true after 2nd Statement.closeOnCompletion().", testStatement.isCloseOnCompletion());

        // test Statement.close()
        testResultSet1 = testStatement.executeQuery("SELECT 1");

        assertFalse(testStep + ".ST:0. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:0. Statement.isClosed(): false.", testStatement.isClosed());

        testStatement.close();

        assertFalse(testStep + ".ST:0. ResultSet.isClosed(): false after Statement.Close().", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:0. Statement.isClosed(): true after Statement.Close().", testStatement.isClosed());

        // **testing PreparedStatement**
        // ResultSets should stay open when owning PreparedStatement is closed
        testPrepStatement = (com.mysql.jdbc.PreparedStatement) testConnection.prepareStatement("SELECT 1");

        assertFalse(testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): false by default.", testPrepStatement.isCloseOnCompletion());
        assertFalse(testStep + ".PS:0. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        testPrepStatement.closeOnCompletion();

        assertTrue(testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): true after Statement.closeOnCompletion().",
                testPrepStatement.isCloseOnCompletion());
        assertFalse(testStep + ".PS:0. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        testPrepStatement.closeOnCompletion();

        assertTrue(testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): true after 2nd Statement.closeOnCompletion().",
                testPrepStatement.isCloseOnCompletion());

        // test PreparedStatement.close()
        testPrepStatement.execute();
        testResultSet1 = testPrepStatement.getResultSet();

        assertFalse(testStep + ".PS:0. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".PS:0. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        testPrepStatement.close();

        assertFalse(testStep + ".PS:0. ResultSet.isClosed(): false after PreparedStatement.close().", testResultSet1.isClosed());
        assertTrue(testStep + ".PS:0. PreparedStatement.isClosed(): true after PreparedStatement.close().", testPrepStatement.isClosed());

        /*
         * SUB-STEP 1: One ResultSet (holdResultsOpenOverStatementClose=true)
         */
        // **testing Statement**
        // Statement using closeOnCompletion should be closed when last ResultSet is closed
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");

        assertFalse(testStep + ".ST:1. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false.", testStatement.isClosed());

        while (testResultSet1.next()) {
        }

        assertFalse(testStep + ".ST:1. ResultSet.isClosed(): false after ResultSet have reached the end.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".ST:1. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:1. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        // test implicit resultset close keeping statement open, when following with an executeBatch()
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testStatement.addBatch("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)");
        testStatement.executeBatch();

        assertFalse(testStep + ".ST:1. ResultSet.isClosed(): false after executeBatch() in same Statement.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet1 = testStatement.getGeneratedKeys();
        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".ST:1. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:1. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        // test implicit resultset close keeping statement open, when following with an executeUpdate()
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)", Statement.RETURN_GENERATED_KEYS);

        assertFalse(testStep + ".ST:1. ResultSet.isClosed(): false after executeUpdate() in same Statement.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet1 = testStatement.getGeneratedKeys();
        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".ST:1. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:1. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        // **testing PreparedStatement**
        // PreparedStatement using closeOnCompletion should be closed when last ResultSet is closed
        testPrepStatement = (PreparedStatement) testConnection.prepareStatement("SELECT 1");
        testPrepStatement.closeOnCompletion();

        testResultSet1 = testPrepStatement.executeQuery();

        assertFalse(testStep + ".PS:1. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".PS:1. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        while (testResultSet1.next()) {
        }

        assertFalse(testStep + ".PS:1. ResultSet.isClosed(): false after ResultSet have reached the end.", testResultSet1.isClosed());
        assertFalse(testStep + ".PS:1. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".PS:1. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".PS:1. PreparedStatement.isClosed(): true when last ResultSet is closed.", testPrepStatement.isClosed());

        /*
         * SUB-STEP 2: Multiple ResultSets, sequentially (holdResultsOpenOverStatementClose=true)
         */
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testResultSet2 = testStatement.executeQuery("SELECT 2"); // mustn't close testResultSet1

        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false after 2nd Statement.executeQuery().", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:2. Statement.isClosed(): false.", testStatement.isClosed());

        while (testResultSet2.next()) {
        }

        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false after ResultSet have reached the end.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:2. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet3 = testStatement.executeQuery("SELECT 3"); // mustn't close testResultSet1 nor testResultSet2

        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false after 3rd Statement.executeQuery().", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false.", testResultSet3.isClosed());
        assertFalse(testStep + ".ST:2. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet2.close();

        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false.", testResultSet3.isClosed());
        assertFalse(testStep + ".ST:2. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet1.close();
        testResultSet3.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true.", testResultSet3.isClosed());
        assertTrue(testStep + ".ST:2. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        /*
         * SUB-STEP 3: Multiple ResultSets, returned at once (holdResultsOpenOverStatementClose=true)
         */
        // **testing Statement**
        // Statement using closeOnCompletion should be closed when last ResultSet is closed
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        assertTrue(testStep + ".ST:3. There should be some ResultSets.", testStatement.execute("CALL testBug68916_proc"));
        testResultSet1 = testStatement.getResultSet();

        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:3. Statement.isClosed(): false.", testStatement.isClosed());

        assertTrue(testStep + ".ST:3. There should be more ResultSets.", testStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        testResultSet2 = testStatement.getResultSet();

        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.KEEP_CURRENT_RESULT).", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:3. Statement.isClosed(): false.", testStatement.isClosed());

        assertTrue(testStep + ".ST:3. There should be more ResultSets.", testStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS));
        testResultSet3 = testStatement.getResultSet();

        assertTrue(testStep + ".ST:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false.", testResultSet3.isClosed());
        assertFalse(testStep + ".ST:3. Statement.isClosed(): false.", testStatement.isClosed());

        // no more ResultSets, must close Statement
        assertFalse(testStep + ".ST:3. There should be no more ResultSets.", testStatement.getMoreResults());

        assertTrue(testStep + ".ST:3. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:3. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertTrue(testStep + ".ST:3. ResultSet.isClosed(): true after last Satement.getMoreResults().", testResultSet3.isClosed());
        assertTrue(testStep + ".ST:3. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        // **testing CallableStatement**
        // CallableStatement using closeOnCompletion should be closed when last ResultSet is closed
        testCallStatement = (CallableStatement) testConnection.prepareCall("CALL testBug68916_proc");
        testCallStatement.closeOnCompletion();

        assertTrue(testStep + ".CS:3. There should be some ResultSets.", testCallStatement.execute());
        testResultSet1 = testCallStatement.getResultSet();

        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".CS:3. CallableStatement.isClosed(): false.", testCallStatement.isClosed());

        assertTrue(testStep + ".CS:3. There should be more ResultSets.", testCallStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        testResultSet2 = testCallStatement.getResultSet();

        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.KEEP_CURRENT_RESULT).", testResultSet1.isClosed());
        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false.", testResultSet2.isClosed());
        assertFalse(testStep + ".CS:3. CallableStatement.isClosed(): false.", testCallStatement.isClosed());

        assertTrue(testStep + ".CS:3. There should be more ResultSets.", testCallStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS));
        testResultSet3 = testCallStatement.getResultSet();

        assertTrue(testStep + ".CS:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).", testResultSet1.isClosed());
        assertTrue(testStep + ".CS:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).", testResultSet2.isClosed());
        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false.", testResultSet3.isClosed());
        assertFalse(testStep + ".CS:3. CallableStatement.isClosed(): false.", testCallStatement.isClosed());

        // no more ResultSets, must close Statement
        assertFalse(testStep + ".CS:3. There should be no more ResultSets.", testCallStatement.getMoreResults());

        assertTrue(testStep + ".CS:3. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".CS:3. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertTrue(testStep + ".CS:3. ResultSet.isClosed(): true after last Satement.getMoreResults().", testResultSet3.isClosed());
        assertTrue(testStep + ".CS:3. CallableStatement.isClosed(): true when last ResultSet is closed.", testCallStatement.isClosed());

        /*
         * SUB-STEP 4: Generated Keys ResultSet (holdResultsOpenOverStatementClose=true)
         */
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)", Statement.RETURN_GENERATED_KEYS);

        testResultSet1 = testStatement.getGeneratedKeys();
        assertTrue(testStep + ".ST:4. Statement.getGeneratedKeys(): should return some values.", testResultSet1.next());

        assertFalse(testStep + ".ST:4. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:4. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".ST:4. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:4. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        // test again and combine with simple query
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (4), (5), (6)", Statement.RETURN_GENERATED_KEYS);

        testResultSet1 = testStatement.getGeneratedKeys();
        assertTrue(testStep + ".ST:4. Statement.getGeneratedKeys(): should return some values.", testResultSet1.next());

        testResultSet2 = testStatement.executeQuery("SELECT 2");

        assertFalse(testStep + ".ST:4. ResultSet.isClosed(): false after executeQuery() in same Statement.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:4. ResultSet.isClosed(): false.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:4. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet2.close();

        assertFalse(testStep + ".ST:4. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:4. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:4. Statement.isClosed(): false when last ResultSet is closed (still one open).", testStatement.isClosed());

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".ST:4. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:4. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertTrue(testStep + ".ST:4. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        testConnection.close();
    }

    private void subTestBug68916ForDontTrackOpenResources() throws Exception {
        Connection testConnection;
        String testStep;
        ResultSet testResultSet1, testResultSet2, testResultSet3;

        // We are testing against code that was compiled with Java 6, so methods isCloseOnCompletion() and
        // closeOnCompletion() aren't available in the Statement interface. We need to test directly our
        // implementations.
        StatementImpl testStatement = null;
        PreparedStatement testPrepStatement = null;
        CallableStatement testCallStatement = null;

        /*
         * Testing with connection property dontTrackOpenResources=true
         */
        testStep = "Conn. Prop. 'dontTrackOpenResources'";
        testConnection = getConnectionWithProps("dontTrackOpenResources=true");

        /*
         * SUB-STEP 0: The basics (dontTrackOpenResources=true)
         */
        // **testing Statement**
        // ResultSets should stay open when owning Statement is closed
        testStatement = (StatementImpl) testConnection.createStatement();

        assertFalse(testStep + ".ST:0. Statement.isCloseOnCompletion(): false by default.", testStatement.isCloseOnCompletion());
        assertFalse(testStep + ".ST:0. Statement.isClosed(): false.", testStatement.isClosed());

        testStatement.closeOnCompletion();

        assertTrue(testStep + ".ST:0. Statement.isCloseOnCompletion(): true after Statement.closeOnCompletion().", testStatement.isCloseOnCompletion());
        assertFalse(testStep + ".ST:0. Statement.isClosed(): false.", testStatement.isClosed());

        testStatement.closeOnCompletion();

        assertTrue(testStep + ".ST:0. Statement.isCloseOnCompletion(): true after 2nd Statement.closeOnCompletion().", testStatement.isCloseOnCompletion());

        // test Statement.close()
        testResultSet1 = testStatement.executeQuery("SELECT 1");

        assertFalse(testStep + ".ST:0. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:0. Statement.isClosed(): false.", testStatement.isClosed());

        testStatement.close();

        assertFalse(testStep + ".ST:0. ResultSet.isClosed(): false after Statement.Close().", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:0. Statement.isClosed(): true after Statement.Close().", testStatement.isClosed());

        // **testing PreparedStatement**
        // ResultSets should stay open when owning PreparedStatement is closed
        testPrepStatement = (com.mysql.jdbc.PreparedStatement) testConnection.prepareStatement("SELECT 1");

        assertFalse(testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): false by default.", testPrepStatement.isCloseOnCompletion());
        assertFalse(testStep + ".PS:0. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        testPrepStatement.closeOnCompletion();

        assertTrue(testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): true after Statement.closeOnCompletion().",
                testPrepStatement.isCloseOnCompletion());
        assertFalse(testStep + ".PS:0. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        testPrepStatement.closeOnCompletion();

        assertTrue(testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): true after 2nd Statement.closeOnCompletion().",
                testPrepStatement.isCloseOnCompletion());

        // test PreparedStatement.close()
        testPrepStatement.execute();
        testResultSet1 = testPrepStatement.getResultSet();

        assertFalse(testStep + ".PS:0. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".PS:0. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        testPrepStatement.close();

        assertFalse(testStep + ".PS:0. ResultSet.isClosed(): false after PreparedStatement.close().", testResultSet1.isClosed());
        assertTrue(testStep + ".PS:0. PreparedStatement.isClosed(): true after PreparedStatement.close().", testPrepStatement.isClosed());

        /*
         * SUB-STEP 1: One ResultSet (dontTrackOpenResources=true)
         */
        // **testing Statement**
        // Statement, although using closeOnCompletion, shouldn't be closed when last ResultSet is closed
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");

        assertFalse(testStep + ".ST:1. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false.", testStatement.isClosed());

        while (testResultSet1.next()) {
        }

        assertFalse(testStep + ".ST:1. ResultSet.isClosed(): false after ResultSet have reached the end.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet1.close(); // although it's last open ResultSet, Statement mustn't be closed

        assertTrue(testStep + ".ST:1. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false when last ResultSet is closed.", testStatement.isClosed());

        // test implicit resultset (not) close, keeping statement open, when following with an executeBatch()
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testStatement.addBatch("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)");
        testStatement.executeBatch();

        assertFalse(testStep + ".ST:1. ResultSet.isClosed(): false after executeBatch() in same Statement.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet1 = testStatement.getGeneratedKeys();
        testResultSet1.close(); // although it's last open ResultSet, Statement mustn't be closed

        assertTrue(testStep + ".ST:1. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false when last ResultSet is closed.", testStatement.isClosed());

        // test implicit resultset (not) close keeping statement open, when following with an executeUpdate()
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)", Statement.RETURN_GENERATED_KEYS);

        assertFalse(testStep + ".ST:1. ResultSet.isClosed(): false after executeUpdate() in same Statement.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet1 = testStatement.getGeneratedKeys();
        testResultSet1.close(); // although it's last open ResultSet, Statement mustn't be closed

        assertTrue(testStep + ".ST:1. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false when last ResultSet is closed.", testStatement.isClosed());

        // **testing PreparedStatement**
        // PreparedStatement, although using closeOnCompletion, shouldn't be closed when last ResultSet is closed
        testPrepStatement = (PreparedStatement) testConnection.prepareStatement("SELECT 1");
        testPrepStatement.closeOnCompletion();

        testResultSet1 = testPrepStatement.executeQuery();

        assertFalse(testStep + ".PS:1. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".PS:1. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        while (testResultSet1.next()) {
        }

        assertFalse(testStep + ".PS:1. ResultSet.isClosed(): false after ResultSet have reached the end.", testResultSet1.isClosed());
        assertFalse(testStep + ".PS:1. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        testResultSet1.close(); // although it's last open ResultSet, Statement mustn't be closed

        assertTrue(testStep + ".PS:1. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertFalse(testStep + ".PS:1. PreparedStatement.isClosed(): false when last ResultSet is closed.", testPrepStatement.isClosed());

        /*
         * SUB-STEP 2: Multiple ResultSets, sequentially (dontTrackOpenResources=true)
         */
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testResultSet2 = testStatement.executeQuery("SELECT 2"); // mustn't close testResultSet1

        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false after 2nd Statement.executeQuery().", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:2. Statement.isClosed(): false.", testStatement.isClosed());

        while (testResultSet2.next()) {
        }

        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false after ResultSet have reached the end.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:2. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet3 = testStatement.executeQuery("SELECT 3"); // mustn't close testResultSet1 nor testResultSet2

        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false after 3rd Statement.executeQuery().", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false.", testResultSet3.isClosed());
        assertFalse(testStep + ".ST:2. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet2.close();

        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false.", testResultSet3.isClosed());
        assertFalse(testStep + ".ST:2. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet1.close();
        testResultSet3.close(); // although it's last open ResultSet, Statement mustn't be closed

        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true.", testResultSet3.isClosed());
        assertFalse(testStep + ".ST:2. Statement.isClosed(): false when last ResultSet is closed.", testStatement.isClosed());

        /*
         * SUB-STEP 3: Multiple ResultSets, returned at once (dontTrackOpenResources=true)
         */
        // **testing Statement**
        // Statement, although using closeOnCompletion, shouldn't be closed when last ResultSet is closed
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        assertTrue(testStep + ".ST:3. There should be some ResultSets.", testStatement.execute("CALL testBug68916_proc"));
        testResultSet1 = testStatement.getResultSet();

        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:3. Statement.isClosed(): false.", testStatement.isClosed());

        assertTrue(testStep + ".ST:3. There should be more ResultSets.", testStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        testResultSet2 = testStatement.getResultSet();

        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.KEEP_CURRENT_RESULT).", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:3. Statement.isClosed(): false.", testStatement.isClosed());

        assertTrue(testStep + ".ST:3. There should be more ResultSets.", testStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS));
        testResultSet3 = testStatement.getResultSet();

        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false.", testResultSet3.isClosed());
        assertFalse(testStep + ".ST:3. Statement.isClosed(): false.", testStatement.isClosed());

        assertFalse(testStep + ".ST:3. There should be no more ResultSets.", testStatement.getMoreResults());

        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false after last Satement.getMoreResults().", testResultSet3.isClosed());
        assertFalse(testStep + ".ST:3. Statement.isClosed(): false after last Satement.getMoreResults().", testStatement.isClosed());

        // since open ResultSets aren't tracked, we need to close all manually
        testResultSet1.close();
        testResultSet2.close();
        testResultSet3.close();
        // although there are no more ResultSets, Statement mustn't be closed

        assertTrue(testStep + ".ST:3. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:3. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertTrue(testStep + ".ST:3. ResultSet.isClosed(): true.", testResultSet3.isClosed());
        assertFalse(testStep + ".ST:3. Statement.isClosed(): false when last ResultSet is closed.", testStatement.isClosed());

        // **testing CallableStatement**
        // CallableStatement, although using closeOnCompletion, shouldn't be closed when last ResultSet is closed
        testCallStatement = (CallableStatement) testConnection.prepareCall("CALL testBug68916_proc");
        testCallStatement.closeOnCompletion();

        assertTrue(testStep + ".CS:3. There should be some ResultSets.", testCallStatement.execute());
        testResultSet1 = testCallStatement.getResultSet();

        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".CS:3. CallableStatement.isClosed(): false.", testCallStatement.isClosed());

        assertTrue(testStep + ".CS:3. There should be more ResultSets.", testCallStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        testResultSet2 = testCallStatement.getResultSet();

        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.KEEP_CURRENT_RESULT).", testResultSet1.isClosed());
        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false.", testResultSet2.isClosed());
        assertFalse(testStep + ".CS:3. CallableStatement.isClosed(): false.", testCallStatement.isClosed());

        assertTrue(testStep + ".CS:3. There should be more ResultSets.", testCallStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS));
        testResultSet3 = testCallStatement.getResultSet();

        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).", testResultSet1.isClosed());
        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).", testResultSet2.isClosed());
        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false.", testResultSet3.isClosed());
        assertFalse(testStep + ".CS:3. CallableStatement.isClosed(): false.", testCallStatement.isClosed());

        assertFalse(testStep + ".CS:3. There should be no more ResultSets.", testCallStatement.getMoreResults());

        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false.", testResultSet2.isClosed());
        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false after last Satement.getMoreResults().", testResultSet3.isClosed());
        assertFalse(testStep + ".CS:3. CallableStatement.isClosed(): false after last Satement.getMoreResults().", testCallStatement.isClosed());

        // since open ResultSets aren't tracked, we need to close all manually
        testResultSet1.close();
        testResultSet2.close();
        testResultSet3.close();
        // although there are no more ResultSets, Statement mustn't be closed

        assertTrue(testStep + ".CS:3. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".CS:3. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertTrue(testStep + ".CS:3. ResultSet.isClosed(): true.", testResultSet3.isClosed());
        assertFalse(testStep + ".CS:3. CallableStatement.isClosed(): false when last ResultSet is closed.", testCallStatement.isClosed());

        /*
         * SUB-STEP 4: Generated Keys ResultSet (dontTrackOpenResources=true)
         */
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)", Statement.RETURN_GENERATED_KEYS);

        testResultSet1 = testStatement.getGeneratedKeys();
        assertTrue(testStep + ".ST:4. Statement.getGeneratedKeys(): should return some values.", testResultSet1.next());

        assertFalse(testStep + ".ST:4. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:4. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet1.close(); // although it's last open ResultSet, Statement mustn't be closed

        assertTrue(testStep + ".ST:4. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:4. Statement.isClosed(): false when last ResultSet is closed.", testStatement.isClosed());

        // test again and combine with simple query
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (4), (5), (6)", Statement.RETURN_GENERATED_KEYS);

        testResultSet1 = testStatement.getGeneratedKeys();
        assertTrue(testStep + ".ST:4. Statement.getGeneratedKeys(): should return some values.", testResultSet1.next());

        testResultSet2 = testStatement.executeQuery("SELECT 2");

        assertFalse(testStep + ".ST:4. ResultSet.isClosed(): false after executeQuery() in same Statement.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:4. ResultSet.isClosed(): false.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:4. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet2.close();

        assertFalse(testStep + ".ST:4. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:4. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:4. Statement.isClosed(): false when last ResultSet is closed (still one open).", testStatement.isClosed());

        testResultSet1.close(); // although it's last open ResultSet, Statement mustn't be closed

        assertTrue(testStep + ".ST:4. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:4. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:4. Statement.isClosed(): false when last ResultSet is closed.", testStatement.isClosed());

        testConnection.close();
    }

    private void subTestBug68916ForAllowMultiQueries() throws Exception {
        Connection testConnection;
        String testStep;
        ResultSet testResultSet1, testResultSet2, testResultSet3;

        // We are testing against code that was compiled with Java 6, so methods isCloseOnCompletion() and
        // closeOnCompletion() aren't available in the Statement interface. We need to test directly our
        // implementations.
        StatementImpl testStatement = null;
        PreparedStatement testPrepStatement = null;
        CallableStatement testCallStatement = null;

        /*
         * Testing with connection property allowMultiQueries=true
         */
        testStep = "Conn. Prop. 'allowMultiQueries'";
        testConnection = getConnectionWithProps("allowMultiQueries=true");

        /*
         * SUB-STEP 0: The basics (allowMultiQueries=true)
         */
        // **testing Statement**
        // ResultSets should be closed when owning Statement is closed
        testStatement = (StatementImpl) testConnection.createStatement();

        assertFalse(testStep + ".ST:0. Statement.isCloseOnCompletion(): false by default.", testStatement.isCloseOnCompletion());
        assertFalse(testStep + ".ST:0. Statement.isClosed(): false.", testStatement.isClosed());

        testStatement.closeOnCompletion();

        assertTrue(testStep + ".ST:0. Statement.isCloseOnCompletion(): true after Statement.closeOnCompletion().", testStatement.isCloseOnCompletion());
        assertFalse(testStep + ".ST:0. Statement.isClosed(): false.", testStatement.isClosed());

        testStatement.closeOnCompletion();

        assertTrue(testStep + ".ST:0. Statement.isCloseOnCompletion(): true after 2nd Statement.closeOnCompletion().", testStatement.isCloseOnCompletion());

        // test Statement.close()
        testResultSet1 = testStatement.executeQuery("SELECT 1");

        assertFalse(testStep + ".ST:0. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:0. Statement.isClosed(): false.", testStatement.isClosed());

        testStatement.close();

        assertTrue(testStep + ".ST:0. ResultSet.isClosed(): true after Statement.Close().", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:0. Statement.isClosed(): true after Statement.Close().", testStatement.isClosed());

        // **testing PreparedStatement**
        // ResultSets should be closed when owning PreparedStatement is closed
        testPrepStatement = (com.mysql.jdbc.PreparedStatement) testConnection.prepareStatement("SELECT 1");

        assertFalse(testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): false by default.", testPrepStatement.isCloseOnCompletion());
        assertFalse(testStep + ".PS:0. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        testPrepStatement.closeOnCompletion();

        assertTrue(testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): true after Statement.closeOnCompletion().",
                testPrepStatement.isCloseOnCompletion());
        assertFalse(testStep + ".PS:0. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        testPrepStatement.closeOnCompletion();

        assertTrue(testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): true after 2nd Statement.closeOnCompletion().",
                testPrepStatement.isCloseOnCompletion());

        // test PreparedStatement.close()
        testPrepStatement.execute();
        testResultSet1 = testPrepStatement.getResultSet();

        assertFalse(testStep + ".PS:0. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".PS:0. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        testPrepStatement.close();

        assertTrue(testStep + ".PS:0. ResultSet.isClosed(): true after PreparedStatement.close().", testResultSet1.isClosed());
        assertTrue(testStep + ".PS:0. PreparedStatement.isClosed(): true after PreparedStatement.close().", testPrepStatement.isClosed());

        /*
         * SUB-STEP 1: One ResultSet (allowMultiQueries=true)
         */
        // **testing Statement**
        // Statement using closeOnCompletion should be closed when last ResultSet is closed
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");

        assertFalse(testStep + ".ST:1. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false.", testStatement.isClosed());

        while (testResultSet1.next()) {
        }

        assertFalse(testStep + ".ST:1. ResultSet.isClosed(): false after ResultSet have reached the end.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".ST:1. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:1. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        // test implicit resultset close, keeping statement open, when following with an executeBatch()
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testStatement.addBatch("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)");
        testStatement.executeBatch();

        assertTrue(testStep + ".ST:1. ResultSet.isClosed(): true after executeBatch() in same Statement.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet1 = testStatement.getGeneratedKeys();
        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".ST:1. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:1. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        // test implicit resultset close keeping statement open, when following with an executeUpdate()
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)", com.mysql.jdbc.Statement.RETURN_GENERATED_KEYS);

        assertTrue(testStep + ".ST:1. ResultSet.isClosed(): true after executeUpdate() in same Statement.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:1. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet1 = testStatement.getGeneratedKeys();
        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".ST:1. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:1. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        // **testing PreparedStatement**
        // PreparedStatement using closeOnCompletion should be closed when last ResultSet is closed
        testPrepStatement = (PreparedStatement) testConnection.prepareStatement("SELECT 1");
        testPrepStatement.closeOnCompletion();

        testResultSet1 = testPrepStatement.executeQuery();

        assertFalse(testStep + ".PS:1. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".PS:1. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        while (testResultSet1.next()) {
        }

        assertFalse(testStep + ".PS:1. ResultSet.isClosed(): false after ResultSet have reached the end.", testResultSet1.isClosed());
        assertFalse(testStep + ".PS:1. PreparedStatement.isClosed(): false.", testPrepStatement.isClosed());

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".PS:1. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".PS:1. PreparedStatement.isClosed(): true when last ResultSet is closed.", testPrepStatement.isClosed());

        /*
         * SUB-STEP 2: Multiple ResultSets, sequentially (allowMultiQueries=true)
         */
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testResultSet2 = testStatement.executeQuery("SELECT 2; SELECT 3"); // closes testResultSet1

        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true after 2nd Statement.executeQuery().", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:2. Statement.isClosed(): false.", testStatement.isClosed());

        while (testResultSet2.next()) {
        }

        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false after ResultSet have reached the end.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:2. Statement.isClosed(): false.", testStatement.isClosed());

        assertTrue(testStep + ".ST:3. There should be more ResultSets.", testStatement.getMoreResults()); // closes
                                                                                                         // testResultSet2
        testResultSet3 = testStatement.getResultSet();

        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true after Statement.getMoreResults().", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:2. ResultSet.isClosed(): false.", testResultSet3.isClosed());
        assertFalse(testStep + ".ST:2. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet3.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertTrue(testStep + ".ST:2. ResultSet.isClosed(): true.", testResultSet3.isClosed());
        assertTrue(testStep + ".ST:2. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        /*
         * SUB-STEP 3: Multiple ResultSets, returned at once (allowMultiQueries=true)
         */
        // **testing Statement**
        // Statement using closeOnCompletion should be closed when last ResultSet is closed
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1; SELECT 2; SELECT 3");

        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:3. Statement.isClosed(): false.", testStatement.isClosed());

        assertTrue(testStep + ".ST:3. There should be more ResultSets.", testStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        testResultSet2 = testStatement.getResultSet();

        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.KEEP_CURRENT_RESULT).", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:3. Statement.isClosed(): false.", testStatement.isClosed());

        assertTrue(testStep + ".ST:3. There should be more ResultSets.", testStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS));
        testResultSet3 = testStatement.getResultSet();

        assertTrue(testStep + ".ST:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:3. ResultSet.isClosed(): false.", testResultSet3.isClosed());
        assertFalse(testStep + ".ST:3. Statement.isClosed(): false.", testStatement.isClosed());

        // no more ResultSets, must close Statement
        assertFalse(testStep + ".ST:3. There should be no more ResultSets.", testStatement.getMoreResults());

        assertTrue(testStep + ".ST:3. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:3. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertTrue(testStep + ".ST:3. ResultSet.isClosed(): true after last Satement.getMoreResults().", testResultSet3.isClosed());
        assertTrue(testStep + ".ST:3. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        // **testing CallableStatement**
        // CallableStatement using closeOnCompletion should be closed when last ResultSet is closed
        testCallStatement = (CallableStatement) testConnection.prepareCall("CALL testBug68916_proc");
        testCallStatement.closeOnCompletion();

        assertTrue(testStep + ".CS:3. There should be some ResultSets.", testCallStatement.execute());
        testResultSet1 = testCallStatement.getResultSet();

        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".CS:3. CallableStatement.isClosed(): false.", testCallStatement.isClosed());

        assertTrue(testStep + ".CS:3. There should be more ResultSets.", testCallStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        testResultSet2 = testCallStatement.getResultSet();

        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.KEEP_CURRENT_RESULT).", testResultSet1.isClosed());
        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false.", testResultSet2.isClosed());
        assertFalse(testStep + ".CS:3. CallableStatement.isClosed(): false.", testCallStatement.isClosed());

        assertTrue(testStep + ".CS:3. There should be more ResultSets.", testCallStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS));
        testResultSet3 = testCallStatement.getResultSet();

        assertTrue(testStep + ".CS:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).", testResultSet1.isClosed());
        assertTrue(testStep + ".CS:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).", testResultSet2.isClosed());
        assertFalse(testStep + ".CS:3. ResultSet.isClosed(): false.", testResultSet3.isClosed());
        assertFalse(testStep + ".CS:3. CallableStatement.isClosed(): false.", testCallStatement.isClosed());

        // no more ResultSets, must close Statement
        assertFalse(testStep + ".CS:3. There should be no more ResultSets.", testCallStatement.getMoreResults());

        assertTrue(testStep + ".CS:3. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".CS:3. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertTrue(testStep + ".CS:3. ResultSet.isClosed(): true after last Satement.getMoreResults().", testResultSet3.isClosed());
        assertTrue(testStep + ".CS:3. CallableStatement.isClosed(): true when last ResultSet is closed.", testCallStatement.isClosed());

        /*
         * SUB-STEP 4: Generated Keys ResultSet (allowMultiQueries=true)
         */
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3); INSERT INTO testBug68916_tbl (fld2) VALUES (4), (5), (6)",
                Statement.RETURN_GENERATED_KEYS);

        testResultSet1 = testStatement.getGeneratedKeys();
        assertTrue(testStep + ".ST:4. Statement.getGeneratedKeys(): should return some values.", testResultSet1.next());

        assertFalse(testStep + ".ST:4. ResultSet.isClosed(): false.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:4. Statement.isClosed(): false.", testStatement.isClosed());

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testStep + ".ST:4. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:4. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());

        // test again and combine with simple query
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (4), (5), (6)", Statement.RETURN_GENERATED_KEYS);

        testResultSet1 = testStatement.getGeneratedKeys();
        assertTrue(testStep + ".ST:4. Statement.getGeneratedKeys(): should return some values.", testResultSet1.next());

        testResultSet2 = testStatement.executeQuery("SELECT 2; SELECT 3");

        assertTrue(testStep + ".ST:4. ResultSet.isClosed(): true after executeQuery() in same Statement.", testResultSet1.isClosed());
        assertFalse(testStep + ".ST:4. ResultSet.isClosed(): false.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:4. Statement.isClosed(): false.", testStatement.isClosed());

        // last open ResultSet won't close the Statement
        // because we didn't fetch the next one (SELECT 3)
        testResultSet2.close();

        assertTrue(testStep + ".ST:4. ResultSet.isClosed(): true.", testResultSet1.isClosed());
        assertTrue(testStep + ".ST:4. ResultSet.isClosed(): true.", testResultSet2.isClosed());
        assertFalse(testStep + ".ST:4. Statement.isClosed(): true when last ResultSet is closed.", testStatement.isClosed());
        testStatement.close();

        testConnection.close();
    }

    private void subTestBug68916ForConcurrency() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        CompletionService<String> complService = new ExecutorCompletionService<String>(executor);

        String[] connectionProperties = new String[] { "", "holdResultsOpenOverStatementClose=true", "dontTrackOpenResources=true" };
        // overridesCloseOnCompletion[n] refers to the effect of connectionProperties[n] on
        // Statement.closeOnCompletion()
        boolean[] overridesCloseOnCompletion = new boolean[] { false, false, true };
        String[] sampleQueries = new String[] { "SELECT * FROM mysql.help_topic", "SELECT SLEEP(1)",
                "SELECT * FROM mysql.time_zone tz INNER JOIN mysql.time_zone_name tzn ON tz.time_zone_id = tzn.time_zone_id "
                        + "INNER JOIN mysql.time_zone_transition tzt ON tz.time_zone_id = tzt.time_zone_id "
                        + "INNER JOIN mysql.time_zone_transition_type tztt ON tzt.time_zone_id = tztt.time_zone_id "
                        + "AND tzt.transition_type_id = tztt.transition_type_id ORDER BY tzn.name , tztt.abbreviation , tzt.transition_time",
                "SELECT 1" };
        int threadCount = sampleQueries.length;

        for (int c = 0; c < connectionProperties.length; c++) {
            System.out.println("Test Connection with property '" + connectionProperties[c] + "'");
            Connection testConnection = getConnectionWithProps(connectionProperties[c]);

            for (int t = 0; t < threadCount; t++) {
                complService.submit(new subTestBug68916ConcurrentTask(testConnection, sampleQueries[t], overridesCloseOnCompletion[c]));
            }

            for (int t = 0; t < threadCount; t++) {
                try {
                    System.out.println("   " + complService.take().get());
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                } catch (ExecutionException ex) {
                    if (ex.getCause() instanceof Error) {
                        // let JUnit try to report as Failure instead of Error
                        throw (Error) ex.getCause();
                    }
                }
            }

            testConnection.close();
        }
    }

    private class subTestBug68916ConcurrentTask implements Callable<String> {
        Connection testConnection = null;
        String query = null;
        boolean closeOnCompletionIsOverriden = false;

        subTestBug68916ConcurrentTask(Connection testConnection, String query, boolean closeOnCompletionIsOverriden) {
            this.testConnection = testConnection;
            this.query = query;
            this.closeOnCompletionIsOverriden = closeOnCompletionIsOverriden;
        }

        public String call() throws Exception {
            String threadName = Thread.currentThread().getName();
            long startTime = System.currentTimeMillis();
            long stopTime = startTime;
            StatementImpl testStatement = null;
            int count = 0;

            try {
                testStatement = (StatementImpl) this.testConnection.createStatement();
                testStatement.closeOnCompletion();

                System.out.println(threadName + " is executing: " + this.query);
                ResultSet testResultSet = testStatement.executeQuery(this.query);
                while (testResultSet.next()) {
                    count++;
                }
                assertTrue(threadName + ": Query should return some values.", count > 0);
                assertFalse(threadName + ": Statement shouldn't be closed.", testStatement.isClosed());

                testResultSet.close(); // should close statement if not closeOnCompletionIsOverriden
                if (this.closeOnCompletionIsOverriden) {
                    assertFalse(threadName + ": Statement shouldn't be closed.", testStatement.isClosed());
                } else {
                    assertTrue(threadName + ": Statement should be closed.", testStatement.isClosed());
                }

            } catch (SQLException e) {
                e.printStackTrace();
                fail(threadName + ": Something went wrong, maybe Connection or Statement was closed before its time.");

            } finally {
                try {
                    testStatement.close();
                } catch (SQLException e) {
                }
                stopTime = System.currentTimeMillis();
            }
            return threadName + ": processed " + count + " rows in " + (stopTime - startTime) + " milliseconds.";
        }
    }

    /**
     * Tests fix for BUG#73163 - IndexOutOfBoundsException thrown preparing statement.
     * 
     * This bug occurs only if running with Java6+. Duplicated in testsuite.regression.StatementRegressionTest.testBug73163().
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug73163() throws Exception {
        try {
            stmt = conn.prepareStatement("LOAD DATA INFILE ? INTO TABLE testBug73163");
        } catch (SQLException e) {
            if (e.getCause() instanceof IndexOutOfBoundsException && Util.isJdbc4()) {
                fail("IOOBE thrown in Java6+ while preparing a LOAD DATA statement with placeholders.");
            } else {
                throw e;
            }
        }
    }
}
