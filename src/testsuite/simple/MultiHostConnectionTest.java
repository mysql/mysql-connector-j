/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.simple;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import testsuite.BaseTestCase;
import testsuite.UnreliableSocketFactory;

import com.mysql.jdbc.Driver;
import com.mysql.jdbc.NonRegisteringDriver;

public class MultiHostConnectionTest extends BaseTestCase {
    private static final String HOST_1 = "host1";
    private static final String HOST_2 = "host2";
    private static final String HOST_3 = "host3";

    private static final String HOST_1_OK = UnreliableSocketFactory.STATUS_CONNECTED + HOST_1;
    private static final String HOST_1_FAIL = UnreliableSocketFactory.STATUS_FAILED + HOST_1;
    private static final String HOST_2_OK = UnreliableSocketFactory.STATUS_CONNECTED + HOST_2;
    private static final String HOST_2_FAIL = UnreliableSocketFactory.STATUS_FAILED + HOST_2;
    private static final String HOST_3_OK = UnreliableSocketFactory.STATUS_CONNECTED + HOST_3;
    private static final String HOST_3_FAIL = UnreliableSocketFactory.STATUS_FAILED + HOST_3;

    private static final String STMT_CLOSED_ERR_PATTERN = "No operations allowed after statement closed.";
    private static final String COMM_LINK_ERR_PATTERN = "(?s)Communications link failure.*";

    /**
     * Creates a new MultiHostConnectionTest.
     * 
     * @param name
     *            the name of the test
     */
    public MultiHostConnectionTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(MultiHostConnectionTest.class);
    }

    /**
     * Asserts the execution and return for a simple single value query.
     * 
     * @param testStmt
     *            The statement instance that runs the query.
     * @param query
     *            The query.
     * @param result
     *            The expected result.
     */
    private static void assertSingleValueQuery(Statement testStmt, String query, Object result) throws Exception {
        ResultSet testRs = testStmt.executeQuery(query);
        assertTrue(testRs.next());
        assertEquals(result, testRs.getObject(1));
        assertFalse(testRs.next());
        testRs.close();
    }

    /**
     * Asserts the SQLException thrown for connection commit() or rollback();
     * 
     * @param testConn
     *            The connection instance where to issue the command.
     * @param command
     *            The command to issue.
     * @param messageRegEx
     *            The expected message regular expression pattern.
     */
    private static void assertSQLException(final Connection testConn, final String command, String messageRegEx) {
        assertThrows(SQLException.class, messageRegEx, new Callable<Void>() {
            public Void call() throws Exception {
                if ("commit".equals(command)) {
                    testConn.commit();
                } else if ("rollback".equals(command)) {
                    testConn.rollback();
                }
                return null;
            }
        });
    }

    /**
     * Asserts the SQLException thrown for a query execution.
     * 
     * @param testStmt
     *            The statement instance that runs the query.
     * @param query
     *            The query.
     * @param messageRegEx
     *            The expected message regular expression pattern.
     */
    private static void assertSQLException(final Statement testStmt, final String query, String messageRegEx) {
        assertThrows(SQLException.class, messageRegEx, new Callable<Void>() {
            public Void call() throws Exception {
                testStmt.executeQuery(query);
                return null;
            }
        });
    }

    /**
     * Asserts the most recent history of connection attempts from the global data in UnreliableSocketFactory.
     * 
     * @param expectedConnectionsHistory
     *            The list of expected events. Use CO_OK(String) and CO_FAIL(String) to build proper syntax for host identification.
     */
    private static void assertConnectionsHistory(String... expectedConnectionsHistory) {
        List<String> actualConnectionsHistory = UnreliableSocketFactory.getHostsFromLastConnections(expectedConnectionsHistory.length);

        int i = 1;
        String delimiter = "";
        StringBuilder expectedHist = new StringBuilder("[ ");
        for (String hostInfo : expectedConnectionsHistory) {
            expectedHist.append(delimiter).append(i++).append(hostInfo);
            delimiter = " ~ ";
        }
        expectedHist.append(" ]");

        i = 1;
        delimiter = "";
        StringBuilder actualHist = new StringBuilder("[ ");
        for (String hostInfo : actualConnectionsHistory) {
            actualHist.append(delimiter).append(i++).append(hostInfo);
            delimiter = " ~ ";
        }
        actualHist.append(" ]");

        assertEquals("Connections history", expectedHist.toString(), actualHist.toString());
    }

    /**
     * Tests failover connection establishing with multiple up/down combinations of 3 hosts.
     */
    public void testFailoverConnection() throws Exception {
        Properties props = new Driver().parseURL(BaseTestCase.dbUrl, null);

        String host = props.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);
        if (!NonRegisteringDriver.isHostPropertiesList(host)) {
            String port = props.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, "3306");
            host = host + ":" + port;
        }
        String noHost = "nohost:12345";
        String user = props.getProperty(NonRegisteringDriver.USER_PROPERTY_KEY);
        String password = props.getProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY);
        String database = props.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);

        StringBuilder testURL = new StringBuilder("jdbc:mysql://");
        testURL.append(noHost).append(",");
        testURL.append(noHost).append(",");
        testURL.append(noHost).append("/").append(database);
        final String allDownURL = testURL.toString();

        StringBuilder propsListBuilder = new StringBuilder("retriesAllDown=2");
        if (user != null) {
            propsListBuilder.append(",user=").append(user);
        }
        if (password != null) {
            propsListBuilder.append(",password=").append(password);
        }
        final String propsList = propsListBuilder.toString();

        // all hosts down
        assertThrows(SQLException.class, COMM_LINK_ERR_PATTERN, new Callable<Void>() {
            @SuppressWarnings("synthetic-access")
            public Void call() throws Exception {
                getConnectionWithProps(allDownURL, propsList);
                return null;
            }
        });

        // at least one host up
        for (int i = 1; i < 8; i++) {
            testURL = new StringBuilder("jdbc:mysql://");
            testURL.append((i & 1) == 0 ? noHost : host).append(",");
            testURL.append((i & 2) == 0 ? noHost : host).append(",");
            testURL.append((i & 4) == 0 ? noHost : host).append("/").append(database);

            Connection testConn = getConnectionWithProps(testURL.toString(), propsList);

            final Statement testStmt = testConn.createStatement();

            assertSingleValueQuery(testStmt, "SELECT 1", 1L);
            assertSQLException(testStmt, "SELECT * FROM missing_table", "Table '\\w*.missing_table' doesn't exist");
            assertSingleValueQuery(testStmt, "SELECT 2", 2L);

            testStmt.close();
            testConn.close();
        }
    }

    /**
     * Tests failover transitions in a default failover connection using three hosts.
     */
    public void testFailoverTransitions() throws Exception {
        Set<String> downedHosts = new HashSet<String>();

        // from HOST_1 to HOST_2
        testFailoverTransition(HOST_1, HOST_2, null, null, HOST_1_OK, HOST_2_OK);

        // from HOST_1 to HOST_3
        downedHosts.clear();
        downedHosts.add(HOST_2);
        testFailoverTransition(HOST_1, HOST_3, downedHosts, null, HOST_1_OK, HOST_2_FAIL, HOST_3_OK);

        // from HOST_2 to HOST_3
        downedHosts.clear();
        downedHosts.add(HOST_1);
        testFailoverTransition(HOST_2, HOST_3, downedHosts, null, HOST_1_FAIL, HOST_2_OK, HOST_3_OK);

        // from HOST_2 to HOST_1
        downedHosts.clear();
        downedHosts.add(HOST_1);
        downedHosts.add(HOST_3);
        testFailoverTransition(HOST_2, HOST_1, downedHosts, HOST_1, HOST_1_FAIL, HOST_2_OK, HOST_3_FAIL, HOST_2_FAIL, HOST_3_FAIL, HOST_1_OK);

        // from HOST_3 to HOST_1
        downedHosts.clear();
        downedHosts.add(HOST_1);
        downedHosts.add(HOST_2);
        testFailoverTransition(HOST_3, HOST_1, downedHosts, HOST_1, HOST_1_FAIL, HOST_2_FAIL, HOST_3_OK, HOST_2_FAIL, HOST_3_FAIL, HOST_1_OK);

        // from HOST_3 to HOST_2
        downedHosts.clear();
        downedHosts.add(HOST_1);
        downedHosts.add(HOST_2);
        testFailoverTransition(HOST_3, HOST_2, downedHosts, HOST_2, HOST_1_FAIL, HOST_2_FAIL, HOST_3_OK, HOST_2_OK);
    }

    /**
     * Tests a failover transition.
     * 
     * @param fromHost
     *            The host where initially connected to. In order to connect to an host other than the primary all previous hosts must be downed (pinpoint them
     *            in the 'downedHosts' set).
     * @param toHost
     *            The host where to failover. In order to correctly connect to this host, all hosts between (and eventually before) 'fromHost' and 'toHost' must
     *            be downed.
     * @param downedHosts
     *            The set of hosts initially down.
     * @param recoverHost
     *            The host that recovers after first connection.
     * @param expectedConnectionsHistory
     *            The expected connection attempts sequence.
     */
    private void testFailoverTransition(String fromHost, String toHost, Set<String> downedHosts, String recoverHost, String... expectedConnectionsHistory)
            throws Exception {
        Properties props = new Properties();
        props.setProperty("retriesAllDown", "2");

        String fromHostOk = UnreliableSocketFactory.STATUS_CONNECTED + fromHost;
        String toHostOk = UnreliableSocketFactory.STATUS_CONNECTED + toHost;

        Connection testConn = getUnreliableFailoverConnection(new String[] { HOST_1, HOST_2, HOST_3 }, props, downedHosts);
        Statement testStmt = null;

        try {
            if (recoverHost != null) {
                // 'recoverHost' up
                UnreliableSocketFactory.dontDownHost(recoverHost);
            }

            // connected to 'fromHost'
            assertEquals(fromHostOk, UnreliableSocketFactory.getHostFromLastConnection());

            // get new statement
            testStmt = testConn.createStatement();
            assertSingleValueQuery(testStmt, "SELECT 1", 1L);

            // 'fromHost' down
            UnreliableSocketFactory.downHost(fromHost);

            // still connected to 'fromHost'
            assertEquals(fromHostOk, UnreliableSocketFactory.getHostFromLastConnection());

            // connects to 'toHost' on connection error
            assertSQLException(testStmt, "SELECT 1", COMM_LINK_ERR_PATTERN);
            assertEquals(toHostOk, UnreliableSocketFactory.getHostFromLastConnection());

            // statement closed
            assertSQLException(testStmt, "SELECT 1", STMT_CLOSED_ERR_PATTERN);

            // get new statements
            testStmt = testConn.createStatement();
            assertSingleValueQuery(testStmt, "SELECT 1", 1L);

            // still connected to 'toHost'
            assertEquals(toHostOk, UnreliableSocketFactory.getHostFromLastConnection());

            assertConnectionsHistory(expectedConnectionsHistory);

        } finally {
            if (testStmt != null) {
                testStmt.close();
            }
            if (testConn != null) {
                testConn.close();
            }
        }
    }

    /**
     * Tests a default failover connection using three hosts and the following sequence of events:
     * - [/HOST_1 : /HOST_2 : /HOST_3] --> HOST_1
     * - [\HOST_1 : /HOST_2 : /HOST_3] --> HOST_2
     * - [\HOST_1 : \HOST_2 : /HOST_3] --> HOST_3
     * - [/HOST_1 : /HOST_2 : /HOST_3]
     * - [/HOST_1 : /HOST_2 : \HOST_3] --> HOST_2
     * - [/HOST_1 : \HOST_2 : \HOST_3] --> HOST_1
     * 
     * [Legend: "/HOST_n" --> HOST_n up; "\HOST_n" --> HOST_n down]
     */
    public void testFailoverDefaultSettings() throws Exception {
        Properties props = new Properties();
        props.setProperty("retriesAllDown", "2");

        Connection testConn = getUnreliableFailoverConnection(new String[] { HOST_1, HOST_2, HOST_3 }, props);
        Statement testStmt1 = null, testStmt2 = null;

        try {
            // connected to HOST_1 [/HOST_1 : /HOST_2 : /HOST_3]
            assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

            // get new statements
            testStmt1 = testConn.createStatement();
            testStmt2 = testConn.createStatement();
            assertSingleValueQuery(testStmt1, "SELECT 1", 1L);
            assertSingleValueQuery(testStmt2, "SELECT 2", 2L);

            // HOST_1 down [\HOST_1 : /HOST_2 : /HOST_3]
            UnreliableSocketFactory.downHost(HOST_1);

            // still connected to HOST_1
            assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

            // connects to HOST_2 on connection error
            assertSQLException(testStmt1, "SELECT 1", COMM_LINK_ERR_PATTERN);
            assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

            // statements closed
            assertSQLException(testStmt1, "SELECT 1", STMT_CLOSED_ERR_PATTERN);
            assertSQLException(testStmt2, "SELECT 2", STMT_CLOSED_ERR_PATTERN);

            // get new statements
            testStmt1 = testConn.createStatement();
            testStmt2 = testConn.createStatement();
            assertSingleValueQuery(testStmt1, "SELECT 1", 1L);
            assertSingleValueQuery(testStmt2, "SELECT 2", 2L);

            // HOST_2 down [\HOST_1 : \HOST_2 : /HOST_3]
            UnreliableSocketFactory.downHost(HOST_2);

            // still connected to HOST_2
            assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

            // connects to HOST_3 on connection error
            assertSQLException(testStmt1, "SELECT 1", COMM_LINK_ERR_PATTERN);
            assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());

            // statements closed
            assertSQLException(testStmt1, "SELECT 1", STMT_CLOSED_ERR_PATTERN);
            assertSQLException(testStmt2, "SELECT 2", STMT_CLOSED_ERR_PATTERN);

            // HOST_1 + HOST_2 up [/HOST_1 : /HOST_2 : /HOST_3]
            UnreliableSocketFactory.dontDownHost(HOST_1);
            UnreliableSocketFactory.dontDownHost(HOST_2);

            // get new statements
            testStmt1 = testConn.createStatement();
            testStmt2 = testConn.createStatement();
            assertSingleValueQuery(testStmt1, "SELECT 1", 1L);
            assertSingleValueQuery(testStmt2, "SELECT 2", 2L);

            // HOST_3 down [/HOST_1 : /HOST_2 : \HOST_3]
            UnreliableSocketFactory.downHost(HOST_3);

            // still connected to HOST_3
            assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());

            // connects to HOST_2 on connection error (not time to come back to HOST_1 yet)
            assertSQLException(testStmt2, "SELECT 2", COMM_LINK_ERR_PATTERN);
            assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

            // statements closed
            assertSQLException(testStmt1, "SELECT 1", STMT_CLOSED_ERR_PATTERN);
            assertSQLException(testStmt2, "SELECT 2", STMT_CLOSED_ERR_PATTERN);

            // get new statements
            testStmt1 = testConn.createStatement();
            testStmt2 = testConn.createStatement();
            assertSingleValueQuery(testStmt1, "SELECT 1", 1L);
            assertSingleValueQuery(testStmt2, "SELECT 2", 2L);

            // HOST_2 down [/HOST_1 : \HOST_2 : \HOST_3]
            UnreliableSocketFactory.downHost(HOST_2);

            // still connected to HOST_2
            assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

            // connects to HOST_1 on connection error
            assertSQLException(testStmt1, "SELECT 1", COMM_LINK_ERR_PATTERN);
            assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

            // statements closed
            assertSQLException(testStmt1, "SELECT 1", STMT_CLOSED_ERR_PATTERN);
            assertSQLException(testStmt2, "SELECT 2", STMT_CLOSED_ERR_PATTERN);

            // get new statements
            testStmt1 = testConn.createStatement();
            testStmt2 = testConn.createStatement();
            assertSingleValueQuery(testStmt1, "SELECT 1", 1L);
            assertSingleValueQuery(testStmt2, "SELECT 2", 2L);

            assertConnectionsHistory(HOST_1_OK, HOST_2_OK, HOST_3_OK, HOST_2_OK, HOST_3_FAIL, HOST_2_FAIL, HOST_3_FAIL, HOST_1_OK);

        } finally {
            if (testStmt1 != null) {
                testStmt1.close();
            }
            if (testStmt2 != null) {
                testStmt2.close();
            }
            if (testConn != null) {
                testConn.close();
            }
        }
    }

    /**
     * Repeatedly tests a failover connection using three hosts and the following sequence of events, combining distinct failover event triggering:
     * - [/HOST_1 : /HOST_2 : /HOST_3] --> HOST_1
     * - [\HOST_1 : /HOST_2 : /HOST_3] --> HOST_2
     * - [\HOST_1 : \HOST_2 : /HOST_3] --> HOST_3
     * - [/HOST_1 : /HOST_2 : /HOST_3]
     * - [/HOST_1 : /HOST_2 : \HOST_3] --> HOST_2
     * - [/HOST_1 : \HOST_2 : \HOST_3] --> HOST_1
     * 
     * [Legend: "/HOST_n" --> HOST_n up; "\HOST_n" --> HOST_n down]
     */
    public void testFailoverCombinations() throws Exception {
        Properties props = new Properties();
        props.setProperty("retriesAllDown", "2");

        for (int run = 1; run <= 3; run++) {
            Connection testConn = getUnreliableFailoverConnection(new String[] { HOST_1, HOST_2, HOST_3 }, props);
            Statement testStmt1 = null, testStmt2 = null;

            testConn.setAutoCommit(false);

            try {
                // connected to HOST_1 [/HOST_1 : /HOST_2 : /HOST_3]
                assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // get new statements
                testStmt1 = testConn.createStatement();
                testStmt2 = testConn.createStatement();
                assertSingleValueQuery(testStmt1, "SELECT 1", 1L);
                assertSingleValueQuery(testStmt2, "SELECT 2", 2L);

                // HOST_1 down [\HOST_1 : /HOST_2 : /HOST_3]
                UnreliableSocketFactory.downHost(HOST_1);

                // still connected to HOST_1
                assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // connects to HOST_2 on connection error
                if (run == 1) {
                    assertSQLException(testStmt1, "SELECT 1", COMM_LINK_ERR_PATTERN);
                } else if (run == 2) {
                    assertSQLException(testConn, "commit", COMM_LINK_ERR_PATTERN);
                } else {
                    assertSQLException(testConn, "rollback", COMM_LINK_ERR_PATTERN);
                }
                assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // statements closed
                assertSQLException(testStmt1, "SELECT 1", STMT_CLOSED_ERR_PATTERN);
                assertSQLException(testStmt2, "SELECT 2", STMT_CLOSED_ERR_PATTERN);

                // get new statements
                testStmt1 = testConn.createStatement();
                testStmt2 = testConn.createStatement();
                assertSingleValueQuery(testStmt1, "SELECT 1", 1L);
                assertSingleValueQuery(testStmt2, "SELECT 2", 2L);

                // HOST_2 down [\HOST_1 : \HOST_2 : /HOST_3]
                UnreliableSocketFactory.downHost(HOST_2);

                // still connected to HOST_2
                assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // connects to HOST_3 on connection error
                if (run == 1) {
                    assertSQLException(testConn, "commit", COMM_LINK_ERR_PATTERN);
                } else if (run == 2) {
                    assertSQLException(testStmt1, "SELECT 1", COMM_LINK_ERR_PATTERN);
                } else {
                    assertSQLException(testConn, "rollback", COMM_LINK_ERR_PATTERN);
                }
                assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // statements closed
                assertSQLException(testStmt1, "SELECT 1", STMT_CLOSED_ERR_PATTERN);
                assertSQLException(testStmt2, "SELECT 2", STMT_CLOSED_ERR_PATTERN);

                // HOST_1 + HOST_2 up [/HOST_1 : /HOST_2 : /HOST_3]
                UnreliableSocketFactory.dontDownHost(HOST_1);
                UnreliableSocketFactory.dontDownHost(HOST_2);

                // get new statements
                testStmt1 = testConn.createStatement();
                testStmt2 = testConn.createStatement();
                assertSingleValueQuery(testStmt1, "SELECT 1", 1L);
                assertSingleValueQuery(testStmt2, "SELECT 2", 2L);

                // HOST_3 down [/HOST_1 : /HOST_2 : \HOST_3]
                UnreliableSocketFactory.downHost(HOST_3);

                // still connected to HOST_3
                assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // connects to HOST_2 on connection error (not time to come back to HOST_1 yet)
                if (run == 1) {
                    assertSQLException(testConn, "rollback", COMM_LINK_ERR_PATTERN);
                } else if (run == 2) {
                    assertSQLException(testConn, "commit", COMM_LINK_ERR_PATTERN);
                } else {
                    assertSQLException(testStmt2, "SELECT 2", COMM_LINK_ERR_PATTERN);
                }
                assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // statements closed
                assertSQLException(testStmt1, "SELECT 1", STMT_CLOSED_ERR_PATTERN);
                assertSQLException(testStmt2, "SELECT 2", STMT_CLOSED_ERR_PATTERN);

                // get new statements
                testStmt1 = testConn.createStatement();
                testStmt2 = testConn.createStatement();
                assertSingleValueQuery(testStmt1, "SELECT 1", 1L);
                assertSingleValueQuery(testStmt2, "SELECT 2", 2L);

                // HOST_2 down [/HOST_1 : \HOST_2 : \HOST_3]
                UnreliableSocketFactory.downHost(HOST_2);

                // still connected to HOST_2
                assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // connects to HOST_1 on connection error
                if (run == 1) {
                    assertSQLException(testStmt1, "SELECT 1", COMM_LINK_ERR_PATTERN);
                } else if (run == 2) {
                    assertSQLException(testConn, "rollback", COMM_LINK_ERR_PATTERN);
                } else {
                    assertSQLException(testConn, "commit", COMM_LINK_ERR_PATTERN);
                }
                assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // statements closed
                assertSQLException(testStmt1, "SELECT 1", STMT_CLOSED_ERR_PATTERN);
                assertSQLException(testStmt2, "SELECT 2", STMT_CLOSED_ERR_PATTERN);

                // get new statements
                testStmt1 = testConn.createStatement();
                testStmt2 = testConn.createStatement();
                assertSingleValueQuery(testStmt1, "SELECT 1", 1L);
                assertSingleValueQuery(testStmt2, "SELECT 2", 2L);

                assertConnectionsHistory(HOST_1_OK, HOST_2_OK, HOST_3_OK, HOST_2_OK, HOST_3_FAIL, HOST_2_FAIL, HOST_3_FAIL, HOST_1_OK);

            } finally {
                if (testStmt1 != null) {
                    testStmt1.close();
                }
                if (testStmt2 != null) {
                    testStmt2.close();
                }
                if (testConn != null) {
                    testConn.close();
                }
            }
        }
    }

    /**
     * Tests the property 'failOverReadOnly' in a failover connection using three hosts and the following sequence of events:
     * - [\HOST_1 : /HOST_2 : /HOST_3] --> HOST_2
     * - [\HOST_1 : \HOST_2 : /HOST_3] --> HOST_3
     * - [\HOST_1 : /HOST_2 : \HOST_3] --> HOST_2
     * - [/HOST_1 : \HOST_2 : \HOST_3] --> HOST_1
     * - [\HOST_1 : \HOST_2 : /HOST_3] --> HOST_3
     * 
     * [Legend: "/HOST_n" --> HOST_n up; "\HOST_n" --> HOST_n down]
     */
    public void testFailoverReadOnly() throws Exception {
        Set<String> downedHosts = new HashSet<String>();
        downedHosts.add(HOST_1);

        Properties props = new Properties();
        props.setProperty("retriesAllDown", "2");

        for (boolean foReadOnly : new boolean[] { true, false }) {
            props.setProperty("failOverReadOnly", Boolean.toString(foReadOnly));

            Connection testConn = getUnreliableFailoverConnection(new String[] { HOST_1, HOST_2, HOST_3 }, props, downedHosts);
            Statement testStmt = null;

            try {
                // connected ('failOverReadOnly') to HOST_2 [\HOST_1 : /HOST_2 : /HOST_3]
                assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());
                assertEquals(foReadOnly, testConn.isReadOnly());

                // get new statement
                testStmt = testConn.createStatement();
                assertSingleValueQuery(testStmt, "SELECT 1", 1L);

                // HOST_2 down [\HOST_1 : \HOST_2 : /HOST_3]
                UnreliableSocketFactory.downHost(HOST_2);

                // still connected to HOST_2
                assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // connects ('failOverReadOnly') to HOST_3 on connection error
                assertSQLException(testStmt, "SELECT 1", COMM_LINK_ERR_PATTERN);
                assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());
                assertEquals(foReadOnly, testConn.isReadOnly());

                // get new statement
                testStmt = testConn.createStatement();
                assertSingleValueQuery(testStmt, "SELECT 1", 1L);

                // HOST_2 up & HOST_3 down [\HOST_1 : /HOST_2 : \HOST_3]
                UnreliableSocketFactory.dontDownHost(HOST_2);
                UnreliableSocketFactory.downHost(HOST_3);

                // still connected to HOST_3
                assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // connects ('failOverReadOnly') to HOST_2 on connection error
                assertSQLException(testStmt, "SELECT 1", COMM_LINK_ERR_PATTERN);
                assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());
                assertEquals(foReadOnly, testConn.isReadOnly());

                // get new statement
                testStmt = testConn.createStatement();
                assertSingleValueQuery(testStmt, "SELECT 1", 1L);

                // HOST_1 up & HOST_2 down [/HOST_1 : \HOST_2 : \HOST_3]
                UnreliableSocketFactory.dontDownHost(HOST_1);
                UnreliableSocketFactory.downHost(HOST_2);

                // still connected to HOST_2
                assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // connects (r+w) to HOST_1 on connection error
                assertSQLException(testStmt, "SELECT 1", COMM_LINK_ERR_PATTERN);
                assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());
                assertFalse(testConn.isReadOnly());

                // get new statement
                testStmt = testConn.createStatement();
                assertSingleValueQuery(testStmt, "SELECT 1", 1L);

                // HOST_1 down & HOST_3 up [\HOST_1 : \HOST_2 : /HOST_3]
                UnreliableSocketFactory.downHost(HOST_1);
                UnreliableSocketFactory.dontDownHost(HOST_3);

                // still connected to HOST_1
                assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // connects ('failOverReadOnly') to HOST_2 on connection error
                assertSQLException(testStmt, "SELECT 1", COMM_LINK_ERR_PATTERN);
                assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());
                assertEquals(foReadOnly, testConn.isReadOnly());

                assertConnectionsHistory(HOST_2_OK, HOST_3_OK, HOST_2_OK, HOST_3_FAIL, HOST_2_FAIL, HOST_3_FAIL, HOST_1_OK, HOST_2_FAIL, HOST_3_OK);

            } finally {
                if (testStmt != null) {
                    testStmt.close();
                }
                if (testConn != null) {
                    testConn.close();
                }
            }
        }
    }

    /**
     * Tests the property 'queriesBeforeRetryMaster' in a failover connection using three hosts and the following sequence of events:
     * - [/HOST_1 : /HOST_2 : /HOST_3] --> HOST_1
     * - [\HOST_1 : \HOST_2 : /HOST_3] --> HOST_3
     * - [/HOST_1 : /HOST_2 : \HOST_3] --> HOST_1 vs HOST_2
     * 
     * [Legend: "/HOST_n" --> HOST_n up; "\HOST_n" --> HOST_n down]
     */
    public void testFailoverQueriesBeforeRetryMaster() throws Exception {
        Properties props = new Properties();
        props.setProperty("retriesAllDown", "2");

        for (boolean setQueriesBeforeRetryMaster : new boolean[] { true, false }) {
            if (setQueriesBeforeRetryMaster) {
                props.setProperty("queriesBeforeRetryMaster", "10");
            } else {
                props.remove("queriesBeforeRetryMaster"); // default 50
            }

            Connection testConn = getUnreliableFailoverConnection(new String[] { HOST_1, HOST_2, HOST_3 }, props);
            Statement testStmt = null;

            try {
                testConn.setAutoCommit(false); // prevent automatic fall back

                // connected to HOST_1 [/HOST_1 : /HOST_2 : /HOST_3]
                assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // get new statement
                testStmt = testConn.createStatement();
                assertSingleValueQuery(testStmt, "SELECT 1", 1L);

                // HOST_1 + HOST_2 down [\HOST_1 : \HOST_2 : /HOST_3]
                UnreliableSocketFactory.downHost(HOST_1);
                UnreliableSocketFactory.downHost(HOST_2);

                // still connected to HOST_1
                assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // connects to HOST_3 on connection error
                assertSQLException(testStmt, "SELECT 1", COMM_LINK_ERR_PATTERN);
                assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // get new statement
                testStmt = testConn.createStatement();
                for (int i = 0; i < 10; i++) {
                    assertSingleValueQuery(testStmt, "SELECT 1", 1L);
                    assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());
                }

                // HOST_1 + HOST_2 up & HOST_3 down [/HOST_1 : /HOST_2 : \HOST_3]
                UnreliableSocketFactory.dontDownHost(HOST_1);
                UnreliableSocketFactory.dontDownHost(HOST_2);
                UnreliableSocketFactory.downHost(HOST_3);

                // still connected to HOST_3
                assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());

                if (setQueriesBeforeRetryMaster) {
                    // connects to HOST_1 on connection error
                    assertSQLException(testStmt, "SELECT 1", COMM_LINK_ERR_PATTERN);
                    assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

                    assertConnectionsHistory(HOST_1_OK, HOST_2_FAIL, HOST_3_OK, HOST_1_OK);

                } else {
                    // connects to HOST_2 on connection error
                    assertSQLException(testStmt, "SELECT 1", COMM_LINK_ERR_PATTERN);
                    assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

                    assertConnectionsHistory(HOST_1_OK, HOST_2_FAIL, HOST_3_OK, HOST_2_OK);
                }

            } finally {
                if (testStmt != null) {
                    testStmt.close();
                }
                if (testConn != null) {
                    testConn.close();
                }
            }
        }
    }

    /**
     * Tests the property 'secondsBeforeRetryMaster' in a failover connection using three hosts and the following sequence of events:
     * - [/HOST_1 : /HOST_2 : /HOST_3] --> HOST_1
     * - [\HOST_1 : \HOST_2 : /HOST_3] --> HOST_3
     * - [/HOST_1 : /HOST_2 : \HOST_3] --> HOST_1 vs HOST_2
     * 
     * [Legend: "/HOST_n" --> HOST_n up; "\HOST_n" --> HOST_n down]
     */
    public void testFailoverSecondsBeforeRetryMaster() throws Exception {
        Properties props = new Properties();
        props.setProperty("retriesAllDown", "2");

        for (boolean setSecondsBeforeRetryMaster : new boolean[] { true, false }) {
            if (setSecondsBeforeRetryMaster) {
                props.setProperty("secondsBeforeRetryMaster", "1");
            } else {
                props.remove("secondsBeforeRetryMaster"); // default 50
            }

            Connection testConn = getUnreliableFailoverConnection(new String[] { HOST_1, HOST_2, HOST_3 }, props);
            Statement testStmt = null;

            try {
                testConn.setAutoCommit(false); // prevent automatic fall back

                // connected to HOST_1 [/HOST_1 : /HOST_2 : /HOST_3]
                assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // get new statement
                testStmt = testConn.createStatement();
                assertSingleValueQuery(testStmt, "SELECT 1", 1L);

                // HOST_1 + HOST_2 down [\HOST_1 : \HOST_2 : /HOST_3]
                UnreliableSocketFactory.downHost(HOST_1);
                UnreliableSocketFactory.downHost(HOST_2);

                // still connected to HOST_1
                assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // connects to HOST_3 on connection error
                assertSQLException(testStmt, "SELECT 1", COMM_LINK_ERR_PATTERN);
                assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // get new statement
                testStmt = testConn.createStatement();
                long startTime = System.currentTimeMillis();
                do {
                    assertSingleValueQuery(testStmt, "SELECT 1", 1L);
                    assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                    }
                } while (System.currentTimeMillis() - startTime < 2000);

                // HOST_1 + HOST_2 up & HOST_3 down [/HOST_1 : /HOST_2 : \HOST_3]
                UnreliableSocketFactory.dontDownHost(HOST_1);
                UnreliableSocketFactory.dontDownHost(HOST_2);
                UnreliableSocketFactory.downHost(HOST_3);

                // still connected to HOST_3
                assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());

                if (setSecondsBeforeRetryMaster) {
                    // connects to HOST_1 on connection error
                    assertSQLException(testStmt, "SELECT 1", COMM_LINK_ERR_PATTERN);
                    assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

                    assertConnectionsHistory(HOST_1_OK, HOST_2_FAIL, HOST_3_OK, HOST_1_OK);

                } else {
                    // connects to HOST_2 on connection error
                    assertSQLException(testStmt, "SELECT 1", COMM_LINK_ERR_PATTERN);
                    assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

                    assertConnectionsHistory(HOST_1_OK, HOST_2_FAIL, HOST_3_OK, HOST_2_OK);
                }

            } finally {
                if (testStmt != null) {
                    testStmt.close();
                }
                if (testConn != null) {
                    testConn.close();
                }
            }
        }
    }

    /**
     * Tests the automatic fall back to primary host in a failover connection using three hosts and the following sequence of events:
     * + 1.st part:
     * - [\HOST_1 : /HOST_2 : \HOST_3] --> HOST_2
     * - [/HOST_1 : /HOST_2 : /HOST_3] --> no_change vs HOST_1 (auto fall back)
     * - [/HOST_1 : \HOST_2 : /HOST_3] --> HOST_1 vs no_change
     * + 2.nd part:
     * - [\HOST_1 : /HOST_2 : \HOST_3] --> HOST_2
     * - [/HOST_1 : /HOST_2 : /HOST_3] --> no_change
     * - [/HOST_1 : \HOST_2 : /HOST_3] --> HOST_3
     * - [/HOST_1 : /HOST_2 : \HOST_3] --> HOST_1
     * - /HOST_2 & \HOST_3
     * 
     * The automatic fall back only happens at transaction boundaries and at least 'queriesBeforeRetryMaster' or 'secondsBeforeRetryMaster' is greater than 0.
     * [Legend: "/HOST_n" --> HOST_n up; "\HOST_n" --> HOST_n down]
     */
    public void testFailoverAutoFallBack() throws Exception {
        Set<String> downedHosts = new HashSet<String>();
        downedHosts.add(HOST_1);
        downedHosts.add(HOST_3);

        Properties props = new Properties();
        props.setProperty("retriesAllDown", "2");

        // test fall back on ('queriesBeforeRetryMaster' > 0 || 'secondsBeforeRetryMaster' > 0)
        props.setProperty("queriesBeforeRetryMaster", "10");
        props.setProperty("secondsBeforeRetryMaster", "1");

        for (boolean autoCommit : new boolean[] { true, false }) {
            Connection testConn = getUnreliableFailoverConnection(new String[] { HOST_1, HOST_2, HOST_3 }, props, downedHosts);
            Statement testStmt = null;

            try {
                testConn.setAutoCommit(autoCommit);

                // connected to HOST_2 [\HOST_1 : /HOST_2 : \HOST_3]
                assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // get new statement
                testStmt = testConn.createStatement();
                assertSingleValueQuery(testStmt, "SELECT 1", 1L);

                // HOST_1 + HOST_3 up [/HOST_1 : /HOST_2 : /HOST_3]
                UnreliableSocketFactory.dontDownHost(HOST_1);
                UnreliableSocketFactory.dontDownHost(HOST_3);

                // continue with same statement
                long startTime = System.currentTimeMillis();
                boolean hostSwitched = false;
                do {
                    assertSingleValueQuery(testStmt, "SELECT 1", 1L);
                    if (autoCommit) {
                        if (!hostSwitched && UnreliableSocketFactory.getHostFromLastConnection().equals(HOST_1_OK)) {
                            hostSwitched = true;
                        }
                        if (hostSwitched) {
                            assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());
                        } else {
                            assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());
                        }

                    } else {
                        assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                    }
                } while (System.currentTimeMillis() - startTime < 2000);

                // HOST_2 down [/HOST_1 : \HOST_2 : /HOST_3]
                UnreliableSocketFactory.downHost(HOST_2);

                if (autoCommit) {
                    // already switched to HOST_1
                    assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

                } else {
                    // still connected to HOST_2
                    assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

                    // connects to HOST_1 on connection error
                    assertSQLException(testStmt, "SELECT 1", COMM_LINK_ERR_PATTERN);
                    assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());
                }

                assertConnectionsHistory(HOST_2_OK, HOST_1_OK);

            } finally {
                if (testStmt != null) {
                    testStmt.close();
                }
                if (testConn != null) {
                    testConn.close();
                }
            }
        }

        // test fall back off ('queriesBeforeRetryMaster' = 0 && 'secondsBeforeRetryMaster' = 0)
        props.setProperty("queriesBeforeRetryMaster", "0");
        props.setProperty("secondsBeforeRetryMaster", "0");

        for (boolean autoCommit : new boolean[] { true, false }) {
            Connection testConn = getUnreliableFailoverConnection(new String[] { HOST_1, HOST_2, HOST_3 }, props, downedHosts);
            Statement testStmt = null;

            try {
                testConn.setAutoCommit(autoCommit);

                // connected to HOST_2 [\HOST_1 : /HOST_2 : \HOST_3]
                assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // get new statement
                testStmt = testConn.createStatement();
                assertSingleValueQuery(testStmt, "SELECT 1", 1L);

                // HOST_1 + HOST_3 up [/HOST_1 : /HOST_2 : /HOST_3]
                UnreliableSocketFactory.dontDownHost(HOST_1);
                UnreliableSocketFactory.dontDownHost(HOST_3);

                // continue with same statement
                for (int i = 0; i < 55; i++) { // default queriesBeforeRetryMaster == 50
                    assertSingleValueQuery(testStmt, "SELECT 1", 1L);
                    assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());
                }

                // HOST_2 down [/HOST_1 : \HOST_2 : /HOST_3]
                UnreliableSocketFactory.downHost(HOST_2);

                // still connected to HOST_2
                assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // connects to HOST_3 on connection error
                assertSQLException(testStmt, "SELECT 1", COMM_LINK_ERR_PATTERN);
                assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // get new statement
                testStmt = testConn.createStatement();
                assertSingleValueQuery(testStmt, "SELECT 1", 1L);

                // HOST_2 up & HOST_3 down [/HOST_1 : /HOST_2 : \HOST_3]
                UnreliableSocketFactory.dontDownHost(HOST_2);
                UnreliableSocketFactory.downHost(HOST_3);

                // still connected to HOST_3
                assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // connects to HOST_1 on connection error
                assertSQLException(testStmt, "SELECT 1", COMM_LINK_ERR_PATTERN);
                assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

                assertConnectionsHistory(HOST_2_OK, HOST_3_OK, HOST_1_OK);

            } finally {
                if (testStmt != null) {
                    testStmt.close();
                }
                if (testConn != null) {
                    testConn.close();
                }
            }
        }
    }

    /**
     * Tests the property 'autoReconnect' in a failover connection using three hosts and the following sequence of events:
     * - [\HOST_1 : \HOST_2 : /HOST_3] --> HOST_3
     * - [\HOST_1 : /HOST_2 : \HOST_3] --> HOST_2
     * - [/HOST_1 : /HOST_2 : \HOST_3]
     * - [/HOST_1 : \HOST_2 : \HOST_3] --> HOST_1
     * - [/HOST_1 : \HOST_2 : /HOST_3]
     * - [\HOST_1 : \HOST_2 : /HOST_3] --> HOST_3
     * 
     * [Legend: "/HOST_n" --> HOST_n up; "\HOST_n" --> HOST_n down]
     */
    public void testFailoverAutoReconnect() throws Exception {
        Set<String> downedHosts = new HashSet<String>();
        downedHosts.add(HOST_1);
        downedHosts.add(HOST_2);

        Properties props = new Properties();
        props.setProperty("retriesAllDown", "2");
        props.setProperty("maxReconnects", "2");
        props.setProperty("initialTimeout", "1");

        for (boolean foAutoReconnect : new boolean[] { true, false }) {
            props.setProperty("autoReconnect", Boolean.toString(foAutoReconnect));

            Connection testConn = getUnreliableFailoverConnection(new String[] { HOST_1, HOST_2, HOST_3 }, props, downedHosts);
            Statement testStmt1 = null, testStmt2 = null;

            try {
                // connected to HOST_3 [\HOST_1 : \HOST_2 : /HOST_3]
                assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // get new statements
                testStmt1 = testConn.createStatement();
                testStmt2 = testConn.createStatement();
                assertSingleValueQuery(testStmt1, "SELECT 1", 1L);
                assertSingleValueQuery(testStmt2, "SELECT 2", 2L);

                // HOST_2 up & HOST_3 down [\HOST_1 : /HOST_2 : \HOST_3]
                UnreliableSocketFactory.dontDownHost(HOST_2);
                UnreliableSocketFactory.downHost(HOST_3);

                // still connected to HOST_3
                assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // connects to HOST_2 on connection error
                assertSQLException(testStmt1, "SELECT 1", COMM_LINK_ERR_PATTERN);
                assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // HOST_1 up [/HOST_1 : /HOST_2 : \HOST_3]
                UnreliableSocketFactory.dontDownHost(HOST_1);

                if (!foAutoReconnect) {
                    // statements closed
                    assertSQLException(testStmt1, "SELECT 1", STMT_CLOSED_ERR_PATTERN);
                    assertSQLException(testStmt2, "SELECT 2", STMT_CLOSED_ERR_PATTERN);

                    // get new statements
                    testStmt1 = testConn.createStatement();
                    testStmt2 = testConn.createStatement();
                } // else statements reactivated
                assertSingleValueQuery(testStmt1, "SELECT 1", 1L);
                assertSingleValueQuery(testStmt2, "SELECT 2", 2L);

                // HOST_2 down [/HOST_1 : \HOST_2 : \HOST_3]
                UnreliableSocketFactory.downHost(HOST_2);

                // still connected to HOST_2
                assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // connects to HOST_1 on connection error
                assertSQLException(testStmt2, "SELECT 2", COMM_LINK_ERR_PATTERN);
                assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // HOST_3 up [/HOST_1 : \HOST_2 : /HOST_3]
                UnreliableSocketFactory.dontDownHost(HOST_3);

                if (!foAutoReconnect) {
                    // statements closed
                    assertSQLException(testStmt1, "SELECT 1", STMT_CLOSED_ERR_PATTERN);
                    assertSQLException(testStmt2, "SELECT 2", STMT_CLOSED_ERR_PATTERN);

                    // get new statements
                    testStmt1 = testConn.createStatement();
                    testStmt2 = testConn.createStatement();
                }// else statements reactivated
                assertSingleValueQuery(testStmt1, "SELECT 1", 1L);
                assertSingleValueQuery(testStmt2, "SELECT 2", 2L);

                // HOST_1 down [\HOST_1 : \HOST_2 : /HOST_3]
                UnreliableSocketFactory.downHost(HOST_1);

                // still connected to HOST_1
                assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

                // connects to HOST_3 on connection error
                assertSQLException(testStmt1, "SELECT 1", COMM_LINK_ERR_PATTERN);
                assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());

                if (!foAutoReconnect) {
                    // statements closed
                    assertSQLException(testStmt1, "SELECT 1", STMT_CLOSED_ERR_PATTERN);
                    assertSQLException(testStmt2, "SELECT 2", STMT_CLOSED_ERR_PATTERN);

                    // get new statements
                    testStmt1 = testConn.createStatement();
                    testStmt2 = testConn.createStatement();
                }
                assertSingleValueQuery(testStmt1, "SELECT 1", 1L);
                assertSingleValueQuery(testStmt2, "SELECT 2", 2L);

                if (foAutoReconnect) {
                    // with 'autoReconnect=true' each fail counts twice ('maxReconnects=2')
                    assertConnectionsHistory(HOST_1_FAIL, HOST_1_FAIL, HOST_2_FAIL, HOST_2_FAIL, HOST_3_OK, HOST_2_OK, HOST_3_FAIL, HOST_3_FAIL, HOST_2_FAIL,
                            HOST_2_FAIL, HOST_3_FAIL, HOST_3_FAIL, HOST_1_OK, HOST_2_FAIL, HOST_2_FAIL, HOST_3_OK);
                } else {
                    assertConnectionsHistory(HOST_1_FAIL, HOST_2_FAIL, HOST_3_OK, HOST_2_OK, HOST_3_FAIL, HOST_2_FAIL, HOST_3_FAIL, HOST_1_OK, HOST_2_FAIL,
                            HOST_3_OK);
                }

            } finally {
                if (testStmt1 != null) {
                    testStmt1.close();
                }
                if (testStmt2 != null) {
                    testStmt2.close();
                }
                if (testConn != null) {
                    testConn.close();
                }
            }
        }
    }

    /**
     * Tests connection properties synchronization in a failover connection using three hosts and the following sequence of events:
     * - [\HOST_1 : /HOST_2 : \HOST_3] --> HOST_2
     * - [/HOST_1 : \HOST_2 : \HOST_3] --> HOST_1
     * - [\HOST_1 : \HOST_2 : /HOST_3] --> HOST_3
     * 
     * [Legend: "/HOST_n" --> HOST_n up; "\HOST_n" --> HOST_n down]
     */
    public void testFailoverConnectionSynchronization() throws Exception {
        Set<String> downedHosts = new HashSet<String>();
        downedHosts.add(HOST_1);
        downedHosts.add(HOST_3);

        Properties props = new Properties();
        props.setProperty("retriesAllDown", "2");
        props.setProperty("failOverReadOnly", "false");

        com.mysql.jdbc.Connection testConn = (com.mysql.jdbc.Connection) getUnreliableFailoverConnection(new String[] { HOST_1, HOST_2, HOST_3 }, props,
                downedHosts);
        Statement testStmt = null;

        int newTransactionIsolation = testConn.getTransactionIsolation();
        String newCatalog = "fotests";
        createDatabase(newCatalog);

        try {
            // connected to HOST_2 [\HOST_1 : /HOST_2 : \HOST_3]
            assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

            // assert expected default session values
            assertTrue(testConn.getAutoCommit());
            assertEquals(this.conn.getCatalog(), testConn.getCatalog());
            assertEquals(newTransactionIsolation, testConn.getTransactionIsolation());
            assertFalse(testConn.isReadOnly());
            assertEquals(-1, testConn.getSessionMaxRows());

            // change session values
            testConn.setAutoCommit(false);
            testConn.setCatalog(newCatalog);
            newTransactionIsolation = newTransactionIsolation * 2 == 16 ? 1 : newTransactionIsolation * 2;
            testConn.setTransactionIsolation(newTransactionIsolation);
            testConn.setReadOnly(true);
            testConn.setSessionMaxRows(1);

            // assert expected session values after explicit change
            assertFalse(testConn.getAutoCommit());
            assertEquals(newCatalog, testConn.getCatalog());
            assertEquals(newTransactionIsolation, testConn.getTransactionIsolation());
            assertTrue(testConn.isReadOnly());
            assertEquals(1, testConn.getSessionMaxRows());

            // get new statement
            testStmt = testConn.createStatement();
            assertSingleValueQuery(testStmt, "SELECT 1", 1L);

            // HOST_1 up & HOST_2 down [/HOST_1 : \HOST_2 : \HOST_3]
            UnreliableSocketFactory.dontDownHost(HOST_1);
            UnreliableSocketFactory.downHost(HOST_2);

            // still connected to HOST_2
            assertEquals(HOST_2_OK, UnreliableSocketFactory.getHostFromLastConnection());

            // connects to HOST_1 on connection error
            assertSQLException(testStmt, "SELECT 1", COMM_LINK_ERR_PATTERN);
            assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

            // get new statement
            testStmt = testConn.createStatement();
            assertSingleValueQuery(testStmt, "SELECT 1", 1L);

            // assert expected session values after connection synchronization
            assertFalse(testConn.getAutoCommit());
            assertEquals(newCatalog, testConn.getCatalog());
            assertEquals(newTransactionIsolation, testConn.getTransactionIsolation());
            assertTrue(testConn.isReadOnly());
            assertEquals(-1, testConn.getSessionMaxRows()); // this value is reset to default 'maxRows' when the new "internal" connection is created

            // change session values
            testConn.setAutoCommit(true);
            newTransactionIsolation = newTransactionIsolation * 2 == 16 ? 1 : newTransactionIsolation * 2;
            testConn.setTransactionIsolation(newTransactionIsolation);
            testConn.setReadOnly(false);
            testConn.setSessionMaxRows(2);

            // assert expected session values after explicit change
            assertTrue(testConn.getAutoCommit());
            assertEquals(newTransactionIsolation, testConn.getTransactionIsolation());
            assertFalse(testConn.isReadOnly());
            assertEquals(2, testConn.getSessionMaxRows());

            // HOST_1 down & HOST_3 up [\HOST_1 : \HOST_2 : /HOST_3]
            UnreliableSocketFactory.downHost(HOST_1);
            UnreliableSocketFactory.dontDownHost(HOST_3);

            // still connected to HOST_1
            assertEquals(HOST_1_OK, UnreliableSocketFactory.getHostFromLastConnection());

            // connects to HOST_3 on connection error (not time to come back to HOST_1 yet)
            assertSQLException(testStmt, "SELECT 1", COMM_LINK_ERR_PATTERN);
            assertEquals(HOST_3_OK, UnreliableSocketFactory.getHostFromLastConnection());

            // get new statement
            testStmt = testConn.createStatement();
            assertSingleValueQuery(testStmt, "SELECT 1", 1L);

            // assert expected session values after connection synchronization
            assertTrue(testConn.getAutoCommit());
            assertEquals(newCatalog, testConn.getCatalog());
            assertEquals(newTransactionIsolation, testConn.getTransactionIsolation());
            assertFalse(testConn.isReadOnly());
            assertEquals(-1, testConn.getSessionMaxRows()); // this value is reset to default 'maxRows' when the new "internal" connection is created

            assertConnectionsHistory(HOST_1_FAIL, HOST_2_OK, HOST_3_FAIL, HOST_2_FAIL, HOST_3_FAIL, HOST_1_OK, HOST_2_FAIL, HOST_3_OK);

        } finally {
            if (testStmt != null) {
                testStmt.close();
            }
            if (testConn != null) {
                //                testConn.close();
            }
        }
    }
}