/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.regression;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import junit.framework.Test;
import junit.framework.TestSuite;
import testsuite.BaseTestCase;

import com.mysql.jdbc.PacketTooBigException;
import com.mysql.jdbc.jdbc2.optional.ConnectionWrapper;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;

/**
 * Tests a PooledConnection implementation provided by a JDBC driver. Test case provided by Johnny Macchione from bug database record BUG#884. According to
 * the JDBC 2.0 specification:
 * 
 * <p>
 * "Each call to PooledConnection.getConnection() must return a newly constructed Connection object that exhibits the default Connection behavior. Only the most
 * recent Connection object produced from a particular PooledConnection is open. An existing Connection object is automatically closed, if the getConnection()
 * method of its associated Pooled-Connection is called again, before it has been explicitly closed by the application. This gives the application server a way
 * to �take away� a Connection from the application if it wishes, and give it out to someone else. This capability will not likely be used frequently in
 * practice."
 * </p>
 * 
 * <p>
 * "When the application calls Connection.close(), an event is triggered that tells the connection pool it can recycle the physical database connection. In
 * other words, the event signals the connection pool that the PooledConnection object which originally produced the Connection object generating the event can
 * be put back in the connection pool."
 * </p>
 * 
 * <p>
 * "A Connection-EventListener will also be notified when a fatal error occurs, so that it can make a note not to put a bad PooledConnection object back in the
 * cache when the application finishes using it. When an error occurs, the ConnectionEventListener is notified by the JDBC driver, just before the driver throws
 * an SQLException to the application to notify it of the same error. Note that automatic closing of a Connection object as discussed in the previous section
 * does not generate a connection close event."
 * </p>
 * The JDBC 3.0 specification states the same in other words:
 * 
 * <p>
 * "The Connection.close method closes the logical handle, but the physical connection is maintained. The connection pool manager is notified that the
 * underlying PooledConnection object is now available for reuse. If the application attempts to reuse the logical handle, the Connection implementation throws
 * an SQLException."
 * </p>
 * 
 * <p>
 * "For a given PooledConnection object, only the most recently produced logical Connection object will be valid. Any previously existing Connection object is
 * automatically closed when the associated PooledConnection.getConnection method is called. Listeners (connection pool managers) are not notified in this case.
 * This gives the application server a way to take a connection away from a client. This is an unlikely scenario but may be useful if the application server is
 * trying to force an orderly shutdown."
 * </p>
 * 
 * <p>
 * "A connection pool manager shuts down a physical connection by calling the method PooledConnection.close. This method is typically called only in certain
 * circumstances: when the application server is undergoing an orderly shutdown, when the connection cache is being reinitialized, or when the application
 * server receives an event indicating that an unrecoverable error has occurred on the connection."
 * </p>
 * Even though the specification isn't clear about it, I think it is no use
 * generating a close event when calling the method PooledConnection.close(),
 * even if a logical Connection is open for this PooledConnection, bc the
 * PooledConnection will obviously not be returned to the pool.
 */
public final class PooledConnectionRegressionTest extends BaseTestCase {
    private ConnectionPoolDataSource cpds;

    // Count nb of closeEvent.
    protected int closeEventCount;

    // Count nb of connectionErrorEvent
    protected int connectionErrorEventCount;

    /**
     * Creates a new instance of ProgressPooledConnectionTest
     * 
     * @param testname
     */
    public PooledConnectionRegressionTest(String testname) {
        super(testname);
    }

    /**
     * Set up test case before a test is run.
     * 
     * @throws Exception
     */
    @Override
    public void setUp() throws Exception {
        super.setUp();

        // Reset event count.
        this.closeEventCount = 0;
        this.connectionErrorEventCount = 0;

        MysqlConnectionPoolDataSource ds = new MysqlConnectionPoolDataSource();

        ds.setURL(BaseTestCase.dbUrl);

        this.cpds = ds;
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(PooledConnectionRegressionTest.class);
    }

    /**
     * @return a test suite composed of this test case.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite(PooledConnectionRegressionTest.class);

        return suite;
    }

    /**
     * After the test is run.
     */
    @Override
    public void tearDown() throws Exception {
        this.cpds = null;
        super.tearDown();
    }

    /**
     * Tests fix for BUG#7136 ... Statement.getConnection() returning physical
     * connection instead of logical connection.
     */
    public void testBug7136() {
        final ConnectionEventListener conListener = new ConnectionListener();
        PooledConnection pc = null;
        this.closeEventCount = 0;

        try {
            pc = this.cpds.getPooledConnection();

            pc.addConnectionEventListener(conListener);

            Connection _conn = pc.getConnection();

            Connection connFromStatement = _conn.createStatement().getConnection();

            // This should generate a close event.

            connFromStatement.close();

            assertEquals("One close event should've been registered", 1, this.closeEventCount);

            this.closeEventCount = 0;

            _conn = pc.getConnection();

            Connection connFromPreparedStatement = _conn.prepareStatement("SELECT 1").getConnection();

            // This should generate a close event.

            connFromPreparedStatement.close();

            assertEquals("One close event should've been registered", 1, this.closeEventCount);

        } catch (SQLException ex) {
            fail(ex.toString());
        } finally {
            if (pc != null) {
                try {
                    pc.close();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    /**
     * Test the nb of closeEvents generated when a Connection is reclaimed. No
     * event should be generated in that case.
     */
    public void testConnectionReclaim() {
        final ConnectionEventListener conListener = new ConnectionListener();
        PooledConnection pc = null;
        final int NB_TESTS = 5;

        try {
            pc = this.cpds.getPooledConnection();

            pc.addConnectionEventListener(conListener);

            for (int i = 0; i < NB_TESTS; i++) {
                Connection _conn = pc.getConnection();

                try {
                    // Try to reclaim connection.
                    System.out.println("Before connection reclaim.");

                    _conn = pc.getConnection();

                    System.out.println("After connection reclaim.");
                } finally {
                    if (_conn != null) {
                        System.out.println("Before connection.close().");

                        // This should generate a close event.
                        _conn.close();

                        System.out.println("After connection.close().");
                    }
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            fail(ex.toString());
        } finally {
            if (pc != null) {
                try {
                    System.out.println("Before pooledConnection.close().");

                    // This should not generate a close event.
                    pc.close();

                    System.out.println("After pooledConnection.close().");
                } catch (SQLException ex) {
                    ex.printStackTrace();
                    fail(ex.toString());
                }
            }
        }

        assertEquals("Wrong nb of CloseEvents: ", NB_TESTS, this.closeEventCount);
    }

    /**
     * Tests that PacketTooLargeException doesn't clober the connection.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testPacketTooLargeException() throws Exception {
        final ConnectionEventListener conListener = new ConnectionListener();
        PooledConnection pc = null;

        pc = this.cpds.getPooledConnection();

        pc.addConnectionEventListener(conListener);

        createTable("testPacketTooLarge", "(field1 LONGBLOB)");

        Connection connFromPool = pc.getConnection();
        PreparedStatement pstmtFromPool = ((ConnectionWrapper) connFromPool).clientPrepare("INSERT INTO testPacketTooLarge VALUES (?)");

        this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'max_allowed_packet'");
        this.rs.next();

        int maxAllowedPacket = this.rs.getInt(2);

        int numChars = (int) (maxAllowedPacket * 1.2);

        pstmtFromPool.setBinaryStream(1, new BufferedInputStream(new FileInputStream(newTempBinaryFile("testPacketTooLargeException", numChars))), numChars);

        try {
            pstmtFromPool.executeUpdate();
            fail("Expecting PacketTooLargeException");
        } catch (PacketTooBigException ptbe) {
            // We're expecting this one...
        }

        // This should still work okay, even though the last query on the same connection didn't...
        connFromPool.createStatement().executeQuery("SELECT 1");

        assertTrue(this.connectionErrorEventCount == 0);
        assertTrue(this.closeEventCount == 0);
    }

    /**
     * Test the nb of closeEvents generated by a PooledConnection. A
     * JDBC-compliant driver should only generate 1 closeEvent each time
     * connection.close() is called.
     */
    public void testCloseEvent() {
        final ConnectionEventListener conListener = new ConnectionListener();
        PooledConnection pc = null;
        final int NB_TESTS = 5;

        try {
            pc = this.cpds.getPooledConnection();

            pc.addConnectionEventListener(conListener);

            for (int i = 0; i < NB_TESTS; i++) {
                Connection pConn = pc.getConnection();

                System.out.println("Before connection.close().");

                // This should generate a close event.
                pConn.close();

                System.out.println("After connection.close().");
            }
        } catch (SQLException ex) {
            fail(ex.toString());
        } finally {
            if (pc != null) {
                try {
                    System.out.println("Before pooledConnection.close().");

                    // This should not generate a close event.
                    pc.close();

                    System.out.println("After pooledConnection.close().");
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        }
        assertEquals("Wrong nb of CloseEvents: ", NB_TESTS, this.closeEventCount);
    }

    /**
     * Listener for PooledConnection events.
     */
    protected final class ConnectionListener implements ConnectionEventListener {
        /** */
        public void connectionClosed(ConnectionEvent event) {
            PooledConnectionRegressionTest.this.closeEventCount++;
            System.out.println(PooledConnectionRegressionTest.this.closeEventCount + " - Connection closed.");
        }

        /** */
        public void connectionErrorOccurred(ConnectionEvent event) {
            PooledConnectionRegressionTest.this.connectionErrorEventCount++;
            System.out.println("Connection error: " + event.getSQLException());
        }
    }

    /**
     * Tests fix for BUG#35489 - Prepared statements from pooled connections
     * cause NPE when closed() under JDBC4
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug35489() throws Exception {
        MysqlConnectionPoolDataSource pds = new MysqlConnectionPoolDataSource();
        pds.setUrl(dbUrl);
        this.pstmt = pds.getPooledConnection().getConnection().prepareStatement("SELECT 1");
        this.pstmt.execute();
        this.pstmt.close();

        MysqlXADataSource xads = new MysqlXADataSource();
        xads.setUrl(dbUrl);
        this.pstmt = xads.getXAConnection().getConnection().prepareStatement("SELECT 1");
        this.pstmt.execute();
        this.pstmt.close();

        xads = new MysqlXADataSource();
        xads.setUrl(dbUrl);
        xads.setPinGlobalTxToPhysicalConnection(true);
        this.pstmt = xads.getXAConnection().getConnection().prepareStatement("SELECT 1");
        this.pstmt.execute();
        this.pstmt.close();
    }
}