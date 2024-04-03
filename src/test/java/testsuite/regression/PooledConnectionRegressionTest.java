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

package testsuite.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Callable;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mysql.cj.NativeSession;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.PropertyDefinitions.SslMode;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.Blob;
import com.mysql.cj.jdbc.CallableStatementWrapper;
import com.mysql.cj.jdbc.Clob;
import com.mysql.cj.jdbc.CommentClientInfoProvider;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.ConnectionWrapper;
import com.mysql.cj.jdbc.DatabaseMetaData;
import com.mysql.cj.jdbc.DatabaseMetaDataUsingInfoSchema;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.JdbcPropertySetImpl;
import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;
import com.mysql.cj.jdbc.MysqlSQLXML;
import com.mysql.cj.jdbc.MysqlXADataSource;
import com.mysql.cj.jdbc.NClob;
import com.mysql.cj.jdbc.PreparedStatementWrapper;
import com.mysql.cj.jdbc.StatementWrapper;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import com.mysql.cj.jdbc.exceptions.PacketTooBigException;

import testsuite.BaseTestCase;

/**
 * Tests a PooledConnection implementation provided by a JDBC driver. Test case provided by Johnny Macchione from bug database record BUG#884. According to
 * the JDBC 2.0 specification:
 *
 * <p>
 * "Each call to PooledConnection.getConnection() must return a newly constructed Connection object that exhibits the default Connection behavior. Only the most
 * recent Connection object produced from a particular PooledConnection is open. An existing Connection object is automatically closed, if the getConnection()
 * method of its associated Pooled-Connection is called again, before it has been explicitly closed by the application. This gives the application server a way
 * to 'take away' a Connection from the application if it wishes, and give it out to someone else. This capability will not likely be used frequently in
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
 * Even though the specification isn't clear about it, I think it is no use generating a close event when calling the method PooledConnection.close(), even if a
 * logical Connection is open for this PooledConnection, bc the PooledConnection will obviously not be returned to the pool.
 */
public final class PooledConnectionRegressionTest extends BaseTestCase {

    private ConnectionPoolDataSource cpds;

    // Count nb of closeEvent.
    protected int closeEventCount;

    // Count nb of connectionErrorEvent
    protected int connectionErrorEventCount;

    /**
     * Set up test case before a test is run.
     *
     * @throws Exception
     */
    @BeforeEach
    public void setUp() throws Exception {
        // Reset event count.
        this.closeEventCount = 0;
        this.connectionErrorEventCount = 0;

        MysqlConnectionPoolDataSource ds = new MysqlConnectionPoolDataSource();

        ds.setURL(BaseTestCase.dbUrl);
        ds.<SslMode>getEnumProperty(PropertyKey.sslMode).setValue(SslMode.DISABLED);
        ds.getBooleanProperty(PropertyKey.allowPublicKeyRetrieval).setValue(true);

        this.cpds = ds;
    }

    /**
     * After the test is run.
     *
     * @throws Exception
     */
    @AfterEach
    public void tearDown() throws Exception {
        this.cpds = null;
    }

    /**
     * Tests fix for BUG#7136 ... Statement.getConnection() returning physical connection instead of logical connection.
     *
     * @throws Exception
     */
    @Test
    public void testBug7136() throws Exception {
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

            assertEquals(1, this.closeEventCount, "One close event should've been registered");

            this.closeEventCount = 0;

            _conn = pc.getConnection();

            Connection connFromPreparedStatement = _conn.prepareStatement("SELECT 1").getConnection();

            // This should generate a close event.

            connFromPreparedStatement.close();

            assertEquals(1, this.closeEventCount, "One close event should've been registered");

        } finally {
            if (pc != null) {
                pc.close();
            }
        }
    }

    /**
     * Test the nb of closeEvents generated when a Connection is reclaimed. No event should be generated in that case.
     *
     * @throws Exception
     */
    @Test
    public void testConnectionReclaim() throws Exception {
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
        } finally {
            if (pc != null) {
                System.out.println("Before pooledConnection.close().");
                // This should not generate a close event.
                pc.close();
                System.out.println("After pooledConnection.close().");
            }
        }

        assertEquals(NB_TESTS, this.closeEventCount, "Wrong nb of CloseEvents: ");
    }

    /**
     * Tests that PacketTooLargeException doesn't clober the connection.
     *
     * @throws Exception
     */
    @Test
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

        assertThrows(PacketTooBigException.class, () -> {
            pstmtFromPool.executeUpdate();
            return null;
        });

        // This should still work okay, even though the last query on the same connection didn't...
        this.rs = connFromPool.createStatement().executeQuery("SELECT 1");

        assertEquals(0, this.connectionErrorEventCount);
        assertEquals(0, this.closeEventCount);
    }

    /**
     * Test the nb of closeEvents generated by a PooledConnection. A JDBC-compliant driver should only generate 1 closeEvent each time connection.close() is
     * called.
     *
     * @throws Exception
     */
    @Test
    public void testCloseEvent() throws Exception {
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
        assertEquals(NB_TESTS, this.closeEventCount, "Wrong nb of CloseEvents: ");
    }

    /**
     * Listener for PooledConnection events.
     */
    protected final class ConnectionListener implements ConnectionEventListener {

        @Override
        public void connectionClosed(ConnectionEvent event) {
            PooledConnectionRegressionTest.this.closeEventCount++;
            System.out.println(PooledConnectionRegressionTest.this.closeEventCount + " - Connection closed.");
        }

        @Override
        public void connectionErrorOccurred(ConnectionEvent event) {
            PooledConnectionRegressionTest.this.connectionErrorEventCount++;
            System.out.println("Connection error: " + event.getSQLException());
        }

    }

    /**
     * Tests fix for BUG#35489 - Prepared statements from pooled connections cause NPE when closed() under JDBC4
     *
     * @throws Exception
     */
    @Test
    public void testBug35489() throws Exception {
        MysqlConnectionPoolDataSource pds = new MysqlConnectionPoolDataSource();
        pds.setUrl(dbUrl);
        pds.<SslMode>getEnumProperty(PropertyKey.sslMode).setValue(SslMode.DISABLED);
        pds.getBooleanProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName()).setValue(true);
        this.pstmt = pds.getPooledConnection().getConnection().prepareStatement("SELECT 1");
        this.pstmt.execute();
        this.pstmt.close();

        MysqlXADataSource xads = new MysqlXADataSource();
        xads.setUrl(dbUrl);
        xads.<SslMode>getEnumProperty(PropertyKey.sslMode).setValue(SslMode.DISABLED);
        xads.getBooleanProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName()).setValue(true);
        this.pstmt = xads.getXAConnection().getConnection().prepareStatement("SELECT 1");
        this.pstmt.execute();
        this.pstmt.close();

        xads = new MysqlXADataSource();
        xads.setUrl(dbUrl);
        xads.<SslMode>getEnumProperty(PropertyKey.sslMode).setValue(SslMode.DISABLED);
        xads.getBooleanProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName()).setValue(true);
        xads.getProperty(PropertyKey.pinGlobalTxToPhysicalConnection).setValue(true);
        this.pstmt = xads.getXAConnection().getConnection().prepareStatement("SELECT 1");
        this.pstmt.execute();
        this.pstmt.close();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testConnectionWrapperMethods() throws Exception {
        PooledConnection pc = null;
        pc = this.cpds.getPooledConnection();
        ConnectionWrapper cw = (ConnectionWrapper) pc.getConnection();

        assertEquals(PreparedStatementWrapper.class, cw.clientPrepare("SELECT 1").getClass());
        assertEquals(PreparedStatementWrapper.class, cw.clientPrepare("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).getClass());
        assertEquals(PreparedStatementWrapper.class, cw.clientPrepareStatement("SELECT 1").getClass());
        assertEquals(PreparedStatementWrapper.class, cw.clientPrepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS).getClass());
        assertEquals(PreparedStatementWrapper.class, cw.clientPrepareStatement("SELECT 1", new int[] { 1 }).getClass());
        assertEquals(PreparedStatementWrapper.class, cw.clientPrepareStatement("SELECT 1", new String[] { "1" }).getClass());
        assertEquals(PreparedStatementWrapper.class, cw.clientPrepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).getClass());
        assertEquals(PreparedStatementWrapper.class,
                cw.clientPrepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT).getClass());
        assertEquals(PreparedStatementWrapper.class, cw.serverPrepareStatement("SELECT 1").getClass());
        assertEquals(PreparedStatementWrapper.class, cw.serverPrepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS).getClass());
        assertEquals(PreparedStatementWrapper.class, cw.serverPrepareStatement("SELECT 1", new int[] { 1 }).getClass());
        assertEquals(PreparedStatementWrapper.class, cw.serverPrepareStatement("SELECT 1", new String[] { "1" }).getClass());
        assertEquals(PreparedStatementWrapper.class, cw.serverPrepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).getClass());
        assertEquals(PreparedStatementWrapper.class,
                cw.serverPrepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT).getClass());
        assertEquals(PreparedStatementWrapper.class, cw.prepareStatement("SELECT 1").getClass());
        assertEquals(PreparedStatementWrapper.class, cw.prepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS).getClass());
        assertEquals(PreparedStatementWrapper.class, cw.prepareStatement("SELECT 1", new int[] { 1 }).getClass());
        assertEquals(PreparedStatementWrapper.class, cw.prepareStatement("SELECT 1", new String[] { "1" }).getClass());
        assertEquals(PreparedStatementWrapper.class, cw.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).getClass());
        assertEquals(PreparedStatementWrapper.class,
                cw.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT).getClass());

        assertEquals(CallableStatementWrapper.class, cw.prepareCall("SELECT 1").getClass());
        assertEquals(CallableStatementWrapper.class, cw.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).getClass());
        assertEquals(CallableStatementWrapper.class,
                cw.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT).getClass());

        assertEquals(StatementWrapper.class, cw.createStatement().getClass());
        assertEquals(StatementWrapper.class, cw.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).getClass());
        assertEquals(StatementWrapper.class,
                cw.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT).getClass());

        assertEquals(26, cw.getActiveStatementCount());

        assertThrows(SQLFeatureNotSupportedException.class, () -> {
            cw.createArrayOf(String.class.getName(), new Object[] {}).getClass();
            return null;
        });
        assertThrows(SQLFeatureNotSupportedException.class, () -> {
            cw.createStruct(String.class.getName(), new Object[] {}).getClass();
            return null;
        });

        assertEquals(Blob.class, cw.createBlob().getClass());
        assertEquals(Clob.class, cw.createClob().getClass());
        assertEquals(NClob.class, cw.createNClob().getClass());
        assertEquals(MysqlSQLXML.class, cw.createSQLXML().getClass());
        assertEquals(ConnectionImpl.class, cw.getActiveMySQLConnection().getClass());

        assertEquals(this.conn.getAutoCommit(), cw.getAutoCommit());
        assertEquals(((JdbcConnection) this.conn).getAutoIncrementIncrement(), cw.getAutoIncrementIncrement());
        assertEquals(((JdbcConnection) this.conn).getCatalog(), cw.getCatalog());
        assertEquals(((JdbcConnection) this.conn).getCharacterSetMetadata(), cw.getCharacterSetMetadata());
        assertEquals(((JdbcConnection) this.conn).getHoldability(), cw.getHoldability());
        assertEquals(((JdbcConnection) this.conn).getHost(), cw.getHost());
        assertEquals(((JdbcConnection) this.conn).getHostPortPair(), cw.getHostPortPair());
        assertEquals(Properties.class, cw.getProperties().getClass());
        assertEquals(JdbcPropertySetImpl.class, cw.getPropertySet().getClass());
        assertEquals(((JdbcConnection) this.conn).getSchema(), cw.getSchema());
        assertEquals(((JdbcConnection) this.conn).getServerVersion().toString(), cw.getServerVersion().toString());
        assertEquals(NativeSession.class, cw.getSession().getClass());
        assertEquals(((JdbcConnection) this.conn).getSessionMaxRows(), cw.getSessionMaxRows());
        assertEquals(((JdbcConnection) this.conn).getURL(), cw.getURL());
        assertEquals(((JdbcConnection) this.conn).getUser(), cw.getUser());
        assertFalse(cw.isClosed());
        assertFalse(cw.isInGlobalTx());
        assertFalse(cw.isSourceConnection());
        assertFalse(cw.isProxySet());
        assertFalse(cw.isReadOnly());
        assertFalse(cw.isReadOnly(false));
        assertFalse(cw.isReadOnly(true));
        assertEquals(isMysqlRunningLocally(), cw.isServerLocal());
        assertTrue(cw.isValid(10));
        assertTrue(cw.isWrapperFor(Connection.class));
        assertEquals(((JdbcConnection) this.conn).lowerCaseTableNames(), cw.lowerCaseTableNames());

        assertEquals(CommentClientInfoProvider.class, cw.getClientInfoProviderImpl().getClass());
        Properties ci1 = new Properties();
        ci1.setProperty("k1", "v1");
        ci1.setProperty("k2", "v2");
        cw.setClientInfo(ci1);
        cw.setClientInfo("k3", "v3");
        Properties ci2 = cw.getClientInfo();
        assertFalse(ci1.equals(ci2));
        assertEquals("v1", cw.getClientInfo("k1"));
        assertEquals("v2", cw.getClientInfo("k2"));
        assertEquals("v3", cw.getClientInfo("k3"));

        String comment = cw.getStatementComment();
        assertEquals("k3=v3, k2=v2, k1=v1", comment);
        cw.setStatementComment("Test comment");
        assertNotEquals(((JdbcConnection) this.conn).getStatementComment(), cw.getStatementComment());

        assertNull(cw.getExceptionInterceptor());
        assertEquals(((JdbcConnection) this.conn).getNetworkTimeout(), cw.getNetworkTimeout());
        assertEquals(((JdbcConnection) this.conn).getTypeMap(), cw.getTypeMap());
        assertNull(cw.getWarnings());

        // testsuite is built upon non-SSL default connection with additional useSSL=false&allowPublicKeyRetrieval=true properties
        assertFalse(cw.hasSameProperties((JdbcConnection) this.conn));

        assertTrue(cw.isSameResource((JdbcConnection) this.conn));
        assertEquals(((JdbcConnection) this.conn).nativeSQL("SELECT 1"), cw.nativeSQL("SELECT 1"));

        assertEquals(cw.getServerVersion().meetsMinimum(new ServerVersion(8, 0, 3)) ? DatabaseMetaDataUsingInfoSchema.class : DatabaseMetaData.class,
                cw.getMetaData().getClass());

        // TODO find a way to test following methods
        //        cw.getId();
        //        cw.getIdleFor();
        //        cw.getMetadataSafeStatement();
        //        cw.getMultiHostSafeProxy();
        //        cw.resetServerState();

        cw.setCatalog(this.dbName);
        cw.setFailedOver(false);
        cw.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        cw.setInGlobalTx(false);
        // TODO find a way to test following methods
        //        cw.setNetworkTimeout(executor, milliseconds);
        //        cw.setProxy(this.conn);
        //        cw.setReadOnly(readOnly);
        //        cw.setReadOnlyInternal(readOnlyFlag);
        //        cw.setSchema(schema);
        //        cw.setSessionMaxRows(max);
        //        cw.setTypeMap(map);
        assertEquals(((JdbcConnection) this.conn).storesLowerCaseTableName(), cw.storesLowerCaseTableName());

        // TODO find a way to test following methods
        //        cw.getQueryInterceptorsInstances();
        //        cw.initializeSafeQueryInterceptors();
        //        cw.unSafeQueryInterceptors();
        //        cw.unwrap(iface);
        //        cw.initializeResultsMetadataFromCache(sql, cachedMetaData, resultSet);
        //        cw.getCachedMetaData(sql);

        cw.setAutoCommit(false);
        cw.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, cw.getTransactionIsolation());
        cw.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, cw.getTransactionIsolation());
        // TODO find a way to test following methods
        //cw.transactionBegun();
        //cw.transactionCompleted();
        //        cw.commit();
        //        cw.rollback();
        //        cw.rollback(arg0);
        //        cw.setSavepoint();
        //        cw.setSavepoint(arg0);
        //        cw.releaseSavepoint(arg0);
        cw.setAutoCommit(true);

        // TODO find a way to test following methods
        //        cw.registerStatement(this.stmt);
        //        cw.unregisterStatement(this.stmt);
        //        cw.decachePreparedStatement(this.pstmt);
        //        cw.recachePreparedStatement(this.pstmt);

        // TODO find a way to test following methods
        //        cw.clearWarnings();
        //        cw.ping();
        //        cw.pingInternal(checkForClosedConnection, timeoutMillis);
        //        cw.createNewIO(isForReconnect);
        //        cw.changeUser(userName, newPassword);
        //        cw.checkClosed();

        cw.close();
        assertEquals(26, cw.getActiveStatementCount()); // TODO why are they still active? Active statements should be cleaned when connection is returned to pool.
        checkConnectionReturnedToPool(cw);

        cw.normalClose();
        assertEquals(0, cw.getActiveStatementCount());
        checkReallyClosedConnection(cw);

        // TODO find a way to test following methods
        //        cw.realClose(calledExplicitly, issueRollback, skipLocalTeardown, reason);
        //        cw.cleanup(whyCleanedUp);
        //        cw.abort(executor);
        //        cw.abortInternal();
    }

    @SuppressWarnings("deprecation")
    private void checkConnectionReturnedToPool(ConnectionWrapper cw) throws Exception {
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.clientPrepare("SELECT 1");
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.clientPrepare("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.clientPrepareStatement("SELECT 1");
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.clientPrepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.clientPrepareStatement("SELECT 1", new int[] { 1 });
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.clientPrepareStatement("SELECT 1", new String[] { "1" });
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.clientPrepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.clientPrepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.serverPrepareStatement("SELECT 1");
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.serverPrepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.serverPrepareStatement("SELECT 1", new int[] { 1 });
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.serverPrepareStatement("SELECT 1", new String[] { "1" });
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.serverPrepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.serverPrepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.prepareStatement("SELECT 1");
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.prepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.prepareStatement("SELECT 1", new int[] { 1 });
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.prepareStatement("SELECT 1", new String[] { "1" });
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.prepareCall("SELECT 1");
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.createStatement();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            return null;
        });

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.createArrayOf(String.class.getName(), new Object[] {}).getClass();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.createStruct(String.class.getName(), new Object[] {}).getClass();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.createBlob();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.createClob();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.createNClob();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.createSQLXML();
            return null;
        });

        assertEquals(ConnectionImpl.class, cw.getActiveMySQLConnection().getClass());

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.getAutoCommit();
            return null;
        });

        assertEquals(((JdbcConnection) this.conn).getAutoIncrementIncrement(), cw.getAutoIncrementIncrement());

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.getCatalog();
            return null;
        });

        assertEquals(((JdbcConnection) this.conn).getCharacterSetMetadata(), cw.getCharacterSetMetadata());

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.getHoldability();
            return null;
        });
        assertEquals(((JdbcConnection) this.conn).getHost(), cw.getHost());
        assertEquals(((JdbcConnection) this.conn).getHostPortPair(), cw.getHostPortPair());
        assertEquals(Properties.class, cw.getProperties().getClass());
        assertEquals(JdbcPropertySetImpl.class, cw.getPropertySet().getClass());
        assertEquals(((JdbcConnection) this.conn).getSchema(), cw.getSchema());
        assertEquals(((JdbcConnection) this.conn).getServerVersion().toString(), cw.getServerVersion().toString());
        assertEquals(NativeSession.class, cw.getSession().getClass());
        assertEquals(((JdbcConnection) this.conn).getSessionMaxRows(), cw.getSessionMaxRows());
        assertEquals(((JdbcConnection) this.conn).getURL(), cw.getURL());
        assertEquals(((JdbcConnection) this.conn).getUser(), cw.getUser());
        assertTrue(cw.isClosed());
        assertFalse(cw.isInGlobalTx());
        assertFalse(cw.isSourceConnection());
        assertFalse(cw.isProxySet());

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.isReadOnly();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.isReadOnly(false);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.isReadOnly(true);
            return null;
        });
        assertEquals(isMysqlRunningLocally(), cw.isServerLocal());
        assertTrue(cw.isValid(10));
        assertTrue(cw.isWrapperFor(Connection.class));
        assertEquals(((JdbcConnection) this.conn).lowerCaseTableNames(), cw.lowerCaseTableNames());

        assertEquals(CommentClientInfoProvider.class, cw.getClientInfoProviderImpl().getClass());

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            Properties ci1 = new Properties();
            ci1.setProperty("k1", "v1");
            cw.setClientInfo(ci1);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.setClientInfo("k4", "v4");
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.getClientInfo();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.getClientInfo("k1");
            return null;
        });

        assertEquals(ConnectionImpl.class, cw.getActiveMySQLConnection().getClass());
        assertNull(cw.getExceptionInterceptor());
        assertEquals(((JdbcConnection) this.conn).getNetworkTimeout(), cw.getNetworkTimeout());
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.getTypeMap();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.getWarnings();
            return null;
        });

        // testsuite is built upon non-SSL default connection with additional useSSL=false&allowPublicKeyRetrieval=true properties
        assertFalse(cw.hasSameProperties((JdbcConnection) this.conn));

        assertTrue(cw.isSameResource((JdbcConnection) this.conn));
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.nativeSQL("SELECT 1");
            return null;
        });

        String comment = cw.getStatementComment();
        assertEquals("Test comment", comment);
        cw.setStatementComment("Test comment 2");
        assertNotEquals(((JdbcConnection) this.conn).getStatementComment(), cw.getStatementComment());

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.getMetaData();
            return null;
        });

        // TODO find a way to test following methods
        //        cw.getId();
        //        cw.getIdleFor();
        //        cw.getMetadataSafeStatement();
        //        cw.getMultiHostSafeProxy();
        //        cw.resetServerState();

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", new Callable<Void>() {

            @Override
            @SuppressWarnings("synthetic-access")
            public Void call() throws Exception {
                cw.setCatalog(PooledConnectionRegressionTest.this.dbName);
                return null;
            }

        });

        cw.setFailedOver(false);

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
            return null;
        });

        cw.setInGlobalTx(false);
        // TODO find a way to test following methods
        //        cw.setNetworkTimeout(executor, milliseconds);
        //        cw.setProxy(this.conn);
        //        cw.setReadOnly(readOnly);
        //        cw.setReadOnlyInternal(readOnlyFlag);
        //        cw.setSchema(schema);
        //        cw.setSessionMaxRows(max);
        //        cw.setTypeMap(map);
        assertEquals(((JdbcConnection) this.conn).storesLowerCaseTableName(), cw.storesLowerCaseTableName());

        // TODO find a way to test following methods
        //        cw.getQueryInterceptorsInstances();
        //        cw.initializeSafeQueryInterceptors();
        //        cw.unSafeQueryInterceptors();
        //        cw.unwrap(iface);
        //        cw.initializeResultsMetadataFromCache(sql, cachedMetaData, resultSet);
        //        cw.getCachedMetaData(sql);

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.setAutoCommit(false);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.getTransactionIsolation();
            return null;
        });
        cw.transactionBegun();
        cw.transactionCompleted();
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.commit();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.rollback();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.rollback(null);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.setSavepoint();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.setSavepoint("SP1");
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.releaseSavepoint(null);
            return null;
        });

        // TODO find a way to test following methods
        //        cw.registerStatement(this.stmt);
        //        cw.unregisterStatement(this.stmt);
        //        cw.decachePreparedStatement(this.pstmt);
        //        cw.recachePreparedStatement(this.pstmt);
        //        cw.clearWarnings();
        //        cw.ping();
        //        cw.pingInternal(checkForClosedConnection, timeoutMillis);
        //        cw.createNewIO(isForReconnect);
        //        cw.changeUser(userName, newPassword);
        //        cw.checkClosed();
        //        cw.realClose(calledExplicitly, issueRollback, skipLocalTeardown, reason);
        //        cw.cleanup(whyCleanedUp);
        //        cw.abort(executor);
        //        cw.abortInternal();
    }

    @SuppressWarnings("deprecation")
    private void checkReallyClosedConnection(ConnectionWrapper cw) throws Exception {
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.clientPrepare("SELECT 1");
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.clientPrepare("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.clientPrepareStatement("SELECT 1");
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.clientPrepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.clientPrepareStatement("SELECT 1", new int[] { 1 });
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.clientPrepareStatement("SELECT 1", new String[] { "1" });
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.clientPrepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.clientPrepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.serverPrepareStatement("SELECT 1");
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.serverPrepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.serverPrepareStatement("SELECT 1", new int[] { 1 });
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.serverPrepareStatement("SELECT 1", new String[] { "1" });
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.serverPrepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.serverPrepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.prepareStatement("SELECT 1");
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.prepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.prepareStatement("SELECT 1", new int[] { 1 });
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.prepareStatement("SELECT 1", new String[] { "1" });
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.prepareCall("SELECT 1");
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.createStatement();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            return null;
        });

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.createArrayOf(String.class.getName(), new Object[] {}).getClass();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.createStruct(String.class.getName(), new Object[] {}).getClass();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.createBlob();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.createClob();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.createNClob();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.createSQLXML();
            return null;
        });

        assertEquals(ConnectionImpl.class, cw.getActiveMySQLConnection().getClass());

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.getAutoCommit();
            return null;
        });

        assertEquals(((JdbcConnection) this.conn).getAutoIncrementIncrement(), cw.getAutoIncrementIncrement());

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.getCatalog();
            return null;
        });

        assertEquals(((JdbcConnection) this.conn).getCharacterSetMetadata(), cw.getCharacterSetMetadata());

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.getHoldability();
            return null;
        });
        assertEquals(((JdbcConnection) this.conn).getHost(), cw.getHost());
        assertEquals(((JdbcConnection) this.conn).getHostPortPair(), cw.getHostPortPair());
        assertEquals(Properties.class, cw.getProperties().getClass());
        assertEquals(JdbcPropertySetImpl.class, cw.getPropertySet().getClass());
        assertThrows(SQLNonTransientConnectionException.class, "No operations allowed after connection closed.", () -> {
            cw.getSchema();
            return null;
        });
        assertEquals(((JdbcConnection) this.conn).getServerVersion().toString(), cw.getServerVersion().toString());
        assertEquals(NativeSession.class, cw.getSession().getClass());
        assertEquals(((JdbcConnection) this.conn).getSessionMaxRows(), cw.getSessionMaxRows());
        assertEquals(((JdbcConnection) this.conn).getURL(), cw.getURL());
        assertEquals(((JdbcConnection) this.conn).getUser(), cw.getUser());
        assertTrue(cw.isClosed());
        assertFalse(cw.isInGlobalTx());
        assertFalse(cw.isSourceConnection());
        assertFalse(cw.isProxySet());

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.isReadOnly();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.isReadOnly(false);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.isReadOnly(true);
            return null;
        });
        assertThrows(CommunicationsException.class, () -> {
            cw.isServerLocal();
            return null;
        });
        assertFalse(cw.isValid(10));
        assertTrue(cw.isWrapperFor(Connection.class));
        assertEquals(((JdbcConnection) this.conn).lowerCaseTableNames(), cw.lowerCaseTableNames());

        assertEquals(CommentClientInfoProvider.class, cw.getClientInfoProviderImpl().getClass());

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            Properties ci1 = new Properties();
            ci1.setProperty("k1", "v1");
            cw.setClientInfo(ci1);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.setClientInfo("k4", "v4");
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.getClientInfo();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.getClientInfo("k1");
            return null;
        });
        assertEquals(ConnectionImpl.class, cw.getActiveMySQLConnection().getClass());
        assertNull(cw.getExceptionInterceptor());

        String comment = cw.getStatementComment();
        assertEquals("Test comment 2", comment);
        cw.setStatementComment("Test comment 3");
        assertNotEquals(((JdbcConnection) this.conn).getStatementComment(), cw.getStatementComment());

        assertThrows(SQLNonTransientConnectionException.class, "No operations allowed after connection closed.", () -> {
            cw.getNetworkTimeout();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.getTypeMap();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.getWarnings();
            return null;
        });

        // testsuite is built upon non-SSL default connection with additional useSSL=false&allowPublicKeyRetrieval=true properties
        assertFalse(cw.hasSameProperties((JdbcConnection) this.conn));

        assertTrue(cw.isSameResource((JdbcConnection) this.conn));

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.nativeSQL("SELECT 1");
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.getMetaData();
            return null;
        });

        // TODO find a way to test following methods
        //        cw.getId();
        //        cw.getIdleFor();
        //        cw.getMetadataSafeStatement();
        //        cw.getMultiHostSafeProxy();
        //        cw.resetServerState();

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", new Callable<Void>() {

            @Override
            @SuppressWarnings("synthetic-access")
            public Void call() throws Exception {
                cw.setCatalog(PooledConnectionRegressionTest.this.dbName);
                return null;
            }

        });

        cw.setFailedOver(false);

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
            return null;
        });

        cw.setInGlobalTx(false);
        // TODO find a way to test following methods
        //        cw.setNetworkTimeout(executor, milliseconds);
        //        cw.setProxy(this.conn);
        //        cw.setReadOnly(readOnly);
        //        cw.setReadOnlyInternal(readOnlyFlag);
        //        cw.setSchema(schema);
        //        cw.setSessionMaxRows(max);
        //        cw.setTypeMap(map);
        assertEquals(((JdbcConnection) this.conn).storesLowerCaseTableName(), cw.storesLowerCaseTableName());

        // TODO find a way to test following methods
        //        cw.getQueryInterceptorsInstances();
        //        cw.initializeSafeQueryInterceptors();
        //        cw.unSafeQueryInterceptors();
        //        cw.unwrap(iface);
        //        cw.initializeResultsMetadataFromCache(sql, cachedMetaData, resultSet);
        //        cw.getCachedMetaData(sql);

        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.setAutoCommit(false);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.getTransactionIsolation();
            return null;
        });
        cw.transactionBegun();
        cw.transactionCompleted();
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.commit();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.rollback();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.rollback(null);
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.setSavepoint();
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.setSavepoint("SP1");
            return null;
        });
        assertThrows(SQLNonTransientConnectionException.class, "Logical handle no longer valid", () -> {
            cw.releaseSavepoint(null);
            return null;
        });

        // TODO find a way to test following methods
        //        cw.registerStatement(this.stmt);
        //        cw.unregisterStatement(this.stmt);
        //        cw.decachePreparedStatement(this.pstmt);
        //        cw.recachePreparedStatement(this.pstmt);
        //        cw.clearWarnings();
        //        cw.ping();
        //        cw.pingInternal(checkForClosedConnection, timeoutMillis);
        //        cw.createNewIO(isForReconnect);
        //        cw.changeUser(userName, newPassword);
        //        cw.checkClosed();
        //        cw.realClose(calledExplicitly, issueRollback, skipLocalTeardown, reason);
        //        cw.cleanup(whyCleanedUp);
        //        cw.abort(executor);
        //        cw.abortInternal();
    }

}
