/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package testsuite.regression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.concurrent.Callable;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.Name;
import javax.naming.NameParser;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.mysql.cj.MysqlConnection;
import com.mysql.cj.conf.ModifiableProperty;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;
import com.mysql.cj.jdbc.MysqlDataSource;
import com.mysql.cj.jdbc.MysqlDataSourceFactory;
import com.mysql.cj.jdbc.MysqlXADataSource;
import com.mysql.cj.jdbc.MysqlXid;
import com.mysql.cj.jdbc.PreparedStatementWrapper;
import com.mysql.cj.jdbc.StatementWrapper;
import com.mysql.cj.jdbc.integration.jboss.MysqlValidConnectionChecker;

import testsuite.BaseTestCase;
import testsuite.simple.DataSourceTest;

/**
 * Tests fixes for bugs related to datasources.
 */
public class DataSourceRegressionTest extends BaseTestCase {

    private Context ctx;

    private File tempDir;

    /**
     * Creates a new DataSourceRegressionTest suite for the given test name
     * 
     * @param name
     *            the name of the testcase to run.
     */
    public DataSourceRegressionTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(DataSourceTest.class);
    }

    /**
     * Sets up this test, calling registerDataSource() to bind a DataSource into
     * JNDI, using the FSContext JNDI provider from Sun
     * 
     * @throws Exception
     *             if an error occurs.
     */
    @Override
    public void setUp() throws Exception {
        super.setUp();
        createJNDIContext();
    }

    /**
     * Un-binds the DataSource, and cleans up the filesystem
     * 
     * @throws Exception
     *             if an error occurs
     */
    @Override
    public void tearDown() throws Exception {
        this.ctx.unbind(this.tempDir.getAbsolutePath() + "/test");
        this.ctx.unbind(this.tempDir.getAbsolutePath() + "/testNoUrl");
        this.ctx.close();
        this.tempDir.delete();

        super.tearDown();
    }

    /**
     * Tests fix for BUG#4808- Calling .close() twice on a PooledConnection
     * causes NPE.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testBug4808() throws Exception {
        MysqlConnectionPoolDataSource ds = new MysqlConnectionPoolDataSource();
        ds.setURL(BaseTestCase.dbUrl);
        PooledConnection closeMeTwice = ds.getPooledConnection();
        closeMeTwice.close();
        closeMeTwice.close();

    }

    /**
     * Tests fix for Bug#3848, port # alone parsed incorrectly
     * 
     * @throws Exception
     *             ...
     */
    public void testBug3848() throws Exception {
        String jndiName = "/testBug3848";

        String databaseName = System.getProperty(PropertyDefinitions.SYSP_testsuite_ds_db);
        String userName = System.getProperty(PropertyDefinitions.SYSP_testsuite_ds_user);
        String password = System.getProperty(PropertyDefinitions.SYSP_testsuite_ds_password);
        String port = System.getProperty(PropertyDefinitions.SYSP_testsuite_ds_port);

        // Only run this test if at least one of the above are set
        if ((databaseName != null) || (userName != null) || (password != null) || (port != null)) {
            MysqlConnectionPoolDataSource ds = new MysqlConnectionPoolDataSource();

            if (databaseName != null) {
                ds.setDatabaseName(databaseName);
            }

            if (userName != null) {
                ds.setUser(userName);
            }

            if (password != null) {
                ds.setPassword(password);
            }

            if (port != null) {
                ds.setPortNumber(Integer.parseInt(port));
            }

            bindDataSource(jndiName, ds);

            ConnectionPoolDataSource boundDs = null;

            try {
                boundDs = (ConnectionPoolDataSource) lookupDatasourceInJNDI(jndiName);

                assertTrue("Datasource not bound", boundDs != null);

                Connection dsConn = null;

                try {
                    dsConn = boundDs.getPooledConnection().getConnection();
                } finally {
                    if (dsConn != null) {
                        dsConn.close();
                    }
                }
            } finally {
                if (boundDs != null) {
                    this.ctx.unbind(jndiName);
                }
            }
        }
    }

    /**
     * Tests that we can get a connection from the DataSource bound in JNDI
     * during test setup
     * 
     * @throws Exception
     *             if an error occurs
     */
    public void testBug3920() throws Exception {
        String jndiName = "/testBug3920";

        String databaseName = System.getProperty(PropertyDefinitions.SYSP_testsuite_ds_db);
        String userName = System.getProperty(PropertyDefinitions.SYSP_testsuite_ds_user);
        String password = System.getProperty(PropertyDefinitions.SYSP_testsuite_ds_password);
        String port = System.getProperty(PropertyDefinitions.SYSP_testsuite_ds_port);
        String serverName = System.getProperty(PropertyDefinitions.SYSP_testsuite_ds_host);

        // Only run this test if at least one of the above are set
        if ((databaseName != null) || (serverName != null) || (userName != null) || (password != null) || (port != null)) {
            MysqlConnectionPoolDataSource ds = new MysqlConnectionPoolDataSource();

            if (databaseName != null) {
                ds.setDatabaseName(databaseName);
            }

            if (userName != null) {
                ds.setUser(userName);
            }

            if (password != null) {
                ds.setPassword(password);
            }

            if (port != null) {
                ds.setPortNumber(Integer.parseInt(port));
            }

            if (serverName != null) {
                ds.setServerName(serverName);
            }

            bindDataSource(jndiName, ds);

            ConnectionPoolDataSource boundDs = null;

            try {
                boundDs = (ConnectionPoolDataSource) lookupDatasourceInJNDI(jndiName);

                assertTrue("Datasource not bound", boundDs != null);

                Connection dsCon = null;
                Statement dsStmt = null;

                try {
                    dsCon = boundDs.getPooledConnection().getConnection();
                    dsStmt = dsCon.createStatement();
                    dsStmt.executeUpdate("DROP TABLE IF EXISTS testBug3920");
                    dsStmt.executeUpdate("CREATE TABLE testBug3920 (field1 varchar(32))");

                    assertTrue("Connection can not be obtained from data source", dsCon != null);
                } finally {
                    if (dsStmt != null) {
                        dsStmt.executeUpdate("DROP TABLE IF EXISTS testBug3920");
                        dsStmt.close();
                    }
                    if (dsCon != null) {
                        dsCon.close();
                    }
                }
            } finally {
                if (boundDs != null) {
                    this.ctx.unbind(jndiName);
                }
            }
        }
    }

    /**
     * Tests fix for BUG#19169 - ConnectionProperties (and thus some
     * subclasses) are not serializable, even though some J2EE containers
     * expect them to be.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug19169() throws Exception {
        MysqlDataSource toSerialize = new MysqlDataSource();

        toSerialize.<PropertyDefinitions.ZeroDatetimeBehavior> getModifiableProperty(PropertyDefinitions.PNAME_zeroDateTimeBehavior)
                .setValue(PropertyDefinitions.ZeroDatetimeBehavior.CONVERT_TO_NULL);

        toSerialize.<String> getModifiableProperty(PropertyDefinitions.PNAME_loadBalanceStrategy).setValue("test_lb_strategy");

        boolean testBooleanFlag = !toSerialize.getBooleanReadableProperty(PropertyDefinitions.PNAME_allowLoadLocalInfile).getValue();
        toSerialize.<Boolean> getJdbcModifiableProperty(PropertyDefinitions.PNAME_allowLoadLocalInfile).setValue(testBooleanFlag);

        ModifiableProperty<Integer> bscs = toSerialize.<Integer> getModifiableProperty(PropertyDefinitions.PNAME_blobSendChunkSize);
        int testIntFlag = bscs.getValue() + 1;
        bscs.setFromString(String.valueOf(testIntFlag), null);

        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        ObjectOutputStream objOut = new ObjectOutputStream(bOut);
        objOut.writeObject(toSerialize);
        objOut.flush();

        ObjectInputStream objIn = new ObjectInputStream(new ByteArrayInputStream(bOut.toByteArray()));

        MysqlDataSource thawedDs = (MysqlDataSource) objIn.readObject();

        assertEquals(PropertyDefinitions.ZeroDatetimeBehavior.CONVERT_TO_NULL,
                thawedDs.getEnumReadableProperty(PropertyDefinitions.PNAME_zeroDateTimeBehavior).getValue());
        assertEquals("test_lb_strategy", thawedDs.getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceStrategy).getValue());
        assertEquals(testBooleanFlag, thawedDs.getBooleanReadableProperty(PropertyDefinitions.PNAME_allowLoadLocalInfile).getValue().booleanValue());
        assertEquals(testIntFlag, thawedDs.getMemorySizeReadableProperty(PropertyDefinitions.PNAME_blobSendChunkSize).getValue().intValue());
    }

    /**
     * Tests fix for BUG#20242 - MysqlValidConnectionChecker for JBoss doesn't
     * work with MySQLXADataSources.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug20242() throws Exception {
        try {
            Class.forName("org.jboss.resource.adapter.jdbc.ValidConnectionChecker");
        } catch (Exception ex) {
            System.out.println("The testBug20242() is ignored because required class isn't available:");
            ex.printStackTrace();
            return; // class not available for testing
        }

        MysqlXADataSource xaDs = new MysqlXADataSource();
        xaDs.setUrl(dbUrl);

        MysqlValidConnectionChecker checker = new MysqlValidConnectionChecker();
        assertNull(checker.isValidConnection(xaDs.getXAConnection().getConnection()));
    }

    private void bindDataSource(String name, DataSource ds) throws Exception {
        this.ctx.bind(this.tempDir.getAbsolutePath() + name, ds);
    }

    /**
     * This method is separated from the rest of the example since you normally
     * would NOT register a JDBC driver in your code. It would likely be
     * configered into your naming and directory service using some GUI.
     * 
     * @throws Exception
     *             if an error occurs
     */
    private void createJNDIContext() throws Exception {
        this.tempDir = File.createTempFile("jnditest", null);
        this.tempDir.delete();
        this.tempDir.mkdir();
        this.tempDir.deleteOnExit();

        MysqlConnectionPoolDataSource ds;
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.fscontext.RefFSContextFactory");
        this.ctx = new InitialContext(env);
        assertTrue("Naming Context not created", this.ctx != null);
        ds = new MysqlConnectionPoolDataSource();
        ds.setUrl(dbUrl); // from BaseTestCase
        ds.setDatabaseName("test");
        this.ctx.bind(this.tempDir.getAbsolutePath() + "/test", ds);
    }

    private DataSource lookupDatasourceInJNDI(String jndiName) throws Exception {
        NameParser nameParser = this.ctx.getNameParser("");
        Name datasourceName = nameParser.parse(this.tempDir.getAbsolutePath() + jndiName);
        Object obj = this.ctx.lookup(datasourceName);
        DataSource boundDs = null;

        if (obj instanceof DataSource) {
            boundDs = (DataSource) obj;
        } else if (obj instanceof Reference) {
            //
            // For some reason, this comes back as a Reference instance under CruiseControl !?
            //
            Reference objAsRef = (Reference) obj;
            ObjectFactory factory = (ObjectFactory) Class.forName(objAsRef.getFactoryClassName()).newInstance();
            boundDs = (DataSource) factory.getObjectInstance(objAsRef, datasourceName, this.ctx, new Hashtable<>());
        }

        return boundDs;
    }

    public void testCSC4616() throws Exception {
        MysqlConnectionPoolDataSource ds = new MysqlConnectionPoolDataSource();
        ds.setURL(BaseTestCase.dbUrl);
        PooledConnection pooledConn = ds.getPooledConnection();
        Connection physConn = pooledConn.getConnection();
        Statement physStatement = physConn.createStatement();

        Method enableStreamingResultsMethodStmt = Class.forName(StatementWrapper.class.getName()).getMethod("enableStreamingResults", new Class<?>[0]);
        enableStreamingResultsMethodStmt.invoke(physStatement, (Object[]) null);
        this.rs = physStatement.executeQuery("SELECT 1");

        try {
            this.rs = physConn.createStatement().executeQuery("SELECT 2");
            fail("Should have caught a streaming exception here");
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getMessage() != null && sqlEx.getMessage().indexOf("Streaming") != -1);
        } finally {
            if (this.rs != null) {
                this.rs.close();
                this.rs = null;
            }
        }

        PreparedStatement physPrepStmt = physConn.prepareStatement("SELECT 1");
        Method enableStreamingResultsMethodPstmt = Class.forName(PreparedStatementWrapper.class.getName()).getMethod("enableStreamingResults", (Class[]) null);
        enableStreamingResultsMethodPstmt.invoke(physPrepStmt, (Object[]) null);

        this.rs = physPrepStmt.executeQuery();

        try {
            this.rs = physConn.createStatement().executeQuery("SELECT 2");
            fail("Should have caught a streaming exception here");
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getMessage() != null && sqlEx.getMessage().indexOf("Streaming") != -1);
        } finally {
            if (this.rs != null) {
                this.rs.close();
                this.rs = null;
            }
        }
    }

    /**
     * Tests fix for BUG#16791 - NullPointerException in MysqlDataSourceFactory
     * due to Reference containing RefAddrs with null content.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug16791() throws Exception {
        MysqlDataSource myDs = new MysqlDataSource();
        myDs.setUrl(dbUrl);
        Reference asRef = myDs.getReference();
        System.out.println(asRef);

        removeFromRef(asRef, "port");
        removeFromRef(asRef, PropertyDefinitions.PNAME_user);
        removeFromRef(asRef, PropertyDefinitions.PNAME_password);
        removeFromRef(asRef, "serverName");
        removeFromRef(asRef, "databaseName");

        //MysqlDataSource newDs = (MysqlDataSource)
        new MysqlDataSourceFactory().getObjectInstance(asRef, null, null, null);
    }

    private void removeFromRef(Reference ref, String key) {
        int size = ref.size();

        for (int i = 0; i < size; i++) {
            RefAddr refAddr = ref.get(i);
            if (refAddr.getType().equals(key)) {
                ref.remove(i);
                break;
            }
        }
    }

    /**
     * Tests fix for BUG#32101 - When using a connection from our ConnectionPoolDataSource,
     * some Connection.prepareStatement() methods would return null instead of
     * a prepared statement.
     * 
     * @throws Exception
     */
    public void testBug32101() throws Exception {
        MysqlConnectionPoolDataSource ds = new MysqlConnectionPoolDataSource();
        ds.setURL(BaseTestCase.dbUrl);
        PooledConnection pc = ds.getPooledConnection();
        assertNotNull(pc.getConnection().prepareStatement("SELECT 1"));
        assertNotNull(pc.getConnection().prepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS));
        assertNotNull(pc.getConnection().prepareStatement("SELECT 1", new int[0]));
        assertNotNull(pc.getConnection().prepareStatement("SELECT 1", new String[0]));
        assertNotNull(pc.getConnection().prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        assertNotNull(
                pc.getConnection().prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT));
    }

    public void testBug35810() throws Exception {
        int defaultConnectTimeout = ((JdbcConnection) this.conn).getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_connectTimeout)
                .getValue();
        int nonDefaultConnectTimeout = defaultConnectTimeout + 1000 * 2;
        MysqlConnectionPoolDataSource cpds = new MysqlConnectionPoolDataSource();
        String dsUrl = BaseTestCase.dbUrl;
        if (dsUrl.indexOf("?") == -1) {
            dsUrl += "?";
        } else {
            dsUrl += "&";
        }

        dsUrl += "connectTimeout=" + nonDefaultConnectTimeout;
        cpds.setUrl(dsUrl);

        Connection dsConn = cpds.getPooledConnection().getConnection();
        int configuredConnectTimeout = ((JdbcConnection) dsConn).getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_connectTimeout)
                .getValue();

        assertEquals("Connect timeout spec'd by URL didn't take", nonDefaultConnectTimeout, configuredConnectTimeout);
        assertFalse("Connect timeout spec'd by URL didn't take", defaultConnectTimeout == configuredConnectTimeout);
    }

    public void testBug42267() throws Exception {
        MysqlDataSource ds = new MysqlDataSource();
        ds.setUrl(dbUrl);
        Connection c = ds.getConnection();
        String query = "select 1,2,345";
        PreparedStatement ps = c.prepareStatement(query);
        String psString = ps.toString();
        assertTrue("String representation of wrapped ps should contain query string", psString.endsWith(": " + query));
        ps.close();
        ps.toString();
        c.close();
    }

    /**
     * Tests fix for BUG#72890 - Java jdbc driver returns incorrect return code when it's part of XA transaction
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug72890() throws Exception {
        MysqlXADataSource myDs = new MysqlXADataSource();
        myDs.setUrl(BaseTestCase.dbUrl);

        try {
            final Xid xid = new MysqlXid("72890".getBytes(), "72890".getBytes(), 1);

            final XAConnection xaConn = myDs.getXAConnection();
            final XAResource xaRes = xaConn.getXAResource();
            final Connection dbConn = xaConn.getConnection();
            final long connId = ((MysqlConnection) ((MysqlConnection) dbConn).getConnectionMutex()).getSession().getThreadId();

            xaRes.start(xid, XAResource.TMNOFLAGS);
            xaRes.end(xid, XAResource.TMSUCCESS);
            assertEquals(XAResource.XA_OK, xaRes.prepare(xid));

            // Simulate a connection hang and make sure the connection really dies.
            this.stmt.execute("KILL CONNECTION " + connId);
            int connAliveChecks = 4;
            while (connAliveChecks > 0) {
                this.rs = this.stmt.executeQuery("SHOW PROCESSLIST");
                boolean connIsAlive = false;
                while (!connIsAlive && this.rs.next()) {
                    connIsAlive = this.rs.getInt(1) == connId;
                }
                this.rs.close();
                if (connIsAlive) {
                    connAliveChecks--;
                    System.out.println("Connection id " + connId + " is still alive. Checking " + connAliveChecks + " more times.");
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                    }
                } else {
                    connAliveChecks = -1;
                }
            }
            if (connAliveChecks == 0) {
                fail("Failed to kill the Connection id " + connId + " in a timely manner.");
            }

            XAException xaEx = assertThrows(XAException.class, "Undetermined error occurred in the underlying Connection - check your data for consistency",
                    new Callable<Void>() {
                        public Void call() throws Exception {
                            xaRes.commit(xid, false);
                            return null;
                        }
                    });
            assertEquals("XAException error code", XAException.XAER_RMFAIL, xaEx.errorCode);

            dbConn.close();
            xaConn.close();

        } finally {
            /*
             * After MySQL 5.7.7 a prepared XA transaction is no longer rolled back at disconnect. It needs to be rolled back manually to prevent test failures
             * in subsequent runs.
             * Other MySQL versions won't have any transactions to recover.
             */
            final XAConnection xaConnRecovery = myDs.getXAConnection();
            final XAResource xaResRecovery = xaConnRecovery.getXAResource();

            final Xid[] xidsToRecover = xaResRecovery.recover(XAResource.TMSTARTRSCAN);
            for (Xid xidToRecover : xidsToRecover) {
                xaResRecovery.rollback(xidToRecover);
            }

            xaConnRecovery.close();
        }
    }

    /**
     * Tests fix for Bug#72632 - NullPointerException for invalid JDBC URL.
     */
    public void testBug72632() throws Exception {
        final MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUrl("jdbc:mysql:nonsupported:");
        assertThrows(SQLException.class, "Failed to get a connection using the URL 'jdbc:mysql:nonsupported:'.", new Callable<Void>() {
            public Void call() throws Exception {
                dataSource.getConnection();
                return null;
            }
        });
    }
}
