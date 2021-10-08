/*
 * Copyright (c) 2002, 2021, Oracle and/or its affiliates.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mysql.cj.MysqlConnection;
import com.mysql.cj.conf.AbstractRuntimeProperty;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyDefinitions.SslMode;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;
import com.mysql.cj.jdbc.MysqlDataSource;
import com.mysql.cj.jdbc.MysqlDataSourceFactory;
import com.mysql.cj.jdbc.MysqlXADataSource;
import com.mysql.cj.jdbc.MysqlXid;
import com.mysql.cj.jdbc.PreparedStatementWrapper;
import com.mysql.cj.jdbc.StatementWrapper;

import testsuite.BaseTestCase;
import testsuite.MockJndiContextFactory;

/**
 * Tests fixes for bugs related to datasources.
 */
public class DataSourceRegressionTest extends BaseTestCase {
    private Context ctx;

    /**
     * Sets up this test, calling registerDataSource() to bind a DataSource into JNDI, using the FSContext JNDI provider from Sun
     * 
     * @throws Exception
     */
    @BeforeEach
    public void setUp() throws Exception {
        /*
         * This code is separated from the rest of the test since you normally would NOT register a JDBC driver in your code. It would likely be configured into
         * your naming and directory service using some GUI.
         */
        MysqlConnectionPoolDataSource ds;
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, MockJndiContextFactory.class.getName());
        this.ctx = new InitialContext(env);
        assertNotNull(this.ctx, "Naming Context not created");
        ds = new MysqlConnectionPoolDataSource();
        ds.setUrl(dbUrl); // from BaseTestCase
        ds.setDatabaseName(this.dbName);
        this.ctx.bind("/test", ds);
    }

    /**
     * Un-binds the DataSource and closes the context
     * 
     * @throws Exception
     */
    @AfterEach
    public void tearDown() throws Exception {
        this.ctx.unbind("/test");
        this.ctx.close();
    }

    /**
     * Tests fix for BUG#4808- Calling .close() twice on a PooledConnection causes NPE.
     * 
     * @throws Exception
     */
    @Test
    public void testBug4808() throws Exception {
        MysqlConnectionPoolDataSource ds = new MysqlConnectionPoolDataSource();
        ds.setURL(BaseTestCase.dbUrl);
        ds.<SslMode>getEnumProperty(PropertyKey.sslMode).setValue(SslMode.DISABLED);
        ds.getBooleanProperty(PropertyKey.allowPublicKeyRetrieval).setValue(true);
        PooledConnection closeMeTwice = ds.getPooledConnection();
        closeMeTwice.close();
        closeMeTwice.close();
    }

    /**
     * Tests fix for Bug#3848, port # alone parsed incorrectly
     * 
     * @throws Exception
     */
    @Test
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

                assertNotNull(boundDs, "Datasource not bound");

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
     * Tests that we can get a connection from the DataSource bound in JNDI during test setup
     * 
     * @throws Exception
     */
    @Test
    public void testBug3920() throws Exception {
        String jndiName = "/testBug3920";

        String databaseName = System.getProperty(PropertyDefinitions.SYSP_testsuite_ds_db);
        String userName = System.getProperty(PropertyDefinitions.SYSP_testsuite_ds_user);
        String password = System.getProperty(PropertyDefinitions.SYSP_testsuite_ds_password);
        String port = System.getProperty(PropertyDefinitions.SYSP_testsuite_ds_port);
        String serverName = System.getProperty(PropertyDefinitions.SYSP_testsuite_ds_host);

        assumeTrue((databaseName != null) || (serverName != null) || (userName != null) || (password != null) || (port != null),
                "This test requires that at least one of the following properties is set:\n" + PropertyDefinitions.SYSP_testsuite_ds_db + ",\n"
                        + PropertyDefinitions.SYSP_testsuite_ds_user + ",\n" + PropertyDefinitions.SYSP_testsuite_ds_password + ",\n"
                        + PropertyDefinitions.SYSP_testsuite_ds_port + ",\n" + PropertyDefinitions.SYSP_testsuite_ds_host);

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

            assertNotNull(boundDs, "Datasource not bound");

            Connection dsCon = null;
            Statement dsStmt = null;

            try {
                dsCon = boundDs.getPooledConnection().getConnection();
                dsStmt = dsCon.createStatement();
                dsStmt.executeUpdate("DROP TABLE IF EXISTS testBug3920");
                dsStmt.executeUpdate("CREATE TABLE testBug3920 (field1 varchar(32))");

                assertNotNull(dsCon, "Connection can not be obtained from data source");
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

    /**
     * Tests fix for BUG#19169 - ConnectionProperties (and thus some subclasses) are not serializable, even though some J2EE containers expect them to be.
     * 
     * @throws Exception
     */
    @Test
    public void testBug19169() throws Exception {
        MysqlDataSource toSerialize = new MysqlDataSource();

        toSerialize.<PropertyDefinitions.ZeroDatetimeBehavior>getProperty(PropertyKey.zeroDateTimeBehavior)
                .setValue(PropertyDefinitions.ZeroDatetimeBehavior.CONVERT_TO_NULL);

        toSerialize.<String>getProperty(PropertyKey.ha_loadBalanceStrategy).setValue("test_lb_strategy");

        boolean testBooleanFlag = !toSerialize.getBooleanProperty(PropertyKey.allowLoadLocalInfile).getValue();
        toSerialize.<Boolean>getProperty(PropertyKey.allowLoadLocalInfile).setValue(testBooleanFlag);

        RuntimeProperty<Integer> bscs = toSerialize.<Integer>getProperty(PropertyKey.blobSendChunkSize);
        int testIntFlag = bscs.getValue() + 1;
        ((AbstractRuntimeProperty<?>) bscs).setValueInternal(String.valueOf(testIntFlag), null);

        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        ObjectOutputStream objOut = new ObjectOutputStream(bOut);
        objOut.writeObject(toSerialize);
        objOut.flush();

        ObjectInputStream objIn = new ObjectInputStream(new ByteArrayInputStream(bOut.toByteArray()));

        MysqlDataSource thawedDs = (MysqlDataSource) objIn.readObject();

        assertEquals(PropertyDefinitions.ZeroDatetimeBehavior.CONVERT_TO_NULL, thawedDs.getEnumProperty(PropertyKey.zeroDateTimeBehavior).getValue());
        assertEquals("test_lb_strategy", thawedDs.getStringProperty(PropertyKey.ha_loadBalanceStrategy).getValue());
        assertEquals(testBooleanFlag, thawedDs.getBooleanProperty(PropertyKey.allowLoadLocalInfile).getValue().booleanValue());
        assertEquals(testIntFlag, thawedDs.getMemorySizeProperty(PropertyKey.blobSendChunkSize).getValue().intValue());
    }

    private void bindDataSource(String name, DataSource ds) throws Exception {
        this.ctx.bind(name, ds);
    }

    private DataSource lookupDatasourceInJNDI(String jndiName) throws Exception {
        NameParser nameParser = this.ctx.getNameParser("");
        Name datasourceName = nameParser.parse(jndiName);
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

    @Test
    public void testCSC4616() throws Exception {
        MysqlConnectionPoolDataSource ds = new MysqlConnectionPoolDataSource();
        ds.setURL(BaseTestCase.dbUrl);
        ds.<SslMode>getEnumProperty(PropertyKey.sslMode).setValue(SslMode.DISABLED);
        ds.getBooleanProperty(PropertyKey.allowPublicKeyRetrieval).setValue(true);
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
     * Tests fix for BUG#16791 - NullPointerException in MysqlDataSourceFactory due to Reference containing RefAddrs with null content.
     * 
     * @throws Exception
     */
    @Test
    public void testBug16791() throws Exception {
        MysqlDataSource myDs = new MysqlDataSource();
        myDs.setUrl(dbUrl);
        Reference asRef = myDs.getReference();
        System.out.println(asRef);

        removeFromRef(asRef, "port");
        removeFromRef(asRef, PropertyKey.USER.getKeyName());
        removeFromRef(asRef, PropertyKey.PASSWORD.getKeyName());
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
     * Tests fix for BUG#32101 - When using a connection from our ConnectionPoolDataSource, some Connection.prepareStatement() methods would return null instead
     * of a prepared statement.
     * 
     * @throws Exception
     */
    @Test
    public void testBug32101() throws Exception {
        MysqlConnectionPoolDataSource ds = new MysqlConnectionPoolDataSource();
        ds.setURL(BaseTestCase.dbUrl);
        ds.<SslMode>getEnumProperty(PropertyKey.sslMode).setValue(SslMode.DISABLED);
        ds.getBooleanProperty(PropertyKey.allowPublicKeyRetrieval).setValue(true);
        PooledConnection pc = ds.getPooledConnection();
        assertNotNull(pc.getConnection().prepareStatement("SELECT 1"));
        assertNotNull(pc.getConnection().prepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS));
        assertNotNull(pc.getConnection().prepareStatement("SELECT 1", new int[0]));
        assertNotNull(pc.getConnection().prepareStatement("SELECT 1", new String[0]));
        assertNotNull(pc.getConnection().prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        assertNotNull(
                pc.getConnection().prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT));
    }

    @Test
    public void testBug35810() throws Exception {
        int defaultConnectTimeout = ((JdbcConnection) this.conn).getPropertySet().getIntegerProperty(PropertyKey.connectTimeout).getValue();
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
        cpds.<SslMode>getEnumProperty(PropertyKey.sslMode).setValue(SslMode.DISABLED);
        cpds.getBooleanProperty(PropertyKey.allowPublicKeyRetrieval).setValue(true);

        Connection dsConn = cpds.getPooledConnection().getConnection();
        int configuredConnectTimeout = ((JdbcConnection) dsConn).getPropertySet().getIntegerProperty(PropertyKey.connectTimeout).getValue();

        assertEquals(nonDefaultConnectTimeout, configuredConnectTimeout, "Connect timeout spec'd by URL didn't take");
        assertFalse(defaultConnectTimeout == configuredConnectTimeout, "Connect timeout spec'd by URL didn't take");
    }

    @Test
    public void testBug42267() throws Exception {
        MysqlDataSource ds = new MysqlDataSource();
        ds.setUrl(dbUrl);
        ds.<SslMode>getEnumProperty(PropertyKey.sslMode).setValue(SslMode.DISABLED);
        ds.getBooleanProperty(PropertyKey.allowPublicKeyRetrieval).setValue(true);
        Connection c = ds.getConnection();
        String query = "select 1,2,345";
        PreparedStatement ps = c.prepareStatement(query);
        String psString = ps.toString();
        assertTrue(psString.endsWith(": " + query), "String representation of wrapped ps should contain query string");
        ps.close();
        ps.toString();
        c.close();
    }

    /**
     * Tests fix for BUG#72890 - Java jdbc driver returns incorrect return code when it's part of XA transaction
     * 
     * @throws Exception
     */
    @Test
    public void testBug72890() throws Exception {
        MysqlXADataSource myDs = new MysqlXADataSource();
        myDs.setUrl(BaseTestCase.dbUrl);
        myDs.<SslMode>getEnumProperty(PropertyKey.sslMode).setValue(SslMode.DISABLED);
        myDs.getBooleanProperty(PropertyKey.allowPublicKeyRetrieval).setValue(true);

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
            assertFalse(connAliveChecks == 0, "Failed to kill the Connection id " + connId + " in a timely manner.");

            XAException xaEx = assertThrows(XAException.class, "Undetermined error occurred in the underlying Connection - check your data for consistency",
                    new Callable<Void>() {
                        public Void call() throws Exception {
                            xaRes.commit(xid, false);
                            return null;
                        }
                    });
            assertEquals(XAException.XAER_RMFAIL, xaEx.errorCode, "XAException error code");

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
     * 
     * @throws Exception
     */
    @Test
    public void testBug72632() throws Exception {
        final MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUrl("jdbc:mysql:nonsupported:");
        assertThrows(SQLException.class, "Connector/J cannot handle a connection string 'jdbc:mysql:nonsupported:'.", new Callable<Void>() {
            public Void call() throws Exception {
                dataSource.getConnection();
                return null;
            }
        });
    }
}
