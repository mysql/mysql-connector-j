/*
 Copyright  2002-2006 MySQL AB, 2008 Sun Microsystems

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

import testsuite.BaseTestCase;
import testsuite.simple.DataSourceTest;

import com.mysql.jdbc.ConnectionProperties;
import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.integration.jboss.MysqlValidConnectionChecker;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSourceFactory;
import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;

/**
 * Tests fixes for bugs related to datasources.
 * 
 * @author Mark Matthews
 * 
 * @version $Id: DataSourceRegressionTest.java,v 1.1.2.1 2005/05/13 18:58:38
 *          mmatthews Exp $
 */
public class DataSourceRegressionTest extends BaseTestCase {

	public final static String DS_DATABASE_PROP_NAME = "com.mysql.jdbc.test.ds.db";

	public final static String DS_HOST_PROP_NAME = "com.mysql.jdbc.test.ds.host";

	public final static String DS_PASSWORD_PROP_NAME = "com.mysql.jdbc.test.ds.password";

	public final static String DS_PORT_PROP_NAME = "com.mysql.jdbc.test.ds.port";

	public final static String DS_USER_PROP_NAME = "com.mysql.jdbc.test.ds.user";

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

		String databaseName = System.getProperty(DS_DATABASE_PROP_NAME);
		String userName = System.getProperty(DS_USER_PROP_NAME);
		String password = System.getProperty(DS_PASSWORD_PROP_NAME);
		String port = System.getProperty(DS_PORT_PROP_NAME);

		// Only run this test if at least one of the above are set
		if ((databaseName != null) || (userName != null) || (password != null)
				|| (port != null)) {
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

		String databaseName = System.getProperty(DS_DATABASE_PROP_NAME);
		String userName = System.getProperty(DS_USER_PROP_NAME);
		String password = System.getProperty(DS_PASSWORD_PROP_NAME);
		String port = System.getProperty(DS_PORT_PROP_NAME);
		String serverName = System.getProperty(DS_HOST_PROP_NAME);

		// Only run this test if at least one of the above are set
		if ((databaseName != null) || (serverName != null)
				|| (userName != null) || (password != null) || (port != null)) {
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
					dsStmt
							.executeUpdate("CREATE TABLE testBug3920 (field1 varchar(32))");

					assertTrue(
							"Connection can not be obtained from data source",
							dsCon != null);
				} finally {
					dsStmt.executeUpdate("DROP TABLE IF EXISTS testBug3920");

					dsStmt.close();
					dsCon.close();
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
	 * @throws Exception if the test fails.
	 */
	public void testBug19169() throws Exception {
		MysqlDataSource toSerialize = new MysqlDataSource();
		toSerialize.setZeroDateTimeBehavior("convertToNull");
		
		boolean testBooleanFlag = !toSerialize.getAllowLoadLocalInfile();
		toSerialize.setAllowLoadLocalInfile(testBooleanFlag);
		
		int testIntFlag = toSerialize.getBlobSendChunkSize() + 1;
		toSerialize.setBlobSendChunkSize(String.valueOf(testIntFlag));
		
		ByteArrayOutputStream bOut = new ByteArrayOutputStream();
		ObjectOutputStream objOut = new ObjectOutputStream(bOut);
		objOut.writeObject(toSerialize);
		objOut.flush();
		
		ObjectInputStream objIn = new ObjectInputStream(new ByteArrayInputStream(bOut.toByteArray()));
		
		MysqlDataSource thawedDs = (MysqlDataSource)objIn.readObject();
		
		assertEquals("convertToNull", thawedDs.getZeroDateTimeBehavior());
		assertEquals(testBooleanFlag, thawedDs.getAllowLoadLocalInfile());
		assertEquals(testIntFlag, thawedDs.getBlobSendChunkSize());
	}
	
	/**
	 * Tests fix for BUG#20242 - MysqlValidConnectionChecker for JBoss doesn't
	 * work with MySQLXADataSources.
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug20242() throws Exception {
		if (versionMeetsMinimum(5, 0)) {
			try {
				Class.forName("org.jboss.resource.adapter.jdbc.ValidConnectionChecker");
			} catch (Exception ex) {
				return; // class not available for testing
			}
			
			MysqlXADataSource xaDs = new MysqlXADataSource();
			xaDs.setUrl(dbUrl);
			
			MysqlValidConnectionChecker checker = new MysqlValidConnectionChecker();
			assertNull(checker.isValidConnection(xaDs.getXAConnection().getConnection()));
		}	
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
		Hashtable env = new Hashtable();
		env.put(Context.INITIAL_CONTEXT_FACTORY,
				"com.sun.jndi.fscontext.RefFSContextFactory");
		this.ctx = new InitialContext(env);
		assertTrue("Naming Context not created", this.ctx != null);
		ds = new MysqlConnectionPoolDataSource();
		ds.setUrl(dbUrl); // from BaseTestCase
		ds.setDatabaseName("test");
		this.ctx.bind(this.tempDir.getAbsolutePath() + "/test", ds);
	}

	private DataSource lookupDatasourceInJNDI(String jndiName) throws Exception {
		NameParser nameParser = this.ctx.getNameParser("");
		Name datasourceName = nameParser.parse(this.tempDir.getAbsolutePath()
				+ jndiName);
		Object obj = this.ctx.lookup(datasourceName);
		DataSource boundDs = null;

		if (obj instanceof DataSource) {
			boundDs = (DataSource) obj;
		} else if (obj instanceof Reference) {
			//
			// For some reason, this comes back as a Reference
			// instance under CruiseControl !?
			//
			Reference objAsRef = (Reference) obj;
			ObjectFactory factory = (ObjectFactory) Class.forName(
					objAsRef.getFactoryClassName()).newInstance();
			boundDs = (DataSource) factory.getObjectInstance(objAsRef,
					datasourceName, this.ctx, new Hashtable());
		}

		return boundDs;
	}

	public void testCSC4616() throws Exception {
		MysqlConnectionPoolDataSource ds = new MysqlConnectionPoolDataSource();
		ds.setURL(BaseTestCase.dbUrl);
		PooledConnection pooledConn = ds.getPooledConnection();
		Connection physConn = pooledConn.getConnection();
		Statement physStatement = physConn.createStatement();

		Method enableStreamingResultsMethodStmt = Class.forName(
				"com.mysql.jdbc.jdbc2.optional.StatementWrapper").getMethod(
				"enableStreamingResults", new Class[0]);
		enableStreamingResultsMethodStmt.invoke(physStatement, new Class[0]);
		this.rs = physStatement.executeQuery("SELECT 1");

		try {
			physConn.createStatement().executeQuery("SELECT 2");
			fail("Should have caught a streaming exception here");
		} catch (SQLException sqlEx) {
			assertTrue(sqlEx.getMessage() != null
					&& sqlEx.getMessage().indexOf("Streaming") != -1);
		} finally {
			if (this.rs != null) {
				this.rs.close();
				this.rs = null;
			}
		}

		PreparedStatement physPrepStmt = physConn.prepareStatement("SELECT 1");
		Method enableStreamingResultsMethodPstmt = Class.forName(
				"com.mysql.jdbc.jdbc2.optional.PreparedStatementWrapper")
				.getMethod("enableStreamingResults", new Class[0]);
		enableStreamingResultsMethodPstmt.invoke(physPrepStmt, new Class[0]);

		this.rs = physPrepStmt.executeQuery();

		try {
			physConn.createStatement().executeQuery("SELECT 2");
			fail("Should have caught a streaming exception here");
		} catch (SQLException sqlEx) {
			assertTrue(sqlEx.getMessage() != null
					&& sqlEx.getMessage().indexOf("Streaming") != -1);
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
	 * @throws Exception if the test fails
	 */
	public void testBug16791() throws Exception {
		MysqlDataSource myDs = new MysqlDataSource();
		myDs.setUrl(dbUrl);
		Reference asRef = myDs.getReference();
		System.out.println(asRef);
		
		removeFromRef(asRef, "port");
		removeFromRef(asRef, NonRegisteringDriver.USER_PROPERTY_KEY);
		removeFromRef(asRef, NonRegisteringDriver.PASSWORD_PROPERTY_KEY);
		removeFromRef(asRef, "serverName");
		removeFromRef(asRef, "databaseName");
		
		MysqlDataSource newDs = (MysqlDataSource)new MysqlDataSourceFactory().getObjectInstance(asRef, null, null, null);
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
		assertNotNull(pc.getConnection().prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT));
	}

	public void testBug35810() throws Exception {
		int defaultConnectTimeout = ((ConnectionProperties) this.conn).getConnectTimeout();
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
		int configuredConnectTimeout = ((ConnectionProperties) dsConn).getConnectTimeout();
		
		assertEquals("Connect timeout spec'd by URL didn't take", nonDefaultConnectTimeout, configuredConnectTimeout);
		assertFalse("Connect timeout spec'd by URL didn't take", defaultConnectTimeout == configuredConnectTimeout);
	}
}
