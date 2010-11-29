/*
 Copyright (c) 2002, 2010, Oracle and/or its affiliates. All rights reserved.
 

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

import java.io.File;
import java.sql.Connection;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.Name;
import javax.naming.NameParser;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import javax.sql.DataSource;
import javax.sql.PooledConnection;

import testsuite.BaseTestCase;

import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;

/**
 * 
 * @author Mark Matthews
 * @version $Id$
 */
public class DataSourceTest extends BaseTestCase {
	// ~ Instance fields
	// --------------------------------------------------------

	private Context ctx;

	private File tempDir;

	// ~ Constructors
	// -----------------------------------------------------------

	/**
	 * Creates a new DataSourceTest object.
	 * 
	 * @param name
	 *            DOCUMENT ME!
	 */
	public DataSourceTest(String name) {
		super(name);
	}

	// ~ Methods
	// ----------------------------------------------------------------

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
		registerDataSource();
	}

	/**
	 * Un-binds the DataSource, and cleans up the filesystem
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	public void tearDown() throws Exception {
		try {
			this.ctx.unbind(this.tempDir.getAbsolutePath() + "/test");
			this.ctx.close();
			this.tempDir.delete();
		} finally {
			super.tearDown();
		}
	}

	/**
	 * Tests that we can get a connection from the DataSource bound in JNDI
	 * during test setup
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	public void testDataSource() throws Exception {
		NameParser nameParser = this.ctx.getNameParser("");
		Name datasourceName = nameParser.parse("_test");
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

		assertTrue("Datasource not bound", boundDs != null);

		Connection con = boundDs.getConnection();
		con.close();
		assertTrue("Connection can not be obtained from data source",
				con != null);
	}

	/**
	 * Tests whether Connection.changeUser() (and thus pooled connections)
	 * restore character set information correctly.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testChangeUserAndCharsets() throws Exception {
		if (versionMeetsMinimum(4, 1)) {
			MysqlConnectionPoolDataSource ds = new MysqlConnectionPoolDataSource();
			ds.setURL(BaseTestCase.dbUrl);
			ds.setCharacterEncoding("utf-8");
			PooledConnection pooledConnection = ds.getPooledConnection();

			Connection connToMySQL = pooledConnection.getConnection();
			this.rs = connToMySQL.createStatement().executeQuery(
					"SELECT @@character_set_results");
			assertTrue(this.rs.next());
			
			String toCheck = null;
			
			if (versionMeetsMinimum(4, 1, 15)) {
				if (versionMeetsMinimum(5, 0)) {
					if (versionMeetsMinimum(5, 0, 13)) {
						toCheck = null;
					} else {
						toCheck = "NULL";
					}
				} else {
					toCheck = null;
				}
			} else {
				toCheck = "NULL";
			}
			
			assertEquals(toCheck, this.rs.getString(1));

			this.rs = connToMySQL.createStatement().executeQuery(
					"SHOW SESSION VARIABLES LIKE 'character_set_client'");
			assertTrue(this.rs.next());
			
			//Cause of utf8mb4
			assertEquals(0, this.rs.getString(2).indexOf("utf8"));

			connToMySQL.close();

			connToMySQL = pooledConnection.getConnection();
			this.rs = connToMySQL.createStatement().executeQuery(
					"SELECT @@character_set_results");
			assertTrue(this.rs.next());
			assertEquals(toCheck, this.rs.getString(1));

			this.rs = connToMySQL.createStatement().executeQuery(
					"SHOW SESSION VARIABLES LIKE 'character_set_client'");
			assertTrue(this.rs.next());

			//Cause of utf8mb4
			assertEquals(0, this.rs.getString(2).indexOf("utf8"));

			pooledConnection.getConnection().close();
		}
	}

	/**
	 * Tests whether XADataSources can be bound into JNDI
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testXADataSource() throws Exception {
	
		MysqlXADataSource ds = new MysqlXADataSource();
		ds.setUrl(dbUrl);
		
		String name = "XA";
		this.ctx.rebind(name, ds);

		Object result = this.ctx.lookup(name);

		assertNotNull("XADataSource not bound into JNDI", result);
	}
	
	/**
	 * This method is separated from the rest of the example since you normally
	 * would NOT register a JDBC driver in your code. It would likely be
	 * configered into your naming and directory service using some GUI.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	private void registerDataSource() throws Exception {
		this.tempDir = File.createTempFile("jnditest", null);
		this.tempDir.delete();
		this.tempDir.mkdir();
		this.tempDir.deleteOnExit();

		com.mysql.jdbc.jdbc2.optional.MysqlDataSource ds;
		Hashtable env = new Hashtable();
		env.put(Context.INITIAL_CONTEXT_FACTORY,
				"com.sun.jndi.fscontext.RefFSContextFactory");
		env.put(Context.PROVIDER_URL, this.tempDir.toURI().toString());
		this.ctx = new InitialContext(env);
		assertTrue("Naming Context not created", this.ctx != null);
		ds = new com.mysql.jdbc.jdbc2.optional.MysqlDataSource();
		ds.setUrl(dbUrl); // from BaseTestCase
		this.ctx.bind("_test", ds);
	}
}
