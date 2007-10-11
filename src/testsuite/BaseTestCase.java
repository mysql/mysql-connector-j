/*
 Copyright (C) 2002-2007 MySQL AB

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
package testsuite;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.StringUtils;

import junit.framework.TestCase;

/**
 * Base class for all test cases. Creates connections, statements, etc. and
 * closes them.
 * 
 * @author Mark Matthews
 * @version $Id: BaseTestCase.java 5440 2006-06-27 17:00:53 +0000 (Tue, 27 Jun
 *          2006) mmatthews $
 */
public abstract class BaseTestCase extends TestCase {
	private final static String ADMIN_CONNECTION_PROPERTY_NAME = "com.mysql.jdbc.testsuite.admin-url";

	private final static String NO_MULTI_HOST_PROPERTY_NAME = "com.mysql.jdbc.testsuite.no-multi-hosts-tests";

	/**
	 * JDBC URL, initialized from com.mysql.jdbc.testsuite.url system property,
	 * or defaults to jdbc:mysql:///test
	 */
	protected static String dbUrl = "jdbc:mysql:///test";

	/** Instance counter */
	private static int instanceCount = 1;

	/** Connection to server, initialized in setUp() Cleaned up in tearDown(). */
	protected Connection conn = null;

	/** list of schema objects to be dropped in tearDown */
	private List createdObjects;

	/** The driver to use */
	protected String dbClass = "com.mysql.jdbc.Driver";

	/** My instance number */
	private int myInstanceNumber = 0;

	/**
	 * PreparedStatement to be used in tests, not initialized. Cleaned up in
	 * tearDown().
	 */
	protected PreparedStatement pstmt = null;

	/**
	 * ResultSet to be used in tests, not initialized. Cleaned up in tearDown().
	 */
	protected ResultSet rs = null;

	/**
	 * Statement to be used in tests, initialized in setUp(). Cleaned up in
	 * tearDown().
	 */
	protected Statement stmt = null;

	private boolean runningOnJdk131 = false;

	/**
	 * Creates a new BaseTestCase object.
	 * 
	 * @param name
	 *            The name of the JUnit test case
	 */
	public BaseTestCase(String name) {
		super(name);
		this.myInstanceNumber = instanceCount++;

		String newDbUrl = System.getProperty("com.mysql.jdbc.testsuite.url");

		if ((newDbUrl != null) && (newDbUrl.trim().length() != 0)) {
			dbUrl = newDbUrl;
		} else {
			String defaultDbUrl = System
					.getProperty("com.mysql.jdbc.testsuite.url.default");

			if ((defaultDbUrl != null) && (defaultDbUrl.trim().length() != 0)) {
				dbUrl = defaultDbUrl;
			}
		}

		String newDriver = System
				.getProperty("com.mysql.jdbc.testsuite.driver");

		if ((newDriver != null) && (newDriver.trim().length() != 0)) {
			this.dbClass = newDriver;
		}

		try {
			Blob.class.getMethod("truncate", new Class[] { Long.TYPE });
			this.runningOnJdk131 = false;
		} catch (NoSuchMethodException nsme) {
			this.runningOnJdk131 = true;
		}
	}

	protected void createSchemaObject(String objectType, String objectName,
			String columnsAndOtherStuff) throws SQLException {
		this.createdObjects.add(new String[] {objectType, objectName});
		dropSchemaObject(objectType, objectName);

		StringBuffer createSql = new StringBuffer(objectName.length()
				+ objectType.length() + columnsAndOtherStuff.length() + 10);
		createSql.append("CREATE  ");
		createSql.append(objectType);
		createSql.append(" ");
		createSql.append(objectName);
		createSql.append(" ");
		createSql.append(columnsAndOtherStuff);
		this.stmt.executeUpdate(createSql.toString());
	}

	protected void createFunction(String functionName, String functionDefn)
			throws SQLException {
		createSchemaObject("FUNCTION", functionName, functionDefn);
	}
	
	protected void dropFunction(String functionName) throws SQLException {
		dropSchemaObject("FUNCTION", functionName);
	}
	
	protected void createProcedure(String procedureName, String procedureDefn)
			throws SQLException {
		createSchemaObject("PROCEDURE", procedureName, procedureDefn);
	}

	protected void dropProcedure(String procedureName) throws SQLException {
		dropSchemaObject("PROCEDURE", procedureName);
	}

	protected void createTable(String tableName, String columnsAndOtherStuff)
			throws SQLException {
		createSchemaObject("TABLE", tableName, columnsAndOtherStuff);
	}

	protected void dropTable(String tableName) throws SQLException {
		dropSchemaObject("TABLE", tableName);
	}

	protected void dropSchemaObject(String objectType, String objectName)
			throws SQLException {
		this.stmt.executeUpdate("DROP " + objectType + " IF EXISTS "
				+ objectName);
	}

	protected Connection getAdminConnection() throws SQLException {
		return getAdminConnectionWithProps(new Properties());
	}

	protected Connection getAdminConnectionWithProps(Properties props)
			throws SQLException {
		String adminUrl = System.getProperty(ADMIN_CONNECTION_PROPERTY_NAME);

		if (adminUrl != null) {
			return DriverManager.getConnection(adminUrl, props);
		} else {
			return null;
		}
	}

	protected Connection getConnectionWithProps(String propsList) throws SQLException {
		return getConnectionWithProps(dbUrl, propsList);
	}

	protected Connection getConnectionWithProps(String url, String propsList) throws SQLException {
		Properties props = new Properties();
		
		if (propsList != null) {
			List keyValuePairs = StringUtils.split(propsList, ",", false);
			
			Iterator iter = keyValuePairs.iterator();
			
			while (iter.hasNext()) {
				String kvp = (String)iter.next();
				List splitUp = StringUtils.split(kvp, "=", false);
				props.setProperty(splitUp.get(0).toString().trim(), splitUp.get(1).toString());
			}
		}
		
		return getConnectionWithProps(url, props);
	}
	
	/**
	 * Returns a new connection with the given properties
	 * 
	 * @param props
	 *            the properties to use (the URL will come from the standard for
	 *            this testcase).
	 * 
	 * @return a new connection using the given properties.
	 * 
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	protected Connection getConnectionWithProps(Properties props)
			throws SQLException {
		return DriverManager.getConnection(dbUrl, props);
	}

	protected Connection getConnectionWithProps(String url, Properties props)
			throws SQLException {
		return DriverManager.getConnection(url, props);
	}

	/**
	 * Returns the per-instance counter (for messages when multi-threading
	 * stress tests)
	 * 
	 * @return int the instance number
	 */
	protected int getInstanceNumber() {
		return this.myInstanceNumber;
	}

	protected String getMysqlVariable(Connection c, String variableName)
			throws SQLException {
		Object value = getSingleIndexedValueWithQuery(c, 2,
				"SHOW VARIABLES LIKE '" + variableName + "'");

		if (value != null) {
			if (value instanceof byte[]) {
				// workaround for bad 4.1.x bugfix
				return new String((byte[]) value);
			}

			return value.toString();
		}

		return null;

	}

	/**
	 * Returns the named MySQL variable from the currently connected server.
	 * 
	 * @param variableName
	 *            the name of the variable to return
	 * 
	 * @return the value of the given variable, or NULL if it doesn't exist
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	protected String getMysqlVariable(String variableName) throws SQLException {
		return getMysqlVariable(this.conn, variableName);
	}

	/**
	 * Returns the properties that represent the default URL used for
	 * connections for all testcases.
	 * 
	 * @return properties parsed from com.mysql.jdbc.testsuite.url
	 * 
	 * @throws SQLException
	 *             if parsing fails
	 */
	protected Properties getPropertiesFromTestsuiteUrl() throws SQLException {
		Properties props = new NonRegisteringDriver().parseURL(dbUrl, null);

		String hostname = props
				.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);

		if (hostname == null) {
			props.setProperty(NonRegisteringDriver.HOST_PROPERTY_KEY,
					"localhost");
		} else if (hostname.startsWith(":")) {
			props.setProperty(NonRegisteringDriver.HOST_PROPERTY_KEY,
					"localhost");
			props.setProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, hostname
					.substring(1));
		}

		String portNumber = props
				.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);

		if (portNumber == null) {
			props.setProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, "3306");
		}

		return props;
	}

	protected int getRowCount(String tableName) throws SQLException {
		ResultSet countRs = null;

		try {
			countRs = this.stmt.executeQuery("SELECT COUNT(*) FROM "
					+ tableName);

			countRs.next();

			return countRs.getInt(1);
		} finally {
			if (countRs != null) {
				countRs.close();
			}
		}
	}

	protected Object getSingleIndexedValueWithQuery(Connection c,
			int columnIndex, String query) throws SQLException {
		ResultSet valueRs = null;

		Statement svStmt = null;

		try {
			svStmt = c.createStatement();

			valueRs = svStmt.executeQuery(query);

			if (!valueRs.next()) {
				return null;
			}

			return valueRs.getObject(columnIndex);
		} finally {
			if (valueRs != null) {
				valueRs.close();
			}

			if (svStmt != null) {
				svStmt.close();
			}
		}
	}

	protected Object getSingleIndexedValueWithQuery(int columnIndex,
			String query) throws SQLException {
		return getSingleIndexedValueWithQuery(this.conn, columnIndex, query);
	}

	protected Object getSingleValue(String tableName, String columnName,
			String whereClause) throws SQLException {
		return getSingleValueWithQuery("SELECT " + columnName + " FROM "
				+ tableName + ((whereClause == null) ? "" : " " + whereClause));
	}

	protected Object getSingleValueWithQuery(String query) throws SQLException {
		return getSingleIndexedValueWithQuery(1, query);
	}

	protected boolean isAdminConnectionConfigured() {
		return System.getProperty(ADMIN_CONNECTION_PROPERTY_NAME) != null;
	}

	protected boolean isServerRunningOnWindows() throws SQLException {
		return (getMysqlVariable("datadir").indexOf('\\') != -1);
	}

	public void logDebug(String message) {
		if (System.getProperty("com.mysql.jdbc.testsuite.noDebugOutput") == null) {
			System.err.println(message);
		}
	}

	protected File newTempBinaryFile(String name, long size) throws IOException {
		File tempFile = File.createTempFile(name, "tmp");
		tempFile.deleteOnExit();
		
		cleanupTempFiles(tempFile, name);
		
		FileOutputStream fos = new FileOutputStream(tempFile);
		BufferedOutputStream bos = new BufferedOutputStream(fos);
		for (long i = 0; i < size; i++) {
			bos.write((byte) i);
		}
		bos.close();
		assertTrue(tempFile.exists());
		assertEquals(size, tempFile.length());
		return tempFile;
	}

	protected final boolean runLongTests() {
		return runTestIfSysPropDefined("com.mysql.jdbc.testsuite.runLongTests");
	}

	/**
	 * Checks whether a certain system property is defined, in order to
	 * run/not-run certain tests
	 * 
	 * @param propName
	 *            the property name to check for
	 * 
	 * @return true if the property is defined.
	 */
	protected boolean runTestIfSysPropDefined(String propName) {
		String prop = System.getProperty(propName);

		return (prop != null) && (prop.length() > 0);
	}

	protected boolean runMultiHostTests() {
		return !runTestIfSysPropDefined(NO_MULTI_HOST_PROPERTY_NAME);
	}

	/**
	 * Creates resources used by all tests.
	 * 
	 * @throws Exception
	 *             if an error occurs.
	 */
	public void setUp() throws Exception {
		System.out.println("Loading JDBC driver '" + this.dbClass + "'");
		Class.forName(this.dbClass).newInstance();
		System.out.println("Done.\n");
		this.createdObjects = new ArrayList();


		if (this.dbClass.equals("gwe.sql.gweMysqlDriver")) {
			try {
				this.conn = DriverManager.getConnection(dbUrl, "", "");
			} catch (Exception ex) {
				ex.printStackTrace();
				fail();
			}
		} else {
			try {
				this.conn = DriverManager.getConnection(dbUrl);
			} catch (Exception ex) {
				ex.printStackTrace();
				fail();
			}
		}

		System.out.println("Done.\n");
		this.stmt = this.conn.createStatement();

		try {
			if (dbUrl.indexOf("mysql") != -1) {
				this.rs = this.stmt.executeQuery("SELECT VERSION()");
				this.rs.next();
				logDebug("Connected to " + this.rs.getString(1));
				this.rs.close();
				this.rs = null;
			} else {
				logDebug("Connected to "
						+ this.conn.getMetaData().getDatabaseProductName()
						+ " / "
						+ this.conn.getMetaData().getDatabaseProductVersion());
			}
		} finally {
			if (this.rs != null) {
				this.rs.close();
			}
		}
	}

	/**
	 * Destroys resources created during the test case.
	 * 
	 * @throws Exception
	 *             DOCUMENT ME!
	 */
	public void tearDown() throws Exception {
		if (this.rs != null) {
			try {
				this.rs.close();
			} catch (SQLException SQLE) {
				;
			}
		}

		for (int i = 0; i < this.createdObjects.size(); i++) {
			try {
				String[] objectInfo = (String[])this.createdObjects.get(i);
				
				dropSchemaObject(objectInfo[0], objectInfo[1]);
			} catch (SQLException SQLE) {
				;
			}
		}

		if (this.stmt != null) {
			try {
				this.stmt.close();
			} catch (SQLException SQLE) {
				;
			}
		}

		if (this.pstmt != null) {
			try {
				this.pstmt.close();
			} catch (SQLException SQLE) {
				;
			}
		}

		if (this.conn != null) {
			try {
				this.conn.close();
			} catch (SQLException SQLE) {
				;
			}
		}
	}

	/**
	 * Checks whether the database we're connected to meets the given version
	 * minimum
	 * 
	 * @param major
	 *            the major version to meet
	 * @param minor
	 *            the minor version to meet
	 * 
	 * @return boolean if the major/minor is met
	 * 
	 * @throws SQLException
	 *             if an error occurs.
	 */
	protected boolean versionMeetsMinimum(int major, int minor)
			throws SQLException {
		return versionMeetsMinimum(major, minor, 0);
	}

	/**
	 * Checks whether the database we're connected to meets the given version
	 * minimum
	 * 
	 * @param major
	 *            the major version to meet
	 * @param minor
	 *            the minor version to meet
	 * 
	 * @return boolean if the major/minor is met
	 * 
	 * @throws SQLException
	 *             if an error occurs.
	 */
	protected boolean versionMeetsMinimum(int major, int minor, int subminor)
			throws SQLException {
		return (((com.mysql.jdbc.Connection) this.conn).versionMeetsMinimum(
				major, minor, subminor));
	}

	protected boolean isRunningOnJdk131() {
		return this.runningOnJdk131;
	}

	protected boolean isClassAvailable(String classname) {
		try {
			Class.forName(classname);
			return true;
		} catch (ClassNotFoundException e) {
			return false;
		}
	}

	protected void closeMemberJDBCResources() {
		if (this.rs != null) {
			ResultSet toClose = this.rs;
			this.rs = null;

			try {
				toClose.close();
			} catch (SQLException sqlEx) {
				// ignore
			}
		}

		if (this.pstmt != null) {
			PreparedStatement toClose = this.pstmt;
			this.pstmt = null;

			try {
				toClose.close();
			} catch (SQLException sqlEx) {
				// ignore
			}
		}
	}

	protected boolean isRunningOnJRockit() {
		String vmVendor = System.getProperty("java.vm.vendor");

		return (vmVendor != null && vmVendor.toUpperCase(Locale.US).startsWith(
				"BEA"));
	}

	protected String randomString() {
		int length = (int)(Math.random() * 32);
		
		StringBuffer buf = new StringBuffer(length);
		
		for (int i = 0; i < length; i++) {
			buf.append((char)((Math.random() * 26) + 'a'));
		}
		
		return buf.toString();
	}

	protected void cleanupTempFiles(final File exampleTempFile, final String tempfilePrefix) {
	
		File tempfilePath = exampleTempFile.getParentFile();
		
		File[] possibleFiles = tempfilePath.listFiles(new FilenameFilter() {
	
			public boolean accept(File dir, String name) {
				return (name.indexOf(tempfilePrefix) != -1 
						&& !exampleTempFile.getName().equals(name));
			}});
		
		for (int i = 0; i < possibleFiles.length; i++) {
			try {
				possibleFiles[i].delete();
			} catch (Throwable t) {
				// ignore, we're only making a best effort cleanup attempt here
			}
		}
	}
}