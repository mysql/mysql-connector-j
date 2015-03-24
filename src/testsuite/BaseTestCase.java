/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

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

package testsuite;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import junit.framework.TestCase;

import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.ReplicationConnection;
import com.mysql.jdbc.ReplicationDriver;
import com.mysql.jdbc.StringUtils;
import com.mysql.jdbc.Util;

/**
 * Base class for all test cases. Creates connections, statements, etc. and closes them.
 */
public abstract class BaseTestCase extends TestCase {
    protected final static String SSL_CIPHERS_FOR_576 = "TLS_RSA_WITH_AES_128_CBC_SHA,SSL_RSA_WITH_RC4_128_SHA,SSL_RSA_WITH_3DES_EDE_CBC_SHA,"
            + "SSL_RSA_WITH_RC4_128_MD5,SSL_RSA_WITH_DES_CBC_SHA";

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
    private List<String[]> createdObjects;

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

    private boolean isOnCSFS = true;

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
            String defaultDbUrl = System.getProperty("com.mysql.jdbc.testsuite.url.default");

            if ((defaultDbUrl != null) && (defaultDbUrl.trim().length() != 0)) {
                dbUrl = defaultDbUrl;
            }
        }

        String newDriver = System.getProperty("com.mysql.jdbc.testsuite.driver");

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

    protected void createSchemaObject(String objectType, String objectName, String columnsAndOtherStuff) throws SQLException {
        this.createdObjects.add(new String[] { objectType, objectName });
        dropSchemaObject(objectType, objectName);

        StringBuilder createSql = new StringBuilder(objectName.length() + objectType.length() + columnsAndOtherStuff.length() + 10);
        createSql.append("CREATE  ");
        createSql.append(objectType);
        createSql.append(" ");
        createSql.append(objectName);
        createSql.append(" ");
        createSql.append(columnsAndOtherStuff);

        try {
            this.stmt.executeUpdate(createSql.toString());
        } catch (SQLException sqlEx) {
            if ("42S01".equals(sqlEx.getSQLState())) {
                System.err.println("WARN: Stale mysqld table cache preventing table creation - flushing tables and trying again");
                this.stmt.executeUpdate("FLUSH TABLES"); // some bug in 5.1 on the mac causes tables to not disappear from the cache
                this.stmt.executeUpdate(createSql.toString());
            } else {
                throw sqlEx;
            }
        }
    }

    protected void createFunction(String functionName, String functionDefn) throws SQLException {
        createSchemaObject("FUNCTION", functionName, functionDefn);
    }

    protected void dropFunction(String functionName) throws SQLException {
        dropSchemaObject("FUNCTION", functionName);
    }

    protected void createProcedure(String procedureName, String procedureDefn) throws SQLException {
        createSchemaObject("PROCEDURE", procedureName, procedureDefn);
    }

    protected void dropProcedure(String procedureName) throws SQLException {
        dropSchemaObject("PROCEDURE", procedureName);
    }

    protected void createTable(String tableName, String columnsAndOtherStuff) throws SQLException {
        createSchemaObject("TABLE", tableName, columnsAndOtherStuff);
    }

    protected void createTable(String tableName, String columnsAndOtherStuff, String engine) throws SQLException {
        createSchemaObject("TABLE", tableName, columnsAndOtherStuff + " " + getTableTypeDecl() + " = " + engine);
    }

    protected void dropTable(String tableName) throws SQLException {
        dropSchemaObject("TABLE", tableName);
    }

    protected void createView(String viewName, String columnsAndOtherStuff) throws SQLException {
        createSchemaObject("VIEW", viewName, columnsAndOtherStuff);
    }

    protected void dropView(String viewName) throws SQLException {
        dropSchemaObject("VIEW", viewName);
    }

    protected void createDatabase(String databaseName) throws SQLException {
        createSchemaObject("DATABASE", databaseName, "");
    }

    protected void dropDatabase(String databaseName) throws SQLException {
        dropSchemaObject("DATABASE", databaseName);
    }

    protected void dropSchemaObject(String objectType, String objectName) throws SQLException {
        this.stmt.executeUpdate("DROP " + objectType + " IF EXISTS " + objectName);
    }

    protected Connection getAdminConnection() throws SQLException {
        return getAdminConnectionWithProps(new Properties());
    }

    protected Connection getAdminConnectionWithProps(Properties props) throws SQLException {
        String adminUrl = System.getProperty(ADMIN_CONNECTION_PROPERTY_NAME);

        if (adminUrl != null) {
            return DriverManager.getConnection(adminUrl, props);
        }
        return null;
    }

    protected Connection getConnectionWithProps(String propsList) throws SQLException {
        return getConnectionWithProps(dbUrl, propsList);
    }

    protected Connection getConnectionWithProps(String url, String propsList) throws SQLException {
        Properties props = new Properties();

        if (propsList != null) {
            List<String> keyValuePairs = StringUtils.split(propsList, ",", false);

            for (String kvp : keyValuePairs) {
                List<String> splitUp = StringUtils.split(kvp, "=", false);
                StringBuilder value = new StringBuilder();

                for (int i = 1; i < splitUp.size(); i++) {
                    if (i != 1) {
                        value.append("=");
                    }

                    value.append(splitUp.get(i));

                }

                props.setProperty(splitUp.get(0).toString().trim(), value.toString());
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
     */
    public Connection getConnectionWithProps(Properties props) throws SQLException {
        return DriverManager.getConnection(dbUrl, props);
    }

    protected Connection getConnectionWithProps(String url, Properties props) throws SQLException {
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

    protected String getMysqlVariable(Connection c, String variableName) throws SQLException {
        Object value = getSingleIndexedValueWithQuery(c, 2, "SHOW VARIABLES LIKE '" + variableName + "'");

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

        String hostname = props.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);

        if (hostname == null) {
            props.setProperty(NonRegisteringDriver.HOST_PROPERTY_KEY, "localhost");
        } else if (hostname.startsWith(":")) {
            props.setProperty(NonRegisteringDriver.HOST_PROPERTY_KEY, "localhost");
            props.setProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, hostname.substring(1));
        }

        String portNumber = props.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);

        if (portNumber == null) {
            props.setProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, "3306");
        }

        return props;
    }

    protected int getRowCount(String tableName) throws SQLException {
        ResultSet countRs = null;

        try {
            countRs = this.stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);

            countRs.next();

            return countRs.getInt(1);
        } finally {
            if (countRs != null) {
                countRs.close();
            }
        }
    }

    protected Object getSingleIndexedValueWithQuery(Connection c, int columnIndex, String query) throws SQLException {
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

    protected Object getSingleIndexedValueWithQuery(int columnIndex, String query) throws SQLException {
        return getSingleIndexedValueWithQuery(this.conn, columnIndex, query);
    }

    protected Object getSingleValue(String tableName, String columnName, String whereClause) throws SQLException {
        return getSingleValueWithQuery("SELECT " + columnName + " FROM " + tableName + ((whereClause == null) ? "" : " " + whereClause));
    }

    protected Object getSingleValueWithQuery(String query) throws SQLException {
        return getSingleIndexedValueWithQuery(1, query);
    }

    protected String getTableTypeDecl() throws SQLException {
        if (versionMeetsMinimum(5, 0)) {
            return "ENGINE";
        }
        return "TYPE";
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
    @Override
    public void setUp() throws Exception {
        System.out.println("Loading JDBC driver '" + this.dbClass + "'");
        Class.forName(this.dbClass).newInstance();
        System.out.println("Done.\n");
        this.createdObjects = new ArrayList<String[]>();

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
                logDebug("Connected to " + this.conn.getMetaData().getDatabaseProductName() + " / " + this.conn.getMetaData().getDatabaseProductVersion());
            }
        } finally {
            if (this.rs != null) {
                this.rs.close();
            }
        }

        this.isOnCSFS = !this.conn.getMetaData().storesLowerCaseIdentifiers();
    }

    /**
     * Destroys resources created during the test case.
     */
    @Override
    public void tearDown() throws Exception {
        if (this.rs != null) {
            try {
                this.rs.close();
            } catch (SQLException SQLE) {
            }
        }

        if (System.getProperty("com.mysql.jdbc.testsuite.retainArtifacts") == null) {
            for (int i = 0; i < this.createdObjects.size(); i++) {
                try {
                    String[] objectInfo = this.createdObjects.get(i);

                    dropSchemaObject(objectInfo[0], objectInfo[1]);
                } catch (SQLException SQLE) {
                }
            }
        }

        if (this.stmt != null) {
            try {
                this.stmt.close();
            } catch (SQLException SQLE) {
            }
        }

        if (this.pstmt != null) {
            try {
                this.pstmt.close();
            } catch (SQLException SQLE) {
            }
        }

        if (this.conn != null) {
            try {
                this.conn.close();
            } catch (SQLException SQLE) {
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
    protected boolean versionMeetsMinimum(int major, int minor) throws SQLException {
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
    protected boolean versionMeetsMinimum(int major, int minor, int subminor) throws SQLException {
        return (((com.mysql.jdbc.Connection) this.conn).versionMeetsMinimum(major, minor, subminor));
    }

    /**
     * Checks whether the server we're connected to is a MySQL Community edition
     */
    protected boolean isCommunityEdition() {
        return Util.isCommunityEdition(((MySQLConnection) this.conn).getServerVersion());
    }

    /**
     * Checks whether the server we're connected to is an MySQL Enterprise edition
     */
    protected boolean isEnterpriseEdition() {
        return Util.isEnterpriseEdition(((MySQLConnection) this.conn).getServerVersion());
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

    protected boolean isRunningOnJRockit() {
        String vmVendor = System.getProperty("java.vm.vendor");

        return (vmVendor != null && vmVendor.toUpperCase(Locale.US).startsWith("BEA"));
    }

    protected String randomString() {
        int length = (int) (Math.random() * 32);

        StringBuilder buf = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            buf.append((char) ((Math.random() * 26) + 'a'));
        }

        return buf.toString();
    }

    protected void cleanupTempFiles(final File exampleTempFile, final String tempfilePrefix) {

        File tempfilePath = exampleTempFile.getParentFile();

        File[] possibleFiles = tempfilePath.listFiles(new FilenameFilter() {

            public boolean accept(File dir, String name) {
                return (name.indexOf(tempfilePrefix) != -1 && !exampleTempFile.getName().equals(name));
            }
        });

        for (int i = 0; i < possibleFiles.length; i++) {
            try {
                possibleFiles[i].delete();
            } catch (Throwable t) {
                // ignore, we're only making a best effort cleanup attempt here
            }
        }
    }

    protected void assertResultSetLength(ResultSet rset, int len) throws Exception {
        int oldRowPos = rset.getRow();
        rset.last();
        assertEquals("Result set length", len, rset.getRow());
        if (oldRowPos > 0) {
            rset.absolute(oldRowPos);
        } else {
            rset.beforeFirst();
        }
    }

    protected void assertResultSetsEqual(ResultSet control, ResultSet test) throws Exception {
        int controlNumCols = control.getMetaData().getColumnCount();
        int testNumCols = test.getMetaData().getColumnCount();
        assertEquals(controlNumCols, testNumCols);

        StringBuilder rsAsString = new StringBuilder();

        while (control.next()) {
            test.next();
            rsAsString.append("\n");
            for (int i = 0; i < controlNumCols; i++) {
                Object controlObj = control.getObject(i + 1);
                Object testObj = test.getObject(i + 1);

                rsAsString.append("" + controlObj);
                rsAsString.append("\t = \t");
                rsAsString.append("" + testObj);
                rsAsString.append(", ");

                if (controlObj == null) {
                    assertNull("Expected null, see last row: \n" + rsAsString.toString(), testObj);
                } else {
                    assertNotNull("Expected non-null, see last row: \n" + rsAsString.toString(), testObj);
                }

                if (controlObj instanceof Float) {
                    assertEquals("Float comparison failed, see last row: \n" + rsAsString.toString(), ((Float) controlObj).floatValue(),
                            ((Float) testObj).floatValue(), 0.1);
                } else if (controlObj instanceof Double) {
                    assertEquals("Double comparison failed, see last row: \n" + rsAsString.toString(), ((Double) controlObj).doubleValue(),
                            ((Double) testObj).doubleValue(), 0.1);
                } else {
                    assertEquals("Value comparison failed, see last row: \n" + rsAsString.toString(), controlObj, testObj);
                }
            }
        }

        int howMuchMore = 0;

        while (test.next()) {
            rsAsString.append("\n");
            howMuchMore++;
            for (int i = 0; i < controlNumCols; i++) {
                rsAsString.append("\t = \t");
                rsAsString.append("" + test.getObject(i + 1));
                rsAsString.append(", ");
            }
        }

        assertTrue("Found " + howMuchMore + " extra rows in result set to be compared: ", howMuchMore == 0);
    }

    protected static <EX extends Throwable> EX assertThrows(Class<EX> throwable, Callable<?> testRoutine) {
        try {
            testRoutine.call();
        } catch (Throwable t) {
            if (!throwable.isAssignableFrom(t.getClass())) {
                fail("Expected exception of type '" + throwable.getName() + "' but instead a exception of type '" + t.getClass().getName() + "' was thrown.");
            }

            return throwable.cast(t);
        }
        fail("Expected exception of type '" + throwable.getName() + "'.");

        // never reaches here
        return null;
    }

    protected static <EX extends Throwable> EX assertThrows(Class<EX> throwable, String msgMatchesRegex, Callable<?> testRoutine) {
        try {
            testRoutine.call();
        } catch (Throwable t) {
            if (!throwable.isAssignableFrom(t.getClass())) {
                fail("Expected exception of type '" + throwable.getName() + "' but instead a exception of type '" + t.getClass().getName() + "' was thrown.");
            }

            if (!t.getMessage().matches(msgMatchesRegex)) {
                fail("The error message «" + t.getMessage() + "» was expected to match «" + msgMatchesRegex + "».");
            }

            return throwable.cast(t);
        }
        fail("Expected exception of type '" + throwable.getName() + "'.");

        // never reaches here
        return null;
    }

    protected void assertByteArrayEquals(String message, byte[] expected, byte[] actual) {
        assertEquals(message + " - array lenght", expected.length, actual.length);
        for (int i = 0, s = expected.length; i < s; i++) {
            assertEquals(message + " - element at " + i, expected[i], actual[i]);
        }
    }

    /*
     * Set default values for primitives. (prevents NPE in Java 1.4 when calling via reflection)
     */
    protected void fillPrimitiveDefaults(Class<?> types[], Object vals[], int count) {
        for (int i = 0; i < count; ++i) {
            if (vals[i] != null) {
                continue;
            }
            String type = types[i].toString();
            if (type.equals("short")) {
                vals[i] = new Short((short) 0);
            } else if (type.equals("int")) {
                vals[i] = new Integer(0);
            } else if (type.equals("long")) {
                vals[i] = new Long(0);
            } else if (type.equals("boolean")) {
                vals[i] = new Boolean(false);
            } else if (type.equals("byte")) {
                vals[i] = new Byte((byte) 0);
            } else if (type.equals("double")) {
                vals[i] = new Double(0.0);
            } else if (type.equals("float")) {
                vals[i] = new Float(0.0);
            }
        }
    }

    /**
     * Retrieve the current system time in milliseconds, using the nanosecond
     * time if possible.
     */
    protected long currentTimeMillis() {
        try {
            Method mNanoTime = System.class.getDeclaredMethod("nanoTime", (Class[]) null);
            return ((Long) mNanoTime.invoke(null, (Object[]) null)).longValue() / 1000000;
        } catch (Exception ex) {
            return System.currentTimeMillis();
        }
    }

    protected Connection getMasterSlaveReplicationConnection() throws SQLException {

        Connection replConn = new ReplicationDriver().connect(getMasterSlaveUrl(), getMasterSlaveProps());

        return replConn;
    }

    protected String getMasterSlaveUrl() throws SQLException {

        Properties defaultProps = getPropertiesFromTestsuiteUrl();
        String hostname = defaultProps.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);

        if (NonRegisteringDriver.isHostPropertiesList(hostname)) {
            String url = String.format("jdbc:mysql://%s,%s/", hostname, hostname);

            return url;
        }

        StringBuilder urlBuf = new StringBuilder("jdbc:mysql://");

        String portNumber = defaultProps.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, "3306");

        hostname = (hostname == null ? "localhost" : hostname);

        for (int i = 0; i < 2; i++) {
            urlBuf.append(hostname);
            urlBuf.append(":");
            urlBuf.append(portNumber);

            if (i == 0) {
                urlBuf.append(",");
            }
        }
        urlBuf.append("/");

        return urlBuf.toString();
    }

    protected Properties getMasterSlaveProps() throws SQLException {
        Properties props = getPropertiesFromTestsuiteUrl();

        removeHostRelatedProps(props);

        return props;
    }

    protected void removeHostRelatedProps(Properties props) {
        props.remove(NonRegisteringDriver.HOST_PROPERTY_KEY);
        props.remove(NonRegisteringDriver.PORT_PROPERTY_KEY);

        int numHosts = Integer.parseInt(props.getProperty(NonRegisteringDriver.NUM_HOSTS_PROPERTY_KEY));

        for (int i = 1; i <= numHosts; i++) {
            props.remove(NonRegisteringDriver.HOST_PROPERTY_KEY + "." + i);
            props.remove(NonRegisteringDriver.PORT_PROPERTY_KEY + "." + i);
        }

        props.remove(NonRegisteringDriver.NUM_HOSTS_PROPERTY_KEY);
    }

    protected Connection getLoadBalancedConnection(int customHostLocation, String customHost, Properties props) throws SQLException {
        Properties parsedProps = new NonRegisteringDriver().parseURL(dbUrl, null);

        String defaultHost = parsedProps.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);

        if (!NonRegisteringDriver.isHostPropertiesList(defaultHost)) {
            String port = parsedProps.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, "3306");
            defaultHost = defaultHost + ":" + port;
        }

        if (customHost != null && customHost.length() > 0) {
            customHost = customHost + ",";
        } else {
            customHost = "";
        }

        String hostsString = null;

        switch (customHostLocation) {
            case 1:
                hostsString = customHost + defaultHost;
                break;
            case 2:
                hostsString = defaultHost + "," + customHost + defaultHost;
                break;
            case 3:
                hostsString = defaultHost + "," + customHost;
                hostsString = hostsString.substring(0, hostsString.length() - 1);
                break;
            default:
                throw new IllegalArgumentException();
        }

        if (props != null) {
            parsedProps.putAll(props);
        }

        Connection lbConn = DriverManager.getConnection("jdbc:mysql:loadbalance://" + hostsString, parsedProps);

        return lbConn;
    }

    protected Connection getLoadBalancedConnection() throws SQLException {
        return getLoadBalancedConnection(1, "", null);
    }

    protected Connection getLoadBalancedConnection(Properties props) throws SQLException {
        return getLoadBalancedConnection(1, "", props);
    }

    protected void copyBasePropertiesIntoProps(Properties props, NonRegisteringDriver d) throws SQLException {
        Properties testCaseProps = d.parseURL(BaseTestCase.dbUrl, null);
        String user = testCaseProps.getProperty(NonRegisteringDriver.USER_PROPERTY_KEY);

        if (user != null) {
            props.setProperty(NonRegisteringDriver.USER_PROPERTY_KEY, user);
        }

        String password = testCaseProps.getProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY);

        if (password != null) {
            props.setProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY, password);
        }

        String port = testCaseProps.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);

        if (port != null) {
            props.setProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, port);
        } else {
            String host = testCaseProps.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);

            if (host != null) {
                String[] hostPort = host.split(":");

                if (hostPort.length > 1) {
                    props.setProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, hostPort[1]);
                }
            }
        }
    }

    protected String getPort(Properties props, NonRegisteringDriver d) throws SQLException {
        String port = d.parseURL(BaseTestCase.dbUrl, props).getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);
        if (port == null) {
            port = "3306";
        }
        return port;
    }

    protected String getPortFreeHostname(Properties props, NonRegisteringDriver d) throws SQLException {
        String host = d.parseURL(BaseTestCase.dbUrl, props).getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);

        if (host == null) {
            host = "localhost";
        }

        host = host.split(":")[0];
        return host;
    }

    protected Connection getUnreliableMultiHostConnection(String haMode, String[] hostNames, Properties props, Set<String> downedHosts) throws Exception {
        if (props == null) {
            props = new Properties();
        }

        if (downedHosts == null) {
            downedHosts = new HashSet<String>();
        }

        NonRegisteringDriver driver = new NonRegisteringDriver();

        copyBasePropertiesIntoProps(props, driver);
        props.setProperty("socketFactory", "testsuite.UnreliableSocketFactory");

        Properties parsedProps = driver.parseURL(BaseTestCase.dbUrl, props);
        String db = parsedProps.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
        String port = parsedProps.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);
        String host = getPortFreeHostname(props, driver);

        UnreliableSocketFactory.flushAllStaticData();

        StringBuilder hostString = new StringBuilder();
        String delimiter = "";
        for (String hostName : hostNames) {
            UnreliableSocketFactory.mapHost(hostName, host);
            hostString.append(delimiter);
            delimiter = ",";
            hostString.append(hostName + ":" + (port == null ? "3306" : port));

            if (downedHosts.contains(hostName)) {
                UnreliableSocketFactory.downHost(hostName);
            }
        }

        props.remove(NonRegisteringDriver.HOST_PROPERTY_KEY);

        if (haMode == null) {
            haMode = "";
        } else if (haMode.length() > 0) {
            haMode += ":";
        }

        return getConnectionWithProps("jdbc:mysql:" + haMode + "//" + hostString.toString() + "/" + db, props);
    }

    protected Connection getUnreliableFailoverConnection(String[] hostNames, Properties props) throws Exception {
        return getUnreliableFailoverConnection(hostNames, props, null);
    }

    protected Connection getUnreliableFailoverConnection(String[] hostNames, Properties props, Set<String> downedHosts) throws Exception {
        return getUnreliableMultiHostConnection(null, hostNames, props, downedHosts);
    }

    protected Connection getUnreliableLoadBalancedConnection(String[] hostNames, Properties props) throws Exception {
        return getUnreliableLoadBalancedConnection(hostNames, props, null);
    }

    protected Connection getUnreliableLoadBalancedConnection(String[] hostNames, Properties props, Set<String> downedHosts) throws Exception {
        return getUnreliableMultiHostConnection("loadbalance", hostNames, props, downedHosts);
    }

    protected ReplicationConnection getUnreliableReplicationConnection(String[] hostNames, Properties props) throws Exception {
        return getUnreliableReplicationConnection(hostNames, props, null);
    }

    protected ReplicationConnection getUnreliableReplicationConnection(String[] hostNames, Properties props, Set<String> downedHosts) throws Exception {
        return (ReplicationConnection) getUnreliableMultiHostConnection("replication", hostNames, props, downedHosts);
    }

    public static class MockConnectionConfiguration {
        String hostName;
        String port;
        String serverType;
        boolean isDowned = false;

        public MockConnectionConfiguration(String hostName, String serverType, String port, boolean isDowned) {
            this.hostName = hostName;
            this.serverType = serverType;
            this.isDowned = isDowned;
            this.port = port;
        }

        public String getAddress(boolean withTrailingPort) {
            return "address=(protocol=tcp)(host=" + this.hostName + ")(port=" + this.port + ")(type=" + this.serverType + ")"
                    + (withTrailingPort ? (":" + this.port) : "");
        }

        public String getAddress() {
            return getAddress(false);
        }
    }

    protected ReplicationConnection getUnreliableReplicationConnection(Set<MockConnectionConfiguration> configs, Properties props) throws Exception {
        NonRegisteringDriver d = new NonRegisteringDriver();
        this.copyBasePropertiesIntoProps(props, d);
        props.setProperty("socketFactory", "testsuite.UnreliableSocketFactory");
        Properties parsed = d.parseURL(BaseTestCase.dbUrl, props);
        String db = parsed.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
        String port = parsed.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);
        String host = getPortFreeHostname(props, d);
        UnreliableSocketFactory.flushAllStaticData();
        StringBuilder hostString = new StringBuilder();
        String glue = "";
        for (MockConnectionConfiguration config : configs) {
            UnreliableSocketFactory.mapHost(config.hostName, host);
            hostString.append(glue);
            glue = ",";
            if (config.port == null) {
                config.port = (port == null ? "3306" : port);
            }
            hostString.append(config.getAddress());
            if (config.isDowned) {
                UnreliableSocketFactory.downHost(config.hostName);
            }

        }
        props.remove(NonRegisteringDriver.HOST_PROPERTY_KEY);

        return (ReplicationConnection) getConnectionWithProps("jdbc:mysql:replication://" + hostString.toString() + "/" + db, props);

    }

    protected boolean assertEqualsFSAware(String matchStr, String inStr) throws Exception {
        if (this.isOnCSFS) {
            return matchStr.equals(inStr);
        }
        return matchStr.equalsIgnoreCase(inStr);
    }

    protected String removeSqlMode(String mode, String fromStr) throws Exception {
        String res = fromStr;
        if (res != null && mode != null) {
            res = res.replaceFirst("'" + mode + "'", "").replaceFirst(mode, "").replaceFirst(",,", ",");
        }
        return res;
    }
}
