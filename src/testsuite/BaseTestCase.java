/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

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

package testsuite;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Method;
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

import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.ReplicationConnection;
import com.mysql.jdbc.ReplicationDriver;
import com.mysql.jdbc.StringUtils;
import com.mysql.jdbc.Util;

import junit.framework.TestCase;

/**
 * Base class for all test cases. Creates connections, statements, etc. and closes them.
 */
public abstract class BaseTestCase extends TestCase {

    private final static String ADMIN_CONNECTION_PROPERTY_NAME = "com.mysql.jdbc.testsuite.admin-url";

    private final static String NO_MULTI_HOST_PROPERTY_NAME = "com.mysql.jdbc.testsuite.no-multi-hosts-tests";

    // next variables disable some tests
    protected boolean DISABLED_testBug15121 = true; // TODO needs to be fixed on server
    protected boolean DISABLED_testBug7033 = true; // TODO disabled for unknown reason
    protected boolean DISABLED_testBug2654 = true; // TODO check if it's still a server-level bug
    protected boolean DISABLED_testBug5136 = true; // TODO disabled for unknown reason
    protected boolean DISABLED_testBug65503 = true; // TODO disabled for unknown reason
    protected boolean DISABLED_testContention = true; // TODO disabled for unknown reason

    /**
     * JDBC URL, initialized from com.mysql.jdbc.testsuite.url system property,
     * or defaults to jdbc:mysql:///test
     */
    protected static String dbUrl = "jdbc:mysql:///test";

    /**
     * JDBC URL, initialized from com.mysql.jdbc.testsuite.url.sha256default system property
     */
    protected static String sha256Url = null;

    /** Instance counter */
    private static int instanceCount = 1;

    /** Connection to server, initialized in setUp() Cleaned up in tearDown(). */
    protected Connection conn = null;

    protected Connection sha256Conn = null;

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

    protected ResultSet sha256Rs = null;

    /**
     * Statement to be used in tests, initialized in setUp(). Cleaned up in
     * tearDown().
     */
    protected Statement stmt = null;

    protected Statement sha256Stmt = null;

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

        String defaultSha256Url = System.getProperty("com.mysql.jdbc.testsuite.url.sha256default");

        if ((defaultSha256Url != null) && (defaultSha256Url.trim().length() != 0)) {
            sha256Url = defaultSha256Url;
        }

        String newDriver = System.getProperty("com.mysql.jdbc.testsuite.driver");

        if ((newDriver != null) && (newDriver.trim().length() != 0)) {
            this.dbClass = newDriver;
        }
    }

    protected void createSchemaObject(String objectType, String objectName, String columnsAndOtherStuff) throws SQLException {
        createSchemaObject(this.stmt, objectType, objectName, columnsAndOtherStuff);
    }

    protected void createSchemaObject(Statement st, String objectType, String objectName, String columnsAndOtherStuff) throws SQLException {
        if (st != null) {
            this.createdObjects.add(new String[] { objectType, objectName });
            try {
                dropSchemaObject(st, objectType, objectName);
            } catch (SQLException ex) {
                // ignore DROP USER failures
                if (!ex.getMessage().startsWith("Operation DROP USER failed")) {
                    throw ex;
                }
            }

            StringBuilder createSql = new StringBuilder(objectName.length() + objectType.length() + columnsAndOtherStuff.length() + 10);
            createSql.append("CREATE  ");
            createSql.append(objectType);
            createSql.append(" ");
            createSql.append(objectName);
            createSql.append(" ");
            createSql.append(columnsAndOtherStuff);

            try {
                st.executeUpdate(createSql.toString());
            } catch (SQLException sqlEx) {
                if ("42S01".equals(sqlEx.getSQLState())) {
                    System.err.println("WARN: Stale mysqld table cache preventing table creation - flushing tables and trying again");
                    st.executeUpdate("FLUSH TABLES"); // some bug in 5.1 on the mac causes tables to not disappear from the cache
                    st.executeUpdate(createSql.toString());
                } else {
                    throw sqlEx;
                }
            }
        }
    }

    protected void createFunction(Statement st, String functionName, String functionDefn) throws SQLException {
        createSchemaObject(st, "FUNCTION", functionName, functionDefn);
    }

    protected void createFunction(String functionName, String functionDefn) throws SQLException {
        createFunction(this.stmt, functionName, functionDefn);
    }

    protected void dropFunction(Statement st, String functionName) throws SQLException {
        dropSchemaObject(st, "FUNCTION", functionName);
    }

    protected void dropFunction(String functionName) throws SQLException {
        dropFunction(this.stmt, functionName);
    }

    protected void createProcedure(Statement st, String procedureName, String procedureDefn) throws SQLException {
        createSchemaObject(st, "PROCEDURE", procedureName, procedureDefn);
    }

    protected void createProcedure(String procedureName, String procedureDefn) throws SQLException {
        createProcedure(this.stmt, procedureName, procedureDefn);
    }

    protected void dropProcedure(Statement st, String procedureName) throws SQLException {
        dropSchemaObject(st, "PROCEDURE", procedureName);
    }

    protected void dropProcedure(String procedureName) throws SQLException {
        dropProcedure(this.stmt, procedureName);
    }

    protected void createTable(Statement st, String tableName, String columnsAndOtherStuff) throws SQLException {
        createSchemaObject(st, "TABLE", tableName, columnsAndOtherStuff);
    }

    protected void createTable(String tableName, String columnsAndOtherStuff) throws SQLException {
        createTable(this.stmt, tableName, columnsAndOtherStuff);
    }

    protected void createTable(Statement st, String tableName, String columnsAndOtherStuff, String engine) throws SQLException {
        createSchemaObject(st, "TABLE", tableName, columnsAndOtherStuff + " " + getTableTypeDecl() + " = " + engine);
    }

    protected void createTable(String tableName, String columnsAndOtherStuff, String engine) throws SQLException {
        createTable(this.stmt, tableName, columnsAndOtherStuff, engine);
    }

    protected void dropTable(Statement st, String tableName) throws SQLException {
        dropSchemaObject(st, "TABLE", tableName);
    }

    protected void dropTable(String tableName) throws SQLException {
        dropTable(this.stmt, tableName);
    }

    protected void createView(Statement st, String viewName, String columnsAndOtherStuff) throws SQLException {
        createSchemaObject(st, "VIEW", viewName, columnsAndOtherStuff);
    }

    protected void createView(String viewName, String columnsAndOtherStuff) throws SQLException {
        createView(this.stmt, viewName, columnsAndOtherStuff);
    }

    protected void dropView(Statement st, String viewName) throws SQLException {
        dropSchemaObject(st, "VIEW", viewName);
    }

    protected void dropView(String viewName) throws SQLException {
        dropView(this.stmt, viewName);
    }

    protected void createDatabase(Statement st, String databaseName) throws SQLException {
        createSchemaObject(st, "DATABASE", databaseName, "");
    }

    protected void createDatabase(String databaseName) throws SQLException {
        createDatabase(this.stmt, databaseName);
    }

    protected void dropDatabase(Statement st, String databaseName) throws SQLException {
        dropSchemaObject(st, "DATABASE", databaseName);
    }

    protected void dropDatabase(String databaseName) throws SQLException {
        dropDatabase(this.stmt, databaseName);
    }

    protected void createUser(Statement st, String userName, String otherStuff) throws SQLException {
        createSchemaObject(st, "USER", userName, otherStuff);
    }

    protected void createUser(String userName, String otherStuff) throws SQLException {
        createUser(this.stmt, userName, otherStuff);
    }

    protected void dropUser(Statement st, String user) throws SQLException {
        dropSchemaObject(st, "USER", user);
    }

    protected void dropUser(String user) throws SQLException {
        dropUser(this.stmt, user);
    }

    protected void dropSchemaObject(String objectType, String objectName) throws SQLException {
        dropSchemaObject(this.stmt, objectType, objectName);
    }

    protected void dropSchemaObject(Statement st, String objectType, String objectName) throws SQLException {
        if (st != null) {
            if (!objectType.equalsIgnoreCase("USER") || ((ConnectionImpl) st.getConnection()).versionMeetsMinimum(5, 7, 8)) {
                st.executeUpdate("DROP " + objectType + " IF EXISTS " + objectName);
            } else {
                st.executeUpdate("DROP " + objectType + " " + objectName);
            }
            st.executeUpdate("flush privileges");
        }
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

    protected Connection getNewConnection() throws SQLException {
        return DriverManager.getConnection(dbUrl);
    }

    protected Connection getNewSha256Connection() throws SQLException {
        if (sha256Url != null) {
            Properties props = new Properties();
            props.setProperty("allowPublicKeyRetrieval", "true");
            return DriverManager.getConnection(sha256Url, props);
        }
        return null;
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

    protected Properties getHostFreePropertiesFromTestsuiteUrl() throws SQLException {
        return getHostFreePropertiesFromTestsuiteUrl(null);
    }

    protected Properties getHostFreePropertiesFromTestsuiteUrl(Properties props) throws SQLException {
        Properties parsedProps = getPropertiesFromTestsuiteUrl();
        if (props != null) {
            parsedProps.putAll(props);
        }
        removeHostRelatedProps(parsedProps);
        return parsedProps;
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
                this.sha256Conn = sha256Url == null ? null : DriverManager.getConnection(sha256Url, "", "");
            } catch (Exception ex) {
                ex.printStackTrace();
                fail();
            }
        } else {
            try {
                Properties props = new Properties();
                props.setProperty("useSSL", "false"); // testsuite is built upon non-SSL default connection
                this.conn = DriverManager.getConnection(dbUrl, props);

                props.setProperty("allowPublicKeyRetrieval", "true");
                this.sha256Conn = sha256Url == null ? null : DriverManager.getConnection(sha256Url, props);
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
            } else {
                logDebug("Connected to " + this.conn.getMetaData().getDatabaseProductName() + " / " + this.conn.getMetaData().getDatabaseProductVersion());
            }
        } finally {
            if (this.rs != null) {
                this.rs.close();
                this.rs = null;
            }
        }

        this.isOnCSFS = !this.conn.getMetaData().storesLowerCaseIdentifiers();

        if (this.sha256Conn != null) {
            this.sha256Stmt = this.sha256Conn.createStatement();

            try {
                if (sha256Url.indexOf("mysql") != -1) {
                    this.sha256Rs = this.sha256Stmt.executeQuery("SELECT VERSION()");
                    this.sha256Rs.next();
                    logDebug("Connected to " + this.sha256Rs.getString(1));
                } else {
                    logDebug("Connected to " + this.sha256Conn.getMetaData().getDatabaseProductName() + " / "
                            + this.sha256Conn.getMetaData().getDatabaseProductVersion());
                }
            } finally {
                if (this.sha256Rs != null) {
                    this.sha256Rs.close();
                    this.sha256Rs = null;
                }
            }
        }
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

        if (this.sha256Rs != null) {
            try {
                this.sha256Rs.close();
            } catch (SQLException SQLE) {
            }
        }

        if (System.getProperty("com.mysql.jdbc.testsuite.retainArtifacts") == null) {
            Statement st = this.conn == null || this.conn.isClosed() ? getNewConnection().createStatement() : this.conn.createStatement();
            Statement sha256st;
            if (this.sha256Conn == null || this.sha256Conn.isClosed()) {
                Connection c = getNewSha256Connection();
                sha256st = c == null ? null : c.createStatement();
            } else {
                sha256st = this.sha256Conn.createStatement();
            }

            for (int i = 0; i < this.createdObjects.size(); i++) {
                String[] objectInfo = this.createdObjects.get(i);

                try {
                    dropSchemaObject(st, objectInfo[0], objectInfo[1]);
                } catch (SQLException SQLE) {
                }

                try {
                    dropSchemaObject(sha256st, objectInfo[0], objectInfo[1]);
                } catch (SQLException SQLE) {
                }
            }
            st.close();
            if (sha256st != null) {
                sha256st.close();
            }
        }

        if (this.stmt != null) {
            try {
                this.stmt.close();
            } catch (SQLException SQLE) {
            }
        }

        if (this.sha256Stmt != null) {
            try {
                this.sha256Stmt.close();
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

        if (this.sha256Conn != null) {
            try {
                this.sha256Conn.close();
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

        if (possibleFiles != null) {
            for (int i = 0; i < possibleFiles.length; i++) {
                try {
                    possibleFiles[i].delete();
                } catch (Throwable t) {
                    // ignore, we're only making a best effort cleanup attempt here
                }
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
        return assertThrows("", throwable, null, testRoutine);
    }

    protected static <EX extends Throwable> EX assertThrows(String message, Class<EX> throwable, Callable<?> testRoutine) {
        return assertThrows(message, throwable, null, testRoutine);
    }

    protected static <EX extends Throwable> EX assertThrows(Class<EX> throwable, String msgMatchesRegex, Callable<?> testRoutine) {
        return assertThrows("", throwable, msgMatchesRegex, testRoutine);
    }

    protected static <EX extends Throwable> EX assertThrows(String message, Class<EX> throwable, String msgMatchesRegex, Callable<?> testRoutine) {
        if (message.length() > 0) {
            message += " ";
        }
        try {
            testRoutine.call();
        } catch (Throwable t) {
            if (!throwable.isAssignableFrom(t.getClass())) {
                fail(message + "expected exception of type '" + throwable.getName() + "' but instead a exception of type '" + t.getClass().getName()
                        + "' was thrown.");
            }

            if (msgMatchesRegex != null && !t.getMessage().matches(msgMatchesRegex)) {
                fail(message + "the error message «" + t.getMessage() + "» was expected to match «" + msgMatchesRegex + "».");
            }

            return throwable.cast(t);
        }
        fail(message + "expected exception of type '" + throwable.getName() + "'.");

        // never reaches here
        return null;
    }

    protected void assertByteArrayEquals(String message, byte[] expected, byte[] actual) {
        assertEquals(message + " - array lenght", expected.length, actual.length);
        for (int i = 0, s = expected.length; i < s; i++) {
            assertEquals(message + " - element at " + i, expected[i], actual[i]);
        }
    }

    /**
     * Asserts the most recent history of connection attempts from the global data in UnreliableSocketFactory.
     * 
     * @param expectedConnectionsHistory
     *            The list of expected events. Use UnreliableSocketFactory.getHostConnectedStatus(String), UnreliableSocketFactory.getHostFailedStatus(String)
     *            and UnreliableSocketFactory.getHostUnknownStatus(String) to build proper syntax for host+status identification.
     */
    protected static void assertConnectionsHistory(String... expectedConnectionsHistory) {
        List<String> actualConnectionsHistory = UnreliableSocketFactory.getHostsFromLastConnections(expectedConnectionsHistory.length);

        int i = 1;
        String delimiter = "";
        StringBuilder expectedHist = new StringBuilder("");
        for (String hostInfo : expectedConnectionsHistory) {
            expectedHist.append(delimiter).append(i++).append(hostInfo);
            delimiter = " ~ ";
        }

        i = 1;
        delimiter = "";
        StringBuilder actualHist = new StringBuilder("");
        for (String hostInfo : actualConnectionsHistory) {
            actualHist.append(delimiter).append(i++).append(hostInfo);
            delimiter = " ~ ";
        }

        assertEquals("Connections history", expectedHist.toString(), actualHist.toString());
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
    protected static final long currentTimeMillis() {
        try {
            Method mNanoTime = System.class.getDeclaredMethod("nanoTime", (Class[]) null);
            return ((Long) mNanoTime.invoke(null, (Object[]) null)).longValue() / 1000000;
        } catch (Exception ex) {
            return System.currentTimeMillis();
        }
    }

    protected Connection getFailoverConnection() throws SQLException {
        return getFailoverConnection(null);
    }

    protected Connection getFailoverConnection(Properties props) throws SQLException {
        return DriverManager.getConnection(getMasterSlaveUrl(), getHostFreePropertiesFromTestsuiteUrl(props));
    }

    protected Connection getMasterSlaveReplicationConnection() throws SQLException {
        return getMasterSlaveReplicationConnection(null);
    }

    protected Connection getMasterSlaveReplicationConnection(Properties props) throws SQLException {
        return new ReplicationDriver().connect(getMasterSlaveUrl(), getHostFreePropertiesFromTestsuiteUrl(props));
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

    protected Connection getLoadBalancedConnection(int customHostLocation, String customHost, Properties props) throws SQLException {
        Properties parsedProps = new NonRegisteringDriver().parseURL(dbUrl, null);

        String defaultHost = parsedProps.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);

        if (!NonRegisteringDriver.isHostPropertiesList(defaultHost)) {
            String port = parsedProps.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, "3306");
            defaultHost = defaultHost + ":" + port;
        }
        removeHostRelatedProps(parsedProps);

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
        if (downedHosts == null) {
            downedHosts = new HashSet<String>();
        }

        NonRegisteringDriver driver = new NonRegisteringDriver();

        props = getHostFreePropertiesFromTestsuiteUrl(props);
        props.setProperty("socketFactory", "testsuite.UnreliableSocketFactory");

        Properties parsedProps = driver.parseURL(BaseTestCase.dbUrl, props);
        String db = parsedProps.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
        String port = parsedProps.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);
        String host = getPortFreeHostname(parsedProps, driver);

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
        props = getHostFreePropertiesFromTestsuiteUrl(props);
        props.setProperty("socketFactory", "testsuite.UnreliableSocketFactory");

        Properties parsed = d.parseURL(BaseTestCase.dbUrl, props);
        String db = parsed.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
        String port = parsed.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);
        String host = getPortFreeHostname(parsed, d);

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
