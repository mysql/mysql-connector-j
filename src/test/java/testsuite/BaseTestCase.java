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

package testsuite;

import static com.mysql.cj.util.StringUtils.isNullOrEmpty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.NonRegisteringDriver;
import com.mysql.cj.jdbc.ha.ReplicationConnection;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;

/**
 * Base class for all test cases. Creates connections, statements, etc. and closes them.
 */
public abstract class BaseTestCase {

    /**
     * JDBC URL, initialized from com.mysql.cj.testsuite.url system property, or defaults to jdbc:mysql:///test and its connection URL.
     */
    public static String dbUrl = "jdbc:mysql:///test";
    public static String timeZoneFreeDbUrl = "jdbc:mysql:///test";
    protected static ConnectionUrl mainConnectionUrl = null;
    protected boolean isOpenSSL = false;

    /** Instance counter */
    private static int instanceCount = 1;

    /** Connection to server, initialized in setUp() Cleaned up in tearDown(). */
    protected Connection conn = null;

    /** Server version `this.conn' is connected to. */
    protected ServerVersion serverVersion;

    /** list of schema objects to be dropped in tearDown */
    private List<String[]> createdObjects;

    /** The driver to use */
    protected String dbClass = "com.mysql.cj.jdbc.Driver";

    /** My instance number */
    private int myInstanceNumber = 0;

    /** Is MySQL running locally? */
    private Boolean mysqlRunningLocally = null;

    /**
     * Default catalog.
     */
    protected String dbName;

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

    private boolean isOnCSFS = true;

    /**
     * Creates a new BaseTestCase object.
     */
    public BaseTestCase() {
        this.myInstanceNumber = instanceCount++;

        String newDbUrl = System.getProperty(PropertyDefinitions.SYSP_testsuite_url);

        if ((newDbUrl != null) && (newDbUrl.trim().length() != 0)) {
            dbUrl = sanitizeDbName(newDbUrl);
        }
        mainConnectionUrl = ConnectionUrl.getConnectionUrlInstance(dbUrl, null);
        this.dbName = mainConnectionUrl.getDatabase();

        timeZoneFreeDbUrl = dbUrl.replaceAll(PropertyKey.connectionTimeZone.getKeyName() + "=", PropertyKey.connectionTimeZone.getKeyName() + "VOID=")
                .replaceAll("serverTimezone=", "serverTimezoneVOID=");
    }

    private String sanitizeDbName(String url) {
        ConnectionUrl parsedUrl = ConnectionUrl.getConnectionUrlInstance(url, null);
        if (StringUtils.isNullOrEmpty(parsedUrl.getDatabase())) {
            List<String> splitUp = StringUtils.split(url, "\\?", true);
            StringBuilder value = new StringBuilder();
            for (int i = 0; i < splitUp.size(); i++) {
                value.append(splitUp.get(i));
                if (i == 0) {
                    if (!splitUp.get(i).endsWith("/")) {
                        value.append("/");
                    }
                    value.append("cjtest_8_0?");
                }
            }
            url = value.toString();
        }
        return url;
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
        createSchemaObject(st, "TABLE", tableName, columnsAndOtherStuff + " ENGINE = " + engine);
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
            if (!objectType.equalsIgnoreCase("USER") || ((JdbcConnection) st.getConnection()).getSession().versionMeetsMinimum(5, 7, 8)) {
                st.executeUpdate("DROP " + objectType + " IF EXISTS " + objectName);
            } else {
                st.executeUpdate("DROP " + objectType + " " + objectName);
            }
            st.executeUpdate("flush privileges");
        }
    }

    public Connection getConnectionWithProps(String propsList) throws SQLException {
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
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        return DriverManager.getConnection(dbUrl, props);
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
        return getPropertiesFromUrl(mainConnectionUrl);
    }

    protected Properties getPropertiesFromUrl(ConnectionUrl url) throws SQLException {
        return url.getMainHost().exposeAsProperties();
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
        props.remove(PropertyKey.HOST.getKeyName());
        props.remove(PropertyKey.PORT.getKeyName());
    }

    /**
     * Some tests build connections strings for internal usage but, in order for them to work, they may require some connection properties set in the main test
     * suite URL. For example 'connectionTimeZone' is one of those properties.
     * 
     * @param props
     *            the Properties object where to add the missing connection properties
     * @return
     *         the modified Properties objects or a new one if <code>props</code> is <code>null</code>
     */
    protected Properties appendRequiredProperties(Properties props) {
        if (props == null) {
            props = new Properties();
        }

        // Add 'connectionTimeZone' if set in test suite URL and missing from props.
        String propKey = PropertyKey.connectionTimeZone.getKeyName();
        String origTzValue = null;
        if (!props.containsKey(propKey) && (origTzValue = mainConnectionUrl.getOriginalProperties().get(propKey)) != null) {
            props.setProperty(propKey, origTzValue);
        }

        return props;
    }

    protected String getHostFromTestsuiteUrl() throws SQLException {
        String host = mainConnectionUrl.getMainHost().getHost();
        return host;
    }

    protected int getPortFromTestsuiteUrl() throws SQLException {
        int port = mainConnectionUrl.getMainHost().getPort();
        return port;
    }

    protected String getEncodedHostFromTestsuiteUrl() throws SQLException {
        return TestUtils.encodePercent(getHostFromTestsuiteUrl());
    }

    protected String getEncodedHostPortPairFromTestsuiteUrl() throws SQLException {
        String hostPortPair = mainConnectionUrl.getMainHost().getHostPortPair();
        hostPortPair = TestUtils.encodePercent(hostPortPair);
        return hostPortPair;
    }

    protected String getNoDbUrl(String url) throws SQLException {
        Properties props = getPropertiesFromUrl(ConnectionUrl.getConnectionUrlInstance(url, null));
        final String host = props.getProperty(PropertyKey.HOST.getKeyName(), "localhost");
        final String port = props.getProperty(PropertyKey.PORT.getKeyName(), "3306");
        props.remove(PropertyKey.DBNAME.getKeyName());
        removeHostRelatedProps(props);

        final StringBuilder urlBuilder = new StringBuilder("jdbc:mysql://").append(host).append(":").append(port).append("/?");

        Enumeration<Object> keyEnum = props.keys();
        while (keyEnum.hasMoreElements()) {
            String key = (String) keyEnum.nextElement();
            urlBuilder.append(key);
            urlBuilder.append("=");
            urlBuilder.append(props.get(key));
            if (keyEnum.hasMoreElements()) {
                urlBuilder.append("&");
            }
        }
        return urlBuilder.toString();
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

    protected boolean isServerRunningOnWindows() throws SQLException {
        return (getMysqlVariable("datadir").indexOf('\\') != -1);
    }

    public void logDebug(String message) {
        if (System.getProperty(PropertyDefinitions.SYSP_testsuite_noDebugOutput) == null) {
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
        return isSysPropDefined(PropertyDefinitions.SYSP_testsuite_runLongTests);
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
    protected boolean isSysPropDefined(String propName) {
        String prop = System.getProperty(propName);
        return (prop != null) && (prop.length() > 0);
    }

    /**
     * Creates resources used by all tests.
     * 
     * @param testInfo
     * 
     * @throws Exception
     *             if an error occurs.
     */
    @BeforeEach
    public void setUpBase(TestInfo testInfo) throws Exception {
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("Running test " + testInfo.getTestClass().get().getName() + "#" + testInfo.getDisplayName());
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        Class.forName(this.dbClass);
        this.createdObjects = new ArrayList<>();

        Properties props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false"); // testsuite is built upon non-SSL default connection
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.createDatabaseIfNotExist.getKeyName(), "true");
        if (StringUtils.isNullOrEmpty(mainConnectionUrl.getDatabase())) {
            props.setProperty(PropertyKey.DBNAME.getKeyName(), this.dbName);
        }
        this.conn = DriverManager.getConnection(dbUrl, props);

        this.serverVersion = ((JdbcConnection) this.conn).getServerVersion();

        this.stmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        try {
            if (dbUrl.indexOf("mysql") != -1) {
                this.rs = this.stmt.executeQuery("SELECT VERSION()");
                this.rs.next();
                logDebug("Connected to " + this.rs.getString(1));
                this.rs.close();
                this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'auto_generate_certs'");
                if (this.rs.next()) {
                    this.isOpenSSL = true;
                }

                // ensure max_connections value is enough to run tests
                this.rs.close();
                this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'max_connections'");
                this.rs.next();
                int maxConnections = this.rs.getInt(2);

                this.rs = this.stmt.executeQuery("show status like 'threads_connected'");
                this.rs.next();
                int usedConnections = this.rs.getInt(2);

                if (maxConnections - usedConnections < 200) {
                    this.stmt.executeUpdate("SET GLOBAL max_connections=" + (maxConnections + 200));
                }
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
    }

    /**
     * Destroys resources created during the test case.
     * 
     * @throws Exception
     */
    @AfterEach
    public void tearDownBase() throws Exception {
        if (this.rs != null) {
            try {
                this.rs.close();
            } catch (SQLException SQLE) {
            }
        }

        if (System.getProperty(PropertyDefinitions.SYSP_testsuite_retainArtifacts) == null) {
            Statement st = (this.conn == null || this.conn.isClosed() ? getNewConnection() : this.conn).createStatement();

            for (int i = 0; i < this.createdObjects.size(); i++) {
                String[] objectInfo = this.createdObjects.get(i);

                try {
                    dropSchemaObject(st, objectInfo[0], objectInfo[1]);
                } catch (SQLException SQLE) {
                }
            }
            st.close();
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
     * @param subminor
     *            the subminor version to meet
     * 
     * @return boolean if the major/minor is met
     * 
     * @throws SQLException
     *             if an error occurs.
     */
    public boolean versionMeetsMinimum(int major, int minor, int subminor) throws SQLException {
        return (((JdbcConnection) this.conn).getSession().versionMeetsMinimum(major, minor, subminor));
    }

    /**
     * Checks whether the server we're connected to is a MySQL Community edition
     * 
     * @return true if connected to a community/gpl server
     */
    protected boolean isCommunityEdition() {
        return Util.isCommunityEdition(((JdbcConnection) this.conn).getServerVersion().toString());
    }

    /**
     * Checks whether the server we're connected to is an MySQL Enterprise edition
     * 
     * @return true if connected to an enterprise/commercial server
     */
    protected boolean isEnterpriseEdition() {
        return Util.isEnterpriseEdition(((JdbcConnection) this.conn).getServerVersion().toString());
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
        String vmVendor = System.getProperty(PropertyDefinitions.SYSP_java_vm_vendor);

        return (vmVendor != null && vmVendor.toUpperCase(Locale.US).startsWith("BEA"));
    }

    protected boolean isMysqlRunningLocally() {
        if (this.mysqlRunningLocally != null) {
            return this.mysqlRunningLocally;
        }
        try {
            String clientHostname = InetAddress.getLocalHost().getHostName();

            this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'hostname'");
            this.rs.next();
            String serverHostname = this.rs.getString(2);

            this.mysqlRunningLocally = clientHostname.equalsIgnoreCase(serverHostname);
        } catch (UnknownHostException | SQLException e) {
            this.mysqlRunningLocally = false;
        }
        return this.mysqlRunningLocally;
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
        assertEquals(len, rset.getRow(), "Result set length");
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
                    assertNull(testObj, "Expected null, see last row: \n" + rsAsString.toString());
                } else {
                    assertNotNull(testObj, "Expected non-null, see last row: \n" + rsAsString.toString());
                }

                if (controlObj instanceof Float) {
                    assertEquals(((Float) controlObj).floatValue(), ((Float) testObj).floatValue(), 0.1,
                            "Float comparison failed, see last row: \n" + rsAsString.toString());
                } else if (controlObj instanceof Double) {
                    assertEquals(((Double) controlObj).doubleValue(), ((Double) testObj).doubleValue(), 0.1,
                            "Double comparison failed, see last row: \n" + rsAsString.toString());
                } else {
                    assertEquals(controlObj, testObj, "Value comparison failed, see last row: \n" + rsAsString.toString());
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

        assertTrue(howMuchMore == 0, "Found " + howMuchMore + " extra rows in result set to be compared: ");
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
            assertTrue(throwable.isAssignableFrom(t.getClass()), message + "expected exception of type '" + throwable.getName()
                    + "' but instead an exception of type '" + t.getClass().getName() + "' was thrown.");
            assertFalse(msgMatchesRegex != null && !t.getMessage().matches(msgMatchesRegex),
                    message + "the error message «" + t.getMessage() + "» was expected to match «" + msgMatchesRegex + "».");
            return throwable.cast(t);
        }
        fail(message + "expected exception of type '" + throwable.getName() + "'.");

        // never reaches here
        return null;
    }

    protected void assertByteArrayEquals(String message, byte[] expected, byte[] actual) {
        assertEquals(expected.length, actual.length, message + " - array lenght");
        for (int i = 0, s = expected.length; i < s; i++) {
            assertEquals(expected[i], actual[i], message + " - element at " + i);
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

        assertEquals(expectedHist.toString(), actualHist.toString(), "Connections history");
    }

    protected static void assertSecureConnection(Connection conn) throws Exception {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SHOW STATUS LIKE 'Ssl_version'")) {
            assertTrue(rs.next());
            assertNotEquals("", rs.getString(1));
        }
    }

    protected static void assertSecureConnection(Connection conn, String user) throws Exception {
        assertSecureConnection(conn);
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT CURRENT_USER()")) {
            assertTrue(rs.next());
            assertEquals(user, rs.getString(1).split("@")[0]);
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
     * 
     * @return current time in milliseconds
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
        return DriverManager.getConnection(getSourceReplicaUrl(), getHostFreePropertiesFromTestsuiteUrl(props));
    }

    protected Connection getSourceReplicaReplicationConnection() throws SQLException {
        return getSourceReplicaReplicationConnection(null);
    }

    protected Connection getSourceReplicaReplicationConnection(Properties props) throws SQLException {
        String replicationUrl = getSourceReplicaUrl(ConnectionUrl.Type.REPLICATION_CONNECTION.getScheme());
        Connection replConn = new NonRegisteringDriver().connect(replicationUrl, getHostFreePropertiesFromTestsuiteUrl(props));
        return replConn;
    }

    protected String getSourceReplicaUrl() throws SQLException {
        return getSourceReplicaUrl(ConnectionUrl.Type.FAILOVER_CONNECTION.getScheme());
    }

    protected String getSourceReplicaUrl(String protocol) throws SQLException {
        HostInfo hostInfo = mainConnectionUrl.getMainHost();
        String hostPortPair = TestUtils.encodePercent(hostInfo.getHostPortPair());
        return String.format("%s//%s,%s/", protocol, hostPortPair, hostPortPair);
    }

    protected Connection getLoadBalancedConnection(int customHostLocation, String customHost, Properties props) throws SQLException {
        if (customHostLocation > 3) {
            throw new IllegalArgumentException();
        }
        Properties urlProps = getHostFreePropertiesFromTestsuiteUrl();
        if (props != null) {
            urlProps.putAll(props);
        }

        /*
         * 1: customHost,defaultHost
         * 2: defaultHost,customHost,defaultHost
         * 3: defaultHost,customHost
         */
        StringJoiner hostsString = new StringJoiner(",");
        if (customHostLocation > 1) {
            hostsString.add(getEncodedHostPortPairFromTestsuiteUrl());
        }
        if (!isNullOrEmpty(customHost)) {
            hostsString.add(customHost);
        }
        if (customHostLocation < 3) {
            hostsString.add(getEncodedHostPortPairFromTestsuiteUrl());
        }

        Connection lbConn = DriverManager.getConnection(ConnectionUrl.Type.LOADBALANCE_CONNECTION.getScheme() + "//" + hostsString, urlProps);
        return lbConn;
    }

    protected Connection getLoadBalancedConnection() throws SQLException {
        return getLoadBalancedConnection(1, "", null);
    }

    protected Connection getLoadBalancedConnection(Properties props) throws SQLException {
        return getLoadBalancedConnection(1, "", props);
    }

    protected String getPort(Properties props) throws SQLException {
        String port;
        if (props == null || (port = props.getProperty(PropertyKey.PORT.getKeyName())) == null) {
            return String.valueOf(mainConnectionUrl.getMainHost().getPort());
        }
        return port;
    }

    protected String getPortFreeHostname(Properties props) throws SQLException {
        String host;
        if (props == null || (host = props.getProperty(PropertyKey.HOST.getKeyName())) == null) {
            return mainConnectionUrl.getMainHost().getHost();
        }
        return ConnectionUrlParser.parseHostPortPair(host).left;
    }

    protected Connection getUnreliableMultiHostConnection(String haMode, String[] hostNames, Properties props, Set<String> downedHosts) throws Exception {
        if (downedHosts == null) {
            downedHosts = new HashSet<>();
        }

        props = getHostFreePropertiesFromTestsuiteUrl(props);
        props.setProperty(PropertyKey.socketFactory.getKeyName(), "testsuite.UnreliableSocketFactory");

        HostInfo defaultHost = mainConnectionUrl.getMainHost();
        String db = defaultHost.getDatabase();
        String port = String.valueOf(defaultHost.getPort());
        String host = defaultHost.getHost();

        UnreliableSocketFactory.flushAllStaticData();

        StringBuilder hostString = new StringBuilder();
        String delimiter = "";
        for (String hostName : hostNames) {
            UnreliableSocketFactory.mapHost(hostName, host);
            hostString.append(delimiter);
            delimiter = ",";
            hostString.append(hostName + ":" + port);

            if (downedHosts.contains(hostName)) {
                UnreliableSocketFactory.downHost(hostName);
            }
        }

        if (haMode == null) {
            haMode = "";
        } else if (haMode.length() > 0) {
            haMode += ":";
        }

        return getConnectionWithProps(ConnectionUrl.Type.FAILOVER_CONNECTION.getScheme() + haMode + "//" + hostString.toString() + "/" + db, props);
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

        public String getHostPortPair() {
            return this.hostName + ":" + this.port;
        }
    }

    protected ReplicationConnection getUnreliableReplicationConnection(Set<MockConnectionConfiguration> configs, Properties props) throws Exception {
        props = getHostFreePropertiesFromTestsuiteUrl(props);
        props.setProperty(PropertyKey.socketFactory.getKeyName(), "testsuite.UnreliableSocketFactory");

        HostInfo defaultHost = mainConnectionUrl.getMainHost();
        String db = defaultHost.getDatabase();
        String port = String.valueOf(defaultHost.getPort());
        String host = defaultHost.getHost();

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

        return (ReplicationConnection) getConnectionWithProps(ConnectionUrl.Type.REPLICATION_CONNECTION.getScheme() + "//" + hostString.toString() + "/" + db,
                props);
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

    protected boolean supportsTimeZoneNames(Statement st) throws Exception {
        ResultSet rs1 = st.executeQuery("SELECT COUNT(*) FROM mysql.time_zone_name");
        return rs1.next() && rs1.getInt(1) > 0;
    }

    protected boolean supportsLoadLocalInfile(Statement st) throws Exception {
        ResultSet rs1 = st.executeQuery("SHOW VARIABLES LIKE 'local_infile'");
        return rs1.next() && "ON".equalsIgnoreCase(rs1.getString(2));
    }

    protected boolean supportsTestCertificates(Statement st) throws Exception {
        ResultSet rs1 = st.executeQuery("SHOW VARIABLES LIKE 'ssl_ca'");
        return rs1.next() && rs1.getString(2).contains("ssl-test-certs");
    }

    protected boolean supportsTestSha256PasswordKeys(Statement st) throws Exception {
        ResultSet rs1 = st.executeQuery("SHOW VARIABLES LIKE 'sha256_password_public_key_path'");
        return rs1.next() && rs1.getString(2).contains("ssl-test-certs");
    }

    protected boolean supportsTestCachingSha2PasswordKeys(Statement st) throws Exception {
        ResultSet rs1 = st.executeQuery("SHOW VARIABLES LIKE 'caching_sha2_password_private_key_path'");
        return rs1.next() && rs1.getString(2).contains("ssl-test-certs");
    }
}
