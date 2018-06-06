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

package testsuite;

import static com.mysql.cj.util.StringUtils.isNullOrEmpty;

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
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.Callable;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyDefinitions.PropertyKey;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.NonRegisteringDriver;
import com.mysql.cj.jdbc.ha.ReplicationConnection;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;

import junit.framework.TestCase;

/**
 * Base class for all test cases. Creates connections, statements, etc. and closes them.
 */
public abstract class BaseTestCase extends TestCase {

    // next variables disable some tests
    protected boolean DISABLED_testBug15121 = true; // TODO needs to be fixed on server
    protected boolean DISABLED_testBug7033 = true; // TODO disabled for unknown reason
    protected boolean DISABLED_testBug2654 = true; // TODO check if it's still a server-level bug
    protected boolean DISABLED_testBug5136 = true; // TODO disabled for unknown reason
    protected boolean DISABLED_testBug65503 = true; // TODO disabled for unknown reason
    protected boolean DISABLED_testContention = true; // TODO disabled for unknown reason
    protected boolean DISABLED_testBug3620new = true; // TODO this test is working in c/J 5.1 but fails here; disabled for later analysis
    protected boolean DISABLED_testBug5874 = true; // TODO this test is working in c/J 5.1 but fails here; disabled for later analysis

    /**
     * JDBC URL, initialized from com.mysql.cj.testsuite.url system property, or defaults to jdbc:mysql:///test and its connection URL.
     */
    public static String dbUrl = "jdbc:mysql:///test";
    protected static ConnectionUrl mainConnectionUrl = null;

    /**
     * JDBC URL, initialized from com.mysql.cj.testsuite.url.openssl system property and its connection URL
     */
    protected static String sha256Url = null;
    protected static ConnectionUrl sha256ConnectionUrl = null;

    /** Instance counter */
    private static int instanceCount = 1;

    /** Connection to server, initialized in setUp() Cleaned up in tearDown(). */
    protected Connection conn = null;

    protected Connection sha256Conn = null;

    /** Server version `this.conn' is connected to. */
    protected ServerVersion serverVersion;

    /** list of schema objects to be dropped in tearDown */
    private List<String[]> createdObjects;

    /** The driver to use */
    protected String dbClass = "com.mysql.cj.jdbc.Driver";

    /** My instance number */
    private int myInstanceNumber = 0;

    /**
     * Default catalog.
     */
    protected final String dbName;

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

        String newDbUrl = System.getProperty(PropertyDefinitions.SYSP_testsuite_url);

        if ((newDbUrl != null) && (newDbUrl.trim().length() != 0)) {
            dbUrl = newDbUrl;
        }
        mainConnectionUrl = ConnectionUrl.getConnectionUrlInstance(dbUrl, null);
        this.dbName = mainConnectionUrl.getDatabase();

        String defaultSha256Url = System.getProperty(PropertyDefinitions.SYSP_testsuite_url_openssl);

        if ((defaultSha256Url != null) && (defaultSha256Url.trim().length() != 0)) {
            sha256Url = defaultSha256Url;
            sha256ConnectionUrl = ConnectionUrl.getConnectionUrlInstance(sha256Url, null);
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

    protected Connection getAdminConnection() throws SQLException {
        return getAdminConnectionWithProps(new Properties());
    }

    protected Connection getAdminConnectionWithProps(Properties props) throws SQLException {
        String adminUrl = System.getProperty(PropertyDefinitions.SYSP_testsuite_url_admin);

        if (adminUrl != null) {
            return DriverManager.getConnection(adminUrl, props);
        }
        return null;
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
        return DriverManager.getConnection(dbUrl);
    }

    protected Connection getNewSha256Connection() throws SQLException {
        if (sha256Url != null) {
            Properties props = new Properties();
            props.setProperty(PropertyDefinitions.PNAME_allowPublicKeyRetrieval, "true");
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

    protected boolean isAdminConnectionConfigured() {
        return System.getProperty(PropertyDefinitions.SYSP_testsuite_url_admin) != null;
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
        return runTestIfSysPropDefined(PropertyDefinitions.SYSP_testsuite_runLongTests);
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
        return !runTestIfSysPropDefined(PropertyDefinitions.SYSP_testsuite_disable_multihost_tests);
    }

    /**
     * Creates resources used by all tests.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    @Override
    public void setUp() throws Exception {
        System.out.println("Running test " + getClass().getName() + "#" + getName());
        System.out.println("################################################################################");
        Class.forName(this.dbClass).newInstance();
        this.createdObjects = new ArrayList<>();

        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useSSL, "false"); // testsuite is built upon non-SSL default connection
        props.setProperty(PropertyDefinitions.PNAME_allowPublicKeyRetrieval, "true");
        this.conn = DriverManager.getConnection(dbUrl, props);

        this.sha256Conn = sha256Url == null ? null : DriverManager.getConnection(sha256Url, props);

        this.serverVersion = ((JdbcConnection) this.conn).getServerVersion();

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

        if (System.getProperty(PropertyDefinitions.SYSP_testsuite_retainArtifacts) == null) {
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
        return (((JdbcConnection) this.conn).getSession().versionMeetsMinimum(major, minor, subminor));
    }

    /**
     * Checks whether the server we're connected to is a MySQL Community edition
     */
    protected boolean isCommunityEdition() {
        return Util.isCommunityEdition(((JdbcConnection) this.conn).getServerVersion().toString());
    }

    /**
     * Checks whether the server we're connected to is an MySQL Enterprise edition
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
        String replicationUrl = getMasterSlaveUrl(ConnectionUrl.Type.REPLICATION_CONNECTION.getScheme());
        Connection replConn = new NonRegisteringDriver().connect(replicationUrl, getHostFreePropertiesFromTestsuiteUrl(props));
        return replConn;
    }

    protected String getMasterSlaveUrl() throws SQLException {
        return getMasterSlaveUrl(ConnectionUrl.Type.FAILOVER_CONNECTION.getScheme());
    }

    protected String getMasterSlaveUrl(String protocol) throws SQLException {
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
        props.setProperty(PropertyDefinitions.PNAME_socketFactory, "testsuite.UnreliableSocketFactory");

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
        props.setProperty(PropertyDefinitions.PNAME_socketFactory, "testsuite.UnreliableSocketFactory");

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
}
