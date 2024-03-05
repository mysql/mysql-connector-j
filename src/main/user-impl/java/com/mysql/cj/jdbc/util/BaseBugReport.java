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

package com.mysql.cj.jdbc.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.mysql.cj.jdbc.Driver;

/**
 * Base class to help file bug reports for Connector/J.
 *
 * <p>
 * MySQL AB, 2008 Sun Microsystems, 2009 Oracle Corporation <b>really</b>
 * appreciates repeatable testcases when reporting bugs, so we're giving you this class to make that job a bit easier (and standarized).
 *
 * <p>
 * To create a testcase, create a class that inherits from this class (com.mysql.cj.jdbc.util.BaseBugReport), and override the methods 'setUp', 'tearDown' and
 * 'runTest'.
 *
 * <p>
 * In the 'setUp' method, create code that creates your tables, and populates them with any data needed to demonstrate the bug.
 *
 * <p>
 * In the 'runTest' method, create code that demonstrates the bug using the tables and data you created in the 'setUp' method.
 *
 * <p>
 * In the 'tearDown' method, drop any tables you created in the 'setUp' method.
 *
 * <p>
 * In any of the above three methods, you should use one of the variants of the 'getConnection' method to create a JDBC connection to MySQL, which will use the
 * default JDBC URL of 'jdbc:mysql:///test'.
 *
 * <p>
 * If you need to use a JDBC URL that is different than 'jdbc:mysql:///test', then override the method 'getUrl' as well.
 *
 * <p>
 * Use the 'assertTrue' methods to create conditions that must be met in your testcase demonstrating the behavior you are expecting (vs. the behavior you are
 * observing, which is why you are most likely filing a bug report).
 *
 * <p>
 * Finally, create a 'main' method that creates a new instance of your testcase, and calls the 'run' method:
 *
 * <pre>
 *
 * public static void main(String[] args) throws Exception {
 *     new MyBugReport().run();
 * }
 * </pre>
 *
 * <p>
 * When filing a potential bug with MySQL Connector/J at http://bugs.mysql.com/ or on the bugs mailing list, please include the code that you have just written
 * using this class.
 */
public abstract class BaseBugReport {

    private Connection conn;

    private Driver driver;

    /**
     * Constructor for this BugReport, sets up JDBC driver used to create
     * connections.
     */
    public BaseBugReport() {
        try {
            this.driver = new Driver();
        } catch (SQLException ex) {
            throw new RuntimeException(ex.toString());
        }
    }

    /**
     * Override this method with code that sets up the testcase for
     * demonstrating your bug (creating tables, populating data, etc).
     *
     * @throws Exception
     *             if an error occurs during the 'setUp' phase.
     */
    public abstract void setUp() throws Exception;

    /**
     * Override this method with code that cleans up anything created in the
     * setUp() method.
     *
     * @throws Exception
     *             if an error occurs during the 'tearDown' phase.
     */
    public abstract void tearDown() throws Exception;

    /**
     * Override this method with code that demonstrates the bug. This method
     * will be called after setUp(), and before tearDown().
     *
     * @throws Exception
     *             if an error occurs during your test run.
     */
    public abstract void runTest() throws Exception;

    /**
     * Runs the testcase by calling the setUp(), runTest() and tearDown()
     * methods. The tearDown() method is run regardless of any errors occuring
     * in the other methods.
     *
     * @throws Exception
     *             if an error occurs in any of the aforementioned methods.
     */
    public final void run() throws Exception {
        try {
            setUp();
            runTest();

        } finally {
            tearDown();
        }
    }

    /**
     * Throws an exception with the given message if condition evalutates to
     * 'false'.
     *
     * @param message
     *            the message to use in the exception
     * @param condition
     *            the condition to test for
     * @throws Exception
     *             if !condition
     */
    protected final void assertTrue(String message, boolean condition) throws Exception {
        if (!condition) {
            throw new Exception("Assertion failed: " + message);
        }
    }

    /**
     * Throws an exception if condition evalutates to 'false'.
     *
     * @param condition
     *            the condition to test for
     * @throws Exception
     *             if !condition
     */
    protected final void assertTrue(boolean condition) throws Exception {
        assertTrue("(no message given)", condition);
    }

    /**
     * Provides the JDBC URL to use to demonstrate the bug. The
     * java.sql.Connection that you use to demonstrate this bug will be provided
     * by the getConnection() method using this URL.
     *
     * The default value is 'jdbc:mysql:///test'
     *
     * @return URL
     */
    public String getUrl() {
        return "jdbc:mysql:///test";
    }

    /**
     * Provides a connection to the JDBC URL specified in getUrl().
     *
     * If a connection already exists, that connection is returned. Otherwise a
     * new connection is created.
     *
     * @return a connection to the JDBC URL specified in getUrl().
     *
     * @throws SQLException
     *             if an error is caused while creating the connection.
     */
    public final synchronized Connection getConnection() throws SQLException {
        if (this.conn == null || this.conn.isClosed()) {
            this.conn = getNewConnection();
        }

        return this.conn;
    }

    /**
     * Use this if you need to get a new connection for your bug report (i.e.
     * there's more than one connection involved).
     *
     * @return a new connection to the JDBC URL specified in getUrl().
     *
     * @throws SQLException
     *             if an error is caused while creating the connection.
     */
    public final synchronized Connection getNewConnection() throws SQLException {
        return getConnection(getUrl());
    }

    /**
     * Returns a connection using the given URL.
     *
     * @param url
     *            the JDBC URL to use
     * @return a new java.sql.Connection to the JDBC URL.
     * @throws SQLException
     *             if an error occurs getting the connection.
     */
    public final synchronized Connection getConnection(String url) throws SQLException {
        return getConnection(url, null);
    }

    /**
     * Returns a connection using the given URL and properties.
     *
     * @param url
     *            the JDBC URL to use
     * @param props
     *            the JDBC properties to use
     * @return a new java.sql.Connection to the JDBC URL.
     * @throws SQLException
     *             if an error occurs getting the connection.
     */
    public final synchronized Connection getConnection(String url, Properties props) throws SQLException {
        // Don't follow this example in your own code
        // This is to bypass the java.sql.DriverManager

        return this.driver.connect(url, props);
    }

}
