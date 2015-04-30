/*
  Copyright (c) 2014, 2015, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.regression.jdbc4;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Callable;

import com.mysql.jdbc.Messages;
import com.mysql.jdbc.Util;

import javax.sql.PooledConnection;

import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;
import com.mysql.jdbc.jdbc2.optional.JDBC4MysqlPooledConnection;
import com.mysql.jdbc.jdbc2.optional.JDBC4MysqlXAConnection;
import com.mysql.jdbc.jdbc2.optional.JDBC4SuspendableXAConnection;

import testsuite.BaseTestCase;
import testsuite.regression.ConnectionRegressionTest.Bug72712StatementInterceptor;
import testsuite.regression.ConnectionRegressionTest.Bug75168LoadBalanceExceptionChecker;

public class ConnectionRegressionTest extends BaseTestCase {
    /**
     * Creates a new ConnectionRegressionTest.
     * 
     * @param name
     *            the name of the test
     */
    public ConnectionRegressionTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(ConnectionRegressionTest.class);
    }

    /**
     * Tests fix for Bug#75168 - loadBalanceExceptionChecker interface cannot work using JDBC4/JDK7
     * 
     * Bug observed only with JDBC4 classes. This test is duplicated in testsuite.regression.ConnectionRegressionTest#testBug75168().
     * The two nested static classes, Bug75168LoadBalanceExceptionChecker and Bug75168StatementInterceptor are shared between the two tests.
     * 
     * @throws Exception
     */
    public void testBug75168() throws Exception {
        final Properties props = new Properties();
        props.setProperty("loadBalanceExceptionChecker", Bug75168LoadBalanceExceptionChecker.class.getName());
        props.setProperty("statementInterceptors", testsuite.regression.ConnectionRegressionTest.Bug75168StatementInterceptor.class.getName());

        Connection connTest = getLoadBalancedConnection(2, null, props); // get a load balancing connection with two default servers
        for (int i = 0; i < 3; i++) {
            Statement stmtTest = null;
            try {
                stmtTest = connTest.createStatement();
                stmtTest.execute("SELECT * FROM nonexistent_table");
                fail("'Table doesn't exist' exception was expected.");
            } catch (SQLException e) {
                assertTrue("'Table doesn't exist' exception was expected.", e.getMessage().endsWith("nonexistent_table' doesn't exist"));
            } finally {
                if (stmtTest != null) {
                    stmtTest.close();
                }
            }
        }
        connTest.close();

        boolean stop = false;
        do {
            connTest = getLoadBalancedConnection(2, null, props); // get a load balancing connection with two default servers
            for (int i = 0; i < 3; i++) {
                PreparedStatement pstmtTest = null;
                try {
                    pstmtTest = connTest.prepareStatement("SELECT * FROM nonexistent_table");
                    pstmtTest.execute();
                    fail("'Table doesn't exist' exception was expected.");
                } catch (SQLException e) {
                    assertTrue("'Table doesn't exist' exception was expected.", e.getMessage().endsWith("nonexistent_table' doesn't exist"));
                } finally {
                    if (pstmtTest != null) {
                        pstmtTest.close();
                    }
                }
            }
            connTest.close();

            // do it again with server prepared statements
            props.setProperty("useServerPrepStmts", "true");
        } while (stop = !stop);
    }

    /**
     * Tests fix for BUG#20685022 - SSL CONNECTION TO MYSQL 5.7.6 COMMUNITY SERVER FAILS
     * 
     * This test is duplicated in testuite.regression.ConnectionRegressionTest.testBug20685022().
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug20685022() throws Exception {
        final boolean sslCipherSuitesReqFor576 = Util.getJVMVersion() < 8 && versionMeetsMinimum(5, 7, 6) && isCommunityEdition();
        final Properties props = new Properties();
        final Callable<Void> callableInstance = new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        };

        /*
         * case 1: non verifying server certificate
         */
        props.clear();
        props.setProperty("useSSL", "true");
        props.setProperty("requireSSL", "true");
        props.setProperty("verifyServerCertificate", "false");

        if (sslCipherSuitesReqFor576) {
            assertThrows(SQLException.class, Messages.getString("CommunicationsException.incompatibleSSLCipherSuites"), callableInstance);

            props.setProperty("enabledSSLCipherSuites", SSL_CIPHERS_FOR_576);
        }
        getConnectionWithProps(props);

        /*
         * case 2: verifying server certificate using key store provided by connection properties
         */
        props.clear();
        props.setProperty("useSSL", "true");
        props.setProperty("requireSSL", "true");
        props.setProperty("verifyServerCertificate", "true");
        props.setProperty("trustCertificateKeyStoreUrl", "file:src/testsuite/ssl-test-certs/test-cert-store");
        props.setProperty("trustCertificateKeyStoreType", "JKS");
        props.setProperty("trustCertificateKeyStorePassword", "password");

        if (sslCipherSuitesReqFor576) {
            assertThrows(SQLException.class, Messages.getString("CommunicationsException.incompatibleSSLCipherSuites"), callableInstance);

            props.setProperty("enabledSSLCipherSuites", SSL_CIPHERS_FOR_576);
        }

        getConnectionWithProps(props);

        /*
         * case 3: verifying server certificate using key store provided by system properties
         */
        props.clear();
        props.setProperty("useSSL", "true");
        props.setProperty("requireSSL", "true");
        props.setProperty("verifyServerCertificate", "true");

        String trustStorePath = "src/testsuite/ssl-test-certs/test-cert-store";
        System.setProperty("javax.net.ssl.keyStore", trustStorePath);
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStore", trustStorePath);
        System.setProperty("javax.net.ssl.trustStorePassword", "password");

        if (sslCipherSuitesReqFor576) {
            assertThrows(SQLException.class, Messages.getString("CommunicationsException.incompatibleSSLCipherSuites"), callableInstance);

            props.setProperty("enabledSSLCipherSuites", SSL_CIPHERS_FOR_576);
        }

        getConnectionWithProps(props);
    }

    /**
     * Tests fix for BUG#62452 - NPE thrown in JDBC4MySQLPooledException when statement is closed.
     * 
     * @throws Exception
     */
    public void testBug62452() throws Exception {
        PooledConnection con = null;

        MysqlConnectionPoolDataSource pds = new MysqlConnectionPoolDataSource();
        pds.setUrl(dbUrl);
        con = pds.getPooledConnection();
        assertTrue(con instanceof JDBC4MysqlPooledConnection);
        testBug62452WithConnection(con);
        
        MysqlXADataSource xads = new MysqlXADataSource();
        xads.setUrl(dbUrl);

        xads.setPinGlobalTxToPhysicalConnection(false);
        con = xads.getXAConnection();
        assertTrue(con instanceof JDBC4MysqlXAConnection);
        testBug62452WithConnection(con);

        xads.setPinGlobalTxToPhysicalConnection(true);
        con = xads.getXAConnection();
        assertTrue(con instanceof JDBC4SuspendableXAConnection);
        testBug62452WithConnection(con);

    }

    private void testBug62452WithConnection(PooledConnection con) throws Exception {
        this.pstmt = con.getConnection().prepareStatement("SELECT 1");
        this.rs = this.pstmt.executeQuery();           
        con.close();

        // If PooledConnection is already closed by some reason a NullPointerException was thrown on the next line
        // because the closed connection has nulled out the list that it synchronises on when the closed event is fired.
        this.pstmt.close();
    }
}
