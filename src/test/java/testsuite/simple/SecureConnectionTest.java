/*
 * Copyright (c) 2002, 2023, Oracle and/or its affiliates.
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

package testsuite.simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mysql.cj.MysqlConnection;
import com.mysql.cj.conf.PropertyDefinitions.SslMode;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.JdbcPropertySet;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import com.mysql.cj.protocol.a.NativeServerSession;

import testsuite.BaseTestCase;

/**
 * Tests SSL functionality in the driver.
 */
public class SecureConnectionTest extends BaseTestCase {
    private String sslFreeBaseUrl = "";

    @BeforeEach
    public void setupSecureSessionTest() {
        System.clearProperty("javax.net.ssl.trustStore");
        System.clearProperty("javax.net.ssl.trustStoreType");
        System.clearProperty("javax.net.ssl.trustStorePassword");

        this.sslFreeBaseUrl = dbUrl;
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.sslMode.getKeyName() + "=", PropertyKey.sslMode.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.trustCertificateKeyStoreUrl.getKeyName() + "=",
                PropertyKey.trustCertificateKeyStoreUrl.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.trustCertificateKeyStoreType.getKeyName() + "=",
                PropertyKey.trustCertificateKeyStoreType.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.trustCertificateKeyStorePassword.getKeyName() + "=",
                PropertyKey.trustCertificateKeyStorePassword.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.fallbackToSystemTrustStore.getKeyName() + "=",
                PropertyKey.fallbackToSystemTrustStore.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.clientCertificateKeyStoreUrl.getKeyName() + "=",
                PropertyKey.clientCertificateKeyStoreUrl.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.clientCertificateKeyStoreType.getKeyName() + "=",
                PropertyKey.clientCertificateKeyStoreType.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.clientCertificateKeyStorePassword.getKeyName() + "=",
                PropertyKey.clientCertificateKeyStorePassword.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.fallbackToSystemKeyStore.getKeyName() + "=",
                PropertyKey.fallbackToSystemKeyStore.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.tlsCiphersuites.getKeyName() + "=",
                PropertyKey.tlsCiphersuites.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.tlsVersions.getKeyName() + "=", PropertyKey.tlsVersions.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.fipsCompliantJsse.getKeyName() + "=",
                PropertyKey.fipsCompliantJsse.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.keyManagerFactoryProvider.getKeyName() + "=",
                PropertyKey.keyManagerFactoryProvider.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.trustManagerFactoryProvider.getKeyName() + "=",
                PropertyKey.trustManagerFactoryProvider.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.keyStoreProvider.getKeyName() + "=",
                PropertyKey.keyStoreProvider.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.sslContextProvider.getKeyName() + "=",
                PropertyKey.sslContextProvider.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.xdevapiTlsCiphersuites.getKeyName() + "=",
                PropertyKey.xdevapiTlsCiphersuites.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.xdevapiTlsVersions.getKeyName() + "=",
                PropertyKey.xdevapiTlsVersions.getKeyName() + "VOID=");
        if (!this.sslFreeBaseUrl.contains("?")) {
            this.sslFreeBaseUrl += "?";
        }
    }

    @AfterEach
    public void teardownConnectionTest() {
        System.clearProperty("javax.net.ssl.trustStore");
        System.clearProperty("javax.net.ssl.trustStoreType");
        System.clearProperty("javax.net.ssl.trustStorePassword");
        System.clearProperty("javax.net.ssl.keyStore");
        System.clearProperty("javax.net.ssl.keyStoreType");
        System.clearProperty("javax.net.ssl.keyStorePassword");
    }

    /**
     * Tests SSL Connection
     * 
     * @throws Exception
     */
    @Test
    public void testConnect() throws Exception {
        assumeTrue((((MysqlConnection) this.conn).getSession().getServerSession().getCapabilities().getCapabilityFlags() & NativeServerSession.CLIENT_SSL) != 0,
                "This test requires server with SSL support.");
        assumeTrue(supportsTLSv1_2(((MysqlConnection) this.conn).getSession().getServerSession().getServerVersion()),
                "This test requires server with TLSv1.2+ support.");
        assumeTrue(supportsTestCertificates(this.stmt),
                "This test requires the server configured with SSL certificates from ConnectorJ/src/test/config/ssl-test-certs");

        System.setProperty("javax.net.debug", "all");

        String dbUrlLocal = dbUrl;
        dbUrlLocal = dbUrlLocal.replaceAll("(?i)useSSL", "deletedProp");
        dbUrlLocal = dbUrlLocal.replaceAll("(?i)sslMode", "deletedProp");
        StringBuilder sslUrl = new StringBuilder(dbUrlLocal);
        if (dbUrl.indexOf("?") == -1) {
            sslUrl.append("?");
        } else {
            sslUrl.append("&");
        }
        sslUrl.append("sslMode=REQUIRED");

        getConnectionWithProps(sslUrl.toString(), "");

        System.out.println("<<<<<<<<<<< Look for SSL debug output >>>>>>>>>>>");
    }

    /**
     * Test authentication with a user that requires an SSL connection.
     * 
     * This test requires the CA truststore and the client keystore available in src/test/config/ssl-test-certs.
     * The server needs to be configured with the CA and server certificates from src/test/config/ssl-test-certs.
     * 
     * @throws Exception
     */
    @Test
    public void testUserRequireSSL() throws Exception {
        assumeTrue(versionMeetsMinimum(5, 7, 6), "MySQL 5.7.6+ is required to run this test.");
        assumeTrue(supportsTestCertificates(this.stmt),
                "This test requires the server configured with SSL certificates from ConnectorJ/src/test/config/ssl-test-certs");

        Connection testConn;
        Statement testStmt;

        final String user = "testUserReqSSL";
        final String password = "testUserReqSSL";

        final Properties props = new Properties();
        props.setProperty(PropertyKey.USER.getKeyName(), user);
        props.setProperty(PropertyKey.PASSWORD.getKeyName(), password);

        createUser("'" + user + "'@'%'", "IDENTIFIED BY '" + password + "' REQUIRE SSL");
        this.stmt.execute("GRANT SELECT ON *.* TO '" + user + "'@'%'");

        /*
         * No SSL.
         */
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        assertThrows(SQLException.class, "Access denied for user '" + user + "'@.*", new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        });

        /*
         * SSL: no server certificate validation & no client certificate.
         */
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.REQUIRED.name());
        testConn = getConnectionWithProps(props);
        testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT CURRENT_USER()");
        assertTrue(this.rs.next());
        assertEquals(user, this.rs.getString(1).split("@")[0]);
        testConn.close();

        /*
         * SSL: server certificate validation & no client certificate.
         */
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.VERIFY_CA.name());
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/ca-truststore");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "password");
        testConn = getConnectionWithProps(props);
        testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT CURRENT_USER()");
        assertTrue(this.rs.next());
        assertEquals(user, this.rs.getString(1).split("@")[0]);
        testConn.close();

        /*
         * SSL: server certificate validation & client certificate.
         */
        props.setProperty(PropertyKey.clientCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/client-keystore");
        props.setProperty(PropertyKey.clientCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.clientCertificateKeyStorePassword.getKeyName(), "password");
        testConn = getConnectionWithProps(props);
        testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT CURRENT_USER()");
        assertTrue(this.rs.next());
        assertEquals(user, this.rs.getString(1).split("@")[0]);
        testConn.close();

        /*
         * SSL: no server certificate validation & client certificate.
         */
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.REQUIRED.name());
        props.remove(PropertyKey.trustCertificateKeyStoreUrl.getKeyName());
        props.remove(PropertyKey.trustCertificateKeyStoreType.getKeyName());
        props.remove(PropertyKey.trustCertificateKeyStorePassword.getKeyName());
        testConn = getConnectionWithProps(props);
        testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT CURRENT_USER()");
        assertTrue(this.rs.next());
        assertEquals(user, this.rs.getString(1).split("@")[0]);
        testConn.close();
    }

    /**
     * Test authentication with a user that requires an SSL connection and an authorized client certificate.
     * 
     * This test requires the CA truststore and the client keystore available in src/test/config/ssl-test-certs.
     * The server needs to be configured with the CA and server certificates from src/test/config/ssl-test-certs.
     * 
     * @throws Exception
     */
    @Test
    public void testUserRequireX509() throws Exception {
        assumeTrue(versionMeetsMinimum(5, 7, 6), "MySQL 5.7.6+ is required to run this test.");
        assumeTrue(supportsTestCertificates(this.stmt),
                "This test requires the server configured with SSL certificates from ConnectorJ/src/test/config/ssl-test-certs");

        Connection testConn;
        Statement testStmt;

        final String user = "testUserReqX509";
        final String password = "testUserReqX509";

        final Properties props = new Properties();
        props.setProperty(PropertyKey.USER.getKeyName(), user);
        props.setProperty(PropertyKey.PASSWORD.getKeyName(), password);

        createUser("'" + user + "'@'%'", "IDENTIFIED BY '" + password + "' REQUIRE X509");
        this.stmt.execute("GRANT SELECT ON *.* TO '" + user + "'@'%'");

        /*
         * No SSL.
         */
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        assertThrows(SQLException.class, "Access denied for user '" + user + "'@.*", new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        });

        /*
         * SSL: no server certificate validation & no client certificate.
         */
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.REQUIRED.name());
        assertThrows(SQLException.class, "Access denied for user '" + user + "'@.*", new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        });

        /*
         * SSL: server certificate validation & no client certificate.
         */
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.VERIFY_CA.name());
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/ca-truststore");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "password");
        assertThrows(SQLException.class, "Access denied for user '" + user + "'@.*", new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        });

        /*
         * SSL: server certificate validation & client certificate.
         */
        props.setProperty(PropertyKey.clientCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/client-keystore");
        props.setProperty(PropertyKey.clientCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.clientCertificateKeyStorePassword.getKeyName(), "password");
        testConn = getConnectionWithProps(props);
        testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT CURRENT_USER()");
        assertTrue(this.rs.next());
        assertEquals(user, this.rs.getString(1).split("@")[0]);
        testConn.close();

        /*
         * SSL: no server certificate validation & client certificate.
         */
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.REQUIRED.name());
        props.remove(PropertyKey.trustCertificateKeyStoreUrl.getKeyName());
        props.remove(PropertyKey.trustCertificateKeyStoreType.getKeyName());
        props.remove(PropertyKey.trustCertificateKeyStorePassword.getKeyName());
        testConn = getConnectionWithProps(props);
        testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT CURRENT_USER()");
        assertTrue(this.rs.next());
        assertEquals(user, this.rs.getString(1).split("@")[0]);
        testConn.close();
    }

    /**
     * Tests that given SSL/TLS related connection properties values are processed as expected.
     * 
     * @throws Exception
     */
    @Test
    public void testSslConnectionOptions() throws Exception {
        assumeTrue((((MysqlConnection) this.conn).getSession().getServerSession().getCapabilities().getCapabilityFlags() & NativeServerSession.CLIENT_SSL) != 0,
                "This test requires server with SSL support.");
        assumeTrue(supportsTLSv1_2(((MysqlConnection) this.conn).getSession().getServerSession().getServerVersion()),
                "This test requires server with TLSv1.2+ support.");
        assumeTrue(supportsTestCertificates(this.stmt),
                "This test requires the server configured with SSL certificates from ConnectorJ/src/test/config/ssl-test-certs");

        Connection testConn;
        JdbcPropertySet propSet;

        testConn = getConnectionWithProps(this.sslFreeBaseUrl, "");
        propSet = ((JdbcConnection) testConn).getPropertySet();
        assertEquals(SslMode.PREFERRED, propSet.getProperty(PropertyKey.sslMode).getValue());
        assertNull(propSet.getProperty(PropertyKey.trustCertificateKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.trustCertificateKeyStoreType).getValue());
        assertNull(propSet.getProperty(PropertyKey.trustCertificateKeyStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.fallbackToSystemTrustStore).getValue());
        assertNull(propSet.getProperty(PropertyKey.clientCertificateKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.clientCertificateKeyStoreType).getValue());
        assertNull(propSet.getProperty(PropertyKey.clientCertificateKeyStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.fallbackToSystemKeyStore).getValue());
        testConn.close();

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.toString());
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "trust-cert-keystore-url");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "trust-cert-keystore-type");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "trust-cert-keystore-pwd");
        props.setProperty(PropertyKey.fallbackToSystemTrustStore.getKeyName(), "false");
        props.setProperty(PropertyKey.clientCertificateKeyStoreUrl.getKeyName(), "client-cert-keystore-url");
        props.setProperty(PropertyKey.clientCertificateKeyStoreType.getKeyName(), "client-cert-keystore-type");
        props.setProperty(PropertyKey.clientCertificateKeyStorePassword.getKeyName(), "client-cert-keystore-pwd");
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "false");

        testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        propSet = ((JdbcConnection) testConn).getPropertySet();
        assertEquals(SslMode.DISABLED, propSet.getProperty(PropertyKey.sslMode).getValue());
        assertEquals("trust-cert-keystore-url", propSet.getProperty(PropertyKey.trustCertificateKeyStoreUrl).getValue());
        assertEquals("trust-cert-keystore-type", propSet.getProperty(PropertyKey.trustCertificateKeyStoreType).getValue());
        assertEquals("trust-cert-keystore-pwd", propSet.getProperty(PropertyKey.trustCertificateKeyStorePassword).getValue());
        assertFalse(propSet.getBooleanProperty(PropertyKey.fallbackToSystemTrustStore).getValue());
        assertEquals("client-cert-keystore-url", propSet.getProperty(PropertyKey.clientCertificateKeyStoreUrl).getValue());
        assertEquals("client-cert-keystore-type", propSet.getProperty(PropertyKey.clientCertificateKeyStoreType).getValue());
        assertEquals("client-cert-keystore-pwd", propSet.getProperty(PropertyKey.clientCertificateKeyStorePassword).getValue());
        assertFalse(propSet.getBooleanProperty(PropertyKey.fallbackToSystemKeyStore).getValue());
        testConn.close();

        props.setProperty(PropertyKey.fallbackToSystemTrustStore.getKeyName(), "true");
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "true");

        testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        propSet = ((JdbcConnection) testConn).getPropertySet();
        assertEquals(SslMode.DISABLED, propSet.getProperty(PropertyKey.sslMode).getValue());
        assertEquals("trust-cert-keystore-url", propSet.getProperty(PropertyKey.trustCertificateKeyStoreUrl).getValue());
        assertEquals("trust-cert-keystore-type", propSet.getProperty(PropertyKey.trustCertificateKeyStoreType).getValue());
        assertEquals("trust-cert-keystore-pwd", propSet.getProperty(PropertyKey.trustCertificateKeyStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.fallbackToSystemTrustStore).getValue());
        assertEquals("client-cert-keystore-url", propSet.getProperty(PropertyKey.clientCertificateKeyStoreUrl).getValue());
        assertEquals("client-cert-keystore-type", propSet.getProperty(PropertyKey.clientCertificateKeyStoreType).getValue());
        assertEquals("client-cert-keystore-pwd", propSet.getProperty(PropertyKey.clientCertificateKeyStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.fallbackToSystemKeyStore).getValue());
        testConn.close();
    }

    /**
     * Tests connection property 'testFallbackToSystemTrustStore' behavior.
     * 
     * @throws Exception
     */
    @Test
    public void testFallbackToSystemTrustStore() throws Exception {
        assumeTrue((((MysqlConnection) this.conn).getSession().getServerSession().getCapabilities().getCapabilityFlags() & NativeServerSession.CLIENT_SSL) != 0,
                "This test requires server with SSL support.");
        assumeTrue(supportsTLSv1_2(((MysqlConnection) this.conn).getSession().getServerSession().getServerVersion()),
                "This test requires server with TLSv1.2+ support.");
        assumeTrue(supportsTestCertificates(this.stmt),
                "This test requires the server configured with SSL certificates from ConnectorJ/src/test/config/ssl-test-certs");

        Connection testConn;

        /*
         * Valid system-wide TrustStore.
         */
        System.setProperty("javax.net.ssl.trustStore", "file:src/test/config/ssl-test-certs/ca-truststore");
        System.setProperty("javax.net.ssl.trustStoreType", "JKS");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");

        // No connection-local TrustStore.
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, "sslMode=REQUIRED");
        assertSecureConnection(testConn);
        testConn.close();
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, "sslMode=VERIFY_CA");
        assertSecureConnection(testConn);
        testConn.close();
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, "sslMode=VERIFY_CA,fallbackToSystemTrustStore=true");
        assertSecureConnection(testConn);
        testConn.close();
        assertThrows(SQLNonTransientConnectionException.class,
                () -> getConnectionWithProps(this.sslFreeBaseUrl, "sslMode=VERIFY_CA,fallbackToSystemTrustStore=false"));

        // Invalid connection-local TrustStore:
        testConn = getConnectionWithProps(this.sslFreeBaseUrl,
                "sslMode=REQUIRED,trustCertificateKeyStoreUrl=file:src/test/config/ssl-test-certs/ca-truststore-ext,"
                        + "trustCertificateKeyStoreType=JKS,trustCertificateKeyStorePassword=password");
        assertSecureConnection(testConn);
        testConn.close();
        assertThrows(CommunicationsException.class,
                () -> getConnectionWithProps(this.sslFreeBaseUrl,
                        "sslMode=VERIFY_CA,trustCertificateKeyStoreUrl=file:src/test/config/ssl-test-certs/ca-truststore-ext,"
                                + "trustCertificateKeyStoreType=JKS,trustCertificateKeyStorePassword=password"));
        assertThrows(CommunicationsException.class,
                () -> getConnectionWithProps(this.sslFreeBaseUrl,
                        "sslMode=VERIFY_CA,fallbackToSystemTrustStore=true,trustCertificateKeyStoreUrl=file:src/test/config/ssl-test-certs/ca-truststore-ext,"
                                + "trustCertificateKeyStoreType=JKS,trustCertificateKeyStorePassword=password"));
        assertThrows(CommunicationsException.class,
                () -> getConnectionWithProps(this.sslFreeBaseUrl,
                        "sslMode=VERIFY_CA,fallbackToSystemTrustStore=false,trustCertificateKeyStoreUrl=file:src/test/config/ssl-test-certs/ca-truststore-ext,"
                                + "trustCertificateKeyStoreType=JKS,trustCertificateKeyStorePassword=password"));

        /*
         * Invalid system-wide TrustStore.
         */
        System.setProperty("javax.net.ssl.trustStore", "file:src/test/config/ssl-test-certs/ca-truststore-ext");
        System.setProperty("javax.net.ssl.trustStoreType", "JKS");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");

        // No connection-local TrustStore.
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, "sslMode=REQUIRED");
        assertSecureConnection(testConn);
        testConn.close();
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, "sslMode=VERIFY_CA"));
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, "sslMode=VERIFY_CA,fallbackToSystemTrustStore=true"));
        assertThrows(SQLNonTransientConnectionException.class,
                () -> getConnectionWithProps(this.sslFreeBaseUrl, "sslMode=VERIFY_CA,fallbackToSystemTrustStore=false"));

        // Valid connection-local TrustStore:
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, "sslMode=REQUIRED,trustCertificateKeyStoreUrl=file:src/test/config/ssl-test-certs/ca-truststore,"
                + "trustCertificateKeyStoreType=JKS,trustCertificateKeyStorePassword=password");
        assertSecureConnection(testConn);
        testConn.close();
        testConn = getConnectionWithProps(this.sslFreeBaseUrl,
                "sslMode=VERIFY_CA,trustCertificateKeyStoreUrl=file:src/test/config/ssl-test-certs/ca-truststore,"
                        + "trustCertificateKeyStoreType=JKS,trustCertificateKeyStorePassword=password");
        assertSecureConnection(testConn);
        testConn.close();
        testConn = getConnectionWithProps(this.sslFreeBaseUrl,
                "sslMode=VERIFY_CA,fallbackToSystemTrustStore=true,trustCertificateKeyStoreUrl=file:src/test/config/ssl-test-certs/ca-truststore,"
                        + "trustCertificateKeyStoreType=JKS,trustCertificateKeyStorePassword=password");
        assertSecureConnection(testConn);
        testConn.close();
        testConn = getConnectionWithProps(this.sslFreeBaseUrl,
                "sslMode=VERIFY_CA,fallbackToSystemTrustStore=false,trustCertificateKeyStoreUrl=file:src/test/config/ssl-test-certs/ca-truststore,"
                        + "trustCertificateKeyStoreType=JKS,trustCertificateKeyStorePassword=password");
        assertSecureConnection(testConn);
        testConn.close();
    }

    /**
     * Tests connection property 'testFallbackToSystemKeyStore' behavior.
     * 
     * @throws Exception
     */
    @Test
    public void testFallbackToSystemKeyStore() throws Exception {
        assumeTrue(versionMeetsMinimum(5, 7, 6), "MySQL 5.7.6+ is required to run this test.");
        assumeTrue(supportsTestCertificates(this.stmt),
                "This test requires the server configured with SSL certificates from ConnectorJ/src/test/config/ssl-test-certs");

        final String user = "testFbToSysKS";
        createUser(user, "IDENTIFIED BY 'password' REQUIRE X509");
        this.stmt.execute("GRANT ALL ON *.* TO '" + user + "'@'%'");

        final Properties props = new Properties();
        props.setProperty(PropertyKey.USER.getKeyName(), user);
        props.setProperty(PropertyKey.PASSWORD.getKeyName(), "password");
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.REQUIRED.toString());

        Connection testConn;

        /*
         * Valid system-wide KeyStore.
         */
        System.setProperty("javax.net.ssl.keyStore", "file:src/test/config/ssl-test-certs/client-keystore");
        System.setProperty("javax.net.ssl.keyStoreType", "JKS");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");

        // No connection-local KeyStore.
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        assertSecureConnection(testConn, user);
        testConn.close();
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "true");
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        assertSecureConnection(testConn, user);
        testConn.close();
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "false");
        assertThrows(SQLException.class, "Access denied for user '" + user + "'@.*", () -> getConnectionWithProps(this.sslFreeBaseUrl, props));

        props.remove(PropertyKey.fallbackToSystemKeyStore.getKeyName());

        // Invalid connection-local KeyStore:
        props.setProperty(PropertyKey.clientCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/client-keystore-ext");
        props.setProperty(PropertyKey.clientCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.clientCertificateKeyStorePassword.getKeyName(), "password");
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "true");
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "false");
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));

        props.remove(PropertyKey.clientCertificateKeyStoreUrl.getKeyName());
        props.remove(PropertyKey.clientCertificateKeyStoreType.getKeyName());
        props.remove(PropertyKey.clientCertificateKeyStorePassword.getKeyName());
        props.remove(PropertyKey.fallbackToSystemKeyStore.getKeyName());

        /*
         * Invalid system-wide KeyStore.
         */
        System.setProperty("javax.net.ssl.keyStore", "file:src/test/config/ssl-test-certs/client-keystore-ext");
        System.setProperty("javax.net.ssl.keyStoreType", "JKS");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");

        // No connection-local KeyStore.
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "true");
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "false");
        assertThrows(SQLException.class, "Access denied for user '" + user + "'@.*", () -> getConnectionWithProps(this.sslFreeBaseUrl, props));

        props.remove(PropertyKey.fallbackToSystemKeyStore.getKeyName());

        // Valid connection-local KeyStore:
        props.setProperty(PropertyKey.clientCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/client-keystore");
        props.setProperty(PropertyKey.clientCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.clientCertificateKeyStorePassword.getKeyName(), "password");
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        assertSecureConnection(testConn, user);
        testConn.close();
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "true");
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        assertSecureConnection(testConn, user);
        testConn.close();
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "false");
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        assertSecureConnection(testConn, user);
        testConn.close();
    }

    /**
     * Tests WL#14835, Align TLS option checking across connectors
     * 
     * @throws Exception
     */
    @Test
    public void testTlsConflictingOptions() throws Exception {
        assumeTrue((((MysqlConnection) this.conn).getSession().getServerSession().getCapabilities().getCapabilityFlags() & NativeServerSession.CLIENT_SSL) != 0,
                "This test requires server with SSL support.");

        Connection testConn;
        String options;

        // FR.1a.1 Create a JDBC connection to a MySQL server with the options sslMode=DISABLED and serverRSAPublicKeyFile=(path_to_valid_key).
        //         Assess that the connection is established successfully and not using encryption.
        options = String.format("%s=DISABLED,%s=src/test/config/ssl-test-certs/mykey.pub", PropertyKey.sslMode.getKeyName(),
                PropertyKey.serverRSAPublicKeyFile.getKeyName());
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, options);
        assertNonSecureConnection(testConn);
        testConn.close();

        // FR.1a.2 Create a JDBC connection to a MySQL server with the options sslMode=DISABLED and allowPublicKeyRetrieval=true.
        //         Assess that the connection is established successfully and not using encryption.
        options = String.format("%s=DISABLED,%s=true", PropertyKey.sslMode.getKeyName(), PropertyKey.allowPublicKeyRetrieval.getKeyName());
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, options);
        assertNonSecureConnection(testConn);
        testConn.close();

        // FR.1a.3 Create a JDBC connection to a MySQL server with the options sslMode=DISABLED and trustCertificateKeyStoreUrl=foo.
        //         Assess that the connection is established successfully and not using encryption.
        options = String.format("%s=DISABLED,%s=foo", PropertyKey.sslMode.getKeyName(), PropertyKey.trustCertificateKeyStoreUrl.getKeyName());
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, options);
        assertNonSecureConnection(testConn);
        testConn.close();

        // FR.1a.4 Create a JDBC connection to a MySQL server with the options sslMode=DISABLED and trustCertificateKeyStoreType=foo.
        //         Assess that the connection is established successfully and not using encryption.
        options = String.format("%s=DISABLED,%s=foo", PropertyKey.sslMode.getKeyName(), PropertyKey.trustCertificateKeyStoreType.getKeyName());
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, options);
        assertNonSecureConnection(testConn);
        testConn.close();

        // FR.1a.5 Create a JDBC connection to a MySQL server with the options sslMode=DISABLED and trustCertificateKeyStorePassword=foo.
        //         Assess that the connection is established successfully and not using encryption.
        options = String.format("%s=DISABLED,%s=foo", PropertyKey.sslMode.getKeyName(), PropertyKey.trustCertificateKeyStorePassword.getKeyName());
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, options);
        assertNonSecureConnection(testConn);
        testConn.close();

        // FR.1a.6 Create a JDBC connection to a MySQL server with the options sslMode=DISABLED and fallbackToSystemTrustStore=false.
        //         Assess that the connection is established successfully and not using encryption.
        options = String.format("%s=DISABLED,%s=false", PropertyKey.sslMode.getKeyName(), PropertyKey.fallbackToSystemTrustStore.getKeyName());
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, options);
        assertNonSecureConnection(testConn);
        testConn.close();

        // FR.1a.7 Create a JDBC connection to a MySQL server with the options sslMode=DISABLED and clientCertificateKeyStoreUrl=foo.
        //         Assess that the connection is established successfully and not using encryption.
        options = String.format("%s=DISABLED,%s=foo", PropertyKey.sslMode.getKeyName(), PropertyKey.clientCertificateKeyStoreUrl.getKeyName());
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, options);
        assertNonSecureConnection(testConn);
        testConn.close();

        // FR.1a.8 Create a JDBC connection to a MySQL server with the options sslMode=DISABLED and clientCertificateKeyStoreType=foo.
        //         Assess that the connection is established successfully and not using encryption.
        options = String.format("%s=DISABLED,%s=foo", PropertyKey.sslMode.getKeyName(), PropertyKey.clientCertificateKeyStoreType.getKeyName());
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, options);
        assertNonSecureConnection(testConn);
        testConn.close();

        // FR.1a.9 Create a JDBC connection to a MySQL server with the options sslMode=DISABLED and clientCertificateKeyStorePassword=foo.
        //         Assess that the connection is established successfully and not using encryption.
        options = String.format("%s=DISABLED,%s=foo", PropertyKey.sslMode.getKeyName(), PropertyKey.clientCertificateKeyStorePassword.getKeyName());
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, options);
        assertNonSecureConnection(testConn);
        testConn.close();

        // FR.1a.10 Create a JDBC connection to a MySQL server with the options sslMode=DISABLED and fallbackToSystemKeyStore=false.
        //          Assess that the connection is established successfully and not using encryption.
        options = String.format("%s=DISABLED,%s=false", PropertyKey.sslMode.getKeyName(), PropertyKey.fallbackToSystemKeyStore.getKeyName());
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, options);
        assertNonSecureConnection(testConn);
        testConn.close();

        // FR.1a.11 Create a JDBC connection to a MySQL server with the options sslMode=DISABLED and tlsCiphersuites=foo.
        //          Assess that the connection is established successfully and not using encryption.
        options = String.format("%s=DISABLED,%s=foo", PropertyKey.sslMode.getKeyName(), PropertyKey.tlsCiphersuites.getKeyName());
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, options);
        assertNonSecureConnection(testConn);
        testConn.close();

        // FR.1a.12 Create a JDBC connection to a MySQL server with the options sslMode=DISABLED and tlsVersions=foo.
        //          Assess that the connection is established successfully and not using encryption.
        options = String.format("%s=DISABLED,%s=foo", PropertyKey.sslMode.getKeyName(), PropertyKey.tlsVersions.getKeyName());
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, options);
        assertNonSecureConnection(testConn);
        testConn.close();

        // FR.2.1 Create a JDBC connection to a MySQL server with the options sslMode=DISABLED and sslMode=REQUIRED by this order.
        //        Assess that the connection is established successfully. It is acceptable that the connection is established over a secure or non secure
        //        channel, but it is usually expected that the next test delivers the opposite result.
        options = String.format("&%s=DISABLED&%s=REQUIRED", PropertyKey.sslMode.getKeyName(), PropertyKey.sslMode.getKeyName());
        testConn = getConnectionWithProps(this.sslFreeBaseUrl + options, "");
        assertTrue(((MysqlConnection) testConn).getURL().contains(options));
        assertSecureConnection(testConn);
        testConn.close();

        // FR.2.2 Create a JDBC connection to a MySQL server with the options sslMode=REQUIRED and sslMode=DISABLED by this order.
        //        Assess that the connection is established successfully. It is acceptable that the connection is established over a secure or non secure
        //        channel, but it is usually expected that the previous test delivers the opposite result.
        options = String.format("&%s=REQUIRED&%s=DISABLED", PropertyKey.sslMode.getKeyName(), PropertyKey.sslMode.getKeyName());
        testConn = getConnectionWithProps(this.sslFreeBaseUrl + options, "");
        assertTrue(((MysqlConnection) testConn).getURL().contains(options));
        assertNonSecureConnection(testConn);
        testConn.close();
    }

    /**
     * Tests connection property 'fipsCompliantJsse' behavior.
     * 
     * @throws Exception
     */
    @Test
    public void testFipsCompliantJsse() throws Exception {
        assumeTrue((((MysqlConnection) this.conn).getSession().getServerSession().getCapabilities().getCapabilityFlags() & NativeServerSession.CLIENT_SSL) != 0,
                "This test requires server with SSL support.");

        Connection testConn;
        Properties props = new Properties();

        /*
         * A. 'fipsCompliantJsse=true' & 'sslMode=DISABLED': creates non-secure connection.
         */
        props.setProperty(PropertyKey.fipsCompliantJsse.getKeyName(), "true");
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        assertNonSecureConnection(testConn);
        testConn.close();
        props.clear();

        /*
         * B. 'fipsCompliantJsse=true' & 'sslMode=PREFERRED'.
         */
        props.setProperty(PropertyKey.fipsCompliantJsse.getKeyName(), "true");
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.PREFERRED.name());
        // B1. 'fallbackToSystemTrustStore=true' fails due to failed certificate validation.
        props.setProperty(PropertyKey.fallbackToSystemTrustStore.getKeyName(), "true");
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
        // B2. 'fallbackToSystemTrustStore=false' fails due to missing trust store.
        props.setProperty(PropertyKey.fallbackToSystemTrustStore.getKeyName(), "false");
        assertThrows(SQLNonTransientConnectionException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
        // B3. B2 & trust store: creates secure connection.
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/ca-truststore");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "password");
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        assertSecureConnection(testConn);
        testConn.close();
        props.clear();

        /*
         * C. 'fipsCompliantJsse=true' & 'sslMode=REQUIRED'.
         */
        props.setProperty(PropertyKey.fipsCompliantJsse.getKeyName(), "true");
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.REQUIRED.name());
        // B1. 'fallbackToSystemTrustStore=true' fails due to failed certificate validation.
        props.setProperty(PropertyKey.fallbackToSystemTrustStore.getKeyName(), "true");
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
        // B2. 'fallbackToSystemTrustStore=false' fails due to missing trust store.
        props.setProperty(PropertyKey.fallbackToSystemTrustStore.getKeyName(), "false");
        assertThrows(SQLNonTransientConnectionException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
        // B3. B2 & trust store: creates secure connection.
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/ca-truststore");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "password");
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        assertSecureConnection(testConn);
        testConn.close();
        props.clear();

        /*
         * D. 'fipsCompliantJsse=true' & 'sslMode=VERIFY_CA'.
         */
        props.setProperty(PropertyKey.fipsCompliantJsse.getKeyName(), "true");
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.VERIFY_CA.name());
        // B1. 'fallbackToSystemTrustStore=true' fails due to failed certificate validation.
        props.setProperty(PropertyKey.fallbackToSystemTrustStore.getKeyName(), "true");
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
        // B2. 'fallbackToSystemTrustStore=false' fails due to missing trust store.
        props.setProperty(PropertyKey.fallbackToSystemTrustStore.getKeyName(), "false");
        assertThrows(SQLNonTransientConnectionException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
        // B3. B2 & trust store: creates secure connection.
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/ca-truststore");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "password");
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        assertSecureConnection(testConn);
        testConn.close();
        props.clear();

        /*
         * D. 'fipsCompliantJsse=true' & 'sslMode=VERIFY_IDENTITY'.
         */
        props.setProperty(PropertyKey.fipsCompliantJsse.getKeyName(), "true");
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.VERIFY_IDENTITY.name());
        // B1. 'fallbackToSystemTrustStore=true' fails due to failed certificate validation.
        props.setProperty(PropertyKey.fallbackToSystemTrustStore.getKeyName(), "true");
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
        // B2. 'fallbackToSystemTrustStore=false' fails due to missing trust store.
        props.setProperty(PropertyKey.fallbackToSystemTrustStore.getKeyName(), "false");
        assertThrows(SQLNonTransientConnectionException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
        // B3. B2 & trust store: fails due to missing trust store hostname validation.
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/ca-truststore");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "password");
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
        props.clear();
    }

    /**
     * Tests connection property 'keyManagerFactoryProvider' behavior.
     * 
     * @throws Exception
     */
    @Test
    public void testKeyManagerFactoryProvider() throws Exception {
        assumeTrue((((MysqlConnection) this.conn).getSession().getServerSession().getCapabilities().getCapabilityFlags() & NativeServerSession.CLIENT_SSL) != 0,
                "This test requires server with SSL support.");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.REQUIRED.name());
        props.setProperty(PropertyKey.keyManagerFactoryProvider.getKeyName(), "SunJSSE");
        Connection testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        assertSecureConnection(testConn);
        testConn.close();

        props.setProperty(PropertyKey.keyManagerFactoryProvider.getKeyName(), "FooBar");
        assertThrows(SQLNonTransientConnectionException.class,
                "Specified TrustManager or KeyManager Provider is invalid\\. Ensure it is property registered\\.",
                () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
    }

    /**
     * Tests connection property 'trustManagerFactoryProvider' behavior.
     * 
     * @throws Exception
     */
    @Test
    public void testTrustManagerFactoryProvider() throws Exception {
        assumeTrue((((MysqlConnection) this.conn).getSession().getServerSession().getCapabilities().getCapabilityFlags() & NativeServerSession.CLIENT_SSL) != 0,
                "This test requires server with SSL support.");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.REQUIRED.name());
        props.setProperty(PropertyKey.trustManagerFactoryProvider.getKeyName(), "SunJSSE");
        Connection testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        assertSecureConnection(testConn);
        testConn.close();

        props.setProperty(PropertyKey.trustManagerFactoryProvider.getKeyName(), "FooBar");
        assertThrows(SQLNonTransientConnectionException.class,
                "Specified TrustManager or KeyManager Provider is invalid\\. Ensure it is property registered\\.",
                () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
    }

    /**
     * Tests connection property 'keyStoreProvider' behavior.
     * 
     * @throws Exception
     */
    @Test
    public void testKeyStoreProvider() throws Exception {
        assumeTrue((((MysqlConnection) this.conn).getSession().getServerSession().getCapabilities().getCapabilityFlags() & NativeServerSession.CLIENT_SSL) != 0,
                "This test requires server with SSL support.");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.REQUIRED.name());
        props.setProperty(PropertyKey.clientCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/client-keystore");
        props.setProperty(PropertyKey.clientCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.clientCertificateKeyStorePassword.getKeyName(), "password");
        props.setProperty(PropertyKey.keyStoreProvider.getKeyName(), "SUN");
        Connection testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        assertSecureConnection(testConn);
        testConn.close();

        props.setProperty(PropertyKey.keyStoreProvider.getKeyName(), "FooBar");
        assertThrows(SQLNonTransientConnectionException.class, "Specified KeyStore Provider is invalid\\. Ensure it is property registered\\.",
                () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
    }

    /**
     * Tests connection property 'sslContextProvider' behavior.
     * 
     * @throws Exception
     */
    @Test
    public void testSslContextProvider() throws Exception {
        assumeTrue((((MysqlConnection) this.conn).getSession().getServerSession().getCapabilities().getCapabilityFlags() & NativeServerSession.CLIENT_SSL) != 0,
                "This test requires server with SSL support.");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.REQUIRED.name());
        props.setProperty(PropertyKey.sslContextProvider.getKeyName(), "SunJSSE");
        Connection testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        assertSecureConnection(testConn);
        testConn.close();

        props.setProperty(PropertyKey.sslContextProvider.getKeyName(), "FooBar");
        assertThrows(SQLNonTransientConnectionException.class, "Specified SSLContext Provider is invalid\\. Ensure it is property registered\\.",
                () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
    }
}
