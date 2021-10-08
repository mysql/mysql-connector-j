/*
 * Copyright (c) 2017, 2021, Oracle and/or its affiliates.
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

package testsuite.x.devapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

import javax.net.ssl.SSLContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.mysql.cj.CoreSession;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyDefinitions.AuthMech;
import com.mysql.cj.conf.PropertyDefinitions.SslMode;
import com.mysql.cj.conf.PropertyDefinitions.XdevapiSslMode;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.SSLParamsException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.x.XAuthenticationProvider;
import com.mysql.cj.protocol.x.XProtocol;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.Client;
import com.mysql.cj.xdevapi.ClientFactory;
import com.mysql.cj.xdevapi.Row;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionImpl;
import com.mysql.cj.xdevapi.SqlResult;

public class SecureSessionTest extends DevApiBaseTestCase {
    final String trustStoreUrl = "file:src/test/config/ssl-test-certs/ca-truststore";
    final String trustStorePath = "src/test/config/ssl-test-certs/ca-truststore";
    final String trustStorePassword = "password";

    final String clientKeyStoreUrl = "file:src/test/config/ssl-test-certs/client-keystore";
    final String clientKeyStorePath = "src/test/config/ssl-test-certs/client-keystore";
    final String clientKeyStorePassword = "password";

    final Properties sslFreeTestProperties = (Properties) this.testProperties.clone();
    String sslFreeBaseUrl = this.baseUrl;

    @BeforeEach
    public void setupSecureSessionTest() {
        assumeTrue(this.isSetForXTests, PropertyDefinitions.SYSP_testsuite_url_mysqlx + " must be set to run this test.");
        if (setupTestSession()) {
            System.clearProperty("javax.net.ssl.trustStore");
            System.clearProperty("javax.net.ssl.trustStoreType");
            System.clearProperty("javax.net.ssl.trustStorePassword");
            System.clearProperty("javax.net.ssl.keyStore");
            System.clearProperty("javax.net.ssl.keyStoreType");
            System.clearProperty("javax.net.ssl.keyStorePassword");

            this.sslFreeTestProperties.remove(PropertyKey.xdevapiSslMode.getKeyName());
            this.sslFreeTestProperties.remove(PropertyKey.xdevapiSslTrustStoreUrl.getKeyName());
            this.sslFreeTestProperties.remove(PropertyKey.xdevapiSslTrustStoreType.getKeyName());
            this.sslFreeTestProperties.remove(PropertyKey.xdevapiSslTrustStorePassword.getKeyName());
            this.sslFreeTestProperties.remove(PropertyKey.xdevapiSslKeyStoreUrl.getKeyName());
            this.sslFreeTestProperties.remove(PropertyKey.xdevapiSslKeyStoreType.getKeyName());
            this.sslFreeTestProperties.remove(PropertyKey.xdevapiSslKeyStorePassword.getKeyName());
            this.sslFreeTestProperties.remove(PropertyKey.xdevapiTlsCiphersuites.getKeyName());
            this.sslFreeTestProperties.remove(PropertyKey.xdevapiTlsVersions.getKeyName());
            // Also remove global security properties.
            this.sslFreeTestProperties.remove(PropertyKey.sslMode.getKeyName());
            this.sslFreeTestProperties.remove(PropertyKey.trustCertificateKeyStoreUrl.getKeyName());
            this.sslFreeTestProperties.remove(PropertyKey.trustCertificateKeyStoreType.getKeyName());
            this.sslFreeTestProperties.remove(PropertyKey.trustCertificateKeyStorePassword.getKeyName());
            this.sslFreeTestProperties.remove(PropertyKey.clientCertificateKeyStoreUrl.getKeyName());
            this.sslFreeTestProperties.remove(PropertyKey.clientCertificateKeyStoreType.getKeyName());
            this.sslFreeTestProperties.remove(PropertyKey.clientCertificateKeyStorePassword.getKeyName());
            this.sslFreeTestProperties.remove(PropertyKey.tlsCiphersuites.getKeyName());
            this.sslFreeTestProperties.remove(PropertyKey.tlsVersions.getKeyName());

            this.sslFreeBaseUrl = this.baseUrl;
            this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.xdevapiSslMode.getKeyName() + "=",
                    PropertyKey.xdevapiSslMode.getKeyName() + "VOID=");
            this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.xdevapiSslTrustStoreUrl.getKeyName() + "=",
                    PropertyKey.xdevapiSslTrustStoreUrl.getKeyName() + "VOID=");
            this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.xdevapiSslTrustStorePassword.getKeyName() + "=",
                    PropertyKey.xdevapiSslTrustStorePassword.getKeyName() + "VOID=");
            this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.xdevapiSslTrustStoreType.getKeyName() + "=",
                    PropertyKey.xdevapiSslTrustStoreType.getKeyName() + "VOID=");
            this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.xdevapiSslKeyStoreUrl.getKeyName() + "=",
                    PropertyKey.xdevapiSslKeyStoreUrl.getKeyName() + "VOID=");
            this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.xdevapiSslKeyStorePassword.getKeyName() + "=",
                    PropertyKey.xdevapiSslKeyStorePassword.getKeyName() + "VOID=");
            this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.xdevapiSslKeyStoreType.getKeyName() + "=",
                    PropertyKey.xdevapiSslKeyStoreType.getKeyName() + "VOID=");
            this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.xdevapiTlsCiphersuites.getKeyName() + "=",
                    PropertyKey.xdevapiTlsCiphersuites.getKeyName() + "VOID=");
            this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.xdevapiTlsVersions.getKeyName() + "=",
                    PropertyKey.xdevapiTlsVersions.getKeyName() + "VOID=");
            // Also remove global security properties.
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
            this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.xdevapiTlsCiphersuites.getKeyName() + "=",
                    PropertyKey.xdevapiTlsCiphersuites.getKeyName() + "VOID=");
            this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.xdevapiTlsVersions.getKeyName() + "=",
                    PropertyKey.xdevapiTlsVersions.getKeyName() + "VOID=");

            if (!this.sslFreeBaseUrl.contains("?")) {
                this.sslFreeBaseUrl += "?";
            }
        }
    }

    @AfterEach
    public void teardownSecureSessionTest() {
        System.clearProperty("javax.net.ssl.trustStore");
        System.clearProperty("javax.net.ssl.trustStoreType");
        System.clearProperty("javax.net.ssl.trustStorePassword");
        System.clearProperty("javax.net.ssl.keyStore");
        System.clearProperty("javax.net.ssl.keyStoreType");
        System.clearProperty("javax.net.ssl.keyStorePassword");
        destroyTestSession();
    }

    /**
     * Tests non-secure {@link Session}s created via URL and properties map.
     */
    @Test
    public void testNonSecureSession() {
        try {
            Session testSession = this.fact.getSession(this.baseUrl);
            testSession.sql("CREATE USER IF NOT EXISTS 'testPlainAuth'@'%' IDENTIFIED WITH mysql_native_password BY 'pwd'").execute();
            testSession.sql("GRANT SELECT ON *.* TO 'testPlainAuth'@'%'").execute();
            testSession.close();

            String userAndSslFreeBaseUrl = this.sslFreeBaseUrl;
            userAndSslFreeBaseUrl = userAndSslFreeBaseUrl.replaceAll(getTestUser() + ":" + getTestPassword() + "@", "");
            userAndSslFreeBaseUrl = userAndSslFreeBaseUrl.replaceAll(PropertyKey.USER.getKeyName() + "=", PropertyKey.USER.getKeyName() + "VOID=");
            userAndSslFreeBaseUrl = userAndSslFreeBaseUrl.replaceAll(PropertyKey.PASSWORD.getKeyName() + "=", PropertyKey.PASSWORD.getKeyName() + "VOID=");

            testSession = this.fact.getSession(userAndSslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.DISABLED)
                    + makeParam(PropertyKey.USER, "testPlainAuth") + makeParam(PropertyKey.PASSWORD, "pwd"));
            assertNonSecureSession(testSession);
            testSession.close();

            Properties props = new Properties(this.sslFreeTestProperties);
            props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), XdevapiSslMode.DISABLED.toString());
            props.setProperty(PropertyKey.USER.getKeyName(), "testPlainAuth");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "pwd");
            testSession = this.fact.getSession(props);
            assertNonSecureSession(testSession);
            testSession.close();

        } catch (Throwable t) {
            throw t;
        } finally {
            Session testSession = this.fact.getSession(this.baseUrl);
            testSession.sql("DROP USER if exists testPlainAuth").execute();
            testSession.close();
        }
    }

    /**
     * Tests secure {@link Session}s created via URL and properties map. This is the default if no ssl-mode is provided.
     */
    @Test
    public void testSecureSessionDefaultAndRequired() {
        Session testSession = this.fact.getSession(this.sslFreeBaseUrl);
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.REQUIRED));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeTestProperties);
        assertSecureSession(testSession);
        testSession.close();

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), XdevapiSslMode.REQUIRED.toString());
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();
    }

    /**
     * Tests secure {@link Session}s created via URL and properties map, with the SSL system properties also defined.
     */
    @Test
    public void testSecureSessionDefaultAndRequiredWithSystemPropsPresent() {
        assumeTrue(supportsTestCertificates(this.session),
                "This test requires the server configured with SSL certificates from ConnectorJ/src/test/config/ssl-test-certs");

        System.setProperty("javax.net.ssl.trustStore", this.trustStorePath);
        System.setProperty("javax.net.ssl.trustStorePassword", this.trustStorePassword);

        Session testSession = this.fact.getSession(this.sslFreeBaseUrl);
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_CA));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeTestProperties);
        assertSecureSession(testSession);
        testSession.close();

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), XdevapiSslMode.REQUIRED.toString());
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();
    }

    /**
     * Tests secure {@link Session}s created via URL and properties map, verifying server certificate.
     */
    @Test
    public void testSecureSessionVerifyServerCertificate() {
        assumeTrue(supportsTestCertificates(this.session),
                "This test requires the server configured with SSL certificates from ConnectorJ/src/test/config/ssl-test-certs");

        Session testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_CA)
                + makeParam(PropertyKey.xdevapiSslTrustStoreUrl, this.trustStoreUrl)
                + makeParam(PropertyKey.xdevapiSslTrustStorePassword, this.trustStorePassword));
        assertSecureSession(testSession);
        testSession.close();

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), XdevapiSslMode.VERIFY_CA.toString());
        props.setProperty(PropertyKey.xdevapiSslTrustStoreUrl.getKeyName(), this.trustStoreUrl);
        props.setProperty(PropertyKey.xdevapiSslTrustStorePassword.getKeyName(), this.trustStorePassword);
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();
    }

    /**
     * Tests secure {@link Session}s created via URL and properties map combined with SSL system properties, verifying server certificate.
     */
    @Test
    public void testSecureSessionVerifyServerCertificateUsingSystemProps() {
        assumeTrue(supportsTestCertificates(this.session),
                "This test requires the server configured with SSL certificates from ConnectorJ/src/test/config/ssl-test-certs");

        System.setProperty("javax.net.ssl.trustStore", this.trustStorePath);
        System.setProperty("javax.net.ssl.trustStorePassword", this.trustStorePassword);

        Session testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_CA));
        assertSecureSession(testSession);
        testSession.close();

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), XdevapiSslMode.VERIFY_CA.toString());
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();
    }

    /**
     * Tests secure {@link Session}s created via URL and properties map, verifying server certificate.
     * This test would pass if the server certificate had "CN=<host_name>", with <host_name> equals to the host name in the test URL.
     */
    @Test
    @Disabled("requires a certificate with CN=<host_name> equals to the host name in the test URL")
    public void testSecureSessionVerifyServerCertificateIdentity() {
        Session testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_IDENTITY)
                + makeParam(PropertyKey.xdevapiSslTrustStoreUrl, this.trustStoreUrl)
                + makeParam(PropertyKey.xdevapiSslTrustStorePassword, this.trustStorePassword));
        assertSecureSession(testSession);
        testSession.close();

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), XdevapiSslMode.VERIFY_IDENTITY.toString());
        props.setProperty(PropertyKey.xdevapiSslTrustStoreUrl.getKeyName(), this.trustStoreUrl);
        props.setProperty(PropertyKey.xdevapiSslTrustStorePassword.getKeyName(), this.trustStorePassword);
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();
    }

    /**
     * Tests exception thrown on missing truststore for a secure {@link Session}.
     */
    @Test
    public void testSecureSessionMissingTrustStore() {
        assertThrows(CJCommunicationsException.class, "No truststore provided to verify the Server certificate\\.",
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiFallbackToSystemTrustStore, "false")
                        + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_CA)));

        assertThrows(CJCommunicationsException.class, "No truststore provided to verify the Server certificate\\.",
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiFallbackToSystemTrustStore, "false")
                        + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_IDENTITY)));

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyKey.xdevapiFallbackToSystemTrustStore.getKeyName(), "false");
        props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), XdevapiSslMode.VERIFY_CA.toString());
        assertThrows(CJCommunicationsException.class, "No truststore provided to verify the Server certificate\\.", () -> this.fact.getSession(props));

        props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), XdevapiSslMode.VERIFY_IDENTITY.toString());
        assertThrows(CJCommunicationsException.class, "No truststore provided to verify the Server certificate\\.", () -> this.fact.getSession(props));
    }

    /**
     * Tests exception thrown on verifying server certificate identity failure.
     * The server certificate used in this test has "CN=MySQL Connector/J Server".
     */
    @Test
    public void testSecureSessionVerifyServerCertificateIdentityFailure() {
        // Meaningful error message is deep inside the stack trace.
        assertThrows(CJCommunicationsException.class,
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_IDENTITY)
                        + makeParam(PropertyKey.xdevapiSslTrustStoreUrl, this.trustStoreUrl)
                        + makeParam(PropertyKey.xdevapiSslTrustStorePassword, this.trustStorePassword)));

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), XdevapiSslMode.VERIFY_IDENTITY.toString());
        props.setProperty(PropertyKey.xdevapiSslTrustStoreUrl.getKeyName(), this.trustStoreUrl);
        props.setProperty(PropertyKey.xdevapiSslTrustStorePassword.getKeyName(), this.trustStorePassword);
        // Meaningful error message is deep inside the stack trace.
        assertThrows(CJCommunicationsException.class, () -> this.fact.getSession(props));
    }

    /**
     * Tests exception thrown on incompatible settings for a secure {@link Session}.
     */
    @Test
    public void testSecureSessionIncompatibleSettings() {
        String expectedError = "Incompatible security settings\\. "
                + "The property 'xdevapi.ssl-truststore' requires 'xdevapi.ssl-mode' as 'VERIFY_CA' or 'VERIFY_IDENTITY'\\.";
        assertThrows(CJCommunicationsException.class, expectedError,
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslTrustStoreUrl, this.trustStoreUrl)));

        assertThrows(CJCommunicationsException.class, expectedError, () -> this.fact.getSession(this.sslFreeBaseUrl
                + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.REQUIRED) + makeParam(PropertyKey.xdevapiSslTrustStoreUrl, this.trustStoreUrl)));

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyKey.xdevapiSslTrustStoreUrl.getKeyName(), this.trustStoreUrl);
        assertThrows(CJCommunicationsException.class, expectedError, () -> this.fact.getSession(props));

        props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), XdevapiSslMode.REQUIRED.toString());
        props.setProperty(PropertyKey.xdevapiSslTrustStoreUrl.getKeyName(), this.trustStoreUrl);
        assertThrows(CJCommunicationsException.class, expectedError, () -> this.fact.getSession(props));
    }

    /**
     * Tests that PLAIN, MYSQL41, SHA256_MEMORY, and EXTERNAL authentication mechanisms.
     * 
     * @throws Throwable
     */
    @Test
    public void testAuthMechanisms() throws Throwable {
        try {
            this.session.sql("CREATE USER IF NOT EXISTS 'testAuthMechNative'@'%' IDENTIFIED WITH mysql_native_password BY 'mysqlnative'").execute();
            this.session.sql("GRANT SELECT ON *.* TO 'testAuthMechNative'@'%'").execute();
            this.session.sql("CREATE USER IF NOT EXISTS 'testAuthMechSha256'@'%' IDENTIFIED WITH sha256_password BY 'sha256'").execute();
            this.session.sql("GRANT SELECT ON *.* TO 'testAuthMechSha256'@'%'").execute();
            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3"))) {
                this.session.sql("CREATE USER IF NOT EXISTS 'testAuthMechCachingSha2'@'%' IDENTIFIED WITH caching_sha2_password BY 'cachingsha2'").execute();
                this.session.sql("GRANT SELECT ON *.* TO 'testAuthMechCachingSha2'@'%'").execute();
            }

            final Field sf = SessionImpl.class.getDeclaredField("session");
            sf.setAccessible(true);
            final Field pf = CoreSession.class.getDeclaredField("protocol");
            pf.setAccessible(true);
            final Field mf = XAuthenticationProvider.class.getDeclaredField("authMech");
            mf.setAccessible(true);

            Function<Session, AuthMech> getAuthMech = s -> {
                try {
                    return (AuthMech) mf.get(((XProtocol) pf.get(sf.get(s))).getAuthenticationProvider());
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            };

            /*
             * Access denied (ER_ACCESS_DENIED_ERROR) error message from servers up to version 8.0.11.
             * In MySQL 8.0.12 this message changed due to fix for Bug#27675699 - FAILED AUTHENTICATION AT X PLUGIN ALWAYS RETURNS ER_ACCESS_DENIED_ERROR.
             * This variable may be redefined as needed along the test.
             */
            String accessDeniedErrMsg = "ERROR 1045 \\(HY000\\) Invalid user or password";

            /*
             * Authenticate using (default) TLS first. As per MySQL 8.0.4 X Plugin this is required so that SHA2[56] logins get cached in SHA2_MEMORY.
             */
            Session testSession = null;
            Properties props = new Properties(this.sslFreeTestProperties);

            // With default auth mechanism for secure connections (PLAIN).

            // *** User: mysqlnative; Auth: default.
            props.setProperty(PropertyKey.USER.getKeyName(), "testAuthMechNative");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "mysqlnative");

            testSession = this.fact.getSession(props);
            assertSecureSession(testSession);
            assertEquals(AuthMech.PLAIN, getAuthMech.apply(testSession)); // Connection is secure, passwords are safe & account gets cached.
            assertUser("testAuthMechNative", testSession);
            testSession.close();

            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3"))) {
                // *** User: testAuthMechSha256; Auth: default.
                props.setProperty(PropertyKey.USER.getKeyName(), "testAuthMechSha256");
                props.setProperty(PropertyKey.PASSWORD.getKeyName(), "sha256");

                testSession = this.fact.getSession(props);
                assertSecureSession(testSession);
                assertEquals(AuthMech.PLAIN, getAuthMech.apply(testSession)); // Connection is secure, passwords are safe & account gets cached.
                assertUser("testAuthMechSha256", testSession);
                testSession.close();
            }

            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3"))) {
                // *** User: testAuthMechCachingSha2; Auth: default.
                props.setProperty(PropertyKey.USER.getKeyName(), "testAuthMechCachingSha2");
                props.setProperty(PropertyKey.PASSWORD.getKeyName(), "cachingsha2");

                testSession = this.fact.getSession(props);
                assertSecureSession(testSession);
                assertEquals(AuthMech.PLAIN, getAuthMech.apply(testSession)); // Connection is secure, passwords are safe & account gets cached.
                assertUser("testAuthMechCachingSha2", testSession);
                testSession.close();
            }

            // Forcing an auth mechanism.

            // *** User: testAuthMechNative; Auth: PLAIN.
            props.setProperty(PropertyKey.USER.getKeyName(), "testAuthMechNative");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "mysqlnative");
            props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "PLAIN");

            testSession = this.fact.getSession(props);
            assertSecureSession(testSession);
            assertEquals(AuthMech.PLAIN, getAuthMech.apply(testSession)); // Connection is secure, passwords are safe.
            assertUser("testAuthMechNative", testSession);
            testSession.close();

            // *** User: testAuthMechNative; Auth: MYSQL41.
            props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "MYSQL41");

            testSession = this.fact.getSession(props);
            assertSecureSession(testSession);
            assertEquals(AuthMech.MYSQL41, getAuthMech.apply(testSession)); // Matching auth mechanism.
            assertUser("testAuthMechNative", testSession);
            testSession.close();

            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4"))) { // SHA256_MEMORY support added in MySQL 8.0.4.
                // *** User: testAuthMechNative; Auth: SHA256_MEMORY.
                props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "SHA256_MEMORY");

                testSession = this.fact.getSession(props);
                assertSecureSession(testSession);
                assertEquals(AuthMech.SHA256_MEMORY, getAuthMech.apply(testSession)); // Account is cached by now.
                assertUser("testAuthMechNative", testSession);
                testSession.close();
            }

            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3"))) {
                // *** User: testAuthMechSha256; Auth: PLAIN.
                props.setProperty(PropertyKey.USER.getKeyName(), "testAuthMechSha256");
                props.setProperty(PropertyKey.PASSWORD.getKeyName(), "sha256");
                props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "PLAIN");

                testSession = this.fact.getSession(props);
                assertSecureSession(testSession);
                assertEquals(AuthMech.PLAIN, getAuthMech.apply(testSession)); // Connection is secure, passwords are safe.
                assertUser("testAuthMechSha256", testSession);
                testSession.close();

                // *** User: testAuthMechSha256; Auth: MYSQL41.
                props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "MYSQL41");
                if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.12"))) {
                    accessDeniedErrMsg = "ERROR 1045 \\(HY000\\) Access denied for user 'testAuthMechSha256'@.*";
                }

                assertThrows(XProtocolError.class, accessDeniedErrMsg, () -> this.fact.getSession(props)); // Auth mech mismatch.

                if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4"))) { // SHA256_MEMORY support added in MySQL 8.0.4.
                    // *** User: testAuthMechSha256; Auth: SHA256_MEMORY.
                    props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "SHA256_MEMORY");

                    testSession = this.fact.getSession(props);
                    assertSecureSession(testSession);
                    assertEquals(AuthMech.SHA256_MEMORY, getAuthMech.apply(testSession)); // Account is cached by now.
                    assertUser("testAuthMechSha256", testSession);
                    testSession.close();
                }
            }

            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3"))) {
                // *** User: testAuthMechCachingSha2; Auth: PLAIN.
                props.setProperty(PropertyKey.USER.getKeyName(), "testAuthMechCachingSha2");
                props.setProperty(PropertyKey.PASSWORD.getKeyName(), "cachingsha2");
                props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "PLAIN");

                testSession = this.fact.getSession(props);
                assertSecureSession(testSession);
                assertEquals(AuthMech.PLAIN, getAuthMech.apply(testSession)); // Connection is secure, passwords are safe.
                assertUser("testAuthMechCachingSha2", testSession);
                testSession.close();

                // *** User: testAuthMechCachingSha2; Auth: MYSQL41.
                props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "MYSQL41");
                if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.12"))) {
                    accessDeniedErrMsg = "ERROR 1045 \\(HY000\\) Access denied for user 'testAuthMechCachingSha2'@.*";
                }

                assertThrows(XProtocolError.class, accessDeniedErrMsg, () -> this.fact.getSession(props)); // Auth mech mismatch.

                if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4"))) { // SHA256_MEMORY support added in MySQL 8.0.4.
                    // User: testAuthMechCachingSha2; Auth: SHA256_MEMORY.
                    props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "SHA256_MEMORY");

                    testSession = this.fact.getSession(props);
                    assertSecureSession(testSession);
                    assertEquals(AuthMech.SHA256_MEMORY, getAuthMech.apply(testSession)); // Account is cached by now.
                    assertUser("testAuthMechCachingSha2", testSession);
                    testSession.close();
                }
            }

            // *** User: external; Auth: EXTERNAL.
            props.setProperty(PropertyKey.USER.getKeyName(), "external");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "external");
            props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "EXTERNAL");

            assertThrows(XProtocolError.class, "ERROR 1251 \\(HY000\\) Invalid authentication method EXTERNAL", () -> this.fact.getSession(props));

            props.remove(PropertyKey.xdevapiAuth.getKeyName());

            /*
             * Authenticate using non-secure connections.
             */
            props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), PropertyDefinitions.XdevapiSslMode.DISABLED.toString());

            // With default auth mechanism for non-secure connections (MYSQL41|SHA2_MEMORY).

            // *** User: testAuthMechNative; Auth: default.
            props.setProperty(PropertyKey.USER.getKeyName(), "testAuthMechNative");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "mysqlnative");

            testSession = this.fact.getSession(props);
            assertNonSecureSession(testSession);
            assertEquals(AuthMech.MYSQL41, getAuthMech.apply(testSession)); // Matching auth mechanism.
            assertUser("testAuthMechNative", testSession);
            testSession.close();

            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4"))) { // SHA256_PASSWORD requires secure connections in MySQL 8.0.3 and below.
                // *** User: testAuthMechSha256; Auth: default.
                props.setProperty(PropertyKey.USER.getKeyName(), "testAuthMechSha256");
                props.setProperty(PropertyKey.PASSWORD.getKeyName(), "sha256");

                testSession = this.fact.getSession(props);
                assertNonSecureSession(testSession);
                assertEquals(AuthMech.SHA256_MEMORY, getAuthMech.apply(testSession)); // Account is cached by now.
                assertUser("testAuthMechSha256", testSession);
                testSession.close();
            }

            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4"))) { // CACHING_SHA2_PASSWORD requires secure connections in MySQL 8.0.3 and below.
                // *** User: testAuthMechCachingSha2; Auth: default.
                props.setProperty(PropertyKey.USER.getKeyName(), "testAuthMechCachingSha2");
                props.setProperty(PropertyKey.PASSWORD.getKeyName(), "cachingsha2");

                testSession = this.fact.getSession(props);
                assertNonSecureSession(testSession);
                assertEquals(AuthMech.SHA256_MEMORY, getAuthMech.apply(testSession)); // Account is cached by now.
                assertUser("testAuthMechCachingSha2", testSession);
                testSession.close();
            }

            // Forcing an auth mechanism.

            // *** User: testAuthMechNative; Auth: PLAIN.
            props.setProperty(PropertyKey.USER.getKeyName(), "testAuthMechNative");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "mysqlnative");
            props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "PLAIN");

            assertThrows(XProtocolError.class, "PLAIN authentication is not allowed via unencrypted connection\\.", () -> this.fact.getSession(props));

            // *** User: testAuthMechNative; Auth: MYSQL41.
            props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "MYSQL41");

            testSession = this.fact.getSession(props);
            assertNonSecureSession(testSession);
            assertEquals(AuthMech.MYSQL41, getAuthMech.apply(testSession)); // Matching auth mechanism.
            assertUser("testAuthMechNative", testSession);
            testSession.close();

            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4"))) { // SHA256_MEMORY support added in MySQL 8.0.4.
                // *** User: testAuthMechNative; Auth: SHA256_MEMORY.
                props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "SHA256_MEMORY");

                testSession = this.fact.getSession(props);
                assertNonSecureSession(testSession);
                assertEquals(AuthMech.SHA256_MEMORY, getAuthMech.apply(testSession)); // Account is cached by now.
                assertUser("testAuthMechNative", testSession);
                testSession.close();
            }

            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3"))) {
                // *** User: testAuthMechSha256; Auth: PLAIN.
                props.setProperty(PropertyKey.USER.getKeyName(), "testAuthMechSha256");
                props.setProperty(PropertyKey.PASSWORD.getKeyName(), "sha256");
                props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "PLAIN");

                assertThrows(XProtocolError.class, "PLAIN authentication is not allowed via unencrypted connection\\.", () -> this.fact.getSession(props));

                // *** User: testAuthMechSha256; Auth: MYSQL41.
                props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "MYSQL41");
                if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.12"))) {
                    accessDeniedErrMsg = "ERROR 1045 \\(HY000\\) Access denied for user 'testAuthMechSha256'@.*";
                }

                assertThrows(XProtocolError.class, accessDeniedErrMsg, () -> this.fact.getSession(props)); // Auth mech mismatch.

                if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4"))) { // SHA256_MEMORY support added in MySQL 8.0.4.
                    // *** User: testAuthMechSha256; Auth: SHA256_MEMORY.
                    props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "SHA256_MEMORY");

                    testSession = this.fact.getSession(props);
                    assertNonSecureSession(testSession);
                    assertEquals(AuthMech.SHA256_MEMORY, getAuthMech.apply(testSession)); // Account is cached by now.
                    assertUser("testAuthMechSha256", testSession);
                    testSession.close();
                }
            }

            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3"))) {
                // *** User: testAuthMechCachingSha2; Auth: PLAIN.
                props.setProperty(PropertyKey.USER.getKeyName(), "testAuthMechCachingSha2");
                props.setProperty(PropertyKey.PASSWORD.getKeyName(), "cachingsha2");
                props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "PLAIN");

                assertThrows(XProtocolError.class, "PLAIN authentication is not allowed via unencrypted connection\\.", () -> this.fact.getSession(props));

                // *** User: testAuthMechCachingSha2; Auth: MYSQL41.
                props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "MYSQL41");
                if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.12"))) {
                    accessDeniedErrMsg = "ERROR 1045 \\(HY000\\) Access denied for user 'testAuthMechCachingSha2'@.*";
                }

                assertThrows(XProtocolError.class, accessDeniedErrMsg, () -> this.fact.getSession(props)); // Auth mech mismatch.

                if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4"))) { // SHA256_MEMORY support added in MySQL 8.0.4.
                    // *** User: testAuthMechCachingSha2; Auth: SHA256_MEMORY.
                    props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "SHA256_MEMORY");

                    testSession = this.fact.getSession(props);
                    assertNonSecureSession(testSession);
                    assertEquals(AuthMech.SHA256_MEMORY, getAuthMech.apply(testSession)); // Account is cached by now.
                    assertUser("testAuthMechCachingSha2", testSession);
                    testSession.close();
                }
            }

            // *** User: external; Auth: EXTERNAL.
            props.setProperty(PropertyKey.USER.getKeyName(), "external");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "external");
            props.setProperty(PropertyKey.xdevapiAuth.getKeyName(), "EXTERNAL");

            assertThrows(XProtocolError.class, "ERROR 1251 \\(HY000\\) Invalid authentication method EXTERNAL", () -> this.fact.getSession(props));

            props.remove(PropertyKey.xdevapiAuth.getKeyName());
        } finally {
            this.session.sql("DROP USER IF EXISTS testAuthMechNative").execute();
            this.session.sql("DROP USER IF EXISTS testAuthMechSha256").execute();
            this.session.sql("DROP USER IF EXISTS testAuthMechCachingSha2").execute();
        }
    }

    /**
     * Tests TLSv1.2
     * 
     * @throws Exception
     */
    @Test
    public void testTLSv1_2() throws Exception {
        assumeTrue(supportsTestCertificates(this.session),
                "This test requires the server configured with SSL certificates from ConnectorJ/src/test/config/ssl-test-certs");

        // newer GPL servers, like 8.0.4+, are using OpenSSL and can use RSA encryption, while old ones compiled with yaSSL cannot
        Session sess = this.fact.getSession(this.sslFreeBaseUrl);
        boolean gplWithRSA = allowsRsa(sess);
        String highestCommonTlsVersion = getHighestCommonTlsVersion(sess);
        sess.close();

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), PropertyDefinitions.XdevapiSslMode.VERIFY_CA.toString());
        props.setProperty(PropertyKey.xdevapiSslTrustStoreUrl.getKeyName(), this.trustStoreUrl);
        props.setProperty(PropertyKey.xdevapiSslTrustStorePassword.getKeyName(), this.trustStorePassword);

        // defaults to TLSv1.1
        Session testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        assertTlsVersion(testSession, highestCommonTlsVersion);
        testSession.close();

        // restricted to TLSv1
        props.setProperty(PropertyKey.tlsVersions.getKeyName(), "TLSv1");
        assertThrows(CJCommunicationsException.class, ".+TLS protocols TLSv1 and TLSv1.1 are not supported. Accepted values are TLSv1.2 and TLSv1.3.",
                () -> this.fact.getSession(props));

        // TLSv1.2 should fail with servers compiled with yaSSL
        props.setProperty(PropertyKey.tlsVersions.getKeyName(), "TLSv1.2,TLSv1.1,TLSv1");
        if (gplWithRSA) {
            testSession = this.fact.getSession(props);
            assertSecureSession(testSession);
            assertTlsVersion(testSession, "TLSv1.2");
            testSession.close();
        } else {
            assertThrows(CJCommunicationsException.class, "javax.net.ssl.SSLHandshakeException: Remote host closed connection during handshake",
                    () -> this.fact.getSession(props));
        }
    }

    private boolean allowsRsa(Session sess) {
        boolean allowsRSA = false;
        SqlResult rset = sess.sql("SHOW STATUS LIKE 'Rsa_public_key'").execute();
        if (rset.hasNext()) {
            String value = rset.fetchOne().getString(1);
            allowsRSA = (value != null && value.length() > 0);
        }
        return allowsRSA;
    }

    private void assertUser(String user, Session sess) {
        SqlResult rows = sess.sql("SELECT USER(),CURRENT_USER()").execute();
        Row row = rows.fetchOne();
        assertEquals(user, row.getString(0).split("@")[0]);
        assertEquals(user, row.getString(1).split("@")[0]);
    }

    private void assertTlsVersion(Session sess, String expectedTlsVersion) {
        SqlResult rs = sess.sql("SHOW SESSION STATUS LIKE 'mysqlx_ssl_version'").execute();
        String actual = rs.fetchOne().getString(1);
        assertEquals(expectedTlsVersion, actual);
    }

    private String getHighestCommonTlsVersion(Session sess) throws Exception {
        // Find out which TLS protocol versions are supported by this JVM.
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, null, null);
        List<String> jvmSupportedProtocols = Arrays.asList(sslContext.createSSLEngine().getSupportedProtocols());

        SqlResult rset = sess.sql("SHOW GLOBAL VARIABLES LIKE 'tls_version'").execute();
        String value = rset.fetchOne().getString(1);
        //  this.rs = sslConn.createStatement().executeQuery("SHOW GLOBAL VARIABLES LIKE 'tls_version'");
        //  assertTrue(this.rs.next());

        List<String> serverSupportedProtocols = Arrays.asList(value.trim().split("\\s*,\\s*"));
        String highestCommonTlsVersion = "";
        for (String p : new String[] { "TLSv1.3", "TLSv1.2", "TLSv1.1", "TLSv1" }) {
            if (jvmSupportedProtocols.contains(p) && serverSupportedProtocols.contains(p)) {
                highestCommonTlsVersion = p;
                break;
            }
        }
        System.out.println("Server supports TLS protocols: " + serverSupportedProtocols);
        System.out.println("Highest common TLS protocol: " + highestCommonTlsVersion);

        return highestCommonTlsVersion;

    }

    /**
     * Tests fix for Bug#25494338, ENABLEDSSLCIPHERSUITES PARAMETER NOT WORKING AS EXPECTED WITH X-PLUGIN.
     */
    @Test
    public void testBug25494338() {
        assumeTrue(supportsTestCertificates(this.session),
                "This test requires the server configured with SSL certificates from ConnectorJ/src/test/config/ssl-test-certs");

        Session testSession = null;

        try {
            Properties props = new Properties(this.sslFreeTestProperties);
            testSession = this.fact.getSession(props);

            testSession.sql("CREATE USER 'bug25494338user'@'%' IDENTIFIED WITH mysql_native_password BY 'pwd' REQUIRE CIPHER 'AES128-SHA'").execute();
            testSession.sql("GRANT SELECT ON *.* TO 'bug25494338user'@'%'").execute();

            props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), PropertyDefinitions.XdevapiSslMode.VERIFY_CA.toString());
            props.setProperty(PropertyKey.xdevapiSslTrustStoreUrl.getKeyName(), this.trustStoreUrl);
            props.setProperty(PropertyKey.xdevapiSslTrustStorePassword.getKeyName(), this.trustStorePassword);
            props.setProperty(PropertyKey.clientCertificateKeyStoreUrl.getKeyName(), this.clientKeyStoreUrl);
            props.setProperty(PropertyKey.clientCertificateKeyStorePassword.getKeyName(), this.clientKeyStorePassword);

            // 1. Allow only TLS_DHE_RSA_WITH_AES_128_CBC_SHA cipher
            props.setProperty(PropertyKey.tlsCiphersuites.getKeyName(), "TLS_DHE_RSA_WITH_AES_128_CBC_SHA");
            Session sess = this.fact.getSession(props);
            assertSessionStatusEquals(sess, "mysqlx_ssl_cipher", "DHE-RSA-AES128-SHA");
            sess.close();

            // 2. Allow only TLS_RSA_WITH_AES_128_CBC_SHA cipher
            props.setProperty(PropertyKey.tlsCiphersuites.getKeyName(), "TLS_RSA_WITH_AES_128_CBC_SHA");
            sess = this.fact.getSession(props);
            assertSessionStatusEquals(sess, "mysqlx_ssl_cipher", "AES128-SHA");
            assertSessionStatusEquals(sess, "ssl_cipher", "");
            sess.close();

            // 3. Check connection with required client certificate 
            props.setProperty(PropertyKey.USER.getKeyName(), "bug25494338user");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "pwd");

            sess = this.fact.getSession(props);
            assertSessionStatusEquals(sess, "mysqlx_ssl_cipher", "AES128-SHA");
            assertSessionStatusEquals(sess, "ssl_cipher", "");
            sess.close();

        } catch (Throwable t) {
            throw t;
        } finally {
            if (testSession != null) {
                testSession.sql("DROP USER bug25494338user").execute();
            }
        }
    }

    /**
     * Tests fix for Bug#23597281, GETNODESESSION() CALL WITH SSL PARAMETERS RETURNS CJCOMMUNICATIONSEXCEPTION
     */
    @Test
    public void testBug23597281() {
        assumeTrue(supportsTestCertificates(this.session),
                "This test requires the server configured with SSL certificates from ConnectorJ/src/test/config/ssl-test-certs");

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), PropertyDefinitions.XdevapiSslMode.VERIFY_CA.toString());
        props.setProperty(PropertyKey.xdevapiSslTrustStoreUrl.getKeyName(), this.trustStoreUrl);
        props.setProperty(PropertyKey.xdevapiSslTrustStorePassword.getKeyName(), this.trustStorePassword);

        Session nSession;
        for (int i = 0; i < 100; i++) {
            nSession = this.fact.getSession(props);
            nSession.close();
            nSession = null;
        }
    }

    /**
     * Tests fix for Bug#26227653, WL#10528 DIFF BEHAVIOUR WHEN SYSTEM PROP JAVAX.NET.SSL.TRUSTSTORETYPE IS SET
     * 
     * The actual bug is: if wrong system-wide SSL settings are provided, the session should not fail if 'xdevapi.ssl-mode=REQUIRED'.
     */
    @Test
    public void testBug26227653() {
        System.setProperty("javax.net.ssl.trustStore", "dummy_truststore");
        System.setProperty("javax.net.ssl.trustStorePassword", "some_password");
        System.setProperty("javax.net.ssl.trustStoreType", "wrong_type");

        Session testSession = this.fact.getSession(this.sslFreeBaseUrl);
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, PropertyDefinitions.XdevapiSslMode.REQUIRED));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeTestProperties);
        assertSecureSession(testSession);
        testSession.close();

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), PropertyDefinitions.XdevapiSslMode.REQUIRED.toString());
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();
    }

    /**
     * Tests fix for Bug#27629553, NPE FROM GETSESSION() FOR SSL CONNECTION WHEN NO PASSWORD PASSED.
     */
    @Test
    public void testBug27629553() {
        assumeTrue(supportsTestCertificates(this.session),
                "This test requires the server configured with SSL certificates from ConnectorJ/src/test/config/ssl-test-certs");

        Session testSession = this.fact.getSession(this.baseUrl);
        testSession.sql("CREATE USER IF NOT EXISTS 'testBug27629553'@'%' IDENTIFIED WITH mysql_native_password").execute();
        testSession.sql("GRANT SELECT ON *.* TO 'testBug27629553'@'%'").execute();
        testSession.close();

        Properties props = (Properties) this.sslFreeTestProperties.clone();
        props.setProperty("user", "testBug27629553");
        props.remove("password");
        props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), PropertyDefinitions.XdevapiSslMode.VERIFY_CA.toString());
        props.setProperty(PropertyKey.xdevapiSslTrustStoreUrl.getKeyName(), this.trustStoreUrl);
        props.setProperty(PropertyKey.xdevapiSslTrustStorePassword.getKeyName(), this.trustStorePassword);
        props.setProperty(PropertyKey.clientCertificateKeyStoreUrl.getKeyName(), this.clientKeyStoreUrl);
        props.setProperty(PropertyKey.clientCertificateKeyStorePassword.getKeyName(), this.clientKeyStorePassword);

        testSession = this.fact.getSession(props);
        testSession.close();
    }

    @Test
    public void testXdevapiTlsVersionsAndCiphersuites() throws Exception {
        assumeTrue(supportsTestCertificates(this.session),
                "This test requires the server configured with SSL certificates from ConnectorJ/src/test/config/ssl-test-certs");
        assumeTrue(supportsTestCertificates(this.session), "This test requires the server with RSA support.");

        // newer GPL servers, like 8.0.4+, are using OpenSSL and can use RSA encryption, while old ones compiled with yaSSL cannot
        Session sess = this.fact.getSession(this.sslFreeBaseUrl);
        String highestCommonTlsVersion = getHighestCommonTlsVersion(sess);
        sess.close();

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), PropertyDefinitions.XdevapiSslMode.VERIFY_CA.toString());
        props.setProperty(PropertyKey.xdevapiSslTrustStoreUrl.getKeyName(), this.trustStoreUrl);
        props.setProperty(PropertyKey.xdevapiSslTrustStorePassword.getKeyName(), this.trustStorePassword);

        Session testSession;
        final ClientFactory cf = new ClientFactory();

        // TS.FR.1_1. Create an X DevAPI session using a connection string containing the connection property xdevapi.tls-versions with a single TLS protocol.
        // Assess that the connection property is initialized with the correct values and that the correct protocol was used (consult status variable ssl_version for details).
        //
        // UPD: Behaviour was changed by WL#14805.
        assertThrows(SSLParamsException.class, "TLS protocols TLSv1 and TLSv1.1 are not supported. Accepted values are TLSv1.2 and TLSv1.3.",
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsVersions, "TLSv1")));

        // TS.FR.1_2. Create an X DevAPI session using a connection string containing the connection property xdevapi.tls-versions with a valid list of TLS protocols.
        // Assess that the connection property is initialized with the correct values and that the correct protocol was used (consult status variable ssl_version for details).
        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsVersions, "TLSv1.2,TLSv1.1,TLSv1"));
        assertSecureSession(testSession);
        assertTlsVersion(testSession, "TLSv1.2");
        testSession.close();

        // TS.FR.1_3. Create an X DevAPI session using a connection properties map containing the connection property xdevapi.tls-versions with a single TLS protocol.
        // Assess that the connection property is initialized with the correct values and that the correct protocol was used (consult status variable ssl_version for details).
        //
        // UPD: Behaviour was changed by WL#14805.
        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), "TLSv1");
        assertThrows(SSLParamsException.class, "TLS protocols TLSv1 and TLSv1.1 are not supported. Accepted values are TLSv1.2 and TLSv1.3.",
                () -> this.fact.getSession(props));

        // TS.FR.1_4. Create an X DevAPI session using a connection properties map containing the connection property xdevapi.tls-versions with a valid list of TLS protocols.
        // Assess that the connection property is initialized with the correct values and that the correct protocol was used (consult status variable ssl_version for details).
        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), "TLSv1.2,TLSv1.1,TLSv1");
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        assertTlsVersion(testSession, "TLSv1.2");
        testSession.close();

        // TS.FR.1_5. Repeat the tests TS.FR.1_1 and TS.FR.1_2 using a ClientFactory instead of a SessionFactory.
        //
        // UPD: Behaviour was changed by WL#14805.
        assertThrows(SSLParamsException.class, "TLS protocols TLSv1 and TLSv1.1 are not supported. Accepted values are TLSv1.2 and TLSv1.3.", () -> {
            Client cli1 = cf.getClient(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsVersions, "TLSv1"), "{\"pooling\": {\"enabled\": true}}");
            cli1.getSession();
            return null;
        });

        Client cli = cf.getClient(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsVersions, "TLSv1.2,TLSv1.1,TLSv1"),
                "{\"pooling\": {\"enabled\": true}}");
        testSession = cli.getSession();
        assertSecureSession(testSession);
        assertTlsVersion(testSession, "TLSv1.2");
        cli.close();

        // TS.FR.2_1. Create an X DevAPI session using a connection string containing the connection property xdevapi.tls-versions without any value.
        // Assess that the code terminates with a WrongArgumentException containing the defined message.
        //
        // UPD: Behaviour was changed by WL#14805.
        assertThrows(SSLParamsException.class, "Specified list of TLS versions is empty. Accepted values are TLSv1.2 and TLSv1.3.",
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsVersions, "")));

        // TS.FR.2_2. Create an X DevAPI session using a connection properties map containing the connection property xdevapi.tls-versions without any value.
        // Assess that the code terminates with a WrongArgumentException containing the defined message.
        //
        // UPD: Behaviour was changed by WL#14805.
        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), "");
        assertThrows(SSLParamsException.class, "Specified list of TLS versions is empty. Accepted values are TLSv1.2 and TLSv1.3.",
                () -> this.fact.getSession(props));

        // TS.FR.2_3. Repeat the test TS.FR.2_1 using a ClientFactory instead of a SessionFactory.
        //
        // UPD: Behaviour was changed by WL#14805.
        assertThrows(SSLParamsException.class, "Specified list of TLS versions is empty. Accepted values are TLSv1.2 and TLSv1.3.", () -> {
            Client cli1 = cf.getClient(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsVersions, ""), "{\"pooling\": {\"enabled\": true}}");
            cli1.getSession();
            return null;
        });

        // TS.FR.3_1. Create an X DevAPI session using a connection string containing the connection property xdevapi.tls-versions with
        // an invalid value, for example SSLv3. Assess that the code terminates with a WrongArgumentException containing the defined message.
        //
        // UPD: Behaviour was changed by WL#14805.
        assertThrows(SSLParamsException.class, "Specified list of TLS versions only contains non valid TLS protocols. Accepted values are TLSv1.2 and TLSv1.3.",
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsVersions, "SSLv3")));

        // TS.FR.3_2. Create an X DevAPI session using a connection properties map containing the connection property xdevapi.tls-versions with
        // an invalid value, for example SSLv3. Assess that the code terminates with a WrongArgumentException containing the defined message.
        //
        // UPD: Behaviour was changed by WL#14805.
        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), "SSLv3");
        assertThrows(SSLParamsException.class, "Specified list of TLS versions only contains non valid TLS protocols. Accepted values are TLSv1.2 and TLSv1.3.",
                () -> this.fact.getSession(props));

        // TS.FR.3_3. Repeat the test TS.FR.3_1 using a ClientFactory instead of a SessionFactory.
        //
        // UPD: Behaviour was changed by WL#14805.
        assertThrows(SSLParamsException.class, "Specified list of TLS versions only contains non valid TLS protocols. Accepted values are TLSv1.2 and TLSv1.3.",
                () -> {
                    Client cli1 = cf.getClient(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsVersions, "SSLv3"), "{\"pooling\": {\"enabled\": true}}");
                    cli1.getSession();
                    return null;
                });

        // TS.FR.4_1. Create an X DevAPI session using a connection string containing the connection property xdevapi.tls-ciphersuites with a single valid cipher-suite.
        // Assess that the connection property is initialized with the correct values and that the correct protocol was used (consult status variable ssl_cipher for details).
        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsCiphersuites, "TLS_DHE_RSA_WITH_AES_128_CBC_SHA"));
        assertSessionStatusEquals(testSession, "mysqlx_ssl_cipher", "DHE-RSA-AES128-SHA");
        assertTlsVersion(testSession, "TLSv1.2");
        testSession.close();

        // TS.FR.4_2. Create an X DevAPI session using a connection string containing the connection property xdevapi.tls-ciphersuites with a valid list of cipher-suites.
        // Assess that the connection property is initialized with the correct values and that the correct protocol was used (consult status variable ssl_cipher for details).
        testSession = this.fact
                .getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsCiphersuites, "TLS_DHE_RSA_WITH_AES_128_CBC_SHA,AES256-SHA256"));
        assertSessionStatusEquals(testSession, "mysqlx_ssl_cipher", "DHE-RSA-AES128-SHA");
        assertTlsVersion(testSession, "TLSv1.2");
        testSession.close();

        // TS.FR.4_3   Create an X DevAPI session using a connection string containing the connection property xdevapi.tls-ciphersuites with a list of valid and invalid cipher-suites,
        // starting with an invalid one. Assess that the connection property is initialized with the correct values and that the correct protocol was used (consult status variable ssl_cipher for details).
        testSession = this.fact.getSession(
                this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsCiphersuites, "TLS_RSA_EXPORT1024_WITH_RC4_56_MD5,TLS_DHE_RSA_WITH_AES_128_CBC_SHA"));
        assertSessionStatusEquals(testSession, "mysqlx_ssl_cipher", "DHE-RSA-AES128-SHA");
        assertTlsVersion(testSession, "TLSv1.2");
        testSession.close();

        // TS.FR.4_4. Create an X DevAPI session using a connection string containing the connection property xdevapi.tls-ciphersuites with a single invalid cipher-suite.
        // Assess that the connection property is initialized with the correct values and that the connection fails with an SSL error.
        Throwable ex = assertThrows(CJCommunicationsException.class, "Unable to connect to any of the target hosts\\.", () -> {
            this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsCiphersuites, "TLS_RSA_EXPORT1024_WITH_RC4_56_MD5"));
            return null;
        });
        assertNotNull(ex.getCause());
        assertEquals("javax.net.ssl.SSLHandshakeException: No appropriate protocol (protocol is disabled or cipher suites are inappropriate)",
                ex.getCause().getMessage());

        // TS.FR.4_5. Create an X DevAPI session using a connection properties map containing the connection property xdevapi.tls-versions with a single valid cipher-suite.
        // Assess that the connection property is initialized with the correct values and that the correct protocol was used (consult status variable ssl_cipher for details).
        props.remove(PropertyKey.xdevapiTlsVersions.getKeyName());
        props.setProperty(PropertyKey.xdevapiTlsCiphersuites.getKeyName(), "TLS_DHE_RSA_WITH_AES_128_CBC_SHA");
        testSession = this.fact.getSession(props);
        assertSessionStatusEquals(testSession, "mysqlx_ssl_cipher", "DHE-RSA-AES128-SHA");
        testSession.close();

        // TS.FR.4_6. Create an X DevAPI session using a connection properties map containing the connection property xdevapi.tls-versions with a valid list of cipher-suites.
        // Assess that the connection property is initialized with the correct values and that the correct protocol was used (consult status variable ssl_cipher for details).
        props.setProperty(PropertyKey.xdevapiTlsCiphersuites.getKeyName(), "TLS_DHE_RSA_WITH_AES_128_CBC_SHA,AES256-SHA256");
        testSession = this.fact.getSession(props);
        assertSessionStatusEquals(testSession, "mysqlx_ssl_cipher", "DHE-RSA-AES128-SHA");
        testSession.close();

        // TS.FR.4_7. Create an X DevAPI session using a connection properties map containing the connection property xdevapi.tls-versions with a list of valid and invalid cipher-suites,
        // starting with an invalid one. Assess that the connection property is initialized with the correct values and that the correct protocol was used (consult status variable ssl_cipher for details).
        props.setProperty(PropertyKey.xdevapiTlsCiphersuites.getKeyName(), "TLS_RSA_EXPORT1024_WITH_RC4_56_MD5,TLS_DHE_RSA_WITH_AES_128_CBC_SHA");
        testSession = this.fact.getSession(props);
        assertSessionStatusEquals(testSession, "mysqlx_ssl_cipher", "DHE-RSA-AES128-SHA");
        testSession.close();

        // TS.FR.4_8. Create an X DevAPI session using a connection properties map containing the connection property xdevapi.tls-ciphersuites with a single invalid cipher-suite.
        // Assess that the connection property is initialized with the correct values and that the connection fails with an SSL error.
        props.setProperty(PropertyKey.xdevapiTlsCiphersuites.getKeyName(), "TLS_RSA_EXPORT1024_WITH_RC4_56_MD5");
        assertThrows(CJCommunicationsException.class,
                "javax.net.ssl.SSLHandshakeException: No appropriate protocol \\(protocol is disabled or cipher suites are inappropriate\\)", () -> {
                    this.fact.getSession(props);
                    return null;
                });

        // TS.FR.4_9. Repeat the tests TS.FR.4_1 to TS.FR.4_4 using a ClientFactory instead of a SessionFactory.
        cli = cf.getClient(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsCiphersuites, "TLS_DHE_RSA_WITH_AES_128_CBC_SHA"),
                "{\"pooling\": {\"enabled\": true}}");
        testSession = cli.getSession();
        assertSessionStatusEquals(testSession, "mysqlx_ssl_cipher", "DHE-RSA-AES128-SHA");
        assertTlsVersion(testSession, "TLSv1.2");
        testSession.close();

        cli = cf.getClient(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsCiphersuites, "TLS_DHE_RSA_WITH_AES_128_CBC_SHA,AES256-SHA256"),
                "{\"pooling\": {\"enabled\": true}}");
        testSession = cli.getSession();
        assertSessionStatusEquals(testSession, "mysqlx_ssl_cipher", "DHE-RSA-AES128-SHA");
        assertTlsVersion(testSession, "TLSv1.2");
        testSession.close();

        cli = cf.getClient(
                this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsCiphersuites, "TLS_RSA_EXPORT1024_WITH_RC4_56_MD5,TLS_DHE_RSA_WITH_AES_128_CBC_SHA"),
                "{\"pooling\": {\"enabled\": true}}");
        testSession = cli.getSession();
        assertSessionStatusEquals(testSession, "mysqlx_ssl_cipher", "DHE-RSA-AES128-SHA");
        assertTlsVersion(testSession, "TLSv1.2");
        testSession.close();

        ex = assertThrows(CJCommunicationsException.class, "Unable to connect to any of the target hosts\\.", () -> {
            Client cli1 = cf.getClient(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsCiphersuites, "TLS_RSA_EXPORT1024_WITH_RC4_56_MD5"),
                    "{\"pooling\": {\"enabled\": true}}");
            cli1.getSession();
            return null;
        });
        assertNotNull(ex.getCause());
        assertEquals("javax.net.ssl.SSLHandshakeException: No appropriate protocol (protocol is disabled or cipher suites are inappropriate)",
                ex.getCause().getMessage());

        // TS.FR.5_1. Create an X DevAPI session using a connection string without the connection properties xdevapi.tls-versions and xdevapi.tls-ciphersuites.
        // Assess that the session is created successfully and the connection properties are initialized with the expected values.
        testSession = this.fact.getSession(this.sslFreeBaseUrl);
        assertSecureSession(testSession);
        assertTlsVersion(testSession, highestCommonTlsVersion);
        testSession.close();

        // TS.FR.5_2. Create an X DevAPI session using a connection string with the connection property xdevapi.tls-versions but without xdevapi.tls-ciphersuites.
        // Assess that the session is created successfully and the connection property xdevapi.tls-versions is initialized with the expected values.
        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsVersions, "TLSv1.2"));
        assertSecureSession(testSession);
        assertTlsVersion(testSession, "TLSv1.2");
        testSession.close();

        // TS.FR.5_3. Create an X DevAPI session using a connection string with the connection property xdevapi.tls-ciphersuites but without xdevapi.tls-versions.
        // Assess that the session is created successfully and the connection property xdevapi.tls-ciphersuites is initialized with the expected values.
        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsCiphersuites, "TLS_DHE_RSA_WITH_AES_128_CBC_SHA"));
        assertSessionStatusEquals(testSession, "mysqlx_ssl_cipher", "DHE-RSA-AES128-SHA");
        assertTlsVersion(testSession, "TLSv1.2");
        testSession.close();

        // TS.FR.5_4. Create an X DevAPI session using a connection properties map without the connection properties xdevapi.tls-versions and xdevapi.tls-ciphersuites.
        // Assess that the session is created successfully and the connection properties are initialized with the expected values.
        props.remove(PropertyKey.xdevapiTlsVersions.getKeyName());
        props.remove(PropertyKey.xdevapiTlsCiphersuites.getKeyName());
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        assertTlsVersion(testSession, highestCommonTlsVersion);
        testSession.close();

        // TS.FR.5_5. Create an X DevAPI session using a connection properties map with the connection property xdevapi.tls-versions but without xdevapi.tls-ciphersuites.
        // Assess that the session is created successfully and the connection property xdevapi.tls-versions is initialized with the expected values.
        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), "TLSv1.2");
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        assertTlsVersion(testSession, "TLSv1.2");
        testSession.close();

        // TS.FR.5_6. Create an X DevAPI session using a connection properties map with the connection property xdevapi.tls-ciphersuites but without xdevapi.tls-versions.
        // Assess that the session is created successfully and the connection property xdevapi.tls-ciphersuites is initialized with the expected values.
        props.remove(PropertyKey.xdevapiTlsVersions.getKeyName());
        props.setProperty(PropertyKey.xdevapiTlsCiphersuites.getKeyName(), "TLS_DHE_RSA_WITH_AES_128_CBC_SHA");
        testSession = this.fact.getSession(props);
        assertTlsVersion(testSession, "TLSv1.2");
        assertSessionStatusEquals(testSession, "mysqlx_ssl_cipher", "DHE-RSA-AES128-SHA");
        testSession.close();

        // TS.FR.5_7. Repeat the tests TS.FR.5_1 to TS.FR.5_3 using a ClientFactory instead of a SessionFactory.
        cli = cf.getClient(this.sslFreeBaseUrl, "{\"pooling\": {\"enabled\": true}}");
        testSession = cli.getSession();
        assertSecureSession(testSession);
        assertTlsVersion(testSession, highestCommonTlsVersion);
        cli.close();

        cli = cf.getClient(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsVersions, "TLSv1.2"), "{\"pooling\": {\"enabled\": true}}");
        testSession = cli.getSession();
        assertSecureSession(testSession);
        assertTlsVersion(testSession, "TLSv1.2");
        cli.close();

        cli = cf.getClient(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiTlsCiphersuites, "TLS_DHE_RSA_WITH_AES_128_CBC_SHA"),
                "{\"pooling\": {\"enabled\": true}}");
        testSession = cli.getSession();
        assertSecureSession(testSession);
        assertTlsVersion(testSession, "TLSv1.2");
        cli.close();

        // TS.FR.6_1. Create an X DevAPI session using a connection string with the connection property xdevapi.ssl-mode=DISABLED and both the connection properties
        // xdevapi.tls-versions and xdevapi.tls-ciphersuites. Assess that the code terminates with a WrongArgumentException containing the defined message.
        String xdevapiSSLMode = makeParam(PropertyKey.xdevapiSslMode, PropertyDefinitions.XdevapiSslMode.DISABLED.toString());
        assertThrows(WrongArgumentException.class,
                "Option '" + PropertyKey.xdevapiTlsVersions.getKeyName() + "' can not be specified when SSL connections are disabled.",
                () -> this.fact.getSession(this.sslFreeBaseUrl + xdevapiSSLMode + makeParam(PropertyKey.xdevapiTlsVersions, "TLSv1.2")
                        + makeParam(PropertyKey.xdevapiTlsCiphersuites, "TLS_DHE_RSA_WITH_AES_128_CBC_SHA")));

        // TS.FR.6_2. Create an X DevAPI session using a connection string with the connection property xdevapi.ssl-mode=DISABLED and the connection property xdevapi.tls-versions
        // but not xdevapi.tls-ciphersuites. Assess that the code terminates with a WrongArgumentException containing the defined message.
        assertThrows(WrongArgumentException.class,
                "Option '" + PropertyKey.xdevapiTlsVersions.getKeyName() + "' can not be specified when SSL connections are disabled.",
                () -> this.fact.getSession(this.sslFreeBaseUrl + xdevapiSSLMode + makeParam(PropertyKey.xdevapiTlsVersions, "TLSv1.2")));

        //
        // TS.FR.6_3. Create an X DevAPI session using a connection string with the connection property xdevapi.ssl-mode=DISABLED and the connection property xdevapi.tls-ciphersuites
        // but not xdevapi.tls-versions. Assess that the code terminates with a WrongArgumentException containing the defined message.
        assertThrows(WrongArgumentException.class,
                "Option '" + PropertyKey.xdevapiTlsCiphersuites.getKeyName() + "' can not be specified when SSL connections are disabled.", () -> this.fact
                        .getSession(this.sslFreeBaseUrl + xdevapiSSLMode + makeParam(PropertyKey.xdevapiTlsCiphersuites, "TLS_DHE_RSA_WITH_AES_128_CBC_SHA")));

        //
        // TS.FR.6_4. Create an X DevAPI session using a connection properties map with the connection property xdevapi.ssl-mode=DISABLED and both the connection properties xdevapi.tls-versions
        // and xdevapi.tls-ciphersuites. Assess that the code terminates with a WrongArgumentException containing the defined message.
        props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), PropertyDefinitions.XdevapiSslMode.DISABLED.toString());
        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), "TLSv1.2");
        props.setProperty(PropertyKey.xdevapiTlsCiphersuites.getKeyName(), "TLS_DHE_RSA_WITH_AES_128_CBC_SHA");
        assertThrows(WrongArgumentException.class,
                "Option '" + PropertyKey.xdevapiTlsVersions.getKeyName() + "' can not be specified when SSL connections are disabled.",
                () -> this.fact.getSession(props));

        // TS.FR.6_5. Create an X DevAPI session using a connection properties map with the connection property xdevapi.ssl-mode=DISABLED and the connection property xdevapi.tls-versions
        // but not xdevapi.tls-ciphersuites. Assess that the code terminates with a WrongArgumentException containing the defined message.
        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), "TLSv1.2");
        props.remove(PropertyKey.xdevapiTlsCiphersuites.getKeyName());
        assertThrows(WrongArgumentException.class,
                "Option '" + PropertyKey.xdevapiTlsVersions.getKeyName() + "' can not be specified when SSL connections are disabled.",
                () -> this.fact.getSession(props));

        // TS.FR.6_6. Create an X DevAPI session using a connection properties map with the connection property xdevapi.ssl-mode=DISABLED and the connection property xdevapi.tls-ciphersuites
        // but not xdevapi.tls-versions. Assess that the code terminates with a WrongArgumentException containing the defined message.
        props.remove(PropertyKey.xdevapiTlsVersions.getKeyName());
        props.setProperty(PropertyKey.xdevapiTlsCiphersuites.getKeyName(), "TLS_DHE_RSA_WITH_AES_128_CBC_SHA");
        assertThrows(WrongArgumentException.class,
                "Option '" + PropertyKey.xdevapiTlsCiphersuites.getKeyName() + "' can not be specified when SSL connections are disabled.",
                () -> this.fact.getSession(props));

        // TS.FR.6_7. Repeat the tests TS.FR.6_1 to TS.FR.6_3 using a ClientFactory instead of a SessionFactory.
        assertThrows(WrongArgumentException.class,
                "Option '" + PropertyKey.xdevapiTlsVersions.getKeyName() + "' can not be specified when SSL connections are disabled.", () -> {
                    Client cli1 = cf.getClient(this.sslFreeBaseUrl + xdevapiSSLMode + makeParam(PropertyKey.xdevapiTlsVersions, "TLSv1.2"),
                            "{\"pooling\": {\"enabled\": true}}");
                    cli1.getSession();
                    return null;
                });
        assertThrows(WrongArgumentException.class,
                "Option '" + PropertyKey.xdevapiTlsVersions.getKeyName() + "' can not be specified when SSL connections are disabled.", () -> {
                    Client cli1 = cf.getClient(this.sslFreeBaseUrl + xdevapiSSLMode + makeParam(PropertyKey.xdevapiTlsVersions, "TLSv1.2")
                            + makeParam(PropertyKey.xdevapiTlsCiphersuites, "TLS_DHE_RSA_WITH_AES_128_CBC_SHA"), "{\"pooling\": {\"enabled\": true}}");
                    cli1.getSession();
                    return null;
                });
        assertThrows(WrongArgumentException.class,
                "Option '" + PropertyKey.xdevapiTlsCiphersuites.getKeyName() + "' can not be specified when SSL connections are disabled.", () -> {
                    Client cli1 = cf.getClient(
                            this.sslFreeBaseUrl + xdevapiSSLMode + makeParam(PropertyKey.xdevapiTlsCiphersuites, "TLS_DHE_RSA_WITH_AES_128_CBC_SHA"),
                            "{\"pooling\": {\"enabled\": true}}");
                    cli1.getSession();
                    return null;
                });
    }

    /**
     * Tests that given SSL/TLS related session properties values are processed as expected.
     * 
     * @throws Exception
     */
    @Test
    public void testXdevapiSslConnectionOptions() throws Exception {
        assumeTrue(supportsTestCertificates(this.session),
                "This test requires the server configured with SSL certificates from ConnectorJ/src/test/config/ssl-test-certs");

        Session testSess;
        PropertySet propSet;

        /*
         * Check defaults.
         */
        testSess = this.fact.getSession(this.sslFreeBaseUrl);
        propSet = ((SessionImpl) testSess).getSession().getPropertySet();
        // X DevAPI options.
        assertEquals(XdevapiSslMode.REQUIRED, propSet.getProperty(PropertyKey.xdevapiSslMode).getValue());
        assertNull(propSet.getProperty(PropertyKey.xdevapiSslTrustStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.xdevapiSslTrustStoreType).getValue());
        assertNull(propSet.getProperty(PropertyKey.xdevapiSslTrustStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.xdevapiFallbackToSystemTrustStore).getValue());
        assertNull(propSet.getProperty(PropertyKey.xdevapiSslKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.xdevapiSslKeyStoreType).getValue());
        assertNull(propSet.getProperty(PropertyKey.xdevapiSslKeyStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.xdevapiFallbackToSystemKeyStore).getValue());
        // Global (JDBC) options.
        assertEquals(SslMode.REQUIRED, propSet.getProperty(PropertyKey.sslMode).getValue());
        assertNull(propSet.getProperty(PropertyKey.trustCertificateKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.trustCertificateKeyStoreType).getValue());
        assertNull(propSet.getProperty(PropertyKey.trustCertificateKeyStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.fallbackToSystemTrustStore).getValue());
        assertNull(propSet.getProperty(PropertyKey.clientCertificateKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.clientCertificateKeyStoreType).getValue());
        assertNull(propSet.getProperty(PropertyKey.clientCertificateKeyStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.fallbackToSystemKeyStore).getValue());
        testSess.close();

        /*
         * Check SSL properties set globally (JDBC)
         */
        Properties props = new Properties(this.sslFreeTestProperties);
        // Set global SSL connection properties.
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.VERIFY_CA.toString());
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/ca-truststore");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "password");
        props.setProperty(PropertyKey.fallbackToSystemTrustStore.getKeyName(), "false");
        props.setProperty(PropertyKey.clientCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/client-keystore");
        props.setProperty(PropertyKey.clientCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.clientCertificateKeyStorePassword.getKeyName(), "password");
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "false");

        testSess = this.fact.getSession(props);
        propSet = ((SessionImpl) testSess).getSession().getPropertySet();
        // X DevAPI options keep defaults.
        assertEquals(XdevapiSslMode.REQUIRED, propSet.getProperty(PropertyKey.xdevapiSslMode).getValue());
        assertNull(propSet.getProperty(PropertyKey.xdevapiSslTrustStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.xdevapiSslTrustStoreType).getValue());
        assertNull(propSet.getProperty(PropertyKey.xdevapiSslTrustStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.xdevapiFallbackToSystemTrustStore).getValue());
        assertNull(propSet.getProperty(PropertyKey.xdevapiSslKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.xdevapiSslKeyStoreType).getValue());
        assertNull(propSet.getProperty(PropertyKey.xdevapiSslKeyStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.xdevapiFallbackToSystemKeyStore).getValue());
        // Global (JDBC) were set.
        assertEquals(SslMode.VERIFY_CA, propSet.getProperty(PropertyKey.sslMode).getValue());
        assertEquals("file:src/test/config/ssl-test-certs/ca-truststore", propSet.getProperty(PropertyKey.trustCertificateKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.trustCertificateKeyStoreType).getValue());
        assertEquals("password", propSet.getProperty(PropertyKey.trustCertificateKeyStorePassword).getValue());
        assertFalse(propSet.getBooleanProperty(PropertyKey.fallbackToSystemTrustStore).getValue());
        assertEquals("file:src/test/config/ssl-test-certs/client-keystore", propSet.getProperty(PropertyKey.clientCertificateKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.clientCertificateKeyStoreType).getValue());
        assertEquals("password", propSet.getProperty(PropertyKey.clientCertificateKeyStorePassword).getValue());
        assertFalse(propSet.getBooleanProperty(PropertyKey.fallbackToSystemKeyStore).getValue());
        testSess.close();

        props.setProperty(PropertyKey.fallbackToSystemTrustStore.getKeyName(), "true");
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "true");

        testSess = this.fact.getSession(props);
        propSet = ((SessionImpl) testSess).getSession().getPropertySet();
        // X DevAPI options keep defaults.
        assertEquals(XdevapiSslMode.REQUIRED, propSet.getProperty(PropertyKey.xdevapiSslMode).getValue());
        assertNull(propSet.getProperty(PropertyKey.xdevapiSslTrustStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.xdevapiSslTrustStoreType).getValue());
        assertNull(propSet.getProperty(PropertyKey.xdevapiSslTrustStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.xdevapiFallbackToSystemTrustStore).getValue());
        assertNull(propSet.getProperty(PropertyKey.xdevapiSslKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.xdevapiSslKeyStoreType).getValue());
        assertNull(propSet.getProperty(PropertyKey.xdevapiSslKeyStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.xdevapiFallbackToSystemKeyStore).getValue());
        // Global (JDBC) options were set.
        assertEquals(SslMode.VERIFY_CA, propSet.getProperty(PropertyKey.sslMode).getValue());
        assertEquals("file:src/test/config/ssl-test-certs/ca-truststore", propSet.getProperty(PropertyKey.trustCertificateKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.trustCertificateKeyStoreType).getValue());
        assertEquals("password", propSet.getProperty(PropertyKey.trustCertificateKeyStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.fallbackToSystemTrustStore).getValue());
        assertEquals("file:src/test/config/ssl-test-certs/client-keystore", propSet.getProperty(PropertyKey.clientCertificateKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.clientCertificateKeyStoreType).getValue());
        assertEquals("password", propSet.getProperty(PropertyKey.clientCertificateKeyStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.fallbackToSystemKeyStore).getValue());
        testSess.close();

        /*
         * Check SSL properties set locally on the X DevAPI.
         */
        props = new Properties(this.sslFreeTestProperties);
        // Set global SSL connection properties.
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.toString());
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "trust-cert-keystore-url");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "trust-cert-keystore-type");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "trust-cert-keystore-pwd");
        props.setProperty(PropertyKey.fallbackToSystemTrustStore.getKeyName(), "false");
        props.setProperty(PropertyKey.clientCertificateKeyStoreUrl.getKeyName(), "client-cert-keystore-url");
        props.setProperty(PropertyKey.clientCertificateKeyStoreType.getKeyName(), "client-cert-keystore-type");
        props.setProperty(PropertyKey.clientCertificateKeyStorePassword.getKeyName(), "client-cert-keystore-pwd");
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "false");
        // Set X DevAPI local connection properties.
        props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), XdevapiSslMode.VERIFY_CA.toString());
        props.setProperty(PropertyKey.xdevapiSslTrustStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/ca-truststore");
        props.setProperty(PropertyKey.xdevapiSslTrustStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.xdevapiSslTrustStorePassword.getKeyName(), "password");
        props.setProperty(PropertyKey.xdevapiFallbackToSystemTrustStore.getKeyName(), "false");
        props.setProperty(PropertyKey.xdevapiSslKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/client-keystore");
        props.setProperty(PropertyKey.xdevapiSslKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.xdevapiSslKeyStorePassword.getKeyName(), "password");
        props.setProperty(PropertyKey.xdevapiFallbackToSystemKeyStore.getKeyName(), "false");

        testSess = this.fact.getSession(props);
        propSet = ((SessionImpl) testSess).getSession().getPropertySet();
        // X DevAPI options were set.
        assertEquals(XdevapiSslMode.VERIFY_CA, propSet.getProperty(PropertyKey.xdevapiSslMode).getValue());
        assertEquals("file:src/test/config/ssl-test-certs/ca-truststore", propSet.getProperty(PropertyKey.xdevapiSslTrustStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.xdevapiSslTrustStoreType).getValue());
        assertEquals("password", propSet.getProperty(PropertyKey.xdevapiSslTrustStorePassword).getValue());
        assertFalse(propSet.getBooleanProperty(PropertyKey.xdevapiFallbackToSystemTrustStore).getValue());
        assertEquals("file:src/test/config/ssl-test-certs/client-keystore", propSet.getProperty(PropertyKey.xdevapiSslKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.xdevapiSslKeyStoreType).getValue());
        assertEquals("password", propSet.getProperty(PropertyKey.xdevapiSslKeyStorePassword).getValue());
        assertFalse(propSet.getBooleanProperty(PropertyKey.xdevapiFallbackToSystemKeyStore).getValue());
        // Global (JDBC) options were overridden.
        assertEquals(SslMode.VERIFY_CA, propSet.getProperty(PropertyKey.sslMode).getValue());
        assertEquals("file:src/test/config/ssl-test-certs/ca-truststore", propSet.getProperty(PropertyKey.trustCertificateKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.trustCertificateKeyStoreType).getValue());
        assertEquals("password", propSet.getProperty(PropertyKey.trustCertificateKeyStorePassword).getValue());
        assertFalse(propSet.getBooleanProperty(PropertyKey.fallbackToSystemTrustStore).getValue());
        assertEquals("file:src/test/config/ssl-test-certs/client-keystore", propSet.getProperty(PropertyKey.clientCertificateKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.clientCertificateKeyStoreType).getValue());
        assertEquals("password", propSet.getProperty(PropertyKey.clientCertificateKeyStorePassword).getValue());
        assertFalse(propSet.getBooleanProperty(PropertyKey.fallbackToSystemKeyStore).getValue());
        testSess.close();

        props.setProperty(PropertyKey.xdevapiFallbackToSystemTrustStore.getKeyName(), "true");
        props.setProperty(PropertyKey.xdevapiFallbackToSystemKeyStore.getKeyName(), "true");

        testSess = this.fact.getSession(props);
        propSet = ((SessionImpl) testSess).getSession().getPropertySet();
        // X DevAPI options were set.
        assertEquals(XdevapiSslMode.VERIFY_CA, propSet.getProperty(PropertyKey.xdevapiSslMode).getValue());
        assertEquals("file:src/test/config/ssl-test-certs/ca-truststore", propSet.getProperty(PropertyKey.xdevapiSslTrustStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.xdevapiSslTrustStoreType).getValue());
        assertEquals("password", propSet.getProperty(PropertyKey.xdevapiSslTrustStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.xdevapiFallbackToSystemTrustStore).getValue());
        assertEquals("file:src/test/config/ssl-test-certs/client-keystore", propSet.getProperty(PropertyKey.xdevapiSslKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.xdevapiSslKeyStoreType).getValue());
        assertEquals("password", propSet.getProperty(PropertyKey.xdevapiSslKeyStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.xdevapiFallbackToSystemKeyStore).getValue());
        // Global (JDBC) options were overridden.
        assertEquals(SslMode.VERIFY_CA, propSet.getProperty(PropertyKey.sslMode).getValue());
        assertEquals("file:src/test/config/ssl-test-certs/ca-truststore", propSet.getProperty(PropertyKey.trustCertificateKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.trustCertificateKeyStoreType).getValue());
        assertEquals("password", propSet.getProperty(PropertyKey.trustCertificateKeyStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.fallbackToSystemTrustStore).getValue());
        assertEquals("file:src/test/config/ssl-test-certs/client-keystore", propSet.getProperty(PropertyKey.clientCertificateKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.clientCertificateKeyStoreType).getValue());
        assertEquals("password", propSet.getProperty(PropertyKey.clientCertificateKeyStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.fallbackToSystemKeyStore).getValue());
        testSess.close();
    }

    /**
     * Tests connection property 'xdevapi.fallback-to-system-truststore' behavior.
     * 
     * @throws Exception
     */
    @Test
    public void testFallbackToSystemTrustStore() throws Exception {
        assumeTrue(supportsTestCertificates(this.session),
                "This test requires the server configured with SSL certificates from ConnectorJ/src/test/config/ssl-test-certs");

        Session testSess;

        /*
         * Valid system-wide TrustStore.
         */
        System.setProperty("javax.net.ssl.trustStore", "file:src/test/config/ssl-test-certs/ca-truststore");
        System.setProperty("javax.net.ssl.trustStoreType", "JKS");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");

        // No session-local TrustStore.
        testSess = this.fact.getSession(this.sslFreeBaseUrl);
        assertSecureSession(testSess);
        testSess.close();
        testSess = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_CA));
        assertSecureSession(testSess);
        testSess.close();
        testSess = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_CA)
                + makeParam(PropertyKey.xdevapiFallbackToSystemTrustStore, "true"));
        assertSecureSession(testSess);
        testSess.close();
        assertThrows(CJCommunicationsException.class, () -> this.fact.getSession(this.sslFreeBaseUrl
                + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_CA) + makeParam(PropertyKey.xdevapiFallbackToSystemTrustStore, "false")));

        // Invalid session-local TrustStore:
        assertThrows(CJCommunicationsException.class,
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.REQUIRED)
                        + makeParam(PropertyKey.xdevapiSslTrustStoreUrl, "file:src/test/config/ssl-test-certs/ca-truststore-ext")
                        + makeParam(PropertyKey.xdevapiSslTrustStoreType, "JKS") + makeParam(PropertyKey.xdevapiSslTrustStorePassword, "password")));
        assertThrows(CJCommunicationsException.class,
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_CA)
                        + makeParam(PropertyKey.xdevapiSslTrustStoreUrl, "file:src/test/config/ssl-test-certs/ca-truststore-ext")
                        + makeParam(PropertyKey.xdevapiSslTrustStoreType, "JKS") + makeParam(PropertyKey.xdevapiSslTrustStorePassword, "password")));
        assertThrows(CJCommunicationsException.class,
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_CA)
                        + makeParam(PropertyKey.xdevapiFallbackToSystemTrustStore, "true")
                        + makeParam(PropertyKey.xdevapiSslTrustStoreUrl, "file:src/test/config/ssl-test-certs/ca-truststore-ext")
                        + makeParam(PropertyKey.xdevapiSslTrustStoreType, "JKS") + makeParam(PropertyKey.xdevapiSslTrustStorePassword, "password")));
        assertThrows(CJCommunicationsException.class,
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_CA)
                        + makeParam(PropertyKey.xdevapiFallbackToSystemTrustStore, "false")
                        + makeParam(PropertyKey.xdevapiSslTrustStoreUrl, "file:src/test/config/ssl-test-certs/ca-truststore-ext")
                        + makeParam(PropertyKey.xdevapiSslTrustStoreType, "JKS") + makeParam(PropertyKey.xdevapiSslTrustStorePassword, "password")));

        /*
         * Invalid system-wide TrustStore.
         */
        System.setProperty("javax.net.ssl.trustStore", "file:src/test/config/ssl-test-certs/ca-truststore-ext");
        System.setProperty("javax.net.ssl.trustStoreType", "JKS");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");

        // No session-local TrustStore.
        testSess = this.fact.getSession(this.sslFreeBaseUrl);
        assertSecureSession(testSess);
        testSess.close();
        assertThrows(CJCommunicationsException.class,
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_CA)));
        assertThrows(CJCommunicationsException.class, () -> this.fact.getSession(this.sslFreeBaseUrl
                + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_CA) + makeParam(PropertyKey.xdevapiFallbackToSystemTrustStore, "true")));
        assertThrows(CJCommunicationsException.class, () -> this.fact.getSession(this.sslFreeBaseUrl
                + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_CA) + makeParam(PropertyKey.xdevapiFallbackToSystemTrustStore, "false")));

        // Valid session-local TrustStore:
        assertThrows(CJCommunicationsException.class,
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.REQUIRED)
                        + makeParam(PropertyKey.xdevapiSslTrustStoreUrl, "file:src/test/config/ssl-test-certs/ca-truststore")
                        + makeParam(PropertyKey.xdevapiSslTrustStoreType, "JKS") + makeParam(PropertyKey.xdevapiSslTrustStorePassword, "password")));
        testSess = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_CA)
                + makeParam(PropertyKey.xdevapiSslTrustStoreUrl, "file:src/test/config/ssl-test-certs/ca-truststore")
                + makeParam(PropertyKey.xdevapiSslTrustStoreType, "JKS") + makeParam(PropertyKey.xdevapiSslTrustStorePassword, "password"));
        assertSecureSession(testSess);
        testSess.close();
        testSess = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_CA)
                + makeParam(PropertyKey.xdevapiFallbackToSystemTrustStore, "true")
                + makeParam(PropertyKey.xdevapiSslTrustStoreUrl, "file:src/test/config/ssl-test-certs/ca-truststore")
                + makeParam(PropertyKey.xdevapiSslTrustStoreType, "JKS") + makeParam(PropertyKey.xdevapiSslTrustStorePassword, "password"));
        assertSecureSession(testSess);
        testSess.close();
        testSess = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyKey.xdevapiSslMode, XdevapiSslMode.VERIFY_CA)
                + makeParam(PropertyKey.xdevapiFallbackToSystemTrustStore, "false")
                + makeParam(PropertyKey.xdevapiSslTrustStoreUrl, "file:src/test/config/ssl-test-certs/ca-truststore")
                + makeParam(PropertyKey.xdevapiSslTrustStoreType, "JKS") + makeParam(PropertyKey.xdevapiSslTrustStorePassword, "password"));
        assertSecureSession(testSess);
        testSess.close();
    }

    /**
     * Tests connection property 'xdevapi.fallback-to-system-keystore' behavior.
     * 
     * @throws Exception
     */
    @Test
    public void testFallbackToSystemKeyStore() throws Exception {
        assumeTrue(supportsTestCertificates(this.session),
                "This test requires the server configured with SSL certificates from ConnectorJ/src/test/config/ssl-test-certs");

        final String user = "testFbToSysKS";
        try {
            Session testSession = this.fact.getSession(this.baseUrl);
            testSession.sql("CREATE USER IF NOT EXISTS '" + user + "'@'%' IDENTIFIED BY 'password' REQUIRE X509").execute();
            testSession.sql("GRANT ALL ON *.* TO '" + user + "'@'%'").execute();
            testSession.close();

            final Properties props = new Properties(this.sslFreeTestProperties);
            props.setProperty(PropertyKey.USER.getKeyName(), user);
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "password");
            props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), XdevapiSslMode.REQUIRED.toString());

            Session testSess;

            /*
             * Valid system-wide KeyStore.
             */
            System.setProperty("javax.net.ssl.keyStore", "file:src/test/config/ssl-test-certs/client-keystore");
            System.setProperty("javax.net.ssl.keyStoreType", "JKS");
            System.setProperty("javax.net.ssl.keyStorePassword", "password");

            // No connection-local KeyStore.
            testSess = this.fact.getSession(props);
            assertSecureSession(testSess, user);
            testSess.close();
            props.setProperty(PropertyKey.xdevapiFallbackToSystemKeyStore.getKeyName(), "true");
            testSess = this.fact.getSession(props);
            assertSecureSession(testSess, user);
            testSess.close();
            props.setProperty(PropertyKey.xdevapiFallbackToSystemKeyStore.getKeyName(), "false");
            assertThrows(XProtocolError.class, mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.12")) ? ".*Access denied for user '" + user + "'@.*"
                    : ".*Current account requires TLS to be activate.", () -> this.fact.getSession(props));

            props.remove(PropertyKey.xdevapiFallbackToSystemKeyStore.getKeyName());

            // Invalid connection-local KeyStore:
            props.setProperty(PropertyKey.xdevapiSslKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/client-keystore-ext");
            props.setProperty(PropertyKey.xdevapiSslKeyStoreType.getKeyName(), "JKS");
            props.setProperty(PropertyKey.xdevapiSslKeyStorePassword.getKeyName(), "password");
            assertThrows(CJCommunicationsException.class, () -> this.fact.getSession(props));
            props.setProperty(PropertyKey.xdevapiFallbackToSystemKeyStore.getKeyName(), "true");
            assertThrows(CJCommunicationsException.class, () -> this.fact.getSession(props));
            props.setProperty(PropertyKey.xdevapiFallbackToSystemKeyStore.getKeyName(), "false");
            assertThrows(CJCommunicationsException.class, () -> this.fact.getSession(props));

            props.remove(PropertyKey.xdevapiSslKeyStoreUrl.getKeyName());
            props.remove(PropertyKey.xdevapiSslKeyStoreType.getKeyName());
            props.remove(PropertyKey.xdevapiSslKeyStorePassword.getKeyName());
            props.remove(PropertyKey.xdevapiFallbackToSystemKeyStore.getKeyName());

            /*
             * Invalid system-wide KeyStore.
             */
            System.setProperty("javax.net.ssl.keyStore", "file:src/test/config/ssl-test-certs/client-keystore-ext");
            System.setProperty("javax.net.ssl.keyStoreType", "JKS");
            System.setProperty("javax.net.ssl.keyStorePassword", "password");

            // No connection-local KeyStore.
            assertThrows(CJCommunicationsException.class, () -> this.fact.getSession(props));
            props.setProperty(PropertyKey.xdevapiFallbackToSystemKeyStore.getKeyName(), "true");
            assertThrows(CJCommunicationsException.class, () -> this.fact.getSession(props));
            props.setProperty(PropertyKey.xdevapiFallbackToSystemKeyStore.getKeyName(), "false");
            assertThrows(XProtocolError.class, mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.12")) ? ".*Access denied for user '" + user + "'@.*"
                    : ".*Current account requires TLS to be activate.", () -> this.fact.getSession(props));

            props.remove(PropertyKey.xdevapiFallbackToSystemKeyStore.getKeyName());

            // Valid connection-local KeyStore:
            props.setProperty(PropertyKey.xdevapiSslKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/client-keystore");
            props.setProperty(PropertyKey.xdevapiSslKeyStoreType.getKeyName(), "JKS");
            props.setProperty(PropertyKey.xdevapiSslKeyStorePassword.getKeyName(), "password");
            testSess = this.fact.getSession(props);
            assertSecureSession(testSess, user);
            testSess.close();
            props.setProperty(PropertyKey.xdevapiFallbackToSystemKeyStore.getKeyName(), "true");
            testSess = this.fact.getSession(props);
            assertSecureSession(testSess, user);
            testSess.close();
            props.setProperty(PropertyKey.xdevapiFallbackToSystemKeyStore.getKeyName(), "false");
            testSess = this.fact.getSession(props);
            assertSecureSession(testSess, user);
            testSess.close();
        } finally {
            System.clearProperty("javax.net.ssl.keyStore");
            System.clearProperty("javax.net.ssl.keyStoreType");
            System.clearProperty("javax.net.ssl.keyStorePassword");

            Session testSession = this.fact.getSession(this.baseUrl);
            testSession.sql("DROP USER IF EXISTS " + user).execute();
            testSession.close();
        }
    }

    /**
     * Tests fix for WL#14805, Remove support for TLS 1.0 and 1.1.
     * 
     * @throws Exception
     */
    @Test
    public void testTLSVersionRemoval() throws Exception {
        assumeTrue(supportsTestCertificates(this.session),
                "This test requires the server configured with SSL certificates from ConnectorJ/src/test/config/ssl-test-certs");

        Session sess = null;
        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.REQUIRED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

        // TS.FR.1_1. Create a Connection with the connection property tlsVersions=TLSv1.2. Assess that the connection is created successfully and it is using TLSv1.2.
        props.setProperty(PropertyKey.tlsVersions.getKeyName(), "TLSv1.2");
        sess = this.fact.getSession(props);
        assertSecureSession(sess);
        assertTlsVersion(sess, "TLSv1.2");
        sess.close();

        props.remove(PropertyKey.tlsVersions.getKeyName());
        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), "TLSv1.2");
        sess = this.fact.getSession(props);
        assertSecureSession(sess);
        assertTlsVersion(sess, "TLSv1.2");
        sess.close();

        // TS.FR.1_2. Create a Connection with the connection property enabledTLSProtocols=TLSv1.2. Assess that the connection is created successfully and it is using TLSv1.2.
        props.remove(PropertyKey.xdevapiTlsVersions.getKeyName());
        props.setProperty("enabledTLSProtocols", "TLSv1.2");
        sess = this.fact.getSession(props);
        assertSecureSession(sess);
        assertTlsVersion(sess, "TLSv1.2");
        sess.close();
        props.remove("enabledTLSProtocols");

        // TS.FR.2_1. Create a Connection with the connection property tlsCiphersuites=[valid-cipher-suite]. Assess that the connection is created successfully and it is using the cipher suite specified.
        props.setProperty(PropertyKey.tlsCiphersuites.getKeyName(), "TLS_DHE_RSA_WITH_AES_128_CBC_SHA");
        sess = this.fact.getSession(props);
        assertSecureSession(sess);
        assertSessionStatusEquals(sess, "mysqlx_ssl_cipher", "DHE-RSA-AES128-SHA");
        sess.close();

        props.remove(PropertyKey.tlsCiphersuites.getKeyName());
        props.setProperty(PropertyKey.xdevapiTlsCiphersuites.getKeyName(), "TLS_DHE_RSA_WITH_AES_128_CBC_SHA");
        sess = this.fact.getSession(props);
        assertSecureSession(sess);
        assertSessionStatusEquals(sess, "mysqlx_ssl_cipher", "DHE-RSA-AES128-SHA");
        sess.close();

        // TS.FR.2_2. Create a Connection with the connection property enabledSSLCipherSuites=[valid-cipher-suite] . Assess that the connection is created successfully and it is using the cipher suite specified.
        props.remove(PropertyKey.xdevapiTlsCiphersuites.getKeyName());
        props.setProperty("enabledSSLCipherSuites", "TLS_DHE_RSA_WITH_AES_128_CBC_SHA");
        sess = this.fact.getSession(props);
        assertSecureSession(sess);
        assertSessionStatusEquals(sess, "mysqlx_ssl_cipher", "DHE-RSA-AES128-SHA");
        sess.close();
        props.remove("enabledSSLCipherSuites");

        // TS.FR.3_1. Create a Connection with the connection property tlsVersions=TLSv1. Assess that the connection fails.
        props.setProperty(PropertyKey.tlsVersions.getKeyName(), "TLSv1");
        assertThrows(CJCommunicationsException.class, ".+TLS protocols TLSv1 and TLSv1.1 are not supported. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));

        props.remove(PropertyKey.tlsVersions.getKeyName());
        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), "TLSv1");
        assertThrows(SSLParamsException.class, "TLS protocols TLSv1 and TLSv1.1 are not supported. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));

        // TS.FR.3_2. Create a Connection with the connection property tlsVersions=TLSv1.1. Assess that the connection fails.
        props.remove(PropertyKey.xdevapiTlsVersions.getKeyName());
        props.setProperty(PropertyKey.tlsVersions.getKeyName(), "TLSv1.1");
        assertThrows(CJCommunicationsException.class, ".+TLS protocols TLSv1 and TLSv1.1 are not supported. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));

        props.remove(PropertyKey.tlsVersions.getKeyName());
        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), "TLSv1.1");
        assertThrows(SSLParamsException.class, "TLS protocols TLSv1 and TLSv1.1 are not supported. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));
        props.remove(PropertyKey.xdevapiTlsVersions.getKeyName());

        // TS.FR.3_3. Create a Connection with the connection property enabledTLSProtocols=TLSv1. Assess that the connection fails.
        props.setProperty("enabledTLSProtocols", "TLSv1");
        assertThrows(CJCommunicationsException.class, ".+TLS protocols TLSv1 and TLSv1.1 are not supported. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));
        props.remove("enabledTLSProtocols");

        // TS.FR.3_4. Create a Connection with the connection property enabledTLSProtocols=TLSv1.1. Assess that the connection fails.
        props.setProperty("enabledTLSProtocols", "TLSv1.1");
        assertThrows(CJCommunicationsException.class, ".+TLS protocols TLSv1 and TLSv1.1 are not supported. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));
        props.remove("enabledTLSProtocols");

        // TS.FR.4. Create a Connection with the connection property tlsVersions=TLSv1 and sslMode=DISABLED. Assess that the connection is created successfully and it is not using encryption.
        props.setProperty(PropertyKey.tlsVersions.getKeyName(), "TLSv1");
        props.setProperty(PropertyKey.xdevapiSslMode.getKeyName(), SslMode.DISABLED.name());
        sess = this.fact.getSession(props);
        assertNonSecureSession(sess);
        sess.close();
        props.remove(PropertyKey.tlsVersions.getKeyName());

        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), "TLSv1");
        assertThrows(WrongArgumentException.class,
                "Option '" + PropertyKey.xdevapiTlsVersions.getKeyName() + "' can not be specified when SSL connections are disabled.",
                () -> this.fact.getSession(props));

        props.remove(PropertyKey.xdevapiTlsVersions.getKeyName());
        props.remove(PropertyKey.xdevapiSslMode.getKeyName());

        // TS.FR.5_1. Create a Connection with the connection property tlsVersions=FOO,BAR.
        //            Assess that the connection fails with the error message "Specified list of TLS versions only contains non valid TLS protocols. Accepted values are TLSv1.2 and TLSv1.3."
        props.setProperty(PropertyKey.tlsVersions.getKeyName(), "FOO,BAR");
        assertThrows(CJCommunicationsException.class,
                ".+Specified list of TLS versions only contains non valid TLS protocols. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));

        props.setProperty(PropertyKey.tlsVersions.getKeyName(), "FOO,,,BAR");
        assertThrows(CJCommunicationsException.class,
                ".+Specified list of TLS versions only contains non valid TLS protocols. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));

        props.remove(PropertyKey.tlsVersions.getKeyName());

        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), "FOO,BAR");
        assertThrows(SSLParamsException.class,
                "Specified list of TLS versions only contains non valid TLS protocols. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));

        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), "FOO,,,BAR");
        assertThrows(SSLParamsException.class,
                "Specified list of TLS versions only contains non valid TLS protocols. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));

        props.remove(PropertyKey.xdevapiTlsVersions.getKeyName());

        // TS.FR.5_2. Create a Connection with the connection property tlsVersions=FOO,TLSv1.1.
        //            Assess that the connection fails with the error message "TLS protocols TLSv1 and TLSv1.1 are not supported. Accepted values are TLSv1.2 and TLSv1.3."
        props.setProperty(PropertyKey.tlsVersions.getKeyName(), "FOO,TLSv1.1");
        assertThrows(CJCommunicationsException.class, ".+TLS protocols TLSv1 and TLSv1.1 are not supported. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));

        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), "FOO,TLSv1.1");
        assertThrows(SSLParamsException.class, "TLS protocols TLSv1 and TLSv1.1 are not supported. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));
        props.remove(PropertyKey.xdevapiTlsVersions.getKeyName());

        // TS.FR.5_3. Create a Connection with the connection property tlsVersions=TLSv1,TLSv1.1.
        //            Assess that the connection fails with the error message "TLS protocols TLSv1 and TLSv1.1 are not supported. Accepted values are TLSv1.2 and TLSv1.3."
        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), "TLSv1,TLSv1.1");
        assertThrows(SSLParamsException.class, "TLS protocols TLSv1 and TLSv1.1 are not supported. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));
        props.remove(PropertyKey.xdevapiTlsVersions.getKeyName());

        // TS.FR.6. Create a Connection with the connection property tlsVersions= (empty value).
        //          Assess that the connection fails with the error message "Specified list of TLS versions is empty. Accepted values are TLSv1.2 and TLSv13."
        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), "");
        assertThrows(SSLParamsException.class, "Specified list of TLS versions is empty. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));

        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), "   ");
        assertThrows(SSLParamsException.class, "Specified list of TLS versions is empty. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));

        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), ",,,");
        assertThrows(SSLParamsException.class, "Specified list of TLS versions is empty. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));

        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), ",  ,,");
        assertThrows(SSLParamsException.class, "Specified list of TLS versions is empty. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));

        props.remove(PropertyKey.xdevapiTlsVersions.getKeyName());

        // TS.FR.7. Create a Connection with the connection property tlsVersions=FOO,TLSv1,TLSv1.1,TLSv1.2.
        //          Assess that the connection is created successfully and it is using TLSv1.2.
        props.setProperty(PropertyKey.tlsVersions.getKeyName(), "FOO,TLSv1,TLSv1.1,TLSv1.2");
        sess = this.fact.getSession(props);
        assertSecureSession(sess);
        assertTlsVersion(sess, "TLSv1.2");
        sess.close();

        props.remove(PropertyKey.tlsVersions.getKeyName());
        props.setProperty(PropertyKey.xdevapiTlsVersions.getKeyName(), "FOO,TLSv1,TLSv1.1,TLSv1.2");
        sess = this.fact.getSession(props);
        assertSecureSession(sess);
        assertTlsVersion(sess, "TLSv1.2");
        sess.close();
        props.remove(PropertyKey.xdevapiTlsVersions.getKeyName());

        // TS.FR.9_1. Create an X DevAPI session with the property tlsVersions=TLSv1,TLSv1.1.
        //            Assess that the operation fails with the error message TLS protocols TLSv1 and TLSv1.1 are not supported. Accepted values are TLSv1.2 and TLSv1.3.
        props.setProperty(PropertyKey.tlsVersions.getKeyName(), "TLSv1,TLSv1.1");
        assertThrows(CJCommunicationsException.class, ".+TLS protocols TLSv1 and TLSv1.1 are not supported. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));

        // TS.FR.9_2. Create an Connection X DevAPI session with the property tlsVersions= (empty value).
        //            Assess that the operation fails with the error message Specified list of TLS versions is empty. Accepted values are TLSv1.2 and TLSv13.
        props.setProperty(PropertyKey.tlsVersions.getKeyName(), "");
        assertThrows(CJCommunicationsException.class, ".+Specified list of TLS versions is empty. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));

        props.setProperty(PropertyKey.tlsVersions.getKeyName(), "   ");
        assertThrows(CJCommunicationsException.class, ".+Specified list of TLS versions is empty. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));

        props.setProperty(PropertyKey.tlsVersions.getKeyName(), ",,,");
        assertThrows(CJCommunicationsException.class, ".+Specified list of TLS versions is empty. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));

        props.setProperty(PropertyKey.tlsVersions.getKeyName(), ",  ,,");
        assertThrows(CJCommunicationsException.class, ".+Specified list of TLS versions is empty. Accepted values are TLSv1.2 and TLSv1.3.+",
                () -> this.fact.getSession(props));
        props.remove(PropertyKey.tlsVersions.getKeyName());

        // TS.FR.10. Create an X DevAPI session with the property tlsVersions=TLSv1.2 and xdevapi.ssl-mode=DISABLED.
        //           Assess that the session is created successfully and it is not using encryption.
        props.setProperty(PropertyKey.tlsVersions.getKeyName(), "TLSv1.2");
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        sess = this.fact.getSession(props);
        assertNonSecureSession(sess);
        sess.close();
    }
}
