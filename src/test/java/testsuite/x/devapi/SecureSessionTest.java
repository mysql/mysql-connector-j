/*
 * Copyright (c) 2017, 2018, Oracle and/or its affiliates. All rights reserved.
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

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.Properties;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.mysql.cj.CoreSession;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyDefinitions.AuthMech;
import com.mysql.cj.conf.PropertyDefinitions.PropertyKey;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.protocol.x.XAuthenticationProvider;
import com.mysql.cj.protocol.x.XProtocol;
import com.mysql.cj.protocol.x.XProtocolError;
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
    final Properties sslFreeTestPropertiesOpenSSL = (Properties) this.testPropertiesOpenSSL.clone();
    String sslFreeBaseUrl = this.baseUrl;

    @Before
    public void setupSecureSessionTest() {
        if (setupTestSession()) {
            System.clearProperty("javax.net.ssl.trustStore");
            System.clearProperty("javax.net.ssl.trustStoreType");
            System.clearProperty("javax.net.ssl.trustStorePassword");

            this.sslFreeTestProperties.remove(PropertyDefinitions.PNAME_sslMode);
            this.sslFreeTestProperties.remove(PropertyDefinitions.PNAME_sslTrustStoreUrl);
            this.sslFreeTestProperties.remove(PropertyDefinitions.PNAME_sslTrustStorePassword);
            this.sslFreeTestProperties.remove(PropertyDefinitions.PNAME_sslTrustStoreType);

            this.sslFreeBaseUrl = this.baseUrl;
            this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyDefinitions.PNAME_sslMode + "=", PropertyDefinitions.PNAME_sslMode + "VOID=");
            this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyDefinitions.PNAME_sslTrustStoreUrl + "=",
                    PropertyDefinitions.PNAME_sslTrustStoreUrl + "VOID=");
            this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyDefinitions.PNAME_sslTrustStorePassword + "=",
                    PropertyDefinitions.PNAME_sslTrustStorePassword + "VOID=");
            this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyDefinitions.PNAME_sslTrustStoreType + "=",
                    PropertyDefinitions.PNAME_sslTrustStoreType + "VOID=");
            if (!this.sslFreeBaseUrl.contains("?")) {
                this.sslFreeBaseUrl += "?";
            }
        }
        if (this.isSetForOpensslXTests) {
            this.sslFreeTestPropertiesOpenSSL.remove(PropertyDefinitions.PNAME_sslMode);
            this.sslFreeTestPropertiesOpenSSL.remove(PropertyDefinitions.PNAME_sslTrustStoreUrl);
            this.sslFreeTestPropertiesOpenSSL.remove(PropertyDefinitions.PNAME_sslTrustStorePassword);
            this.sslFreeTestPropertiesOpenSSL.remove(PropertyDefinitions.PNAME_sslTrustStoreType);
        }
    }

    @After
    public void teardownSecureSessionTest() {
        System.clearProperty("javax.net.ssl.trustStore");
        System.clearProperty("javax.net.ssl.trustStoreType");
        System.clearProperty("javax.net.ssl.trustStorePassword");
        destroyTestSession();
    }

    /**
     * Tests non-secure {@link Session}s created via URL and properties map.
     */
    @Test
    public void testNonSecureSession() {
        if (!this.isSetForXTests) {
            return;
        }
        try {
            Session testSession = this.fact.getSession(this.baseUrl);
            testSession.sql("CREATE USER IF NOT EXISTS 'testPlainAuth'@'%' IDENTIFIED WITH mysql_native_password BY 'pwd'").execute();
            testSession.sql("GRANT SELECT ON *.* TO 'testPlainAuth'@'%'").execute();
            testSession.close();

            String userAndSslFreeBaseUrl = this.sslFreeBaseUrl;
            userAndSslFreeBaseUrl = userAndSslFreeBaseUrl.replaceAll(getTestUser() + ":" + getTestPassword() + "@", "");
            userAndSslFreeBaseUrl = userAndSslFreeBaseUrl.replaceAll(PropertyKey.USER.getKeyName() + "=", PropertyKey.USER.getKeyName() + "VOID=");
            userAndSslFreeBaseUrl = userAndSslFreeBaseUrl.replaceAll(PropertyKey.PASSWORD.getKeyName() + "=", PropertyKey.PASSWORD.getKeyName() + "VOID=");

            testSession = this.fact.getSession(userAndSslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.DISABLED)
                    + makeParam(PropertyKey.USER.getKeyName(), "testPlainAuth") + makeParam(PropertyKey.PASSWORD.getKeyName(), "pwd"));
            assertNonSecureSession(testSession);
            testSession.close();

            testSession = this.fact.getSession(userAndSslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.DISABLED)
                    + makeParam(PropertyKey.USER.getKeyName(), "testPlainAuth") + makeParam(PropertyKey.PASSWORD.getKeyName(), "pwd")
                    + makeParam(PropertyDefinitions.PNAME_useAsyncProtocol, "true"));
            assertNonSecureSession(testSession);
            testSession.close();

            Properties props = new Properties(this.sslFreeTestProperties);
            props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.DISABLED.toString());
            props.setProperty(PropertyKey.USER.getKeyName(), "testPlainAuth");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "pwd");
            testSession = this.fact.getSession(props);
            assertNonSecureSession(testSession);
            testSession.close();

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
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
        if (!this.isSetForXTests) {
            return;
        }

        Session testSession = this.fact.getSession(this.sslFreeBaseUrl);
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_useAsyncProtocol, "true"));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED)
                + makeParam(PropertyDefinitions.PNAME_useAsyncProtocol, "true"));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeTestProperties);
        assertSecureSession(testSession);
        testSession.close();

        this.sslFreeTestProperties.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
        testSession = this.fact.getSession(this.sslFreeTestProperties);
        assertSecureSession(testSession);
        testSession.close();
        this.sslFreeTestProperties.remove(PropertyDefinitions.PNAME_useAsyncProtocol);

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED.toString());
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();

        props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();
    }

    /**
     * Tests secure {@link Session}s created via URL and properties map, with the SSL system properties also defined.
     */
    @Test
    public void testSecureSessionDefaultAndRequiredWithSystemPropsPresent() {
        if (!this.isSetForXTests) {
            return;
        }

        System.setProperty("javax.net.ssl.trustStore", this.trustStorePath);
        System.setProperty("javax.net.ssl.trustStorePassword", this.trustStorePassword);

        Session testSession = this.fact.getSession(this.sslFreeBaseUrl);
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_useAsyncProtocol, "true"));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED)
                + makeParam(PropertyDefinitions.PNAME_useAsyncProtocol, "true"));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeTestProperties);
        assertSecureSession(testSession);
        testSession.close();

        this.sslFreeTestProperties.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
        testSession = this.fact.getSession(this.sslFreeTestProperties);
        assertSecureSession(testSession);
        testSession.close();
        this.sslFreeTestProperties.remove(PropertyDefinitions.PNAME_useAsyncProtocol);

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED.toString());
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();

        props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();
    }

    /**
     * Tests secure {@link Session}s created via URL and properties map, verifying server certificate.
     */
    @Test
    public void testSecureSessionVerifyServerCertificate() {
        if (!this.isSetForXTests) {
            return;
        }

        Session testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_CA)
                + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl)
                + makeParam(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_CA)
                + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl)
                + makeParam(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword)
                + makeParam(PropertyDefinitions.PNAME_useAsyncProtocol, "true"));
        assertSecureSession(testSession);
        testSession.close();

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_CA.toString());
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl);
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword);
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();

        props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();
    }

    /**
     * Tests secure {@link Session}s created via URL and properties map combined with SSL system properties, verifying server certificate.
     */
    @Test
    public void testSecureSessionVerifyServerCertificateUsingSystemProps() {
        if (!this.isSetForXTests) {
            return;
        }

        System.setProperty("javax.net.ssl.trustStore", this.trustStorePath);
        System.setProperty("javax.net.ssl.trustStorePassword", this.trustStorePassword);

        Session testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_CA));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_CA)
                + makeParam(PropertyDefinitions.PNAME_useAsyncProtocol, "true"));
        assertSecureSession(testSession);
        testSession.close();

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_CA.toString());
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();

        props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();
    }

    /**
     * Tests secure {@link Session}s created via URL and properties map, verifying server certificate.
     * This test would pass if the server certificate had "CN=<host_name>", with <host_name> equals to the host name in the test URL.
     */
    @Test
    @Ignore
    public void testSecureSessionVerifyServerCertificateIdentity() {
        if (!this.isSetForXTests) {
            return;
        }

        Session testSession = this.fact
                .getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_IDENTITY)
                        + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl)
                        + makeParam(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword)
                        + makeParam(PropertyDefinitions.PNAME_useAsyncProtocol, "true"));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_IDENTITY)
                + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl)
                + makeParam(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_IDENTITY)
                + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl)
                + makeParam(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword)
                + makeParam(PropertyDefinitions.PNAME_useAsyncProtocol, "true"));
        assertSecureSession(testSession);
        testSession.close();

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_IDENTITY.toString());
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl);
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword);
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();

        props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();
    }

    /**
     * Tests exception thrown on missing truststore for a secure {@link Session}.
     */
    @Test
    public void testSecureSessionMissingTrustStore() {
        if (!this.isSetForXTests) {
            return;
        }

        assertThrows(CJCommunicationsException.class, "No truststore provided to verify the Server certificate\\.",
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_CA)));

        assertThrows(CJCommunicationsException.class, "No truststore provided to verify the Server certificate\\.",
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_CA)
                        + makeParam(PropertyDefinitions.PNAME_useAsyncProtocol, "true")));

        assertThrows(CJCommunicationsException.class, "No truststore provided to verify the Server certificate\\.",
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_IDENTITY)));

        assertThrows(CJCommunicationsException.class, "No truststore provided to verify the Server certificate\\.",
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_IDENTITY)
                        + makeParam(PropertyDefinitions.PNAME_useAsyncProtocol, "true")));

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_CA.toString());
        assertThrows(CJCommunicationsException.class, "No truststore provided to verify the Server certificate\\.", () -> this.fact.getSession(props));

        props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
        assertThrows(CJCommunicationsException.class, "No truststore provided to verify the Server certificate\\.", () -> this.fact.getSession(props));

        props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_IDENTITY.toString());
        assertThrows(CJCommunicationsException.class, "No truststore provided to verify the Server certificate\\.", () -> this.fact.getSession(props));

        props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_IDENTITY.toString());
        assertThrows(CJCommunicationsException.class, "No truststore provided to verify the Server certificate\\.", () -> this.fact.getSession(props));
    }

    /**
     * Tests exception thrown on verifying server certificate identity failure.
     * The server certificate used in this test has "CN=MySQL Connector/J Server".
     */
    @Test
    public void testSecureSessionVerifyServerCertificateIdentityFailure() {
        if (!this.isSetForXTests) {
            return;
        }

        // Meaningful error message is deep inside the stack trace.
        assertThrows(CJCommunicationsException.class,
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_IDENTITY)
                        + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl)
                        + makeParam(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword)));

        assertThrows(CJCommunicationsException.class,
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_IDENTITY)
                        + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl)
                        + makeParam(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword)
                        + makeParam(PropertyDefinitions.PNAME_useAsyncProtocol, "true")));

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_IDENTITY.toString());
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl);
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword);
        // Meaningful error message is deep inside the stack trace.
        assertThrows(CJCommunicationsException.class, () -> this.fact.getSession(props));

        props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
        assertThrows(CJCommunicationsException.class, () -> this.fact.getSession(props));
    }

    /**
     * Tests exception thrown on incompatible settings for a secure {@link Session}.
     */
    @Test
    public void testSecureSessionIncompatibleSettings() {
        if (!this.isSetForXTests) {
            return;
        }

        String expectedError = "Incompatible security settings\\. "
                + "The property 'xdevapi.ssl-truststore' requires 'xdevapi.ssl-mode' as 'VERIFY_CA' or 'VERIFY_IDENTITY'\\.";
        assertThrows(CJCommunicationsException.class, expectedError,
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl)));

        assertThrows(CJCommunicationsException.class, expectedError, () -> this.fact.getSession(this.sslFreeBaseUrl
                + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl) + makeParam(PropertyDefinitions.PNAME_useAsyncProtocol, "true")));

        assertThrows(CJCommunicationsException.class, expectedError,
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED)
                        + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl)));

        assertThrows(CJCommunicationsException.class, expectedError,
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED)
                        + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl)
                        + makeParam(PropertyDefinitions.PNAME_useAsyncProtocol, "true")));

        assertThrows(CJCommunicationsException.class, expectedError,
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.DISABLED)
                        + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl)));

        assertThrows(CJCommunicationsException.class, expectedError,
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.DISABLED)
                        + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl)
                        + makeParam(PropertyDefinitions.PNAME_useAsyncProtocol, "true")));

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl);
        assertThrows(CJCommunicationsException.class, expectedError, () -> this.fact.getSession(props));

        props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
        assertThrows(CJCommunicationsException.class, expectedError, () -> this.fact.getSession(props));

        props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED.toString());
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl);
        assertThrows(CJCommunicationsException.class, expectedError, () -> this.fact.getSession(props));

        props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
        assertThrows(CJCommunicationsException.class, expectedError, () -> this.fact.getSession(props));

        props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.DISABLED.toString());
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl);
        assertThrows(CJCommunicationsException.class, expectedError, () -> this.fact.getSession(props));

        props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
        assertThrows(CJCommunicationsException.class, expectedError, () -> this.fact.getSession(props));
    }

    /**
     * Tests that PLAIN, MYSQL41, SHA256_MEMORY, and EXTERNAL authentication mechanisms.
     * 
     * @throws Throwable
     */
    @Test
    public void testAuthMechanisms() throws Throwable {
        if (!this.isSetForXTests) {
            return;
        }

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

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
            testSession = this.fact.getSession(props);
            assertSecureSession(testSession);
            assertEquals(AuthMech.PLAIN, getAuthMech.apply(testSession)); // Connection is secure, passwords are safe & account gets cached.
            assertUser("testAuthMechNative", testSession);
            testSession.close();

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
            testSession = this.fact.getSession(props);
            assertSecureSession(testSession);
            assertEquals(AuthMech.PLAIN, getAuthMech.apply(testSession)); // Connection is secure, passwords are safe & account gets cached.
            assertUser("testAuthMechNative", testSession);
            testSession.close();

            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3"))) {
                // *** User: testAuthMechSha256; Auth: default.
                props.setProperty(PropertyKey.USER.getKeyName(), "testAuthMechSha256");
                props.setProperty(PropertyKey.PASSWORD.getKeyName(), "sha256");

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
                testSession = this.fact.getSession(props);
                assertSecureSession(testSession);
                assertEquals(AuthMech.PLAIN, getAuthMech.apply(testSession)); // Connection is secure, passwords are safe & account gets cached.
                assertUser("testAuthMechSha256", testSession);
                testSession.close();

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
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

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
                testSession = this.fact.getSession(props);
                assertSecureSession(testSession);
                assertEquals(AuthMech.PLAIN, getAuthMech.apply(testSession)); // Connection is secure, passwords are safe & account gets cached.
                assertUser("testAuthMechCachingSha2", testSession);
                testSession.close();

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
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
            props.setProperty(PropertyDefinitions.PNAME_auth, "PLAIN");

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
            testSession = this.fact.getSession(props);
            assertSecureSession(testSession);
            assertEquals(AuthMech.PLAIN, getAuthMech.apply(testSession)); // Connection is secure, passwords are safe.
            assertUser("testAuthMechNative", testSession);
            testSession.close();

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
            testSession = this.fact.getSession(props);
            assertSecureSession(testSession);
            assertEquals(AuthMech.PLAIN, getAuthMech.apply(testSession)); // Connection is secure, passwords are safe.
            assertUser("testAuthMechNative", testSession);
            testSession.close();

            // *** User: testAuthMechNative; Auth: MYSQL41.
            props.setProperty(PropertyDefinitions.PNAME_auth, "MYSQL41");

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
            testSession = this.fact.getSession(props);
            assertSecureSession(testSession);
            assertEquals(AuthMech.MYSQL41, getAuthMech.apply(testSession)); // Matching auth mechanism.
            assertUser("testAuthMechNative", testSession);
            testSession.close();

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
            testSession = this.fact.getSession(props);
            assertSecureSession(testSession);
            assertEquals(AuthMech.MYSQL41, getAuthMech.apply(testSession)); // Matching auth mechanism.
            assertUser("testAuthMechNative", testSession);
            testSession.close();

            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4"))) { // SHA256_MEMORY support added in MySQL 8.0.4.
                // *** User: testAuthMechNative; Auth: SHA256_MEMORY.
                props.setProperty(PropertyDefinitions.PNAME_auth, "SHA256_MEMORY");

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
                testSession = this.fact.getSession(props);
                assertSecureSession(testSession);
                assertEquals(AuthMech.SHA256_MEMORY, getAuthMech.apply(testSession)); // Account is cached by now.
                assertUser("testAuthMechNative", testSession);
                testSession.close();

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
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
                props.setProperty(PropertyDefinitions.PNAME_auth, "PLAIN");

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
                testSession = this.fact.getSession(props);
                assertSecureSession(testSession);
                assertEquals(AuthMech.PLAIN, getAuthMech.apply(testSession)); // Connection is secure, passwords are safe.
                assertUser("testAuthMechSha256", testSession);
                testSession.close();

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
                testSession = this.fact.getSession(props);
                assertSecureSession(testSession);
                assertEquals(AuthMech.PLAIN, getAuthMech.apply(testSession)); // Connection is secure, passwords are safe.
                assertUser("testAuthMechSha256", testSession);
                testSession.close();

                // *** User: testAuthMechSha256; Auth: MYSQL41.
                props.setProperty(PropertyDefinitions.PNAME_auth, "MYSQL41");
                if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.12"))) {
                    accessDeniedErrMsg = "ERROR 1045 \\(HY000\\) Access denied for user 'testAuthMechSha256'@.*";
                }

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
                assertThrows(XProtocolError.class, accessDeniedErrMsg, () -> this.fact.getSession(props)); // Auth mech mismatch.

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
                assertThrows(XProtocolError.class, accessDeniedErrMsg, () -> this.fact.getSession(props)); // Auth mech mismatch.

                if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4"))) { // SHA256_MEMORY support added in MySQL 8.0.4.
                    // *** User: testAuthMechSha256; Auth: SHA256_MEMORY.
                    props.setProperty(PropertyDefinitions.PNAME_auth, "SHA256_MEMORY");

                    props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
                    testSession = this.fact.getSession(props);
                    assertSecureSession(testSession);
                    assertEquals(AuthMech.SHA256_MEMORY, getAuthMech.apply(testSession)); // Account is cached by now.
                    assertUser("testAuthMechSha256", testSession);
                    testSession.close();

                    props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
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
                props.setProperty(PropertyDefinitions.PNAME_auth, "PLAIN");

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
                testSession = this.fact.getSession(props);
                assertSecureSession(testSession);
                assertEquals(AuthMech.PLAIN, getAuthMech.apply(testSession)); // Connection is secure, passwords are safe.
                assertUser("testAuthMechCachingSha2", testSession);
                testSession.close();

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
                testSession = this.fact.getSession(props);
                assertSecureSession(testSession);
                assertEquals(AuthMech.PLAIN, getAuthMech.apply(testSession)); // Connection is secure, passwords are safe.
                assertUser("testAuthMechCachingSha2", testSession);
                testSession.close();

                // *** User: testAuthMechCachingSha2; Auth: MYSQL41.
                props.setProperty(PropertyDefinitions.PNAME_auth, "MYSQL41");
                if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.12"))) {
                    accessDeniedErrMsg = "ERROR 1045 \\(HY000\\) Access denied for user 'testAuthMechCachingSha2'@.*";
                }

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
                assertThrows(XProtocolError.class, accessDeniedErrMsg, () -> this.fact.getSession(props)); // Auth mech mismatch.

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
                assertThrows(XProtocolError.class, accessDeniedErrMsg, () -> this.fact.getSession(props)); // Auth mech mismatch.

                if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4"))) { // SHA256_MEMORY support added in MySQL 8.0.4.
                    // User: testAuthMechCachingSha2; Auth: SHA256_MEMORY.
                    props.setProperty(PropertyDefinitions.PNAME_auth, "SHA256_MEMORY");

                    props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
                    testSession = this.fact.getSession(props);
                    assertSecureSession(testSession);
                    assertEquals(AuthMech.SHA256_MEMORY, getAuthMech.apply(testSession)); // Account is cached by now.
                    assertUser("testAuthMechCachingSha2", testSession);
                    testSession.close();

                    props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
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
            props.setProperty(PropertyDefinitions.PNAME_auth, "EXTERNAL");

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
            assertThrows(XProtocolError.class, "ERROR 1251 \\(HY000\\) Invalid authentication method EXTERNAL", () -> this.fact.getSession(props));

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
            assertThrows(XProtocolError.class, "ERROR 1251 \\(HY000\\) Invalid authentication method EXTERNAL", () -> this.fact.getSession(props));

            props.remove(PropertyDefinitions.PNAME_auth);

            /*
             * Authenticate using non-secure connections.
             */
            props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.DISABLED.toString());

            // With default auth mechanism for non-secure connections (MYSQL41|SHA2_MEMORY).

            // *** User: testAuthMechNative; Auth: default.
            props.setProperty(PropertyKey.USER.getKeyName(), "testAuthMechNative");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "mysqlnative");

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
            testSession = this.fact.getSession(props);
            assertNonSecureSession(testSession);
            assertEquals(AuthMech.MYSQL41, getAuthMech.apply(testSession)); // Matching auth mechanism.
            assertUser("testAuthMechNative", testSession);
            testSession.close();

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
            testSession = this.fact.getSession(props);
            assertNonSecureSession(testSession);
            assertEquals(AuthMech.MYSQL41, getAuthMech.apply(testSession)); // Matching auth mechanism.
            assertUser("testAuthMechNative", testSession);
            testSession.close();

            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4"))) { // SHA256_PASSWORD requires secure connections in MySQL 8.0.3 and below.
                // *** User: testAuthMechSha256; Auth: default.
                props.setProperty(PropertyKey.USER.getKeyName(), "testAuthMechSha256");
                props.setProperty(PropertyKey.PASSWORD.getKeyName(), "sha256");

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
                testSession = this.fact.getSession(props);
                assertNonSecureSession(testSession);
                assertEquals(AuthMech.SHA256_MEMORY, getAuthMech.apply(testSession)); // Account is cached by now.
                assertUser("testAuthMechSha256", testSession);
                testSession.close();

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
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

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
                testSession = this.fact.getSession(props);
                assertNonSecureSession(testSession);
                assertEquals(AuthMech.SHA256_MEMORY, getAuthMech.apply(testSession)); // Account is cached by now.
                assertUser("testAuthMechCachingSha2", testSession);
                testSession.close();

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
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
            props.setProperty(PropertyDefinitions.PNAME_auth, "PLAIN");

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
            assertThrows(XProtocolError.class, "PLAIN authentication is not allowed via unencrypted connection\\.", () -> this.fact.getSession(props));

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
            assertThrows(XProtocolError.class, "PLAIN authentication is not allowed via unencrypted connection\\.", () -> this.fact.getSession(props));

            // *** User: testAuthMechNative; Auth: MYSQL41.
            props.setProperty(PropertyDefinitions.PNAME_auth, "MYSQL41");

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
            testSession = this.fact.getSession(props);
            assertNonSecureSession(testSession);
            assertEquals(AuthMech.MYSQL41, getAuthMech.apply(testSession)); // Matching auth mechanism.
            assertUser("testAuthMechNative", testSession);
            testSession.close();

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
            testSession = this.fact.getSession(props);
            assertNonSecureSession(testSession);
            assertEquals(AuthMech.MYSQL41, getAuthMech.apply(testSession)); // Matching auth mechanism.
            assertUser("testAuthMechNative", testSession);
            testSession.close();

            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4"))) { // SHA256_MEMORY support added in MySQL 8.0.4.
                // *** User: testAuthMechNative; Auth: SHA256_MEMORY.
                props.setProperty(PropertyDefinitions.PNAME_auth, "SHA256_MEMORY");

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
                testSession = this.fact.getSession(props);
                assertNonSecureSession(testSession);
                assertEquals(AuthMech.SHA256_MEMORY, getAuthMech.apply(testSession)); // Account is cached by now.
                assertUser("testAuthMechNative", testSession);
                testSession.close();

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
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
                props.setProperty(PropertyDefinitions.PNAME_auth, "PLAIN");

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
                assertThrows(XProtocolError.class, "PLAIN authentication is not allowed via unencrypted connection\\.", () -> this.fact.getSession(props));

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
                assertThrows(XProtocolError.class, "PLAIN authentication is not allowed via unencrypted connection\\.", () -> this.fact.getSession(props));

                // *** User: testAuthMechSha256; Auth: MYSQL41.
                props.setProperty(PropertyDefinitions.PNAME_auth, "MYSQL41");
                if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.12"))) {
                    accessDeniedErrMsg = "ERROR 1045 \\(HY000\\) Access denied for user 'testAuthMechSha256'@.*";
                }

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
                assertThrows(XProtocolError.class, accessDeniedErrMsg, () -> this.fact.getSession(props)); // Auth mech mismatch.

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
                assertThrows(XProtocolError.class, accessDeniedErrMsg, () -> this.fact.getSession(props)); // Auth mech mismatch.

                if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4"))) { // SHA256_MEMORY support added in MySQL 8.0.4.
                    // *** User: testAuthMechSha256; Auth: SHA256_MEMORY.
                    props.setProperty(PropertyDefinitions.PNAME_auth, "SHA256_MEMORY");

                    props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
                    testSession = this.fact.getSession(props);
                    assertNonSecureSession(testSession);
                    assertEquals(AuthMech.SHA256_MEMORY, getAuthMech.apply(testSession)); // Account is cached by now.
                    assertUser("testAuthMechSha256", testSession);
                    testSession.close();

                    props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
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
                props.setProperty(PropertyDefinitions.PNAME_auth, "PLAIN");

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
                assertThrows(XProtocolError.class, "PLAIN authentication is not allowed via unencrypted connection\\.", () -> this.fact.getSession(props));

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
                assertThrows(XProtocolError.class, "PLAIN authentication is not allowed via unencrypted connection\\.", () -> this.fact.getSession(props));

                // *** User: testAuthMechCachingSha2; Auth: MYSQL41.
                props.setProperty(PropertyDefinitions.PNAME_auth, "MYSQL41");
                if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.12"))) {
                    accessDeniedErrMsg = "ERROR 1045 \\(HY000\\) Access denied for user 'testAuthMechCachingSha2'@.*";
                }

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
                assertThrows(XProtocolError.class, accessDeniedErrMsg, () -> this.fact.getSession(props)); // Auth mech mismatch.

                props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
                assertThrows(XProtocolError.class, accessDeniedErrMsg, () -> this.fact.getSession(props)); // Auth mech mismatch.

                if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4"))) { // SHA256_MEMORY support added in MySQL 8.0.4.
                    // *** User: testAuthMechCachingSha2; Auth: SHA256_MEMORY.
                    props.setProperty(PropertyDefinitions.PNAME_auth, "SHA256_MEMORY");

                    props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
                    testSession = this.fact.getSession(props);
                    assertNonSecureSession(testSession);
                    assertEquals(AuthMech.SHA256_MEMORY, getAuthMech.apply(testSession)); // Account is cached by now.
                    assertUser("testAuthMechCachingSha2", testSession);
                    testSession.close();

                    props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
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
            props.setProperty(PropertyDefinitions.PNAME_auth, "EXTERNAL");

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
            assertThrows(XProtocolError.class, "ERROR 1251 \\(HY000\\) Invalid authentication method EXTERNAL", () -> this.fact.getSession(props));

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
            assertThrows(XProtocolError.class, "ERROR 1251 \\(HY000\\) Invalid authentication method EXTERNAL", () -> this.fact.getSession(props));

            props.remove(PropertyDefinitions.PNAME_auth);
        } finally {
            this.session.sql("DROP USER IF EXISTS testAuthMechNative").execute();
            this.session.sql("DROP USER IF EXISTS testAuthMechSha256").execute();
            this.session.sql("DROP USER IF EXISTS testAuthMechCachingSha2").execute();
        }
    }

    /**
     * Tests TLSv1.2
     * 
     * This test requires two server instances:
     * 1) main xplugin server pointed to by the com.mysql.cj.testsuite.mysqlx.url variable,
     * compiled with yaSSL
     * 2) additional xplugin server instance pointed to by com.mysql.cj.testsuite.mysqlx.url.openssl,
     * variable compiled with OpenSSL.
     * 
     * For example, add these variables to the ant call:
     * -Dcom.mysql.cj.testsuite.mysqlx.url=mysqlx://localhost:33060/cjtest_5_1?user=root&password=pwd
     * -Dcom.mysql.cj.testsuite.mysqlx.url.openssl=mysqlx://localhost:33070/cjtest_5_1?user=root&password=pwd
     */
    @Test
    public void testTLSv1_2() {
        if (!this.isSetForXTests) {
            return;
        }

        // newer GPL servers, like 8.0.4+, are using OpenSSL and can use RSA encryption, while old ones compiled with yaSSL cannot
        boolean gplWithRSA = allowsRsa(this.fact.getSession(this.sslFreeBaseUrl));

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_CA.toString());
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl);
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword);

        /* Against yaSSL server */

        // defaults to TLSv1.1
        Session testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        SqlResult rs = testSession.sql("SHOW SESSION STATUS LIKE 'mysqlx_ssl_version'").execute();
        String actual = rs.fetchOne().getString(1);
        assertEquals("TLSv1.1", actual);
        testSession.close();

        props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        rs = testSession.sql("SHOW SESSION STATUS LIKE 'mysqlx_ssl_version'").execute();
        actual = rs.fetchOne().getString(1);
        assertEquals("TLSv1.1", actual);
        testSession.close();

        // restricted to TLSv1
        props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
        props.setProperty(PropertyDefinitions.PNAME_enabledTLSProtocols, "TLSv1");
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        rs = testSession.sql("SHOW SESSION STATUS LIKE 'mysqlx_ssl_version'").execute();
        actual = rs.fetchOne().getString(1);
        assertEquals("TLSv1", actual);
        testSession.close();

        props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        rs = testSession.sql("SHOW SESSION STATUS LIKE 'mysqlx_ssl_version'").execute();
        actual = rs.fetchOne().getString(1);
        assertEquals("TLSv1", actual);
        testSession.close();

        // TLSv1.2 should fail
        props.setProperty(PropertyDefinitions.PNAME_enabledTLSProtocols, "TLSv1.2,TLSv1");
        if (gplWithRSA) {
            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
            testSession = this.fact.getSession(props);
            assertSecureSession(testSession);
            rs = testSession.sql("SHOW SESSION STATUS LIKE 'mysqlx_ssl_version'").execute();
            actual = rs.fetchOne().getString(1);
            assertEquals("TLSv1.2", actual);
            testSession.close();

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
            testSession = this.fact.getSession(props);
            assertSecureSession(testSession);
            rs = testSession.sql("SHOW SESSION STATUS LIKE 'mysqlx_ssl_version'").execute();
            actual = rs.fetchOne().getString(1);
            assertEquals("TLSv1.2", actual);
            testSession.close();
        } else {
            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
            assertThrows(CJCommunicationsException.class, "javax.net.ssl.SSLHandshakeException: Remote host closed connection during handshake",
                    () -> this.fact.getSession(props));

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
            assertThrows(CJCommunicationsException.class, "Server does not provide enough data to proceed with SSL handshake.",
                    () -> this.fact.getSession(props));
        }

        /* Against OpenSSL server */
        if (this.baseOpensslUrl != null && this.baseOpensslUrl.length() > 0) {
            Properties propsOpenSSL = new Properties(this.sslFreeTestPropertiesOpenSSL);
            propsOpenSSL.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_CA.toString());
            propsOpenSSL.setProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl);
            propsOpenSSL.setProperty(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword);

            // defaults to TLSv1.1
            testSession = this.fact.getSession(propsOpenSSL);
            assertSecureSession(testSession);
            rs = testSession.sql("SHOW SESSION STATUS LIKE 'mysqlx_ssl_version'").execute();
            actual = rs.fetchOne().getString(1);
            assertEquals("TLSv1.1", actual);
            testSession.close();

            propsOpenSSL.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
            testSession = this.fact.getSession(propsOpenSSL);
            assertSecureSession(testSession);
            rs = testSession.sql("SHOW SESSION STATUS LIKE 'mysqlx_ssl_version'").execute();
            actual = rs.fetchOne().getString(1);
            assertEquals("TLSv1.1", actual);
            testSession.close();

            // restricted to TLSv1
            propsOpenSSL.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
            propsOpenSSL.setProperty(PropertyDefinitions.PNAME_enabledTLSProtocols, "TLSv1");
            testSession = this.fact.getSession(propsOpenSSL);
            assertSecureSession(testSession);
            rs = testSession.sql("SHOW SESSION STATUS LIKE 'mysqlx_ssl_version'").execute();
            actual = rs.fetchOne().getString(1);
            assertEquals("TLSv1", actual);
            testSession.close();

            propsOpenSSL.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
            testSession = this.fact.getSession(propsOpenSSL);
            assertSecureSession(testSession);
            rs = testSession.sql("SHOW SESSION STATUS LIKE 'mysqlx_ssl_version'").execute();
            actual = rs.fetchOne().getString(1);
            assertEquals("TLSv1", actual);
            testSession.close();

            // TLSv1.2
            propsOpenSSL.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
            propsOpenSSL.setProperty(PropertyDefinitions.PNAME_enabledTLSProtocols, "TLSv1.2,TLSv1.1,TLSv1");
            testSession = this.fact.getSession(propsOpenSSL);
            assertSecureSession(testSession);
            rs = testSession.sql("SHOW SESSION STATUS LIKE 'mysqlx_ssl_version'").execute();
            actual = rs.fetchOne().getString(1);
            assertEquals("TLSv1.2", actual);
            testSession.close();

            propsOpenSSL.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
            testSession = this.fact.getSession(propsOpenSSL);
            assertSecureSession(testSession);
            rs = testSession.sql("SHOW SESSION STATUS LIKE 'mysqlx_ssl_version'").execute();
            actual = rs.fetchOne().getString(1);
            assertEquals("TLSv1.2", actual);
            testSession.close();
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

    private void assertNonSecureSession(Session sess) {
        assertSessionStatusEquals(sess, "mysqlx_ssl_cipher", "");
    }

    private void assertSecureSession(Session sess) {
        assertSessionStatusNotEquals(sess, "mysqlx_ssl_cipher", "");
    }

    private String makeParam(String key, Enum<?> value) {
        return makeParam(key, value.toString());
    }

    private String makeParam(String key, String value) {
        return "&" + key + "=" + value;
    }

    /**
     * Tests fix for Bug#25494338, ENABLEDSSLCIPHERSUITES PARAMETER NOT WORKING AS EXPECTED WITH X-PLUGIN.
     */
    @Test
    public void testBug25494338() {
        if (!this.isSetForXTests) {
            return;
        }

        Session testSession = null;

        try {
            Properties props = new Properties(this.sslFreeTestProperties);
            testSession = this.fact.getSession(props);

            testSession.sql("CREATE USER 'bug25494338user'@'%' IDENTIFIED WITH mysql_native_password BY 'pwd' REQUIRE CIPHER 'AES128-SHA'").execute();
            testSession.sql("GRANT SELECT ON *.* TO 'bug25494338user'@'%'").execute();

            props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_CA.toString());
            props.setProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl);
            props.setProperty(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword);
            props.setProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreUrl, this.clientKeyStoreUrl);
            props.setProperty(PropertyDefinitions.PNAME_clientCertificateKeyStorePassword, this.clientKeyStorePassword);

            // 1. Allow only TLS_DHE_RSA_WITH_AES_128_CBC_SHA cipher
            props.setProperty(PropertyDefinitions.PNAME_enabledSSLCipherSuites, "TLS_DHE_RSA_WITH_AES_128_CBC_SHA");
            Session sess = this.fact.getSession(props);
            assertSessionStatusEquals(sess, "mysqlx_ssl_cipher", "DHE-RSA-AES128-SHA");
            sess.close();

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
            sess = this.fact.getSession(props);
            assertSessionStatusEquals(sess, "mysqlx_ssl_cipher", "DHE-RSA-AES128-SHA");
            sess.close();

            // 2. Allow only TLS_RSA_WITH_AES_128_CBC_SHA cipher
            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
            props.setProperty(PropertyDefinitions.PNAME_enabledSSLCipherSuites, "TLS_RSA_WITH_AES_128_CBC_SHA");
            sess = this.fact.getSession(props);
            assertSessionStatusEquals(sess, "mysqlx_ssl_cipher", "AES128-SHA");
            assertSessionStatusEquals(sess, "ssl_cipher", "");
            sess.close();

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
            sess = this.fact.getSession(props);
            assertSessionStatusEquals(sess, "mysqlx_ssl_cipher", "AES128-SHA");
            assertSessionStatusEquals(sess, "ssl_cipher", "");
            sess.close();

            // 3. Check connection with required client certificate 
            props.setProperty(PropertyKey.USER.getKeyName(), "bug25494338user");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "pwd");

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
            sess = this.fact.getSession(props);
            assertSessionStatusEquals(sess, "mysqlx_ssl_cipher", "AES128-SHA");
            assertSessionStatusEquals(sess, "ssl_cipher", "");
            sess.close();

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
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
        if (!this.isSetForXTests) {
            return;
        }

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_CA.toString());
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl);
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword);

        Session nSession;
        for (int i = 0; i < 100; i++) {
            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "false");
            nSession = this.fact.getSession(props);
            nSession.close();
            nSession = null;

            props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
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
        if (!this.isSetForXTests) {
            return;
        }

        System.setProperty("javax.net.ssl.trustStore", "dummy_truststore");
        System.setProperty("javax.net.ssl.trustStorePassword", "some_password");
        System.setProperty("javax.net.ssl.trustStoreType", "wrong_type");

        Session testSession = this.fact.getSession(this.sslFreeBaseUrl);
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_useAsyncProtocol, "true"));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED)
                + makeParam(PropertyDefinitions.PNAME_useAsyncProtocol, "true"));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeTestProperties);
        assertSecureSession(testSession);
        testSession.close();

        this.sslFreeTestProperties.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
        testSession = this.fact.getSession(this.sslFreeTestProperties);
        assertSecureSession(testSession);
        testSession.close();
        this.sslFreeTestProperties.remove(PropertyDefinitions.PNAME_useAsyncProtocol);

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED.toString());
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();

        props.setProperty(PropertyDefinitions.PNAME_useAsyncProtocol, "true");
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();
    }
}
