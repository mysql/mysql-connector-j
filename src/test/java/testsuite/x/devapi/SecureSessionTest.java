/*
 * Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.
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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.mysql.cj.api.xdevapi.Row;
import com.mysql.cj.api.xdevapi.Session;
import com.mysql.cj.api.xdevapi.SqlResult;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.x.core.MysqlxSession;
import com.mysql.cj.x.core.XDevAPIError;
import com.mysql.cj.xdevapi.SessionImpl;

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
        if (this.isSetForXTests) {
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
            testSession.close();

            String userAndSslFreeBaseUrl = this.sslFreeBaseUrl;
            userAndSslFreeBaseUrl = userAndSslFreeBaseUrl.replaceAll(PropertyDefinitions.PNAME_user + "=", PropertyDefinitions.PNAME_user + "VOID=");
            userAndSslFreeBaseUrl = userAndSslFreeBaseUrl.replaceAll(PropertyDefinitions.PNAME_password + "=", PropertyDefinitions.PNAME_password + "VOID=");

            testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.DISABLED)
                    + makeParam(PropertyDefinitions.PNAME_user, "testPlainAuth") + makeParam(PropertyDefinitions.PNAME_password, "pwd"));
            assertNonSecureSession(testSession);
            testSession.close();

            Properties props = new Properties(this.sslFreeTestProperties);
            props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.DISABLED.toString());
            props.setProperty(PropertyDefinitions.PNAME_user, "testPlainAuth");
            props.setProperty(PropertyDefinitions.PNAME_password, "pwd");
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

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeTestProperties);
        assertSecureSession(testSession);
        testSession.close();

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED.toString());
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

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeTestProperties);
        assertSecureSession(testSession);
        testSession.close();

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED.toString());
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

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_CA.toString());
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl);
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword);
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

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_CA.toString());
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
                        + makeParam(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword));
        assertSecureSession(testSession);
        testSession.close();

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_IDENTITY.toString());
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl);
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword);
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
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_IDENTITY)));

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_CA.toString());
        assertThrows(CJCommunicationsException.class, "No truststore provided to verify the Server certificate\\.", () -> this.fact.getSession(props));

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

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.VERIFY_IDENTITY.toString());
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl);
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword);
        // Meaningful error message is deep inside the stack trace.
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

        String expectedError = "Incompatible security settings\\. The property 'xdevapi.ssl-truststore' requires 'xdevapi.ssl-mode' as 'VERIFY_CA' or 'VERIFY_IDENTITY'\\.";
        assertThrows(CJCommunicationsException.class, expectedError,
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl)));

        assertThrows(CJCommunicationsException.class, expectedError,
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED)
                        + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl)));

        assertThrows(CJCommunicationsException.class, expectedError,
                () -> this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.DISABLED)
                        + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl)));

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl);
        assertThrows(CJCommunicationsException.class, expectedError, () -> this.fact.getSession(props));

        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED.toString());
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl);
        assertThrows(CJCommunicationsException.class, expectedError, () -> this.fact.getSession(props));

        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.DISABLED.toString());
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl);
        assertThrows(CJCommunicationsException.class, expectedError, () -> this.fact.getSession(props));
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

            // 2. Allow only TLS_RSA_WITH_AES_128_CBC_SHA cipher
            props.setProperty(PropertyDefinitions.PNAME_enabledSSLCipherSuites, "TLS_RSA_WITH_AES_128_CBC_SHA");
            sess = this.fact.getSession(props);
            assertSessionStatusEquals(sess, "mysqlx_ssl_cipher", "AES128-SHA");
            assertSessionStatusEquals(sess, "ssl_cipher", "");
            sess.close();

            // 3. Check connection with required client certificate 
            props.setProperty(PropertyDefinitions.PNAME_user, "bug25494338user");
            props.setProperty(PropertyDefinitions.PNAME_password, "pwd");

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

        testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED));
        assertSecureSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.sslFreeTestProperties);
        assertSecureSession(testSession);
        testSession.close();

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED.toString());
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        testSession.close();
    }

    /**
     * Tests that PLAIN, MYSQL41 and EXTERNAL authentication mechanisms.
     * 
     * @throws Throwable
     */
    @Test
    public void testAuthMechanisns() throws Throwable {
        if (!this.isSetForXTests) {
            return;
        }

        Session testSession = null;

        try {
            testSession = this.fact.getSession(this.sslFreeBaseUrl);
            testSession.sql("CREATE USER IF NOT EXISTS 'testPlainAuth'@'%' IDENTIFIED WITH mysql_native_password BY 'pwd'").execute();
            testSession.sql("CREATE USER IF NOT EXISTS 'testPlainAuthSha256'@'%' IDENTIFIED WITH sha256_password BY 'pwd'").execute();
            testSession.close();

            Field sf = SessionImpl.class.getDeclaredField("session");
            sf.setAccessible(true);

            Field mf = MysqlxSession.class.getDeclaredField("authMech");
            mf.setAccessible(true);

            Properties props = new Properties(this.sslFreeTestProperties);
            props.setProperty(PropertyDefinitions.PNAME_user, "testPlainAuth");
            props.setProperty(PropertyDefinitions.PNAME_password, "pwd");

            // default MYSQL41 if no TLS
            props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.DISABLED.toString());
            testSession = this.fact.getSession(props);
            assertNonSecureSession(testSession);
            assertEquals("MYSQL41", mf.get(sf.get(testSession)));
            testSession.close();

            // default PLAIN if over TLS
            props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED.toString());
            testSession = this.fact.getSession(props);
            assertSecureSession(testSession);
            assertEquals("PLAIN", mf.get(sf.get(testSession)));
            testSession.close();

            // next block to be run under GPL server and GPL servers prior 8.0 not necessarily contain sha256 support 
            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3"))) {
                // PLAIN for sha256 authentication if over TLS
                props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED.toString());
                props.setProperty(PropertyDefinitions.PNAME_user, "testPlainAuthSha256");
                testSession = this.fact.getSession(props);
                assertSecureSession(testSession);
                assertEquals("PLAIN", mf.get(sf.get(testSession)));

                SqlResult rows = testSession.sql("select USER(),CURRENT_USER()").execute();
                Row row = rows.next();
                assertEquals("testPlainAuthSha256", row.getString(0).split("@")[0]);
                assertEquals("testPlainAuthSha256", row.getString(1).split("@")[0]);

                testSession.close();
            }

            // forced MYSQL41 if no TLS
            props.setProperty(PropertyDefinitions.PNAME_user, "testPlainAuth");
            props.setProperty(PropertyDefinitions.PNAME_auth, "MYSQL41");
            props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.DISABLED.toString());
            testSession = this.fact.getSession(props);
            assertNonSecureSession(testSession);
            assertEquals("MYSQL41", mf.get(sf.get(testSession)));
            testSession.close();

            // forced MYSQL41 if over TLS
            props.setProperty(PropertyDefinitions.PNAME_auth, "Mysql41"); // also checks for case insensitivity
            props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED.toString());
            testSession = this.fact.getSession(props);
            assertSecureSession(testSession);
            assertEquals("MYSQL41", mf.get(sf.get(testSession)));
            testSession.close();

            // forced PLAIN if no TLS
            props.setProperty(PropertyDefinitions.PNAME_auth, "PLAIN");
            props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.DISABLED.toString());
            assertThrows(XDevAPIError.class, "PLAIN authentication is not allowed via unencrypted connection.", () -> this.fact.getSession(props));

            // forced PLAIN if over TLS
            props.setProperty(PropertyDefinitions.PNAME_auth, "plain"); // also checks for case insensitivity
            props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED.toString());
            testSession = this.fact.getSession(props);
            assertSecureSession(testSession);
            assertEquals("PLAIN", mf.get(sf.get(testSession)));
            testSession.close();

            // forced EXTERNAL if no TLS
            props.setProperty(PropertyDefinitions.PNAME_auth, "EXTERNAL");
            props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.DISABLED.toString());
            assertThrows(XDevAPIError.class, () -> this.fact.getSession(props)); // ERROR 1251 (HY000) Invalid authentication method EXTERNAL

            // forced EXTERNAL if over TLS
            props.setProperty(PropertyDefinitions.PNAME_auth, "EXTERNAL");
            props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.REQUIRED.toString());
            assertThrows(XDevAPIError.class, () -> this.fact.getSession(props)); // ERROR 1251 (HY000) Invalid authentication method EXTERNAL

            // forced unknown mech
            props.setProperty(PropertyDefinitions.PNAME_auth, "uNkNoWn");
            assertThrows(WrongArgumentException.class, "Unknown authentication mechanism 'UNKNOWN'.", () -> this.fact.getSession(props));

        } catch (Throwable t) {
            throw t;
        } finally {
            testSession = this.fact.getSession(this.sslFreeBaseUrl);
            testSession.sql("DROP USER if exists testPlainAuth").execute();
            testSession.sql("DROP USER if exists testPlainAuthSha256").execute();
            testSession.close();
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

        // restricted to TLSv1
        props.setProperty(PropertyDefinitions.PNAME_enabledTLSProtocols, "TLSv1");
        testSession = this.fact.getSession(props);
        assertSecureSession(testSession);
        rs = testSession.sql("SHOW SESSION STATUS LIKE 'mysqlx_ssl_version'").execute();
        actual = rs.fetchOne().getString(1);
        assertEquals("TLSv1", actual);
        testSession.close();

        // TLSv1.2 should fail
        props.setProperty(PropertyDefinitions.PNAME_enabledTLSProtocols, "TLSv1.2,TLSv1");
        if (gplWithRSA) {
            testSession = this.fact.getSession(props);
            assertSecureSession(testSession);
            rs = testSession.sql("SHOW SESSION STATUS LIKE 'mysqlx_ssl_version'").execute();
            actual = rs.fetchOne().getString(1);
            assertEquals("TLSv1.2", actual);
            testSession.close();
        } else {
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

            // restricted to TLSv1
            propsOpenSSL.setProperty(PropertyDefinitions.PNAME_enabledTLSProtocols, "TLSv1");
            testSession = this.fact.getSession(propsOpenSSL);
            assertSecureSession(testSession);
            rs = testSession.sql("SHOW SESSION STATUS LIKE 'mysqlx_ssl_version'").execute();
            actual = rs.fetchOne().getString(1);
            assertEquals("TLSv1", actual);
            testSession.close();

            // TLSv1.2
            propsOpenSSL.setProperty(PropertyDefinitions.PNAME_enabledTLSProtocols, "TLSv1.2,TLSv1.1,TLSv1");
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

}
