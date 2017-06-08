/*
  Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.x.devapi;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.mysql.cj.api.xdevapi.Session;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJCommunicationsException;

public class SecureSessionTest extends DevApiBaseTestCase {
    final String trustStoreUrl = "file:src/test/config/ssl-test-certs/ca-truststore";
    final String trustStorePath = "src/test/config/ssl-test-certs/ca-truststore";
    final String trustStorePassword = "password";

    final String clientKeyStoreUrl = "file:src/test/config/ssl-test-certs/client-keystore";
    final String clientKeyStorePath = "src/test/config/ssl-test-certs/client-keystore";
    final String clientKeyStorePassword = "password";

    final Properties sslFreeTestProperties = (Properties) this.testProperties.clone();
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

        Session testSession = this.fact.getSession(this.sslFreeBaseUrl + makeParam(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.DISABLED));
        assertNonSecureSession(testSession);
        testSession.close();

        Properties props = new Properties(this.sslFreeTestProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslMode, PropertyDefinitions.SslMode.DISABLED.toString());
        testSession = this.fact.getSession(props);
        assertNonSecureSession(testSession);
        testSession.close();
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
}
