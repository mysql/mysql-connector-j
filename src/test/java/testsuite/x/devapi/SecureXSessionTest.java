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

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.xdevapi.SqlResult;
import com.mysql.cj.api.xdevapi.XSession;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJCommunicationsException;

public class SecureXSessionTest extends DevApiBaseTestCase {
    String trustStoreUrl = "file:src/test/config/ssl-test-certs/test-cert-store";
    String trustStorePath = "src/test/config/ssl-test-certs/test-cert-store";
    String trustStorePassword = "password";

    @Before
    public void setupSecureXSessionTest() {
        if (this.isSetForXTests) {
            System.clearProperty("javax.net.ssl.trustStore");
            System.clearProperty("javax.net.ssl.trustStorePassword");
        }
    }

    /**
     * Tests non-secure {@link XSession}s created via URL and properties map.
     */
    @Test
    public void testNonSecureXSession() {
        if (!this.isSetForXTests) {
            return;
        }

        XSession testSession = this.fact.getSession(this.baseUrl);
        assertNonSecureXSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.baseUrl + makeParam(PropertyDefinitions.PNAME_sslEnable, "false"));
        assertNonSecureXSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.testProperties);
        assertNonSecureXSession(testSession);
        testSession.close();

        Properties props = new Properties(this.testProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslEnable, "false");
        testSession = this.fact.getSession(props);
        assertNonSecureXSession(testSession);
        testSession.close();
    }

    /**
     * Tests secure, non-verifying server certificate {@link XSession}s created via URL and properties map.
     */
    @Test
    public void testSecureXSessionNoVerifyServerCertificate() {
        if (!this.isSetForXTests) {
            return;
        }

        XSession testSession = this.fact.getSession(this.baseUrl + makeParam(PropertyDefinitions.PNAME_sslEnable, "true"));
        assertSecureXSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.baseUrl + makeParam(PropertyDefinitions.PNAME_sslEnable, "true")
                + makeParam(PropertyDefinitions.PNAME_sslVerifyServerCertificate, "false"));
        assertSecureXSession(testSession);
        testSession.close();

        Properties props = new Properties(this.testProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslEnable, "true");
        testSession = this.fact.getSession(props);
        assertSecureXSession(testSession);
        testSession.close();

        props.setProperty(PropertyDefinitions.PNAME_sslVerifyServerCertificate, "false");
        testSession = this.fact.getSession(props);
        assertSecureXSession(testSession);
        testSession.close();
    }

    /**
     * Tests secure, verifying server certificate {@link XSession}s created via URL and properties map.
     */
    @Test
    public void testSecureXSessionVerifyServerCertificate() {
        if (!this.isSetForXTests) {
            return;
        }

        XSession testSession = this.fact.getSession(
                this.baseUrl + makeParam(PropertyDefinitions.PNAME_sslEnable, "true") + makeParam(PropertyDefinitions.PNAME_sslVerifyServerCertificate, "true")
                        + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl)
                        + makeParam(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword));
        assertSecureXSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.baseUrl + makeParam(PropertyDefinitions.PNAME_sslVerifyServerCertificate, "true")
                + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl)
                + makeParam(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword));
        assertSecureXSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.baseUrl + makeParam(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl)
                + makeParam(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword));
        assertSecureXSession(testSession);
        testSession.close();

        Properties props = new Properties(this.testProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslVerifyServerCertificate, "true");
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl, this.trustStoreUrl);
        props.setProperty(PropertyDefinitions.PNAME_sslTrustStorePassword, this.trustStorePassword);
        testSession = this.fact.getSession(props);
        assertSecureXSession(testSession);
        testSession.close();

        props.setProperty(PropertyDefinitions.PNAME_sslEnable, "true");
        testSession = this.fact.getSession(props);
        assertSecureXSession(testSession);
        testSession.close();
    }

    /**
     * Tests secure, verifying server certificate {@link XSession}s created via URL and properties map.
     */
    @Test
    public void testSecureXSessionVerifyServerCertificateUsingSystemProps() {
        if (!this.isSetForXTests) {
            return;
        }

        System.setProperty("javax.net.ssl.trustStore", this.trustStorePath);
        System.setProperty("javax.net.ssl.trustStorePassword", this.trustStorePassword);

        XSession testSession = this.fact.getSession(this.baseUrl + makeParam(PropertyDefinitions.PNAME_sslEnable, "true")
                + makeParam(PropertyDefinitions.PNAME_sslVerifyServerCertificate, "true"));
        assertSecureXSession(testSession);
        testSession.close();

        testSession = this.fact.getSession(this.baseUrl + makeParam(PropertyDefinitions.PNAME_sslVerifyServerCertificate, "true"));
        assertSecureXSession(testSession);
        testSession.close();

        Properties props = new Properties(this.testProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslVerifyServerCertificate, "true");
        testSession = this.fact.getSession(props);
        assertSecureXSession(testSession);
        testSession.close();

        props.setProperty(PropertyDefinitions.PNAME_sslEnable, "true");
        testSession = this.fact.getSession(props);
        assertSecureXSession(testSession);
        testSession.close();
    }

    /**
     * Tests exception thrown on missing truststore for a secure {@link XSession}.
     */
    @Test
    public void testSecureXSessionMissingTrustStore1() {
        if (!this.isSetForXTests) {
            return;
        }

        assertThrows(CJCommunicationsException.class, () -> this.fact.getSession(this.baseUrl + makeParam(PropertyDefinitions.PNAME_sslEnable, "true")
                + makeParam(PropertyDefinitions.PNAME_sslVerifyServerCertificate, "true")));
    }

    /**
     * Tests exception thrown on missing truststore for a secure {@link XSession}.
     */
    @Test
    public void testSecureXSessionMissingTrustStore2() {
        if (!this.isSetForXTests) {
            return;
        }

        assertThrows(CJCommunicationsException.class,
                () -> this.fact.getSession(this.baseUrl + makeParam(PropertyDefinitions.PNAME_sslVerifyServerCertificate, "true")));
    }

    /**
     * Tests exception thrown on missing truststore for a secure {@link XSession}.
     */
    @Test
    public void testSecureXSessionMissingTrustStore3() {
        if (!this.isSetForXTests) {
            return;
        }

        final Properties props = new Properties(this.testProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslEnable, "true");
        props.setProperty(PropertyDefinitions.PNAME_sslVerifyServerCertificate, "true");
        assertThrows(CJCommunicationsException.class, () -> this.fact.getSession(props));
    }

    /**
     * Tests exception thrown on missing truststore for a secure {@link XSession}.
     */
    @Test
    public void testSecureXSessionMissingTrustStore4() {
        if (this.isSetForXTests) {
            return;
        }

        final Properties props = new Properties(this.testProperties);
        props.setProperty(PropertyDefinitions.PNAME_sslVerifyServerCertificate, "true");
        assertThrows(CJCommunicationsException.class, () -> this.fact.getSession(props));
    }

    private void assertNonSecureXSession(XSession xsession) {
        SqlResult rs = xsession.bindToDefaultShard().sql("SHOW SESSION STATUS LIKE 'mysqlx_ssl_cipher'").execute();
        assertEquals("", rs.fetchOne().getString(1));
    }

    private void assertSecureXSession(XSession xsession) {
        SqlResult rs = xsession.bindToDefaultShard().sql("SHOW SESSION STATUS LIKE 'mysqlx_ssl_cipher'").execute();
        Assert.assertNotEquals("", rs.fetchOne().getString(1));
    }

    private String makeParam(String key, String value) {
        return "&" + key + "=" + value;
    }
}
