/*
 * Copyright (c) 2020, 2024, Oracle and/or its affiliates.
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

package testsuite.simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import javax.security.sasl.SaslException;

import org.junit.jupiter.api.Test;

import com.mysql.cj.callback.MysqlCallback;
import com.mysql.cj.callback.MysqlCallbackHandler;
import com.mysql.cj.callback.OpenidConnectAuthenticationCallback;
import com.mysql.cj.conf.PropertyDefinitions.SslMode;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.protocol.AuthenticationPlugin;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.protocol.a.authentication.AuthenticationLdapSaslClientPlugin;
import com.mysql.cj.sasl.ScramShaSaslClient;

import testsuite.BaseTestCase;

public class AuthenticationTest extends BaseTestCase {

    /**
     * Overrides the random parts of the SCRAM-SHA-1 or SCRAM-SHA-256 authentication elements (<code>cnonce</code> and <code>clientFirstMessageBare</code>) with
     * the given values from the official test vector specified in <a href="https://tools.ietf.org/html/rfc5802#section-5">RFC 5802, Section 5</a> and <a
     * href="https://tools.ietf.org/html/rfc7677#section-3">RFC 7677, Section 3</a>.
     *
     * @param authPlugin
     *            the {@link AuthenticationPlugin} where to override the internal randomly generated values.
     * @param nonce
     *            the nonce to inject into the authentication plugin object.
     */
    private void overrideSaslClientData(AuthenticationPlugin<NativePacketPayload> authPlugin, String nonce) {
        try {
            // Get the plugin internal "saslClient" object instance.
            Field field = AuthenticationLdapSaslClientPlugin.class.getDeclaredField("saslClient");
            boolean accessible = field.isAccessible();
            field.setAccessible(true);
            ScramShaSaslClient saslCli = (ScramShaSaslClient) field.get(authPlugin);
            field.setAccessible(accessible);

            // Override "cNonce" with [<nonce>].
            field = ScramShaSaslClient.class.getDeclaredField("cNonce");
            accessible = field.isAccessible();
            field.setAccessible(true);
            field.set(saslCli, nonce);
            field.setAccessible(accessible);

            // Override "clientFirstMessageBare" with [n,,n=user,r=<nonce>].
            field = ScramShaSaslClient.class.getDeclaredField("clientFirstMessageBare");
            accessible = field.isAccessible();
            field.setAccessible(true);
            field.set(saslCli, "n=user,r=" + nonce);
            field.setAccessible(accessible);
        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
            fail("Failed overriding internal plugin data via reflection. Cannot continue test.");
        }
    }

    /**
     * As per <a href="https://tools.ietf.org/html/rfc5802#section-5">RFC 5802, Section 5</a>.
     * Test vector of a SCRAM-SHA-1 authentication exchange when the client doesn't support channel bindings (username 'user' and password 'pencil' are used):
     *
     * <pre>
     * C: n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL
     * S: r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096
     * C: c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=
     * S: v=rmF9pqV8S7suAoZWja4dJRkFsKQ=
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void authLdapSaslCliPluginScramSha1TestVector() throws Exception {
        AuthenticationPlugin<NativePacketPayload> authPlugin = new AuthenticationLdapSaslClientPlugin();

        // Initialize plugin with some protocol (none is needed).
        authPlugin.init(null);

        // Check plugin name.
        assertEquals("authentication_ldap_sasl_client", authPlugin.getProtocolPluginName());

        // Check confidentiality.
        assertFalse(authPlugin.requiresConfidentiality());

        // Check if plugin is reusable.
        assertFalse(authPlugin.isReusable());

        // Set authentication parameters.
        authPlugin.setAuthenticationParameters("user", "pencil");

        // Initial server packet: Protocol::AuthSwitchRequest
        //   [authentication_ldap_sasl_client.SCRAM-SHA-1]
        //   ;; "." --> 0 byte.
        //   ;; first part of the packet is already processed.
        NativePacketPayload challenge = new NativePacketPayload("SCRAM-SHA-1".getBytes("ASCII"));

        // Expected 'client-first-message':
        //   [n,,n=user,r=<CNONCE>]
        //   ;; <CNONCE> is generated internally and needs to be replaced by the expected value from the test vector in order to continue the test.
        List<NativePacketPayload> response = new ArrayList<>();
        authPlugin.nextAuthenticationStep(challenge, response);
        assertEquals(1, response.size());
        String data = response.get(0).readString(StringSelfDataType.STRING_EOF, "UTF-8");
        assertTrue(data.startsWith("n,,n=user,r="));
        assertEquals("n,,n=user,r=".length() + 32, data.length());

        // Replace the internal plugin data in order to match the expected 'client-first-message':
        //   [n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL]
        overrideSaslClientData(authPlugin, "fyko+d2lbbFgONRv9qkxdawL");

        // Server's 'server-first-message':
        //   [r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096]
        challenge = new NativePacketPayload("r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096".getBytes("UTF-8"));

        // Expected 'client-final-message':
        //   [c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=]
        authPlugin.nextAuthenticationStep(challenge, response);
        assertEquals(1, response.size());
        data = response.get(0).readString(StringSelfDataType.STRING_EOF, "UTF-8");
        assertEquals("c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=", data);

        // Server's 'server-final-message':
        //   [v=rmF9pqV8S7suAoZWja4dJRkFsKQ=]
        challenge = new NativePacketPayload("v=rmF9pqV8S7suAoZWja4dJRkFsKQ=".getBytes("UTF-8"));

        // Expected 'nothing'.
        //   ;; If server's proof is verified then no exception is thrown.
        authPlugin.nextAuthenticationStep(challenge, response);
        assertEquals(0, response.size());
    }

    /**
     * As per <a href="https://tools.ietf.org/html/rfc7677#section-3">RFC 7677, Section 3</a>.
     * Test vector of a SCRAM-SHA-256 authentication exchange when the client doesn't support channel bindings. The username 'user' and password 'pencil' are
     * being used.:
     *
     * <pre>
     * C: n,,n=user,r=rOprNGfwEbeRWgbNEkqO
     * S: r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096
     * C: c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=
     * S: v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void authLdapSaslCliPluginScramSha256TestVector() throws Exception {
        AuthenticationPlugin<NativePacketPayload> authPlugin = new AuthenticationLdapSaslClientPlugin();

        // Initialize plugin with some protocol (none is needed).
        authPlugin.init(null);

        // Check plugin name.
        assertEquals("authentication_ldap_sasl_client", authPlugin.getProtocolPluginName());

        // Check confidentiality.
        assertFalse(authPlugin.requiresConfidentiality());

        // Check if plugin is reusable.
        assertFalse(authPlugin.isReusable());

        // Set authentication parameters.
        authPlugin.setAuthenticationParameters("user", "pencil");

        // Initial server packet: Protocol::AuthSwitchRequest
        //   [authentication_ldap_sasl_client.SCRAM-SHA-256]
        //   ;; "." --> 0 byte.
        //   ;; first part of the packet is already processed.
        NativePacketPayload challenge = new NativePacketPayload("SCRAM-SHA-256".getBytes("ASCII"));

        // Expected 'client-first-message':
        //   [n,,n=user,r=<CNONCE>]
        //   ;; <CNONCE> is generated internally and needs to be replaced by the expected value from the test vector in order to continue the test.
        List<NativePacketPayload> response = new ArrayList<>();
        authPlugin.nextAuthenticationStep(challenge, response);
        assertEquals(1, response.size());
        String data = response.get(0).readString(StringSelfDataType.STRING_EOF, "UTF-8");
        assertTrue(data.startsWith("n,,n=user,r="));
        assertEquals("n,,n=user,r=".length() + 32, data.length());

        // Replace the internal plugin data in order to match the expected 'client-first-message':
        //   [n,,n=user,r=rOprNGfwEbeRWgbNEkqO]
        overrideSaslClientData(authPlugin, "rOprNGfwEbeRWgbNEkqO");

        // Server's 'server-first-message':
        //   [r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096]
        challenge = new NativePacketPayload("r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096".getBytes("UTF-8"));

        // Expected 'client-final-message':
        //   [c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=]
        authPlugin.nextAuthenticationStep(challenge, response);
        assertEquals(1, response.size());
        data = response.get(0).readString(StringSelfDataType.STRING_EOF, "UTF-8");
        assertEquals("c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=", data);

        // Server's 'server-final-message':
        //   [v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=]
        challenge = new NativePacketPayload("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=".getBytes("UTF-8"));

        // Expected 'nothing'.
        //   ;; If server's proof is verified then no exception is thrown.
        authPlugin.nextAuthenticationStep(challenge, response);
        assertEquals(0, response.size());
    }

    /**
     * Test wrong 'server-first-message' due to missing attributes.
     * Data based on test vector from <a href="https://tools.ietf.org/html/rfc5802#section-5">RFC 5802, Section 5</a>.
     *
     * @throws Exception
     */
    @Test
    public void authLdapSaslCliPluginChallengeMissingAttributes() throws Exception {
        // Server's 'server-first-message' attributes:
        String ar = "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j";
        String as = "s=QSXCR+Q6sek8bf92";
        String ai = "i=4096";

        for (int i = 0; i < 3; i++) {
            AuthenticationPlugin<NativePacketPayload> authPlugin = new AuthenticationLdapSaslClientPlugin();

            // Initialize plugin with some protocol (none is needed).
            authPlugin.init(null);

            // Set authentication parameters.
            authPlugin.setAuthenticationParameters("user", "pencil");

            // Initial server packet: Protocol::AuthSwitchRequest
            //   [authentication_ldap_sasl_client.SCRAM-SHA-1]
            //   ;; "." --> 0 byte.
            //   ;; first part of the packet is already processed.
            NativePacketPayload challenge = new NativePacketPayload("SCRAM-SHA-1".getBytes("ASCII"));

            // Expected 'client-first-message':
            //   [n,,n=user,r=<CNONCE>]
            //   ;; <CNONCE> is generated internally and needs to be replaced by the expected value from the test vector in order to continue the test.
            List<NativePacketPayload> response = new ArrayList<>();
            authPlugin.nextAuthenticationStep(challenge, response);
            assertEquals(1, response.size());
            String data = response.get(0).readString(StringSelfDataType.STRING_EOF, "UTF-8");
            assertTrue(data.startsWith("n,,n=user,r="));
            assertEquals("n,,n=user,r=".length() + 32, data.length());

            // Server's 'server-first-message':
            //   [r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096]
            //   ;; But skip one of the attributes at a time.
            String sfm = null;
            switch (i) {
                case 0:
                    sfm = String.join(",", as, ai);
                    break;
                case 1:
                    sfm = String.join(",", ar, ai);
                    break;
                case 2:
                    sfm = String.join(",", ar, as);
                    break;
            }
            NativePacketPayload badChallenge = new NativePacketPayload(sfm.getBytes("UTF-8"));

            // Expect Exception.
            CJException ex = assertThrows(CJException.class,
                    "Error while processing an authentication iteration for the authentication mechanism 'SCRAM-SHA-1'\\.",
                    () -> authPlugin.nextAuthenticationStep(badChallenge, response));

            assertEquals(SaslException.class, ex.getCause().getClass());
            assertEquals("Missing required SCRAM attribute from server first message.", ex.getCause().getMessage());
        }
    }

    /**
     * Test wrong 'server-first-message' due to bad server nonce.
     * Data based on test vector from <a href="https://tools.ietf.org/html/rfc5802#section-5">RFC 5802, Section 5</a>.
     *
     * @throws Exception
     */
    @Test
    public void authLdapSaslCliPluginChallengeBadNonce() throws Exception {
        AuthenticationPlugin<NativePacketPayload> authPlugin = new AuthenticationLdapSaslClientPlugin();

        // Initialize plugin with some protocol (none is needed).
        authPlugin.init(null);

        // Set authentication parameters.
        authPlugin.setAuthenticationParameters("user", "pencil");

        // Initial server packet: Protocol::AuthSwitchRequest
        //   [authentication_ldap_sasl_client.SCRAM-SHA-1]
        //   ;; "." --> 0 byte.
        //   ;; first part of the packet is already processed.
        NativePacketPayload challenge = new NativePacketPayload("SCRAM-SHA-1".getBytes("ASCII"));

        // Expected 'client-first-message':
        //   [n,,n=user,r=<CNONCE>]
        //   ;; <CNONCE> is generated internally and needs to be replaced by the expected value from the test vector in order to continue the test.
        List<NativePacketPayload> response = new ArrayList<>();
        authPlugin.nextAuthenticationStep(challenge, response);
        assertEquals(1, response.size());
        String data = response.get(0).readString(StringSelfDataType.STRING_EOF, "UTF-8");
        assertTrue(data.startsWith("n,,n=user,r="));
        assertEquals("n,,n=user,r=".length() + 32, data.length());

        // Replace the internal plugin data in order to match the expected 'client-first-message':
        //   [n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL]
        overrideSaslClientData(authPlugin, "fyko+d2lbbFgONRv9qkxdawL");

        // Server's 'server-first-message':
        //   [r=XXXXXXXXXXXXXXXXXXXXXXXX3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096]
        //   ;; Bad 'r' attribute.
        NativePacketPayload badChallenge = new NativePacketPayload("r=XXXXXXXXXXXXXXXXXXXXXXXX3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096".getBytes("UTF-8"));

        // Expect Exception.
        CJException ex = assertThrows(CJException.class, "Error while processing an authentication iteration for the authentication mechanism 'SCRAM-SHA-1'\\.",
                () -> authPlugin.nextAuthenticationStep(badChallenge, response));

        assertEquals(SaslException.class, ex.getCause().getClass());
        assertEquals("Invalid server nonce for SCRAM-SHA-1 authentication.", ex.getCause().getMessage());
    }

    /**
     * Test wrong 'server-first-message' due to insufficient iterations.
     * Data based on test vector from <a href="https://tools.ietf.org/html/rfc5802#section-5">RFC 5802, Section 5</a>.
     *
     * @throws Exception
     */
    @Test
    public void authLdapSaslCliPluginChallengeBadIterations() throws Exception {
        AuthenticationPlugin<NativePacketPayload> authPlugin = new AuthenticationLdapSaslClientPlugin();

        // Initialize plugin with some protocol (none is needed).
        authPlugin.init(null);

        // Set authentication parameters.
        authPlugin.setAuthenticationParameters("user", "pencil");

        // Initial server packet: Protocol::AuthSwitchRequest
        //   [authentication_ldap_sasl_client.SCRAM-SHA-1]
        //   ;; "." --> 0 byte.
        //   ;; first part of the packet is already processed.
        NativePacketPayload challenge = new NativePacketPayload("SCRAM-SHA-1".getBytes("ASCII"));

        // Expected 'client-first-message':
        //   [n,,n=user,r=<CNONCE>]
        //   ;; <CNONCE> is generated internally and needs to be replaced by the expected value from the test vector in order to continue the test.
        List<NativePacketPayload> response = new ArrayList<>();
        authPlugin.nextAuthenticationStep(challenge, response);
        assertEquals(1, response.size());
        String data = response.get(0).readString(StringSelfDataType.STRING_EOF, "UTF-8");
        assertTrue(data.startsWith("n,,n=user,r="));
        assertEquals("n,,n=user,r=".length() + 32, data.length());

        // Replace the internal plugin data in order to match the expected 'client-first-message':
        //   [n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL]
        overrideSaslClientData(authPlugin, "fyko+d2lbbFgONRv9qkxdawL");

        // Server's 'server-first-message':
        //   [r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=1024]
        //   ;; Bad 'i' attribute.
        NativePacketPayload badChallenge = new NativePacketPayload("r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=1024".getBytes("UTF-8"));

        // Expect Exception.
        CJException ex = assertThrows(CJException.class, "Error while processing an authentication iteration for the authentication mechanism 'SCRAM-SHA-1'\\.",
                () -> authPlugin.nextAuthenticationStep(badChallenge, response));

        assertEquals(SaslException.class, ex.getCause().getClass());
        assertEquals("Announced SCRAM-SHA-1 iteration count is too low.", ex.getCause().getMessage());
    }

    /**
     * Test wrong 'server-final-message' due to missing proof.
     * Data based on test vector from <a href="https://tools.ietf.org/html/rfc5802#section-5">RFC 5802, Section 5</a>.
     *
     * @throws Exception
     */
    @Test
    public void authLdapSaslCliPluginChallengeMissingProof() throws Exception {
        AuthenticationPlugin<NativePacketPayload> authPlugin = new AuthenticationLdapSaslClientPlugin();

        // Initialize plugin with some protocol (none is needed).
        authPlugin.init(null);

        // Set authentication parameters.
        authPlugin.setAuthenticationParameters("user", "pencil");

        // Initial server packet: Protocol::AuthSwitchRequest
        //   [authentication_ldap_sasl_client.SCRAM-SHA-1]
        //   ;; "." --> 0 byte.
        //   ;; first part of the packet is already processed.
        NativePacketPayload challenge = new NativePacketPayload("SCRAM-SHA-1".getBytes("ASCII"));

        // Expected 'client-first-message':
        //   [n,,n=user,r=<CNONCE>]
        //   ;; <CNONCE> is generated internally and needs to be replaced by the expected value from the test vector in order to continue the test.
        List<NativePacketPayload> response = new ArrayList<>();
        authPlugin.nextAuthenticationStep(challenge, response);
        assertEquals(1, response.size());
        String data = response.get(0).readString(StringSelfDataType.STRING_EOF, "UTF-8");
        assertTrue(data.startsWith("n,,n=user,r="));
        assertEquals("n,,n=user,r=".length() + 32, data.length());

        // Replace the internal plugin data in order to match the expected 'client-first-message':
        //   [n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL]
        overrideSaslClientData(authPlugin, "fyko+d2lbbFgONRv9qkxdawL");

        // Server's 'server-first-message':
        //   [r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096]
        challenge = new NativePacketPayload("r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096".getBytes("UTF-8"));

        // Expected 'client-final-message':
        //   [c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=]
        authPlugin.nextAuthenticationStep(challenge, response);
        assertEquals(1, response.size());
        data = response.get(0).readString(StringSelfDataType.STRING_EOF, "UTF-8");
        assertEquals("c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=", data);

        // Server's 'server-final-message':
        //   [x=rmF9pqV8S7suAoZWja4dJRkFsKQ=]
        //   ;; Missing 'v' attribute.
        NativePacketPayload badChallenge = new NativePacketPayload("x=rmF9pqV8S7suAoZWja4dJRkFsKQ=".getBytes("UTF-8"));

        // Expected Exception.
        CJException ex = assertThrows(CJException.class, "Error while processing an authentication iteration for the authentication mechanism 'SCRAM-SHA-1'\\.",
                () -> authPlugin.nextAuthenticationStep(badChallenge, response));

        assertEquals(SaslException.class, ex.getCause().getClass());
        assertEquals("Missing required SCRAM attribute from server final message.", ex.getCause().getMessage());
    }

    /**
     * Test wrong 'server-final-message' due to bad proof.
     * Data based on test vector from <a href="https://tools.ietf.org/html/rfc5802#section-5">RFC 5802, Section 5</a>.
     *
     * @throws Exception
     */
    @Test
    public void authLdapSaslCliPluginChallengeBadProof() throws Exception {
        AuthenticationPlugin<NativePacketPayload> authPlugin = new AuthenticationLdapSaslClientPlugin();

        // Initialize plugin with some protocol (none is needed).
        authPlugin.init(null);

        // Set authentication parameters.
        authPlugin.setAuthenticationParameters("user", "pencil");

        // Initial server packet: Protocol::AuthSwitchRequest
        //   [authentication_ldap_sasl_client.SCRAM-SHA-1]
        //   ;; "." --> 0 byte.
        //   ;; first part of the packet is already processed.
        NativePacketPayload challenge = new NativePacketPayload("SCRAM-SHA-1".getBytes("ASCII"));

        // Expected 'client-first-message':
        //   [n,,n=user,r=<CNONCE>]
        //   ;; <CNONCE> is generated internally and needs to be replaced by the expected value from the test vector in order to continue the test.
        List<NativePacketPayload> response = new ArrayList<>();
        authPlugin.nextAuthenticationStep(challenge, response);
        assertEquals(1, response.size());
        String data = response.get(0).readString(StringSelfDataType.STRING_EOF, "UTF-8");
        assertTrue(data.startsWith("n,,n=user,r="));
        assertEquals("n,,n=user,r=".length() + 32, data.length());

        // Replace the internal plugin data in order to match the expected 'client-first-message':
        //   [n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL]
        overrideSaslClientData(authPlugin, "fyko+d2lbbFgONRv9qkxdawL");

        // Server's 'server-first-message':
        //   [r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096]
        challenge = new NativePacketPayload("r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096".getBytes("UTF-8"));

        // Expected 'client-final-message':
        //   [c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=]
        authPlugin.nextAuthenticationStep(challenge, response);
        assertEquals(1, response.size());
        data = response.get(0).readString(StringSelfDataType.STRING_EOF, "UTF-8");
        assertEquals("c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=", data);

        // Server's 'server-final-message':
        //   [v=XXXXXXXXXXXXXXXXXXXXXXXXXXXX]
        //   ;; Bad 'v' attribute.
        NativePacketPayload badChallenge = new NativePacketPayload("v=XXXXXXXXXXXXXXXXXXXXXXXXXXXX".getBytes("UTF-8"));

        // Expected Exception.
        CJException ex = assertThrows(CJException.class, "Error while processing an authentication iteration for the authentication mechanism 'SCRAM-SHA-1'\\.",
                () -> authPlugin.nextAuthenticationStep(badChallenge, response));

        assertEquals(SaslException.class, ex.getCause().getClass());
        assertEquals("SCRAM-SHA-1 server signature could not be verified.", ex.getCause().getMessage());
    }

    /**
     * Test unsupported SASL mechanism.
     *
     * @throws Exception
     */
    @Test
    public void authLdapSaslCliPluginChallengeUnsupportedMech() throws Exception {
        assertThrows(CJException.class, "Unsupported SASL authentication mechanism 'UNKNOWN-MECH'\\.", () -> {
            AuthenticationPlugin<NativePacketPayload> ap = new AuthenticationLdapSaslClientPlugin();
            ap.init(null);
            ap.nextAuthenticationStep(new NativePacketPayload("UNKNOWN-MECH".getBytes("ASCII")), new ArrayList<>());
            // Must do it twice because there's a chance to run the first iteration with a hashing seed instead of an authentication mechanism.
            ap.nextAuthenticationStep(new NativePacketPayload("UNKNOWN-MECH".getBytes("ASCII")), new ArrayList<>());
            return null;
        });
    }

    /**
     * Test for WL#14650 - Support for MFA (multi factor authentication) authentication.
     *
     * @throws Exception
     */
    @Test
    public void testWl14650() throws Exception {
        assumeTrue(versionMeetsMinimum(8, 0, 27), "MySQL 8.0.27+ is required to run this test.");

        boolean installPluginInRuntime = false;
        try {
            // Install plugin if required.
            installPluginInRuntime = !isPluginActive(this.stmt, "cleartext_plugin_server");

            if (installPluginInRuntime) {
                try {
                    String ext = isServerRunningOnWindows() ? ".dll" : ".so";
                    this.stmt.executeUpdate("INSTALL PLUGIN cleartext_plugin_server SONAME 'auth_test_plugin" + ext + "'");
                } catch (SQLException e) {
                    if (e.getErrorCode() == MysqlErrorNumbers.ER_CANT_OPEN_LIBRARY) {
                        installPluginInRuntime = false; // To disable plugin deinstallation attempt in the finally block.
                        assumeTrue(false, "This test requires a server installed with the test package.");
                    } else {
                        throw e;
                    }
                }
            }

            // Create test users.
            createUser("'wl14650_1fa'@'%'", "IDENTIFIED BY 'testpwd1'");
            this.stmt.executeUpdate("GRANT ALL ON * TO wl14650_1fa");
            createUser("'wl14650_2fa'@'%'", "IDENTIFIED BY 'testpwd1' AND IDENTIFIED WITH cleartext_plugin_server AS 'testpwd2'");
            this.stmt.executeUpdate("GRANT ALL ON * TO wl14650_2fa");
            createUser("'wl14650_3fa'@'%'", "IDENTIFIED BY 'testpwd1' AND IDENTIFIED WITH cleartext_plugin_server AS 'testpwd2' "
                    + "AND IDENTIFIED WITH cleartext_plugin_server AS 'testpwd3'");
            this.stmt.executeUpdate("GRANT ALL ON * TO wl14650_3fa");
            this.stmt.executeUpdate("FLUSH PRIVILEGES");

            final StringBuilder urlBuilder1 = new StringBuilder("jdbc:mysql://").append(getHostFromTestsuiteUrl()).append(":").append(getPortFromTestsuiteUrl())
                    .append("/");
            final String url1 = urlBuilder1.toString();

            // TS.1.1: 2FA successful.
            Properties props = new Properties();
            props.setProperty(PropertyKey.USER.getKeyName(), "wl14650_2fa");
            props.setProperty(PropertyKey.password1.getKeyName(), "testpwd1");
            props.setProperty(PropertyKey.password2.getKeyName(), "testpwd2");
            props.setProperty(PropertyKey.sslMode.getKeyName(), "REQUIRED");
            try (Connection testConn = getConnectionWithProps(url1, props)) {
                Statement testStmt = testConn.createStatement();
                this.rs = testStmt.executeQuery("SELECT USER(), CURRENT_USER()");
                assertTrue(this.rs.next());
                assertEquals("wl14650_2fa", this.rs.getString(1).split("@")[0]);
                assertEquals("wl14650_2fa", this.rs.getString(2).split("@")[0]);
            }

            // TS.1.2: 2FA fail - 1st password wrong.
            props.setProperty(PropertyKey.password1.getKeyName(), "wrongpwd");
            assertThrows(SQLException.class, "Access denied for user 'wl14650_2fa'@.* \\(using password: YES\\)", () -> getConnectionWithProps(url1, props));

            // TS.1.3: 2FA fail - 2nd password wrong.
            props.setProperty(PropertyKey.password1.getKeyName(), "testpwd1");
            props.setProperty(PropertyKey.password2.getKeyName(), "wrongpwd");
            assertThrows(SQLException.class, "Access denied for user 'wl14650_2fa'@.* \\(using password: YES\\)", () -> getConnectionWithProps(url1, props));

            // TS.1.4: 2FA fail - missing required password.
            props.remove(PropertyKey.password2.getKeyName());
            assertThrows(SQLException.class, "Access denied for user 'wl14650_2fa'@.* \\(using password: YES\\)", () -> getConnectionWithProps(url1, props));

            // TS.2.1: 3FA successful.
            props.setProperty(PropertyKey.USER.getKeyName(), "wl14650_3fa");
            props.setProperty(PropertyKey.password1.getKeyName(), "testpwd1");
            props.setProperty(PropertyKey.password2.getKeyName(), "testpwd2");
            props.setProperty(PropertyKey.password3.getKeyName(), "testpwd3");
            props.setProperty(PropertyKey.sslMode.getKeyName(), "REQUIRED");
            try (Connection testConn = getConnectionWithProps(url1, props)) {
                Statement testStmt = testConn.createStatement();
                this.rs = testStmt.executeQuery("SELECT USER(), CURRENT_USER()");
                assertTrue(this.rs.next());
                assertEquals("wl14650_3fa", this.rs.getString(1).split("@")[0]);
                assertEquals("wl14650_3fa", this.rs.getString(2).split("@")[0]);
            }

            // TS.2.2: 2FA fail - 1st password wrong.
            props.setProperty(PropertyKey.password1.getKeyName(), "wrongpwd");
            assertThrows(SQLException.class, "Access denied for user 'wl14650_3fa'@.* \\(using password: YES\\)", () -> getConnectionWithProps(url1, props));

            // TS.2.3: 2FA fail - 2nd password wrong.
            props.setProperty(PropertyKey.password1.getKeyName(), "testpwd1");
            props.setProperty(PropertyKey.password2.getKeyName(), "wrongpwd");
            assertThrows(SQLException.class, "Access denied for user 'wl14650_3fa'@.* \\(using password: YES\\)", () -> getConnectionWithProps(url1, props));

            // TS.2.4: 2FA fail - 3rd password wrong.
            props.setProperty(PropertyKey.password2.getKeyName(), "testpwd2");
            props.setProperty(PropertyKey.password3.getKeyName(), "wrongpwd");
            assertThrows(SQLException.class, "Access denied for user 'wl14650_3fa'@.* \\(using password: YES\\)", () -> getConnectionWithProps(url1, props));

            // TS.2.5: 2FA fail - missing required password.
            props.remove(PropertyKey.password3.getKeyName());
            assertThrows(SQLException.class, "Access denied for user 'wl14650_3fa'@.* \\(using password: YES\\)", () -> getConnectionWithProps(url1, props));

            // TS.3/TS.4/TS.5: new password options don't pollute original ones.
            props.setProperty(PropertyKey.USER.getKeyName(), "wl14650_1fa");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "testpwd1");
            props.setProperty(PropertyKey.password1.getKeyName(), "wrongpwd");
            props.setProperty(PropertyKey.password2.getKeyName(), "wrongpwd");
            props.setProperty(PropertyKey.password3.getKeyName(), "wrongpwd");
            final StringBuilder urlBuilder2 = new StringBuilder("jdbc:mysql://").append(getHostFromTestsuiteUrl()).append(":").append(getPortFromTestsuiteUrl())
                    .append("/?").append("password1=wrongpwd&password2=wrongpwd&password3=wrongpwd");
            final String url2 = urlBuilder2.toString();
            try (Connection testConn = getConnectionWithProps(url2, props)) {
                Statement testStmt = testConn.createStatement();
                this.rs = testStmt.executeQuery("SELECT USER(), CURRENT_USER()");
                assertTrue(this.rs.next());
                assertEquals("wl14650_1fa", this.rs.getString(1).split("@")[0]);
                assertEquals("wl14650_1fa", this.rs.getString(2).split("@")[0]);
            }

            props.remove(PropertyKey.USER.getKeyName());
            props.remove(PropertyKey.PASSWORD.getKeyName());
            final StringBuilder urlBuilder3 = new StringBuilder("jdbc:mysql://").append("wl14650_1fa:testpwd1@").append(getHostFromTestsuiteUrl()).append(":")
                    .append(getPortFromTestsuiteUrl()).append("/?").append("password1=wrongpwd&password2=wrongpwd&password3=wrongpwd");
            final String url3 = urlBuilder3.toString();
            try (Connection testConn = getConnectionWithProps(url3, props)) {
                Statement testStmt = testConn.createStatement();
                this.rs = testStmt.executeQuery("SELECT USER(), CURRENT_USER()");
                assertTrue(this.rs.next());
                assertEquals("wl14650_1fa", this.rs.getString(1).split("@")[0]);
                assertEquals("wl14650_1fa", this.rs.getString(2).split("@")[0]);
            }

            props.remove(PropertyKey.USER.getKeyName());
            props.remove(PropertyKey.PASSWORD.getKeyName());
            final StringBuilder urlBuilder4 = new StringBuilder("jdbc:mysql://").append(getHostFromTestsuiteUrl()).append(":").append(getPortFromTestsuiteUrl())
                    .append("/?").append("user=wl14650_1fa&password=testpwd1&password1=wrongpwd&password2=wrongpwd&password3=wrongpwd");
            final String url4 = urlBuilder4.toString();
            try (Connection testConn = getConnectionWithProps(url4, props)) {
                Statement testStmt = testConn.createStatement();
                this.rs = testStmt.executeQuery("SELECT USER(), CURRENT_USER()");
                assertTrue(this.rs.next());
                assertEquals("wl14650_1fa", this.rs.getString(1).split("@")[0]);
                assertEquals("wl14650_1fa", this.rs.getString(2).split("@")[0]);
            }

            final StringBuilder urlBuilder5 = new StringBuilder("jdbc:mysql://").append(getHostFromTestsuiteUrl()).append(":").append(getPortFromTestsuiteUrl())
                    .append("/?").append("password1=wrongpwd&password2=wrongpwd&password3=wrongpwd&sslMode=REQUIRED");
            final String url5 = urlBuilder5.toString();
            try (Connection testConn = DriverManager.getConnection(url5, "wl14650_1fa", "testpwd1")) {
                Statement testStmt = testConn.createStatement();
                this.rs = testStmt.executeQuery("SELECT USER(), CURRENT_USER()");
                assertTrue(this.rs.next());
                assertEquals("wl14650_1fa", this.rs.getString(1).split("@")[0]);
                assertEquals("wl14650_1fa", this.rs.getString(2).split("@")[0]);
            }

            // TS.6: 1FA successful with password given in 'password1'.
            props.setProperty(PropertyKey.USER.getKeyName(), "wl14650_1fa");
            props.remove(PropertyKey.password1.getKeyName());
            props.remove(PropertyKey.password2.getKeyName());
            props.remove(PropertyKey.password3.getKeyName());
            final StringBuilder urlBuilder6 = new StringBuilder("jdbc:mysql://").append(getHostFromTestsuiteUrl()).append(":").append(getPortFromTestsuiteUrl())
                    .append("/?").append("password1=testpwd1&password2=wrongpwd&password3=wrongpwd");
            final String url6 = urlBuilder6.toString();
            try (Connection testConn = getConnectionWithProps(url6, props)) {
                Statement testStmt = testConn.createStatement();
                this.rs = testStmt.executeQuery("SELECT USER(), CURRENT_USER()");
                assertTrue(this.rs.next());
                assertEquals("wl14650_1fa", this.rs.getString(1).split("@")[0]);
                assertEquals("wl14650_1fa", this.rs.getString(2).split("@")[0]);
            }

            // TS.7: 1FA fail with 'password' wrong and 'password1' correct.
            props.setProperty(PropertyKey.USER.getKeyName(), "wl14650_1fa");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "wrongpwd");
            props.setProperty(PropertyKey.password1.getKeyName(), "testpwd1");
            final StringBuilder urlBuilder7 = new StringBuilder("jdbc:mysql://").append(getHostFromTestsuiteUrl()).append(":").append(getPortFromTestsuiteUrl())
                    .append("/");
            final String url7 = urlBuilder7.toString();
            assertThrows(SQLException.class, "Access denied for user 'wl14650_1fa'@.* \\(using password: YES\\)", () -> getConnectionWithProps(url7, props));
        } finally {
            if (installPluginInRuntime) {
                this.stmt.executeUpdate("UNINSTALL PLUGIN cleartext_plugin_server");
            }
        }
    }

    private static final String WL16490_ID_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJteXN1YmoiLCJpc3MiOiJodHRwczovL215aXNzdWVyLmNvbSIsImV4cCI6Mj"
            + "EzNDgyOTc4Mn0.RTeEvRQsA_P3CDMyS3Q-cH9D9fioWZel5RVPygE9EzGGE94URT_EeyasayAamCIdo8f9_G5q3TAqiyGANZcnPG0prayJONVjGuu6AXCtkk43v7MvGDRVGx-Cn1clekts4z"
            + "09wdHdAGNak8SgVSq5WxKlo00uvNlPQjlh5927B38xBvEg8fISi0yVljpQT8SUCcBlcJL0Kk9uww3aF03qQIKB_TXaMIqsJM_Bm7llE3MtqzxMA8LoP7Weg1sblEMr-gSCPnrJ3HcKYXjFlF"
            + "DPnq6qOLw4tR-o87SFuFUD0D6cgdWieh2rvTjv1dpl9mI0MArRZuVaiCmpreZUxdyrio3dXCkNVg_mWNmrBoCPrM0D_Wr4X22YHiChLb4F8UHU6H6l_YgQCw8jr727LOccYSiEyc4xe5dHeQ"
            + "rsRf4sexQ37_yjnYszvWulIOFLSxbV1HJHuvPPG76Cwe_fUvwxsHqX14JvXcGRSW3QfYN3edqXUu3cIHKgddcWWbO4E52xODHWz8IPpR_LfVttn4lsEJB8ELcSE_dR77fo59kWj-TOTLNXDR"
            + "C6W5X1QoLBrOQn1PDpA3KL7UvWHHKP7FyqoSi64UYGrAvzZCo_zhRoHjsW-gX7FsgRPs_Xujls6j8kGupJ7f8svTlpiPZt832OmxxUPabE52rgxFqDirSkFa8";
    private static final String WL16490_OPENID_SERVER_CONFIG = "JSON://{\"myissuer\":\"{\\\\\"kty\\\\\":\\\\\"RSA\\\\\",\\\\\"n\\\\\":\\\\\"26unngWQbtxxQr7kASZ"
            + "zd1mSAF5fHTIvkcqOqRGc1dEaaZBETuVnLZFiaG8C3fQf-_9J7NwcN42EueDJOLf8SJ_qeFdT1wdMLZNzvlmZVspeIlNlH2YRXw7zYZt5MxVH4kgHkXF9vW4f3QzujP1I7ogva_YAue2GFYF"
            + "tYpeHGMzGsyNrvvRfQoVMPR3yaZDvaH-6A5PHP5nnKoLzYZqKz4nTrXh9c4ZjGSQLdAj3Pe_jkgXgqOrXoQPwKPsE_m8PT5kRJvnKIWqWXFILIFn4s7rNySM6nXNmF1c0_EFx8MBo-I6j9Js"
            + "d5NxbXDVopyuNVJfO-cj_QXBGFNBl1AosfS1MvA-Wej3Nuf_mOwAiDCx7yZIdLkL_IvmHFEhBBUeTn0QB_SxGzinEK6BzVxNK5RlrxOkPSRF_0voXI_9Sa9jJDCnR7rhcfTCmbrVaEPkREU6"
            + "vCVEVHKCC7lQoHCfp5wSM0jEyCBi7P8wDqvpWfk8g8sqZkPjIAQRht84GsMQ5ifBuB-p4ed_v8X3Z_aKsSz4zVai8O4RCnbE-JgP2tzY7eaCqNpIByl8DopDVpJjMF4rFxuxSCD_dNi4UDEf"
            + "Yz9RrUgXRNQ_bxpcxCCwL0t0u95JyJs5IN5YD4b9NV0p1nmuuK8_03q1ZBrd7ODWDqU8DtQ_vav4PbI9tpeknH78\\\\\",\\\\\"e\\\\\":\\\\\"AQAB\\\\\","
            + "\\\\\"alg\\\\\":\\\\\"RS256\\\\\",\\\\\"use\\\\\":\\\\\"sig\\\\\",\\\\\"name\\\\\":\\\\\"https://myissuer.com\\\\\"}\"}";

    /**
     * Test for WL#16490 - OpenID Connect authentication support.
     *
     * @throws Exception
     */
    @Test
    void testWl16490() throws Exception {
        assumeTrue(versionMeetsMinimum(9, 1, 0), "MySQL 9.1.0+ is required to run this test.");
        assumeTrue(isEnterpriseEdition(), "MySQL Commercial edition required to run this test.");

        boolean installPluginInRuntime = false;
        try {
            // Install plugin if required.
            installPluginInRuntime = !isPluginActive(this.stmt, "authentication_openid_connect");

            if (installPluginInRuntime) {
                try {
                    String ext = isServerRunningOnWindows() ? ".dll" : ".so";
                    this.stmt.executeUpdate("INSTALL PLUGIN authentication_openid_connect SONAME 'authentication_openid_connect" + ext + "'");
                } catch (SQLException e) {
                    if (e.getErrorCode() == MysqlErrorNumbers.ER_CANT_OPEN_LIBRARY) {
                        installPluginInRuntime = false;
                        assumeTrue(false, "Failed installing the authentication plugin 'authentication_openid_connect'.");
                    } else {
                        throw e;
                    }
                }
            }

            // Configure the server.
            this.stmt.execute("SET GLOBAL authentication_openid_connect_configuration = '" + WL16490_OPENID_SERVER_CONFIG + "'");

            // Create test users.
            createUser("'testWl16490_1fa'@'%'",
                    "IDENTIFIED WITH 'authentication_openid_connect' AS '{\"identity_provider\":\"myissuer\",\"user\":\"mysubj\"}'");
            this.stmt.executeUpdate("GRANT ALL ON * TO testWl16490_1fa");
            createUser("'testWl16490_2fa'@'%'", "IDENTIFIED BY 'testpwd1' AND "
                    + "IDENTIFIED WITH 'authentication_openid_connect' AS '{\"identity_provider\":\"myissuer\",\"user\":\"mysubj\"}'");
            this.stmt.executeUpdate("GRANT ALL ON * TO testWl16490_2fa");

            // Create temp files containing Identity Tokens.
            Path goodIdTokenFile = Files.createTempFile("wl16490_jwt_good_", ".txt");
            Files.write(goodIdTokenFile, WL16490_ID_TOKEN.getBytes());

            byte[] badIdToken = new byte[10 * 1024 + 1];
            new Random().nextBytes(badIdToken);
            Path badIdTokenFile = Files.createTempFile("wl16490_jwt_bad_", ".txt");
            Files.write(badIdTokenFile, badIdToken);

            String url = String.format("jdbc:mysql://%s:%s/", getHostFromTestsuiteUrl(), getPortFromTestsuiteUrl());
            Properties props = new Properties();

            // TS.1.a: Good Identity Token file (1st factor).
            props.clear();
            props.setProperty(PropertyKey.USER.getKeyName(), "testWl16490_1fa");
            props.setProperty(PropertyKey.idTokenFile.getKeyName(), goodIdTokenFile.toString());
            try (Connection testConn = getConnectionWithProps(url, props)) {
                Statement testStmt = testConn.createStatement();
                this.rs = testStmt.executeQuery("SELECT USER(), CURRENT_USER()");
                assertTrue(this.rs.next());
                assertEquals("testWl16490_1fa", this.rs.getString(1).split("@")[0]);
                assertEquals("testWl16490_1fa", this.rs.getString(2).split("@")[0]);
            }

            // TS.1.b: Good Identity Token file (2nd factor).
            props.clear();
            props.setProperty(PropertyKey.USER.getKeyName(), "testWl16490_2fa");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "testpwd1");
            props.setProperty(PropertyKey.idTokenFile.getKeyName(), goodIdTokenFile.toString());
            try (Connection testConn = getConnectionWithProps(url, props)) {
                Statement testStmt = testConn.createStatement();
                this.rs = testStmt.executeQuery("SELECT USER(), CURRENT_USER()");
                assertTrue(this.rs.next());
                assertEquals("testWl16490_2fa", this.rs.getString(1).split("@")[0]);
                assertEquals("testWl16490_2fa", this.rs.getString(2).split("@")[0]);
            }

            // TS.1.c: Good Identity Token file and 'authentication_openid_connect_client' as the default (1st factor).
            props.clear();
            props.setProperty(PropertyKey.USER.getKeyName(), "testWl16490_1fa");
            props.setProperty(PropertyKey.idTokenFile.getKeyName(), goodIdTokenFile.toString());
            props.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), "authentication_openid_connect_client");
            try (Connection testConn = getConnectionWithProps(url, props)) {
                Statement testStmt = testConn.createStatement();
                this.rs = testStmt.executeQuery("SELECT USER(), CURRENT_USER()");
                assertTrue(this.rs.next());
                assertEquals("testWl16490_1fa", this.rs.getString(1).split("@")[0]);
                assertEquals("testWl16490_1fa", this.rs.getString(2).split("@")[0]);
            }

            // TS.1.d: Good Identity Token file and 'authentication_openid_connect_client' as the default (2st factor).
            props.clear();
            props.setProperty(PropertyKey.USER.getKeyName(), "testWl16490_2fa");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "testpwd1");
            props.setProperty(PropertyKey.idTokenFile.getKeyName(), goodIdTokenFile.toString());
            props.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), "authentication_openid_connect_client");
            try (Connection testConn = getConnectionWithProps(url, props)) {
                Statement testStmt = testConn.createStatement();
                this.rs = testStmt.executeQuery("SELECT USER(), CURRENT_USER()");
                assertTrue(this.rs.next());
                assertEquals("testWl16490_2fa", this.rs.getString(1).split("@")[0]);
                assertEquals("testWl16490_2fa", this.rs.getString(2).split("@")[0]);
            }

            // TS.3: Good Identity Token supplied by custom callback handler.
            props.clear();
            props.setProperty(PropertyKey.USER.getKeyName(), "testWl16490_1fa");
            props.setProperty(PropertyKey.authenticationOpenidConnectCallbackHandler.getKeyName(),
                    Wl16490OpenidConnectIdTokenCustomCallbackHandler.class.getName());
            assertFalse(Wl16490OpenidConnectIdTokenCustomCallbackHandler.handled);
            try (Connection testConn = getConnectionWithProps(url, props)) {
                Statement testStmt = testConn.createStatement();
                this.rs = testStmt.executeQuery("SELECT USER(), CURRENT_USER()");
                assertTrue(this.rs.next());
                assertEquals("testWl16490_1fa", this.rs.getString(1).split("@")[0]);
                assertEquals("testWl16490_1fa", this.rs.getString(2).split("@")[0]);
                assertTrue(Wl16490OpenidConnectIdTokenCustomCallbackHandler.handled);
            }

            // TS.4.2: No idTokenFile property.
            props.clear();
            props.setProperty(PropertyKey.USER.getKeyName(), "testWl16490_1fa");
            assertThrows(SQLException.class,
                    "A path to a file containing an OpenID Identity Token must be specified in the connection property 'idTokenFile'\\.",
                    () -> getConnectionWithProps(url, props));

            // TS.4.3: Wrong location for the Identity Token file.
            props.clear();
            props.setProperty(PropertyKey.USER.getKeyName(), "testWl16490_1fa");
            props.setProperty(PropertyKey.idTokenFile.getKeyName(), "/foobar/jwt.txt");
            assertThrows(SQLException.class, "Failed reading the OpenID Identity Token file specified in the connection property 'idTokenFile'\\.",
                    () -> getConnectionWithProps(url, props));

            // TS.4.4: Identity Token too big.
            props.clear();
            props.setProperty(PropertyKey.USER.getKeyName(), "testWl16490_1fa");
            props.setProperty(PropertyKey.idTokenFile.getKeyName(), badIdTokenFile.toString());
            assertThrows(SQLException.class, "The file specified in the connection property 'idTokenFile' contains an invalid OpenID Identity Token\\.",
                    () -> getConnectionWithProps(url, props));

            // TS.4 SSL disabled.
            props.clear();
            props.setProperty(PropertyKey.USER.getKeyName(), "testWl16490_1fa");
            props.setProperty(PropertyKey.idTokenFile.getKeyName(), goodIdTokenFile.toString());
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.toString());
            assertThrows(SQLException.class, "Access denied for user 'testWl16490_1fa'.*", () -> getConnectionWithProps(url, props));
        } finally {
            if (installPluginInRuntime) {
                this.stmt.executeUpdate("UNINSTALL PLUGIN authentication_openid_connect");
            }
        }
    }

    public static class Wl16490OpenidConnectIdTokenCustomCallbackHandler implements MysqlCallbackHandler {

        public static boolean handled = false;

        @Override
        public void handle(MysqlCallback cb) {
            ((OpenidConnectAuthenticationCallback) cb).setIdentityToken(WL16490_ID_TOKEN.getBytes());
            handled = true;
        }

    }

}
