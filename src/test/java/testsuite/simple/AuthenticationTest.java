/*
 * Copyright (c) 2020, 2021, Oracle and/or its affiliates.
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import javax.security.sasl.SaslException;

import org.junit.jupiter.api.Test;

import com.mysql.cj.exceptions.CJException;
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
}
