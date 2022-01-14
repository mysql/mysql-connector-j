/*
 * Copyright (c) 2020, 2022, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.a.authentication;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.Security;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.mysql.cj.Messages;
import com.mysql.cj.callback.MysqlCallbackHandler;
import com.mysql.cj.callback.UsernameCallback;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.protocol.AuthenticationPlugin;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.sasl.ScramSha1SaslClient;
import com.mysql.cj.sasl.ScramSha256SaslClient;
import com.mysql.cj.sasl.ScramShaSaslProvider;
import com.mysql.cj.util.StringUtils;

/**
 * MySQL 'authentication_ldap_sasl_client' authentication plugin.
 */
public class AuthenticationLdapSaslClientPlugin implements AuthenticationPlugin<NativePacketPayload> {
    public static String PLUGIN_NAME = "authentication_ldap_sasl_client";

    private static final String LOGIN_CONFIG_ENTRY = "MySQLConnectorJ";
    private static final String LDAP_SERVICE_NAME = "ldap";

    private enum AuthenticationMechanisms {
        SCRAM_SHA_1(ScramSha1SaslClient.IANA_MECHANISM_NAME, ScramSha1SaslClient.MECHANISM_NAME), //
        SCRAM_SHA_256(ScramSha256SaslClient.IANA_MECHANISM_NAME, ScramSha256SaslClient.MECHANISM_NAME), //
        GSSAPI("GSSAPI", "GSSAPI");

        private String mechName = null;
        private String saslServiceName = null;

        private AuthenticationMechanisms(String mechName, String serviceName) {
            this.mechName = mechName;
            this.saslServiceName = serviceName;
        }

        static AuthenticationMechanisms fromValue(String mechName) {
            for (AuthenticationMechanisms am : values()) {
                if (am.mechName.equalsIgnoreCase(mechName)) {
                    return am;
                }
            }
            throw ExceptionFactory.createException(Messages.getString("AuthenticationLdapSaslClientPlugin.UnsupportedAuthMech", new String[] { mechName }));
        }

        String getMechName() {
            return this.mechName;
        }

        String getSaslServiceName() {
            return this.saslServiceName;
        }
    }

    private Protocol<?> protocol = null;
    private MysqlCallbackHandler usernameCallbackHandler = null;
    private String user = null;
    private String password = null;

    private AuthenticationMechanisms authMech = null;
    private SaslClient saslClient = null;
    private Subject subject = null;

    private boolean firstPass = true;

    private CallbackHandler credentialsCallbackHandler = (cbs) -> {
        for (Callback cb : cbs) {
            if (NameCallback.class.isAssignableFrom(cb.getClass())) {
                ((NameCallback) cb).setName(this.user);
            } else if (PasswordCallback.class.isAssignableFrom(cb.getClass())) {
                char[] passwordChars = this.password == null ? new char[0] : this.password.toCharArray();
                ((PasswordCallback) cb).setPassword(passwordChars);
            } else {
                throw new UnsupportedCallbackException(cb, cb.getClass().getName());
            }
        }
    };

    @Override
    public void init(Protocol<NativePacketPayload> prot) {
        this.protocol = prot;

        // Register our own SCRAM-SHA SASL Client provider.
        Security.addProvider(new ScramShaSaslProvider());
    }

    @Override
    public void init(Protocol<NativePacketPayload> prot, MysqlCallbackHandler cbh) {
        init(prot);
        this.usernameCallbackHandler = cbh;
    }

    @Override
    public void reset() {
        if (this.saslClient != null) {
            try {
                this.saslClient.dispose();
            } catch (SaslException e) {
                // Ignore exception.
            }
        }
        this.user = null;
        this.password = null;
        this.authMech = null;
        this.saslClient = null;
        this.subject = null;
        // this.firstPass must not be reset to 'true'. It is required to give it a second chance when AuthenticationLdapSaslClientPlugin is the default plugin.
    }

    @Override
    public void destroy() {
        reset();
        this.protocol = null;
        this.usernameCallbackHandler = null;
    }

    @Override
    public String getProtocolPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public boolean requiresConfidentiality() {
        return false;
    }

    @Override
    public boolean isReusable() {
        return false;
    }

    @Override
    public void setAuthenticationParameters(String user, String password) {
        this.user = user;
        this.password = password;

        if (this.user == null) {
            // Fall back to system login user.
            this.user = System.getProperty("user.name");
            if (this.usernameCallbackHandler != null) {
                this.usernameCallbackHandler.handle(new UsernameCallback(this.user));
            }
        }
    }

    @Override
    public boolean nextAuthenticationStep(NativePacketPayload fromServer, List<NativePacketPayload> toServer) {
        toServer.clear();

        if (this.saslClient == null) {
            // First packet: initialize a SASL client for the requested mechanism.
            String authMechId = fromServer.readString(StringSelfDataType.STRING_EOF, "ASCII");
            try {
                this.authMech = AuthenticationMechanisms.fromValue(authMechId);
            } catch (CJException e) {
                if (this.firstPass) {
                    this.firstPass = false;
                    // Payload could be a salt (auth-plugin-data) value instead of an authentication mechanism identifier.
                    // Give it another try in the expectation of receiving a Protocol::AuthSwitchRequest or a Protocol::AuthNextFactor next time.
                    return true;
                }
                throw e;
            }
            this.firstPass = false;

            try {
                switch (this.authMech) {
                    case GSSAPI:
                        // Figure out the LDAP Server hostname.
                        String ldapServerHostname = this.protocol.getPropertySet().getStringProperty(PropertyKey.ldapServerHostname).getValue();
                        if (StringUtils.isNullOrEmpty(ldapServerHostname)) { // Use the default KDC short name instead.
                            String krb5Kdc = System.getProperty("java.security.krb5.kdc");
                            if (!StringUtils.isNullOrEmpty(krb5Kdc)) {
                                ldapServerHostname = krb5Kdc;
                                int dotIndex = krb5Kdc.indexOf('.');
                                if (dotIndex > 0) {
                                    ldapServerHostname = krb5Kdc.substring(0, dotIndex).toLowerCase(Locale.ENGLISH);
                                }
                            }
                        }
                        if (StringUtils.isNullOrEmpty(ldapServerHostname)) {
                            throw ExceptionFactory.createException(Messages.getString("AuthenticationLdapSaslClientPlugin.MissingLdapServerHostname"));
                        }

                        // In-memory login configuration. Used only if system property 'java.security.auth.login.config' is not set.
                        String loginConfigFile = System.getProperty("java.security.auth.login.config");
                        Configuration loginConfig = null;
                        if (StringUtils.isNullOrEmpty(loginConfigFile)) {
                            final String localUser = this.user;
                            final boolean debug = Boolean.getBoolean("sun.security.jgss.debug");
                            loginConfig = new Configuration() {
                                @Override
                                public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                                    Map<String, String> options = new HashMap<>();
                                    options.put("useTicketCache", "true");
                                    options.put("renewTGT", "false");
                                    options.put("principal", localUser);
                                    options.put("debug", Boolean.toString(debug)); // Hook debugging on system property 'sun.security.jgss.debug'.
                                    return new AppConfigurationEntry[] { new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                                            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options) };
                                }
                            };
                        }

                        // Login into Kerberos service and obtain subject/credentials.
                        LoginContext loginContext = new LoginContext(LOGIN_CONFIG_ENTRY, null, this.credentialsCallbackHandler, loginConfig);
                        loginContext.login();
                        this.subject = loginContext.getSubject();

                        // Create a GSSAPI SASL client using the credentials stored in this thread's Subject.
                        try {
                            final String localLdapServerHostname = ldapServerHostname;
                            this.saslClient = Subject.doAs(this.subject,
                                    (PrivilegedExceptionAction<SaslClient>) () -> Sasl.createSaslClient(new String[] { this.authMech.getSaslServiceName() },
                                            null, LDAP_SERVICE_NAME, localLdapServerHostname, null, null));
                        } catch (PrivilegedActionException e) {
                            // SaslException is the only checked exception that can be thrown. 
                            throw (SaslException) e.getException();
                        }
                        break;

                    case SCRAM_SHA_1:
                    case SCRAM_SHA_256:
                        this.saslClient = Sasl.createSaslClient(new String[] { this.authMech.getSaslServiceName() }, null, null, null, null,
                                this.credentialsCallbackHandler);
                        break;
                }
            } catch (LoginException | SaslException e) {
                throw ExceptionFactory.createException(
                        Messages.getString("AuthenticationLdapSaslClientPlugin.FailCreateSaslClient", new Object[] { this.authMech.getMechName() }), e);
            }

            if (this.saslClient == null) {
                throw ExceptionFactory.createException(
                        Messages.getString("AuthenticationLdapSaslClientPlugin.FailCreateSaslClient", new Object[] { this.authMech.getMechName() }));
            }
        }

        if (!this.saslClient.isComplete()) {
            // All packets: send payload to the SASL client.
            try {
                Subject.doAs(this.subject, (PrivilegedExceptionAction<Void>) () -> {
                    byte[] response = this.saslClient.evaluateChallenge(fromServer.readBytes(StringSelfDataType.STRING_EOF));
                    if (response != null) {
                        NativePacketPayload packet = new NativePacketPayload(response);
                        packet.setPosition(0);
                        toServer.add(packet);
                    }
                    return null;
                });
            } catch (PrivilegedActionException e) {
                throw ExceptionFactory.createException(
                        Messages.getString("AuthenticationLdapSaslClientPlugin.ErrProcessingAuthIter", new Object[] { this.authMech.getMechName() }),
                        e.getException());
            }
        }
        return true;
    }
}
