/*
 * Copyright (c) 2021, 2022, Oracle and/or its affiliates.
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
import java.util.HashMap;
import java.util.List;
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
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.protocol.AuthenticationPlugin;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.util.StringUtils;

/**
 * MySQL 'authentication_kerberos_client' authentication plugin.
 */
public class AuthenticationKerberosClient implements AuthenticationPlugin<NativePacketPayload> {
    public static String PLUGIN_NAME = "authentication_kerberos_client";

    private static final String LOGIN_CONFIG_ENTRY = "MySQLConnectorJ";
    private static final String AUTHENTICATION_MECHANISM = "GSSAPI";

    private String sourceOfAuthData = PLUGIN_NAME;

    private MysqlCallbackHandler usernameCallbackHandler = null;
    private String user = null;
    private String password = null;
    private String userPrincipalName = null;

    private Subject subject = null;
    private String cachedPrincipalName = null;

    private CallbackHandler credentialsCallbackHandler = (cbs) -> {
        for (Callback cb : cbs) {
            if (NameCallback.class.isAssignableFrom(cb.getClass())) {
                ((NameCallback) cb).setName(this.userPrincipalName);
            } else if (PasswordCallback.class.isAssignableFrom(cb.getClass())) {
                ((PasswordCallback) cb).setPassword(this.password == null ? new char[0] : this.password.toCharArray());
            } else {
                throw new UnsupportedCallbackException(cb, cb.getClass().getName());
            }
        }
    };

    private SaslClient saslClient = null;

    @Override
    public void init(Protocol<NativePacketPayload> prot, MysqlCallbackHandler cbh) {
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
        this.saslClient = null;
    }

    @Override
    public void destroy() {
        reset();
        this.usernameCallbackHandler = null;
        this.userPrincipalName = null;
        this.subject = null;
        this.cachedPrincipalName = null;
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
            try {
                // Try to obtain the user name from a cached TGT.
                initializeAuthentication();
                int pos = this.cachedPrincipalName.indexOf('@');
                if (pos >= 0) {
                    this.user = this.cachedPrincipalName.substring(0, pos);
                } else {
                    this.user = this.cachedPrincipalName;
                }
            } catch (CJException e) {
                // Fall back to system login user.
                this.user = System.getProperty("user.name");
            }
            if (this.usernameCallbackHandler != null) {
                this.usernameCallbackHandler.handle(new UsernameCallback(this.user));
            }
        }
    }

    @Override
    public void setSourceOfAuthData(String sourceOfAuthData) {
        this.sourceOfAuthData = sourceOfAuthData;
    }

    @Override
    public boolean nextAuthenticationStep(NativePacketPayload fromServer, List<NativePacketPayload> toServer) {
        toServer.clear();

        if (!this.sourceOfAuthData.equals(PLUGIN_NAME) || fromServer.getPayloadLength() == 0) {
            // Cannot do anything with whatever payload comes from the server, so just skip this iteration and wait for a Protocol::AuthSwitchRequest or a
            // Protocol::AuthNextFactor.
            return true;
        }

        if (this.saslClient == null) {
            try {
                // Protocol::AuthSwitchRequest plugin data contains:
                //   int<2> SPN string length
                //   string<VAR> SPN string
                //   int<2> User Principal Name realm string length
                //   string<VAR> User Principal Name realm string
                int servicePrincipalNameLength = (int) fromServer.readInteger(IntegerDataType.INT2);
                String servicePrincipalName = fromServer.readString(StringLengthDataType.STRING_VAR, "ASCII", servicePrincipalNameLength);
                // A typical Kerberos V5 principal has the structure "primary/instance@REALM".
                String primary = "";
                String instance = "";
                // Being a bit lenient here: the spec allows escaping characters (https://tools.ietf.org/html/rfc1964#section-2.1.1).
                int posAt = servicePrincipalName.indexOf('@');
                if (posAt < 0) {
                    posAt = servicePrincipalName.length();
                }
                int posSlash = servicePrincipalName.lastIndexOf('/', posAt);
                if (posSlash >= 0) {
                    primary = servicePrincipalName.substring(0, posSlash);
                    instance = servicePrincipalName.substring(posSlash + 1, posAt);
                } else {
                    primary = servicePrincipalName.substring(0, posAt);
                }

                int userPrincipalRealmLength = (int) fromServer.readInteger(IntegerDataType.INT2);
                String userPrincipalRealm = fromServer.readString(StringLengthDataType.STRING_VAR, "ASCII", userPrincipalRealmLength);
                this.userPrincipalName = this.user + "@" + userPrincipalRealm;

                initializeAuthentication();

                // Create a GSSAPI SASL client using the credentials stored in this thread's Subject.
                try {
                    final String localPrimary = primary;
                    final String localInstance = instance;
                    this.saslClient = Subject.doAs(this.subject, (PrivilegedExceptionAction<SaslClient>) () -> Sasl
                            .createSaslClient(new String[] { AUTHENTICATION_MECHANISM }, null, localPrimary, localInstance, null, null));
                } catch (PrivilegedActionException e) {
                    // SaslException is the only checked exception that can be thrown. 
                    throw (SaslException) e.getException();
                }

            } catch (SaslException e) {
                throw ExceptionFactory.createException(
                        Messages.getString("AuthenticationKerberosClientPlugin.FailCreateSaslClient", new Object[] { AUTHENTICATION_MECHANISM }), e);
            }

            if (this.saslClient == null) {
                throw ExceptionFactory.createException(
                        Messages.getString("AuthenticationKerberosClientPlugin.FailCreateSaslClient", new Object[] { AUTHENTICATION_MECHANISM }));
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
                        Messages.getString("AuthenticationKerberosClientPlugin.ErrProcessingAuthIter", new Object[] { AUTHENTICATION_MECHANISM }),
                        e.getException());
            }
        }
        return true;
    }

    private void initializeAuthentication() {
        if (this.subject != null && this.cachedPrincipalName != null && this.cachedPrincipalName.equals(this.userPrincipalName)) {
            // Already initialized with the right user.
            return;
        }

        // In-memory login configuration. Used only if system property 'java.security.auth.login.config' is not set.
        String loginConfigFile = System.getProperty("java.security.auth.login.config");
        Configuration loginConfig = null;
        if (StringUtils.isNullOrEmpty(loginConfigFile)) {
            final String localUser = this.userPrincipalName;
            final boolean debug = Boolean.getBoolean("sun.security.jgss.debug");
            loginConfig = new Configuration() {
                @Override
                public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                    Map<String, String> options = new HashMap<>();
                    options.put("useTicketCache", "true");
                    options.put("renewTGT", "false");
                    if (localUser != null) {
                        options.put("principal", localUser);
                    }
                    options.put("debug", Boolean.toString(debug)); // Hook debugging on system property 'sun.security.jgss.debug'.
                    return new AppConfigurationEntry[] { new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options) };
                }
            };
        }

        // Login into Kerberos service and obtain the user subject/credentials.
        LoginContext loginContext;
        try {
            loginContext = new LoginContext(LOGIN_CONFIG_ENTRY, null, this.credentialsCallbackHandler, loginConfig);
            loginContext.login();
            this.subject = loginContext.getSubject();
            this.cachedPrincipalName = this.subject.getPrincipals().iterator().next().getName();
        } catch (LoginException e) {
            throw ExceptionFactory.createException(Messages.getString("AuthenticationKerberosClientPlugin.FailAuthenticateUser"), e);
        }
    }
}
