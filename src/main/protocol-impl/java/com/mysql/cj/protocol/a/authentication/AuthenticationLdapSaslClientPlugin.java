/*
 * Copyright (c) 2020, Oracle and/or its affiliates.
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

import java.security.Security;
import java.util.List;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.protocol.AuthenticationPlugin;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.sasl.ScramSha1SaslClient;
import com.mysql.cj.sasl.ScramSha1SaslProvider;

/**
 * MySQL 'authentication_ldap_sasl_client' authentication plugin.
 */
public class AuthenticationLdapSaslClientPlugin implements AuthenticationPlugin<NativePacketPayload> {
    static {
        // Register our SCRAM-SHA-1 SASL Client provider.
        Security.addProvider(new ScramSha1SaslProvider());
    }

    private enum AuthenticationMechanisms {
        SCRAM_SHA_1("SCRAM-SHA-1", ScramSha1SaslClient.MECHANISM_NAME), GSSAPI("GSSAPI", "GSSAPI");

        private String mechName;
        private String saslServiceName;

        private AuthenticationMechanisms(String mechName, String serviceName) {
            this.mechName = mechName;
            this.saslServiceName = serviceName;
        }

        static AuthenticationMechanisms fromValue(String mechName) {
            for (AuthenticationMechanisms am : values()) {
                if (am != GSSAPI && am.mechName.equalsIgnoreCase(mechName)) { // Only SCRAM-SHA-1 is supported.
                    return am;
                }
            }
            throw ExceptionFactory.createException(Messages.getString("AuthenticationLdapSaslClientPlugin.UnsupportedAuthMech"));
        }

        String getMechName() {
            return this.mechName;
        }

        String getSaslServiceName() {
            return this.saslServiceName;
        }
    }

    private AuthenticationMechanisms authMech;
    private SaslClient saslClient;
    private String user;
    private String password;

    @Override
    public void init(Protocol<NativePacketPayload> prot) {
    }

    @Override
    public void reset() {
        if (this.saslClient != null) {
            try {
                this.saslClient.dispose();
                this.saslClient = null;
            } catch (SaslException e) {
                // Ignore exception.
            }
        }
        this.authMech = null;
        this.saslClient = null;
        this.user = null;
        this.password = null;
    }

    @Override
    public void destroy() {
        reset();
    }

    @Override
    public String getProtocolPluginName() {
        return "authentication_ldap_sasl_client";
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
    }

    @Override
    public boolean nextAuthenticationStep(NativePacketPayload fromServer, List<NativePacketPayload> toServer) {
        toServer.clear();

        try {
            if (this.saslClient == null) { // First packet: initialize a SASL client for the requested mechanism. 
                this.authMech = AuthenticationMechanisms.fromValue(fromServer.readString(StringSelfDataType.STRING_EOF, "ASCII"));
                CallbackHandler cbh = (cbs) -> {
                    for (Callback cb : cbs) {
                        if (NameCallback.class.isAssignableFrom(cb.getClass())) {
                            ((NameCallback) cb).setName(this.user);
                        } else if (PasswordCallback.class.isAssignableFrom(cb.getClass())) {
                            ((PasswordCallback) cb).setPassword(this.password.toCharArray());
                        } else {
                            throw new UnsupportedCallbackException(cb);
                        }
                    }
                };
                this.saslClient = Sasl.createSaslClient(new String[] { this.authMech.getSaslServiceName() }, null, null, null, null, cbh);

                if (this.saslClient == null) {
                    throw ExceptionFactory.createException(
                            Messages.getString("AuthenticationLdapSaslClientPlugin.FailCreateSaslClient", new Object[] { this.authMech.getMechName() }));
                }
            }

            // All packets: send payload to the SASL client.
            byte[] response = this.saslClient.evaluateChallenge(fromServer.readBytes(StringSelfDataType.STRING_EOF));
            if (response != null) {
                NativePacketPayload bresp = new NativePacketPayload(response);
                bresp.setPosition(0);
                toServer.add(bresp);
            }
        } catch (SaslException e) {
            throw ExceptionFactory.createException(
                    Messages.getString("AuthenticationLdapSaslClientPlugin.ErrProcessingAuthIter", new Object[] { this.authMech.getMechName() }), e);
        }
        return true;
    }
}
