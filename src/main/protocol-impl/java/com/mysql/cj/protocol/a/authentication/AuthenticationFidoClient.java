/*
 * Copyright (c) 2022, 2023, Oracle and/or its affiliates.
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

import java.util.List;

import com.mysql.cj.Messages;
import com.mysql.cj.callback.FidoAuthenticationCallback;
import com.mysql.cj.callback.MysqlCallbackHandler;
import com.mysql.cj.callback.UsernameCallback;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.protocol.AuthenticationPlugin;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.util.Util;

/**
 * MySQL 'authentication_fido_client' authentication plugin.
 *
 * This authentication plugin requires a callback handler implemented in the client application that performs all the interactions with the authenticator
 * device. This callback handler is injected into the driver via the connection property 'authenticationFidoCallbackHandler'.
 */
public class AuthenticationFidoClient implements AuthenticationPlugin<NativePacketPayload> {

    public static String PLUGIN_NAME = "authentication_fido_client";

    private String sourceOfAuthData = PLUGIN_NAME;

    private MysqlCallbackHandler usernameCallbackHandler = null;
    private MysqlCallbackHandler fidoAuthenticationCallbackHandler = null;

    @Override
    public void init(Protocol<NativePacketPayload> protocol, MysqlCallbackHandler callbackHandler) {
        this.usernameCallbackHandler = callbackHandler;

        String fidoCallbackHandlerClassName = protocol.getPropertySet().getStringProperty(PropertyKey.authenticationFidoCallbackHandler).getValue();
        if (fidoCallbackHandlerClassName == null) {
            throw ExceptionFactory.createException(Messages.getString("AuthenticationFidoClientPlugin.MissingCallbackHandler"));
        }

        this.fidoAuthenticationCallbackHandler = Util.getInstance(MysqlCallbackHandler.class, fidoCallbackHandlerClassName, null, null,
                protocol.getExceptionInterceptor());
    }

    @Override
    public void destroy() {
        reset();
        this.usernameCallbackHandler = null;
        this.fidoAuthenticationCallbackHandler = null;
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
        if (user == null && this.usernameCallbackHandler != null) {
            // Fall back to system login user.
            this.usernameCallbackHandler.handle(new UsernameCallback(System.getProperty("user.name")));
        }
    }

    @Override
    public void setSourceOfAuthData(String sourceOfAuthData) {
        this.sourceOfAuthData = sourceOfAuthData;
    }

    @Override
    public boolean nextAuthenticationStep(NativePacketPayload fromServer, List<NativePacketPayload> toServer) {
        toServer.clear();

        if (!this.sourceOfAuthData.equals(PLUGIN_NAME)) {
            // Cannot do anything with whatever payload comes from the server, so just skip this iteration and wait for a Protocol::AuthSwitchRequest or a
            // Protocol::AuthNextFactor.
            return true;
        }

        if (fromServer.getPayloadLength() == 0) {
            // FIDO device registration process was not completed.
            throw ExceptionFactory.createException(Messages.getString("AuthenticationFidoClientPlugin.IncompleteRegistration"));
        }

        byte[] scramble = fromServer.readBytes(StringSelfDataType.STRING_LENENC);
        String relyingPartyId = fromServer.readString(StringSelfDataType.STRING_LENENC, "UTF-8");
        byte[] credentialId = fromServer.readBytes(StringSelfDataType.STRING_LENENC);

        FidoAuthenticationCallback fidoAuthCallback = new FidoAuthenticationCallback(scramble, relyingPartyId, credentialId);
        this.fidoAuthenticationCallbackHandler.handle(fidoAuthCallback);

        byte[] authenticatorData = fidoAuthCallback.getAuthenticatorData();
        if (authenticatorData == null || authenticatorData.length == 0) {
            throw ExceptionFactory.createException(Messages.getString("AuthenticationFidoClientPlugin.InvalidAuthenticatorData"));
        }
        byte[] signature = fidoAuthCallback.getSignature();
        if (signature == null || signature.length == 0) {
            throw ExceptionFactory.createException(Messages.getString("AuthenticationFidoClientPlugin.InvalidSignature"));
        }

        NativePacketPayload packet = new NativePacketPayload(authenticatorData.length + signature.length + 2); // 1 + 1 Bytes for length encoding.
        packet.writeBytes(StringSelfDataType.STRING_LENENC, authenticatorData);
        packet.writeBytes(StringSelfDataType.STRING_LENENC, signature);
        toServer.add(packet);

        return true;
    }

}
