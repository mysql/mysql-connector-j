/*
 * Copyright (c) 2023, Oracle and/or its affiliates.
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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.List;

import com.mysql.cj.Messages;
import com.mysql.cj.callback.MysqlCallbackHandler;
import com.mysql.cj.callback.UsernameCallback;
import com.mysql.cj.callback.WebAuthnAuthenticationCallback;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.protocol.AuthenticationPlugin;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.util.Util;

/**
 * MySQL 'authentication_webauthn_client' authentication plugin.
 *
 * This authentication plugin requires a callback handler implemented in the client application that performs all the interactions with the authenticator
 * device. This callback handler is injected into the driver via the connection property 'authenticationWebauthnCallbackHandler'.
 */
public class AuthenticationWebAuthnClient implements AuthenticationPlugin<NativePacketPayload> {

    public static String PLUGIN_NAME = "authentication_webauthn_client";
    private static final String CLIENT_DATA_JSON = "{\"type\":\"webauthn.get\",\"challenge\":\"%s\",\"origin\":\"https://%s\",\"crossOrigin\":false }";

    private enum AuthStage {
        INITIAL_DATA, CREDENTIAL_ID, FINISHED;
    }

    private String sourceOfAuthData = PLUGIN_NAME;
    private AuthStage stage = AuthStage.INITIAL_DATA;
    private byte[] challenge = null;
    private String relyingPartyId = null;
    private String clientDataJson = null;
    private byte[] clientDataHash = null;
    private byte[] credentialId = null;

    private MysqlCallbackHandler usernameCallbackHandler = null;
    private MysqlCallbackHandler webAuthnAuthenticationCallbackHandler = null;
    private WebAuthnAuthenticationCallback webAuthnAuthCallback = null;

    @Override
    public void init(Protocol<NativePacketPayload> protocol, MysqlCallbackHandler callbackHandler) {
        this.usernameCallbackHandler = callbackHandler;

        String webAuthnCallbackHandlerClassName = protocol.getPropertySet().getStringProperty(PropertyKey.authenticationWebAuthnCallbackHandler).getValue();
        if (webAuthnCallbackHandlerClassName == null) {
            throw ExceptionFactory.createException(Messages.getString("AuthenticationWebAuthnClientPlugin.MissingCallbackHandler"));
        }

        this.webAuthnAuthenticationCallbackHandler = Util.getInstance(MysqlCallbackHandler.class, webAuthnCallbackHandlerClassName, null, null,
                protocol.getExceptionInterceptor());
    }

    @Override
    public void reset() {
        this.stage = AuthStage.INITIAL_DATA;
        this.challenge = null;
        this.relyingPartyId = null;
        this.clientDataJson = null;
        this.clientDataHash = null;
        this.credentialId = null;
    }

    @Override
    public void destroy() {
        reset();
        this.usernameCallbackHandler = null;
        this.webAuthnAuthenticationCallbackHandler = null;
        this.webAuthnAuthCallback = null;
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

        NativePacketPayload packet;

        switch (this.stage) {
            case INITIAL_DATA:
                if (fromServer.getPayloadLength() == 0) {
                    // FIDO device registration process was not completed.
                    throw ExceptionFactory.createException(Messages.getString("AuthenticationWebAuthnClientPlugin.IncompleteRegistration"));
                }

                fromServer.readInteger(IntegerDataType.INT1); // Reserved for future extensibility.
                this.challenge = fromServer.readBytes(StringSelfDataType.STRING_LENENC);
                this.relyingPartyId = fromServer.readString(StringSelfDataType.STRING_LENENC, "UTF-8");

                // Compute the Client Data Hash.
                Encoder b64Encoder = Base64.getUrlEncoder().withoutPadding();
                String b64EncodedChallenge = b64Encoder.encodeToString(this.challenge);
                this.clientDataJson = String.format(CLIENT_DATA_JSON, b64EncodedChallenge, this.relyingPartyId);
                MessageDigest digest;
                try {
                    digest = MessageDigest.getInstance("SHA-256");
                } catch (NoSuchAlgorithmException e) {
                    throw ExceptionFactory.createException(Messages.getString("AuthenticationWebAuthnClientPlugin.FaileMessageDigestSha256"), e);
                }
                this.clientDataHash = digest.digest(this.clientDataJson.getBytes(StandardCharsets.UTF_8));

                // Request the FIDO Credential Id from the server even though it may not have it, in which case the server returns an empty packet. This request
                // could be avoided if the Authenticator device supports Credentials Management. That information could be passed through
                // WebAuthnAuthenticationCallback.getSupportsCredentialManagement() and then used to decide whether the FIDO Credential Id request should be
                // sent or not, however that would add complexity to the MysqlCallbackHandler implementations.
                packet = new NativePacketPayload(new byte[] { 1 });
                toServer.add(packet);

                this.stage = AuthStage.CREDENTIAL_ID;
                break;
            case CREDENTIAL_ID:
                this.credentialId = fromServer.getPayloadLength() > 0 ? fromServer.readBytes(StringSelfDataType.STRING_LENENC) : new byte[0];

                // All data collected. Get assertions from the authenticator device..
                this.webAuthnAuthCallback = new WebAuthnAuthenticationCallback(this.clientDataHash, this.relyingPartyId, this.credentialId);
                this.webAuthnAuthenticationCallbackHandler.handle(this.webAuthnAuthCallback);

                int assertionsCount = this.webAuthnAuthCallback.getAssertCount();
                int authenticatorDataLength = 0;
                int signaturesLength = 0;
                for (int i = 0; i < assertionsCount; i++) {
                    authenticatorDataLength += this.webAuthnAuthCallback.getAuthenticatorData(i).length;
                    signaturesLength += this.webAuthnAuthCallback.getSignature(i).length;
                }

                int packetLen = 1; // 1 for status tag
                packetLen += 1; // + 1 for number of assertions
                packetLen += authenticatorDataLength + signaturesLength + 2 * assertionsCount; // + ((1 + 1) * assertionsCount) for length encoding
                packetLen += this.clientDataJson.length() + 1; // + 1 for length encoding
                packet = new NativePacketPayload(packetLen);
                packet.writeInteger(IntegerDataType.INT1, 2);
                packet.writeInteger(IntegerDataType.INT_LENENC, assertionsCount);
                for (int i = 0; i < assertionsCount; i++) {
                    byte[] authenticatorData = this.webAuthnAuthCallback.getAuthenticatorData(i);
                    if (authenticatorData == null || authenticatorData.length == 0) {
                        throw ExceptionFactory.createException(Messages.getString("AuthenticationWebAuthnClientPlugin.InvalidAuthenticatorData"));
                    }
                    packet.writeBytes(StringSelfDataType.STRING_LENENC, authenticatorData);
                    byte[] signature = this.webAuthnAuthCallback.getSignature(i);
                    if (signature == null || signature.length == 0) {
                        throw ExceptionFactory.createException(Messages.getString("AuthenticationWebAuthnClientPlugin.InvalidSignature"));
                    }
                    packet.writeBytes(StringSelfDataType.STRING_LENENC, signature);
                }
                packet.writeBytes(StringSelfDataType.STRING_LENENC, this.clientDataJson.getBytes(StandardCharsets.UTF_8));
                toServer.add(packet);

                this.stage = AuthStage.FINISHED;
                break;
            case FINISHED:
                throw ExceptionFactory.createException(Messages.getString("AuthenticationWebAuthnClientPlugin.AuthenticationFactorComplete"));
        }

        return true;
    }

}
