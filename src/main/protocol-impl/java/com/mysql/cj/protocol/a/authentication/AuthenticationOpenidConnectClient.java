/*
 * Copyright (c) 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.a.authentication;

import java.util.List;

import com.mysql.cj.Messages;
import com.mysql.cj.callback.MysqlCallbackHandler;
import com.mysql.cj.callback.OpenidConnectAuthenticationCallback;
import com.mysql.cj.callback.UsernameCallback;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.AuthenticationPlugin;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.util.Util;

/**
 * MySQL 'authentication_openid_connect_client' authentication plugin.
 */
public class AuthenticationOpenidConnectClient implements AuthenticationPlugin<NativePacketPayload> {

    public static String PLUGIN_NAME = "authentication_openid_connect_client";

    private Protocol<NativePacketPayload> protocol = null;
    private MysqlCallbackHandler usernameCallbackHandler = null;
    private String user = null;

    private MysqlCallbackHandler openidConnectAuthenticationCallbackHandler = null;
    private OpenidConnectAuthenticationCallback openidConnectAuthCallback = null;

    @Override
    public void init(Protocol<NativePacketPayload> prot, MysqlCallbackHandler cbh) {
        this.protocol = prot;
        this.usernameCallbackHandler = cbh;

        String webAuthnCallbackHandlerClassName = this.protocol.getPropertySet().getStringProperty(PropertyKey.authenticationOpenidConnectCallbackHandler)
                .getValue();
        if (webAuthnCallbackHandlerClassName == null) {
            throw ExceptionFactory.createException(Messages.getString("AuthenticationWebAuthnClientPlugin.MissingCallbackHandler"));
        }

        this.openidConnectAuthenticationCallbackHandler = Util.getInstance(MysqlCallbackHandler.class, webAuthnCallbackHandlerClassName, null, null,
                this.protocol.getExceptionInterceptor());
    }

    @Override
    public void destroy() {
        reset();
        this.protocol = null;
        this.usernameCallbackHandler = null;
        this.openidConnectAuthenticationCallbackHandler = null;
        this.openidConnectAuthCallback = null;
    }

    @Override
    public String getProtocolPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public boolean requiresConfidentiality() {
        return true;
    }

    @Override
    public boolean isReusable() {
        return true;
    }

    @Override
    public void setAuthenticationParameters(String user, String password) {
        this.user = user;

        if (user == null) {
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

        if (this.openidConnectAuthCallback == null) {
            this.openidConnectAuthCallback = new OpenidConnectAuthenticationCallback(this::getPropertyStringValue);
        }
        this.openidConnectAuthCallback.setUser(this.user);
        this.openidConnectAuthenticationCallbackHandler.handle(this.openidConnectAuthCallback);

        byte[] idToken = this.openidConnectAuthCallback.getIdentityToken();
        if (idToken == null || idToken.length == 0) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("AuthenticationOpenidConnect.EmptyIdentityToken"));
        }

        int packetLen = 10; // 1 Byte for capability flag + up to 9 Bytes lenenc.
        packetLen += idToken.length;

        NativePacketPayload packet = new NativePacketPayload(packetLen);
        packet.writeInteger(IntegerDataType.INT1, 1);
        packet.writeBytes(StringSelfDataType.STRING_LENENC, idToken);
        packet.setPosition(0);
        toServer.add(packet);

        return true;
    }

    private String getPropertyStringValue(PropertyKey key) {
        RuntimeProperty<?> prop = this.protocol.getPropertySet().getProperty(key);
        return prop == null ? null : prop.getStringValue();
    }

}
