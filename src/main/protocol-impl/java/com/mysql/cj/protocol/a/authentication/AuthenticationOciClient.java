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

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.interfaces.RSAPrivateKey;
import java.util.Base64;
import java.util.List;

import com.mysql.cj.Messages;
import com.mysql.cj.callback.MysqlCallbackHandler;
import com.mysql.cj.callback.UsernameCallback;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.RSAException;
import com.mysql.cj.protocol.AuthenticationPlugin;
import com.mysql.cj.protocol.ExportControlled;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.util.StringUtils;
import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.ConfigFileReader.ConfigFile;

/**
 * MySQL 'authentication_iam_client' authentication plugin.
 */
public class AuthenticationOciClient implements AuthenticationPlugin<NativePacketPayload> {
    public static String PLUGIN_NAME = "authentication_oci_client";

    private String sourceOfAuthData = PLUGIN_NAME;

    protected Protocol<NativePacketPayload> protocol = null;
    private MysqlCallbackHandler usernameCallbackHandler = null;
    private String fingerprint = null;
    private RSAPrivateKey privateKey = null;

    @Override
    public void init(Protocol<NativePacketPayload> prot, MysqlCallbackHandler cbh) {
        this.protocol = prot;
        this.usernameCallbackHandler = cbh;
    }

    @Override
    public void reset() {
        this.fingerprint = null;
        this.privateKey = null;
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

        if (!this.sourceOfAuthData.equals(PLUGIN_NAME) || fromServer.getPayloadLength() == 0) {
            // Cannot do anything with whatever payload comes from the server, so just skip this iteration and wait for a Protocol::AuthSwitchRequest or a
            // Protocol::AuthNextFactor.
            toServer.add(new NativePacketPayload(0));
            return true;
        }

        initializePrivateKey();

        byte[] nonce = fromServer.readBytes(StringSelfDataType.STRING_EOF);
        byte[] signature = ExportControlled.sign(nonce, this.privateKey);
        if (signature == null) {
            signature = new byte[0];
        }
        String payload = String.format("{\"fingerprint\":\"%s\", \"signature\":\"%s\"}", this.fingerprint, Base64.getEncoder().encodeToString(signature));
        toServer.add(new NativePacketPayload(payload.getBytes(Charset.defaultCharset())));
        return true;
    }

    private void initializePrivateKey() {
        if (this.privateKey != null) {
            // Already initialized.
            return;
        }

        ConfigFile configFile;
        try {
            String configFilePath = this.protocol.getPropertySet().getStringProperty(PropertyKey.ociConfigFile.getKeyName()).getStringValue();
            if (StringUtils.isNullOrEmpty(configFilePath)) {
                configFile = ConfigFileReader.parseDefault();
            } else if (Files.exists(Paths.get(configFilePath))) {
                configFile = ConfigFileReader.parse(configFilePath);
            } else {
                throw ExceptionFactory.createException("configuration file does not exist");
            }
        } catch (NoClassDefFoundError e) {
            throw ExceptionFactory.createException(Messages.getString("AuthenticationOciClientPlugin.SdkNotFound"), e);
        } catch (IOException e) {
            throw ExceptionFactory.createException(Messages.getString("AuthenticationOciClientPlugin.OciConfigFileError"), e);
        }
        this.fingerprint = configFile.get("fingerprint");
        if (StringUtils.isNullOrEmpty(this.fingerprint)) {
            throw ExceptionFactory.createException(Messages.getString("AuthenticationOciClientPlugin.OciConfigFileMissingEntry"));
        }
        String keyFilePath = configFile.get("key_file");
        if (StringUtils.isNullOrEmpty(keyFilePath)) {
            throw ExceptionFactory.createException(Messages.getString("AuthenticationOciClientPlugin.OciConfigFileMissingEntry"));
        }

        try {
            String key = new String(Files.readAllBytes(Paths.get(keyFilePath)), Charset.defaultCharset());
            this.privateKey = ExportControlled.decodeRSAPrivateKey(key);
        } catch (IOException e) {
            throw ExceptionFactory.createException(Messages.getString("AuthenticationOciClientPlugin.PrivateKeyNotFound"), e);
        } catch (RSAException | IllegalArgumentException e) {
            throw ExceptionFactory.createException(Messages.getString("AuthenticationOciClientPlugin.PrivateKeyNotValid"), e);
        }
    }
}
