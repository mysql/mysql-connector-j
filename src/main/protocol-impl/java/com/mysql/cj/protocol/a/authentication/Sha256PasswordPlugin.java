/*
 * Copyright (c) 2012, 2024, Oracle and/or its affiliates.
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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import com.mysql.cj.Messages;
import com.mysql.cj.callback.MysqlCallbackHandler;
import com.mysql.cj.callback.UsernameCallback;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.UnableToConnectException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.AuthenticationPlugin;
import com.mysql.cj.protocol.ExportControlled;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.Security;
import com.mysql.cj.protocol.a.NativeConstants;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.util.StringUtils;

public class Sha256PasswordPlugin implements AuthenticationPlugin<NativePacketPayload> {

    public static String PLUGIN_NAME = "sha256_password";

    protected Protocol<NativePacketPayload> protocol = null;
    protected MysqlCallbackHandler usernameCallbackHandler = null;
    protected String password = null;
    protected String seed = null;
    protected boolean publicKeyRequested = false;
    protected String publicKeyString = null;
    protected RuntimeProperty<String> serverRSAPublicKeyFile = null;

    @Override
    public void init(Protocol<NativePacketPayload> prot, MysqlCallbackHandler cbh) {
        this.protocol = prot;
        this.usernameCallbackHandler = cbh;
        this.serverRSAPublicKeyFile = this.protocol.getPropertySet().getStringProperty(PropertyKey.serverRSAPublicKeyFile);

        String pkURL = this.serverRSAPublicKeyFile.getValue();
        if (pkURL != null) {
            this.publicKeyString = readRSAKey(pkURL, this.protocol.getPropertySet(), this.protocol.getExceptionInterceptor());
        }
    }

    @Override
    public void destroy() {
        reset();
        this.protocol = null;
        this.usernameCallbackHandler = null;
        this.password = null;
        this.seed = null;
        this.publicKeyRequested = false;
        this.publicKeyString = null;
        this.serverRSAPublicKeyFile = null;
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
        return true;
    }

    @Override
    public void setAuthenticationParameters(String user, String password) {
        this.password = password;
        if (user == null && this.usernameCallbackHandler != null) {
            // Fall back to system login user.
            this.usernameCallbackHandler.handle(new UsernameCallback(System.getProperty("user.name")));
        }
    }

    @Override
    public boolean nextAuthenticationStep(NativePacketPayload fromServer, List<NativePacketPayload> toServer) {
        toServer.clear();

        if (this.password == null || this.password.length() == 0 || fromServer == null) {
            // no password
            NativePacketPayload packet = new NativePacketPayload(new byte[] { 0 });
            toServer.add(packet);

        } else {
            try {
                if (this.protocol.getSocketConnection().isSSLEstablished()) {
                    // allow plain text over SSL
                    NativePacketPayload packet = new NativePacketPayload(
                            StringUtils.getBytes(this.password, this.protocol.getServerSession().getCharsetSettings().getPasswordCharacterEncoding()));
                    packet.setPosition(packet.getPayloadLength());
                    packet.writeInteger(IntegerDataType.INT1, 0);
                    packet.setPosition(0);
                    toServer.add(packet);

                } else if (this.serverRSAPublicKeyFile.getValue() != null) {
                    // encrypt with given key, don't use "Public Key Retrieval"
                    this.seed = fromServer.readString(StringSelfDataType.STRING_TERM, null);
                    NativePacketPayload packet = new NativePacketPayload(encryptPassword());
                    toServer.add(packet);

                } else {
                    if (!this.protocol.getPropertySet().getBooleanProperty(PropertyKey.allowPublicKeyRetrieval).getValue()) {
                        throw ExceptionFactory.createException(UnableToConnectException.class, Messages.getString("Sha256PasswordPlugin.2"),
                                this.protocol.getExceptionInterceptor());

                    }

                    // We must request the public key from the server to encrypt the password
                    if (this.publicKeyRequested && fromServer.getPayloadLength() > NativeConstants.SEED_LENGTH + 1) { // auth data is null terminated
                        // Servers affected by Bug#70865 could send Auth Switch instead of key after Public Key Retrieval,
                        // so we check payload length to detect that.

                        // read key response
                        this.publicKeyString = fromServer.readString(StringSelfDataType.STRING_TERM, null);
                        NativePacketPayload packet = new NativePacketPayload(encryptPassword());
                        toServer.add(packet);
                        this.publicKeyRequested = false;
                    } else {
                        // build and send Public Key Retrieval packet
                        this.seed = fromServer.readString(StringSelfDataType.STRING_TERM, null);
                        NativePacketPayload packet = new NativePacketPayload(new byte[] { 1 });
                        toServer.add(packet);
                        this.publicKeyRequested = true;
                    }
                }
            } catch (CJException e) {
                throw ExceptionFactory.createException(e.getMessage(), e, this.protocol.getExceptionInterceptor());
            }
        }
        return true;
    }

    protected byte[] encryptPassword() {
        return encryptPassword("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
    }

    protected byte[] encryptPassword(String transformation) {
        byte[] input = null;
        input = this.password != null
                ? StringUtils.getBytesNullTerminated(this.password, this.protocol.getServerSession().getCharsetSettings().getPasswordCharacterEncoding())
                : new byte[] { 0 };
        byte[] mysqlScrambleBuff = new byte[input.length];
        Security.xorString(input, mysqlScrambleBuff, this.seed.getBytes(), input.length);
        return ExportControlled.encryptWithRSAPublicKey(mysqlScrambleBuff, ExportControlled.decodeRSAPublicKey(this.publicKeyString), transformation);
    }

    protected static String readRSAKey(String pkPath, PropertySet propertySet, ExceptionInterceptor exceptionInterceptor) {
        String res = null;
        byte[] fileBuf = new byte[2048];

        BufferedInputStream fileIn = null;

        try {
            File f = new File(pkPath);
            String canonicalPath = f.getCanonicalPath();
            fileIn = new BufferedInputStream(new FileInputStream(canonicalPath));

            int bytesRead = 0;

            StringBuilder sb = new StringBuilder();
            while ((bytesRead = fileIn.read(fileBuf)) != -1) {
                sb.append(StringUtils.toAsciiString(fileBuf, 0, bytesRead));
            }
            res = sb.toString();

        } catch (IOException ioEx) {

            throw ExceptionFactory.createException(WrongArgumentException.class,
                    Messages.getString("Sha256PasswordPlugin.0",
                            propertySet.getBooleanProperty(PropertyKey.paranoid).getValue() ? new Object[] { "" } : new Object[] { "'" + pkPath + "'" }),
                    exceptionInterceptor);

        } finally {
            if (fileIn != null) {
                try {
                    fileIn.close();
                } catch (IOException e) {
                    throw ExceptionFactory.createException(Messages.getString("Sha256PasswordPlugin.1"), e, exceptionInterceptor);
                }
            }
        }

        return res;
    }

}
