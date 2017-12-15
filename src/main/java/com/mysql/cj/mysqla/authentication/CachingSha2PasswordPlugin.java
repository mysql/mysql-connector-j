/*
 * Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.mysqla.authentication;

import java.security.DigestException;
import java.util.List;

import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringLengthDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringSelfDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.authentication.Security;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.UnableToConnectException;
import com.mysql.cj.core.io.ExportControlled;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqla.io.Buffer;

public class CachingSha2PasswordPlugin extends Sha256PasswordPlugin {
    public static String PLUGIN_NAME = "caching_sha2_password";

    public enum AuthStage {
        FAST_AUTH_SEND_SCRAMBLE, FAST_AUTH_READ_RESULT, FAST_AUTH_COMPLETE, FULL_AUTH;
    }

    private AuthStage stage = AuthStage.FAST_AUTH_SEND_SCRAMBLE;

    @Override
    public void init(Protocol prot) {
        super.init(prot);
        this.stage = AuthStage.FAST_AUTH_SEND_SCRAMBLE;
    }

    @Override
    public void reset() {
        this.stage = AuthStage.FAST_AUTH_SEND_SCRAMBLE;
    }

    @Override
    public void destroy() {
        this.stage = AuthStage.FAST_AUTH_SEND_SCRAMBLE;
        super.destroy();
    }

    @Override
    public String getProtocolPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public boolean nextAuthenticationStep(PacketPayload fromServer, List<PacketPayload> toServer) {
        toServer.clear();

        if (this.password == null || this.password.length() == 0 || fromServer == null) {
            // no password
            PacketPayload bresp = new Buffer(new byte[] { 0 });
            toServer.add(bresp);

        } else {
            try {
                if (this.stage == AuthStage.FAST_AUTH_SEND_SCRAMBLE) {
                    // send a scramble for fast auth
                    this.seed = fromServer.readString(StringSelfDataType.STRING_TERM, null);
                    toServer.add(new Buffer(Security.scrambleCachingSha2(StringUtils.getBytes(this.password, this.protocol.getPasswordCharacterEncoding()),
                            this.seed.getBytes())));
                    this.stage = AuthStage.FAST_AUTH_READ_RESULT;
                    return true;

                } else if (this.stage == AuthStage.FAST_AUTH_READ_RESULT) {
                    int fastAuthResult = fromServer.readBytes(StringLengthDataType.STRING_FIXED, 1)[0];
                    switch (fastAuthResult) {
                        case 3:
                            this.stage = AuthStage.FAST_AUTH_COMPLETE;
                            return true;
                        case 4:
                            this.stage = AuthStage.FULL_AUTH;
                            break;
                        default:
                            throw ExceptionFactory.createException("Unknown server response after fast auth.", this.protocol.getExceptionInterceptor());
                    }
                }

                if (this.protocol.getSocketConnection().isSSLEstablished()) {
                    // allow plain text over SSL
                    PacketPayload bresp = new Buffer(StringUtils.getBytes(this.password, this.protocol.getPasswordCharacterEncoding()));
                    bresp.setPosition(bresp.getPayloadLength());
                    bresp.writeInteger(IntegerDataType.INT1, 0);
                    bresp.setPosition(0);
                    toServer.add(bresp);

                } else if (this.serverRSAPublicKeyFile.getValue() != null) {
                    // encrypt with given key, don't use "Public Key Retrieval"
                    PacketPayload bresp = new Buffer(
                            encryptPassword(this.password, this.seed, this.publicKeyString, this.protocol.getPasswordCharacterEncoding()));
                    toServer.add(bresp);

                } else {
                    if (!this.protocol.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_allowPublicKeyRetrieval).getValue()) {
                        throw ExceptionFactory.createException(UnableToConnectException.class, Messages.getString("Sha256PasswordPlugin.2"),
                                this.protocol.getExceptionInterceptor());

                    }

                    // We must request the public key from the server to encrypt the password
                    if (this.publicKeyRequested && fromServer.getPayloadLength() > MysqlaConstants.SEED_LENGTH) {
                        // Servers affected by Bug#70865 could send Auth Switch instead of key after Public Key Retrieval,
                        // so we check payload length to detect that.

                        // read key response
                        PacketPayload bresp = new Buffer(encryptPassword(this.password, this.seed, fromServer.readString(StringSelfDataType.STRING_TERM, null),
                                this.protocol.getPasswordCharacterEncoding()));
                        toServer.add(bresp);
                        this.publicKeyRequested = false;
                    } else {
                        // build and send Public Key Retrieval packet
                        PacketPayload bresp = new Buffer(new byte[] { 2 }); //was 1 in sha256_password
                        toServer.add(bresp);
                        this.publicKeyRequested = true;
                    }
                }
            } catch (CJException | DigestException e) {
                throw ExceptionFactory.createException(e.getMessage(), e, this.protocol.getExceptionInterceptor());
            }
        }
        return true;
    }

    private static byte[] encryptPassword(String password, String seed, String key, String passwordCharacterEncoding) {
        byte[] input = password != null ? StringUtils.getBytesNullTerminated(password, passwordCharacterEncoding) : new byte[] { 0 };
        byte[] mysqlScrambleBuff = new byte[input.length];
        Security.xorString(input, mysqlScrambleBuff, seed.getBytes(), input.length);

        // TODO in MySQL 8.0.3 the RSA_PKCS1_PADDING is used in caching_sha2_password full auth stage but in 8.0.4 it should be changed to RSA_PKCS1_OAEP_PADDING, the same as in sha256_password
        return ExportControlled.encryptWithRSAPublicKey(mysqlScrambleBuff, ExportControlled.decodeRSAPublicKey(key), "RSA/ECB/PKCS1Padding");
    }

}
