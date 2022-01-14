/*
 * Copyright (c) 2017, 2022, Oracle and/or its affiliates.
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

import java.security.DigestException;
import java.util.List;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.UnableToConnectException;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.Security;
import com.mysql.cj.protocol.a.NativeConstants;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.util.StringUtils;

public class CachingSha2PasswordPlugin extends Sha256PasswordPlugin {
    public static String PLUGIN_NAME = "caching_sha2_password";

    public enum AuthStage {
        FAST_AUTH_SEND_SCRAMBLE, FAST_AUTH_READ_RESULT, FAST_AUTH_COMPLETE, FULL_AUTH;
    }

    private AuthStage stage = AuthStage.FAST_AUTH_SEND_SCRAMBLE;

    @Override
    public void init(Protocol<NativePacketPayload> prot) {
        super.init(prot);
        this.stage = AuthStage.FAST_AUTH_SEND_SCRAMBLE;
    }

    @Override
    public void reset() {
        this.stage = AuthStage.FAST_AUTH_SEND_SCRAMBLE;
    }

    @Override
    public void destroy() {
        super.destroy();
        this.stage = AuthStage.FAST_AUTH_SEND_SCRAMBLE;
    }

    @Override
    public String getProtocolPluginName() {
        return PLUGIN_NAME;
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
                if (this.stage == AuthStage.FAST_AUTH_SEND_SCRAMBLE) {
                    // send a scramble for fast auth
                    this.seed = fromServer.readString(StringSelfDataType.STRING_TERM, null);
                    toServer.add(new NativePacketPayload(Security.scrambleCachingSha2(
                            StringUtils.getBytes(this.password, this.protocol.getServerSession().getCharsetSettings().getPasswordCharacterEncoding()),
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
                    NativePacketPayload packet = new NativePacketPayload(
                            StringUtils.getBytes(this.password, this.protocol.getServerSession().getCharsetSettings().getPasswordCharacterEncoding()));
                    packet.setPosition(packet.getPayloadLength());
                    packet.writeInteger(IntegerDataType.INT1, 0);
                    packet.setPosition(0);
                    toServer.add(packet);

                } else if (this.serverRSAPublicKeyFile.getValue() != null) {
                    // encrypt with given key, don't use "Public Key Retrieval"
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
                        NativePacketPayload packet = new NativePacketPayload(new byte[] { 2 }); // was 1 in sha256_password
                        toServer.add(packet);
                        this.publicKeyRequested = true;
                    }
                }
            } catch (CJException | DigestException e) {
                throw ExceptionFactory.createException(e.getMessage(), e, this.protocol.getExceptionInterceptor());
            }
        }
        return true;
    }

    @Override
    protected byte[] encryptPassword() {
        if (this.protocol.versionMeetsMinimum(8, 0, 5)) {
            return super.encryptPassword();
        }
        return super.encryptPassword("RSA/ECB/PKCS1Padding");
    }
}
