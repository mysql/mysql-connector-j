/*
 * Copyright (c) 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.protocol.x;

import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.List;

import com.mysql.cj.conf.PropertyDefinitions.AuthMech;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.AuthenticationProvider;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.xdevapi.XDevAPIError;

public class XAuthenticationProvider implements AuthenticationProvider<XMessage> {

    XProtocol protocol;
    private AuthMech authMech = null; // Used in test case SecureSessionTest#testAuthMechanisns() to check what type of the authentication was actually used.
    private XMessageBuilder messageBuilder = new XMessageBuilder();

    @Override
    public void init(Protocol<XMessage> prot, PropertySet propertySet, ExceptionInterceptor exceptionInterceptor) {
        this.protocol = (XProtocol) prot;
    }

    @Override
    public void connect(ServerSession serverSession, String userName, String password, String database) {
        changeUser(serverSession, userName, password, database);
    }

    @Override
    public void changeUser(ServerSession serverSession, String userName, String password, String database) {
        boolean overTLS = ((XServerCapabilities) this.protocol.getServerSession().getCapabilities()).getTls();
        RuntimeProperty<AuthMech> authMechProp = this.protocol.getPropertySet().<AuthMech> getEnumProperty(PropertyKey.xdevapiAuth);
        List<AuthMech> tryAuthMech;
        if (overTLS || authMechProp.isExplicitlySet()) {
            tryAuthMech = Arrays.asList(authMechProp.getValue());
        } else {
            tryAuthMech = Arrays.asList(AuthMech.MYSQL41, AuthMech.SHA256_MEMORY);
        }

        XProtocolError capturedAuthErr = null;
        for (AuthMech am : tryAuthMech) {
            this.authMech = am;
            try {
                switch (this.authMech) {
                    case SHA256_MEMORY:
                        this.protocol.send(this.messageBuilder.buildSha256MemoryAuthStart(), 0);
                        byte[] nonce = this.protocol.readAuthenticateContinue();
                        this.protocol.send(this.messageBuilder.buildSha256MemoryAuthContinue(userName, password, nonce, database), 0);
                        break;
                    case MYSQL41:
                        this.protocol.send(this.messageBuilder.buildMysql41AuthStart(), 0);
                        byte[] salt = this.protocol.readAuthenticateContinue();
                        this.protocol.send(this.messageBuilder.buildMysql41AuthContinue(userName, password, salt, database), 0);
                        break;
                    case PLAIN:
                        if (overTLS) {
                            this.protocol.send(this.messageBuilder.buildPlainAuthStart(userName, password, database), 0);
                        } else {
                            throw new XProtocolError("PLAIN authentication is not allowed via unencrypted connection.");
                        }
                        break;
                    case EXTERNAL:
                        this.protocol.send(this.messageBuilder.buildExternalAuthStart(database), 0);
                        break;
                    default:
                        throw new WrongArgumentException("Unknown authentication mechanism '" + this.authMech + "'.");
                }
            } catch (CJCommunicationsException e) {
                if (capturedAuthErr != null && e.getCause() instanceof ClosedChannelException) {
                    // High probability that the server doesn't support authentication sequences. Ignore this exception and throw the previous one.
                    throw capturedAuthErr;
                }
                throw e;
            }

            try {
                this.protocol.readAuthenticateOk();
                // Clear any captured exception and stop trying remaining auth mechanisms.
                capturedAuthErr = null;
                break;
            } catch (XProtocolError e) {
                if (e.getErrorCode() != 1045) {
                    throw e;
                }
                capturedAuthErr = e;
            }
        }

        if (capturedAuthErr != null) {
            if (tryAuthMech.size() == 1) {
                throw capturedAuthErr;
            }
            // More than one authentication mechanism was tried.
            String errMsg = "Authentication failed using " + StringUtils.joinWithSerialComma(tryAuthMech)
                    + ", check username and password or try a secure connection";
            XDevAPIError ex = new XDevAPIError(errMsg, capturedAuthErr);
            ex.setVendorCode(capturedAuthErr.getErrorCode());
            ex.setSQLState(capturedAuthErr.getSQLState());
            ex.initCause(capturedAuthErr);
            throw ex;
        }

        this.protocol.afterHandshake();
    }

    @Override
    public String getEncodingForHandshake() {
        return null; // TODO
    }
}
