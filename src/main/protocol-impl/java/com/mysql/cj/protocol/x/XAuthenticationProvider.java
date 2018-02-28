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

import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.AuthenticationProvider;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.ServerSession;

public class XAuthenticationProvider implements AuthenticationProvider<XMessage> {

    XProtocol protocol;
    private String authMech = "MYSQL41"; // used in test case to check what type of the authentications was actually used
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
        this.authMech = this.protocol.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_auth).getValue();
        boolean overTLS = ((XServerCapabilities) this.protocol.getServerSession().getCapabilities()).getTls();

        // default choice
        if (this.authMech == null) {
            this.authMech = overTLS ? "PLAIN" : "MYSQL41";
            // TODO see WL#10992 this.authMech = overTLS ? "PLAIN" : (this.protocol.getAuthenticationMechanisms().contains("SHA256_MEMORY") ? "SHA256_MEMORY" : "MYSQL41");
        } else {
            this.authMech = this.authMech.toUpperCase();
        }

        switch (this.authMech) {
            case "MYSQL41":
                this.protocol.send(this.messageBuilder.buildMysql41AuthStart(), 0);
                byte[] salt = this.protocol.readAuthenticateContinue();
                this.protocol.send(this.messageBuilder.buildMysql41AuthContinue(userName, password, salt, database), 0);
                break;
            case "PLAIN":
                if (overTLS) {
                    this.protocol.send(this.messageBuilder.buildPlainAuthStart(userName, password, database), 0);
                } else {
                    throw new XProtocolError("PLAIN authentication is not allowed via unencrypted connection.");
                }
                break;
            case "EXTERNAL":
                this.protocol.send(this.messageBuilder.buildExternalAuthStart(database), 0);
                break;

            default:
                throw new WrongArgumentException("Unknown authentication mechanism '" + this.authMech + "'.");
        }

        this.protocol.readAuthenticateOk();
        this.protocol.afterHandshake();
    }

    @Override
    public String getEncodingForHandshake() {
        return null; // TODO
    }
}
