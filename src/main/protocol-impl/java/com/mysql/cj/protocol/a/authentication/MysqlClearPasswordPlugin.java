/*
 * Copyright (c) 2012, 2022, Oracle and/or its affiliates.
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

import com.mysql.cj.callback.MysqlCallbackHandler;
import com.mysql.cj.callback.UsernameCallback;
import com.mysql.cj.protocol.AuthenticationPlugin;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.util.StringUtils;

/**
 * MySQL Clear Password Authentication Plugin
 */
public class MysqlClearPasswordPlugin implements AuthenticationPlugin<NativePacketPayload> {
    public static String PLUGIN_NAME = "mysql_clear_password";

    private Protocol<NativePacketPayload> protocol = null;
    private MysqlCallbackHandler usernameCallbackHandler = null;
    private String password = null;

    @Override
    public void init(Protocol<NativePacketPayload> prot, MysqlCallbackHandler cbh) {
        this.protocol = prot;
        this.usernameCallbackHandler = cbh;
    }

    public void destroy() {
        reset();
        this.protocol = null;
        this.usernameCallbackHandler = null;
        this.password = null;
    }

    public String getProtocolPluginName() {
        return PLUGIN_NAME;
    }

    public boolean requiresConfidentiality() {
        return true;
    }

    public boolean isReusable() {
        return true;
    }

    public void setAuthenticationParameters(String user, String password) {
        this.password = password;
        if (user == null && this.usernameCallbackHandler != null) {
            // Fall back to system login user.
            this.usernameCallbackHandler.handle(new UsernameCallback(System.getProperty("user.name")));
        }
    }

    public boolean nextAuthenticationStep(NativePacketPayload fromServer, List<NativePacketPayload> toServer) {
        toServer.clear();

        String encoding = this.protocol.getServerSession().getCharsetSettings().getPasswordCharacterEncoding();
        NativePacketPayload packet = new NativePacketPayload(StringUtils.getBytes(this.password != null ? this.password : "", encoding));

        packet.setPosition(packet.getPayloadLength());
        packet.writeInteger(IntegerDataType.INT1, 0);
        packet.setPosition(0);

        toServer.add(packet);
        return true;
    }
}
