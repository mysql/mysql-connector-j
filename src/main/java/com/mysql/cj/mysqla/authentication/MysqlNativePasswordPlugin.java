/*
  Copyright (c) 2012, 2016, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */

package com.mysql.cj.mysqla.authentication;

import java.util.List;

import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.api.mysqla.authentication.AuthenticationPlugin;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringSelfDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.core.authentication.Security;
import com.mysql.cj.mysqla.io.Buffer;

/**
 * MySQL Native Password Authentication Plugin
 */
public class MysqlNativePasswordPlugin implements AuthenticationPlugin {

    private Protocol protocol;
    private String password = null;

    @Override
    public void init(Protocol prot) {
        this.protocol = prot;
    }

    public void destroy() {
        this.password = null;
    }

    public String getProtocolPluginName() {
        return "mysql_native_password";
    }

    public boolean requiresConfidentiality() {
        return false;
    }

    public boolean isReusable() {
        return true;
    }

    public void setAuthenticationParameters(String user, String password) {
        this.password = password;
    }

    public boolean nextAuthenticationStep(PacketPayload fromServer, List<PacketPayload> toServer) {

        toServer.clear();

        PacketPayload bresp = null;

        String pwd = this.password;

        if (fromServer == null || pwd == null || pwd.length() == 0) {
            bresp = new Buffer(new byte[0]);
        } else {
            bresp = new Buffer(Security.scramble411(pwd, fromServer.readBytes(StringSelfDataType.STRING_TERM), this.protocol.getPasswordCharacterEncoding()));
        }
        toServer.add(bresp);

        return true;
    }

}
