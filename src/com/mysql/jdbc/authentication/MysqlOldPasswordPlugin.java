/*
  Copyright (c) 2012, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

package com.mysql.jdbc.authentication;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import com.mysql.jdbc.AuthenticationPlugin;
import com.mysql.jdbc.Buffer;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.StringUtils;
import com.mysql.jdbc.Util;

/**
 * MySQL Native Old-Password Authentication Plugin
 */
public class MysqlOldPasswordPlugin implements AuthenticationPlugin {

    private Properties properties;
    private String password = null;

    public void init(Connection conn, Properties props) throws SQLException {
        this.properties = props;
    }

    public void destroy() {
        this.password = null;
    }

    public String getProtocolPluginName() {
        return "mysql_old_password";
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

    public boolean nextAuthenticationStep(Buffer fromServer, List<Buffer> toServer) throws SQLException {
        toServer.clear();

        Buffer bresp = null;

        String pwd = this.password;
        if (pwd == null) {
            pwd = this.properties.getProperty("password");
        }

        if (fromServer == null || pwd == null || pwd.length() == 0) {
            bresp = new Buffer(new byte[0]);
        } else {
            bresp = new Buffer(StringUtils.getBytes(Util.newCrypt(pwd, fromServer.readString().substring(0, 8))));

            bresp.setPosition(bresp.getBufLength());
            int oldBufLength = bresp.getBufLength();

            bresp.writeByte((byte) 0);

            bresp.setBufLength(oldBufLength + 1);
            bresp.setPosition(0);
        }
        toServer.add(bresp);

        return true;
    }

}
