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

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import com.mysql.jdbc.AuthenticationPlugin;
import com.mysql.jdbc.Buffer;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Messages;
import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.Security;

/**
 * MySQL Native Password Authentication Plugin
 */
public class MysqlNativePasswordPlugin implements AuthenticationPlugin {

    private Connection connection;
    private Properties properties;
    private String password = null;

    public void init(Connection conn, Properties props) throws SQLException {
        this.connection = conn;
        this.properties = props;
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

    public boolean nextAuthenticationStep(Buffer fromServer, List<Buffer> toServer) throws SQLException {

        try {
            toServer.clear();

            Buffer bresp = null;

            String pwd = this.password;
            if (pwd == null) {
                pwd = this.properties.getProperty("password");
            }

            if (fromServer == null || pwd == null || pwd.length() == 0) {
                bresp = new Buffer(new byte[0]);
            } else {
                bresp = new Buffer(Security.scramble411(pwd, fromServer.readString(), this.connection.getPasswordCharacterEncoding()));
            }
            toServer.add(bresp);

        } catch (NoSuchAlgorithmException nse) {
            throw SQLError.createSQLException(Messages.getString("MysqlIO.95") + Messages.getString("MysqlIO.96"), SQLError.SQL_STATE_GENERAL_ERROR, null);
        } catch (UnsupportedEncodingException e) {
            throw SQLError.createSQLException(Messages.getString("MysqlIO.95") + Messages.getString("MysqlIO.96"), SQLError.SQL_STATE_GENERAL_ERROR, null);
        }

        return true;
    }

}
