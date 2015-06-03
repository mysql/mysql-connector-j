/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.io;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.Session;
import com.mysql.cj.api.SessionState;
import com.mysql.cj.api.authentication.AuthenticationFactory;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.io.Protocol;

public class MysqlSession implements Session {

    private SessionState sessionState;
    private AuthenticationFactory authFactory;
    private PropertySet propertySet;
    private transient Protocol protocol;

    public MysqlSession(AuthenticationFactory authFactory) {
        this.sessionState = new MysqlSessionState();
        this.authFactory = authFactory;
    }

    @Override
    public void init(MysqlConnection conn, Protocol prot) {
        this.propertySet = conn.getPropertySet();
        this.protocol = prot;
        this.protocol.setSession(this);
        this.sessionState.init(conn);
    }

    @Override
    public SessionState getSessionState() {
        return this.sessionState;
    }

    /**
     * Re-authenticates as the given user and password
     * 
     * @param userName
     * @param password
     * @param database
     * 
     */
    @Override
    public void changeUser(String userName, String password, String database) {
        this.authFactory.changeUser(userName, password, database);
    }

    public Protocol getProtocol() {
        return this.protocol;
    }

    @Override
    public AuthenticationFactory getAuthenticationFactory() {
        return this.authFactory;
    }

    @Override
    public PropertySet getPropertySet() {
        return this.propertySet;
    }

}
