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

package com.mysql.cj.api;

import com.mysql.cj.api.authentication.AuthenticationProvider;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exception.ExceptionInterceptor;
import com.mysql.cj.api.io.PhysicalConnection;
import com.mysql.cj.api.io.Protocol;

/**
 * Retrieved as a result of successful authentication, introduces methods allowed for
 * authenticated connection.
 * 
 * @author say
 *
 */
public interface Session {

    /**
     * Initialize Session object. Resulting state: ready for authentication.
     * 
     * @param conn
     *            the Connection that is creating us
     * @param physicalConnection
     * @param propertySet
     */
    void init(MysqlConnection conn, PhysicalConnection physicalConnection, PropertySet propertySet);

    /**
     * Authenticate as the given user and password
     * 
     * @param userName
     * @param password
     * @param database
     */
    void authenticate(String userName, String password, String database);

    PropertySet getPropertySet();

    Protocol getProtocol();

    AuthenticationProvider getAuthenticationProvider();

    SessionState getSessionState();

    /**
     * Re-authenticates as the given user and password
     * 
     * @param userName
     * @param password
     * @param database
     * 
     */
    public void changeUser(String userName, String password, String database);

    public ExceptionInterceptor getExceptionInterceptor();

    public void setExceptionInterceptor(ExceptionInterceptor exceptionInterceptor);
}
