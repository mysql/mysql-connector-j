/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.profiler;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.ProfilerEventHandler;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exception.CJException;
import com.mysql.cj.core.exception.ExceptionFactory;
import com.mysql.cj.core.util.Util;

public class ProfilerEventHandlerFactory {

    private MysqlConnection ownerConnection = null;

    protected Log log = null;

    /**
     * Returns the ProfilerEventHandlerFactory that handles profiler events for the given
     * connection.
     * 
     * @param conn
     *            the connection to handle events for
     * @return the ProfilerEventHandlerFactory that handles profiler events
     */
    public static synchronized ProfilerEventHandler getInstance(MysqlConnection conn) {
        ProfilerEventHandler handler = conn.getProfilerEventHandlerInstance();

        if (handler == null) {
            handler = (ProfilerEventHandler) Util.getInstance(conn.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_profilerEventHandler)
                    .getStringValue(), new Class[0], new Object[0], conn.getExceptionInterceptor());

            // we do it this way to not require exposing the connection properties for all who utilize it
            conn.initializeExtension(handler);
            conn.setProfilerEventHandlerInstance(handler);
        }

        return handler;
    }

    public static synchronized void removeInstance(MysqlConnection conn) {
        ProfilerEventHandler handler = conn.getProfilerEventHandlerInstance();

        if (handler != null) {
            handler.destroy();
        }
    }

    private ProfilerEventHandlerFactory(MysqlConnection conn) {
        this.ownerConnection = conn;

        try {
            this.log = this.ownerConnection.getLog();
        } catch (CJException ex) {
            throw ExceptionFactory.createException("Unable to get logger from connection", ex, this.ownerConnection.getExceptionInterceptor());
        }
    }
}