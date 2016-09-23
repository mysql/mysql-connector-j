/*
  Copyright (c) 2007, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.jdbc.ha;

import java.sql.SQLException;

import com.mysql.cj.api.jdbc.ha.LoadBalancedConnection;
import com.mysql.cj.core.Messages;
import com.mysql.cj.jdbc.exceptions.SQLError;

public class LoadBalancedMySQLConnection extends MultiHostMySQLConnection implements LoadBalancedConnection {
    public LoadBalancedMySQLConnection(LoadBalancedConnectionProxy proxy) {
        super(proxy);
    }

    @Override
    public LoadBalancedConnectionProxy getThisAsProxy() {
        return (LoadBalancedConnectionProxy) super.getThisAsProxy();
    }

    @Override
    public void close() throws SQLException {
        getThisAsProxy().doClose();
    }

    @Override
    public void ping() throws SQLException {
        ping(true);
    }

    public void ping(boolean allConnections) throws SQLException {
        if (allConnections) {
            getThisAsProxy().doPing();
        } else {
            getActiveMySQLConnection().ping();
        }
    }

    public boolean addHost(String host) throws SQLException {
        return getThisAsProxy().addHost(host);
    }

    public void removeHost(String host) throws SQLException {
        getThisAsProxy().removeHost(host);
    }

    public void removeHostWhenNotInUse(String host) throws SQLException {
        getThisAsProxy().removeHostWhenNotInUse(host);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // This works for classes that aren't actually wrapping anything
        return iface.isInstance(this);
    }

    @Override
    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
        try {
            // This works for classes that aren't actually wrapping anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException(Messages.getString("Common.UnableToUnwrap", new Object[] { iface.toString() }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }
    }
}
