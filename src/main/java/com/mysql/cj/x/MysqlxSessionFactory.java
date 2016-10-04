/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.x;

import java.util.Properties;

import com.mysql.cj.api.x.NodeSession;
import com.mysql.cj.api.x.XSession;
import com.mysql.cj.api.x.XSessionFactory;
import com.mysql.cj.core.conf.url.ConnectionUrl;
import com.mysql.cj.core.conf.url.HostInfo;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.InvalidConnectionAttributeException;
import com.mysql.cj.mysqlx.devapi.NodeSessionImpl;
import com.mysql.cj.mysqlx.devapi.SessionImpl;

/**
 * Entry point for creating sessions to the X Plugin server.
 */
public class MysqlxSessionFactory implements XSessionFactory {

    private ConnectionUrl parseUrl(String url) {
        ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, null);
        if (connUrl.getType() != ConnectionUrl.Type.MYSQLX_SESSION) {
            throw ExceptionFactory.createException(InvalidConnectionAttributeException.class, "Initialization via URL failed for \"" + url + "\"");
        }
        return connUrl;
    }

    @Override
    public XSession getSession(String url) {
        CJCommunicationsException latestException = null;
        ConnectionUrl connUrl = parseUrl(url);
        for (HostInfo hi : connUrl.getHostsList()) {
            try {
                return new SessionImpl(hi.exposeAsProperties());
            } catch (CJCommunicationsException e) {
                latestException = e;
            }
        }
        if (latestException != null) {
            throw latestException;
        }
        return null;
    }

    @Override
    public XSession getSession(Properties properties) {
        return new SessionImpl(properties);
    }

    @Override
    public NodeSession getNodeSession(String url) {
        ConnectionUrl connUrl = parseUrl(url);
        if (connUrl.getHostsList().size() > 1) {
            throw ExceptionFactory.createException(InvalidConnectionAttributeException.class, "A NodeSession cannot be initialized with a multi-host URL.");
        }
        return new NodeSessionImpl(connUrl.getMainHost().exposeAsProperties());
    }

    @Override
    public NodeSession getNodeSession(Properties properties) {
        return new NodeSessionImpl(properties);
    }
}
