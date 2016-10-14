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

package com.mysql.cj.xdevapi;

import java.util.Properties;

import com.mysql.cj.api.xdevapi.NodeSession;
import com.mysql.cj.api.xdevapi.XSession;
import com.mysql.cj.core.conf.url.ConnectionUrl;
import com.mysql.cj.core.conf.url.HostInfo;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.InvalidConnectionAttributeException;

/**
 * XSessionFactory is used for creation of sessions.
 * 
 * <pre>
 * XSessionFactory xFactory = new XSessionFactory();
 * 
 * {@link XSession} crudSession = xFactory.getSession("<b>mysqlx:</b>//host[:port]/db?user=user1&amp;password=pwd1");
 * 
 * {@link NodeSession} nodeSession = xFactory.getNodeSession("<b>mysqlx:</b>//host[:port]/db?user=user1&amp;password=pwd1");
 * </pre>
 *
 */
public class XSessionFactory {

    private ConnectionUrl parseUrl(String url) {
        ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, null);
        if (connUrl.getType() != ConnectionUrl.Type.XDEVAPI_SESSION) {
            throw ExceptionFactory.createException(InvalidConnectionAttributeException.class, "Initialization via URL failed for \"" + url + "\"");
        }
        return connUrl;
    }

    /**
     * Creates {@link XSession} by given URL.
     * 
     * @param url
     * @return {@link XSession}
     */
    public XSession getSession(String url) {
        CJCommunicationsException latestException = null;
        ConnectionUrl connUrl = parseUrl(url);
        for (HostInfo hi : connUrl.getHostsList()) {
            try {
                return new XSessionImpl(hi.exposeAsProperties());
            } catch (CJCommunicationsException e) {
                latestException = e;
            }
        }
        if (latestException != null) {
            throw latestException;
        }
        return null;
    }

    /**
     * Creates {@link XSession} according to given properties.
     * 
     * @param properties
     * @return {@link XSession}
     */
    public XSession getSession(Properties properties) {
        return new XSessionImpl(properties);
    }

    /**
     * Creates {@link NodeSession} by given URL.
     * 
     * @param url
     * @return {@link NodeSession}
     */
    public NodeSession getNodeSession(String url) {
        ConnectionUrl connUrl = parseUrl(url);
        if (connUrl.getHostsList().size() > 1) {
            throw ExceptionFactory.createException(InvalidConnectionAttributeException.class, "A NodeSession cannot be initialized with a multi-host URL.");
        }
        return new NodeSessionImpl(connUrl.getMainHost().exposeAsProperties());
    }

    /**
     * Creates {@link NodeSession} according to given properties.
     * 
     * @param properties
     * @return {@link NodeSession}
     */
    public NodeSession getNodeSession(Properties properties) {
        return new NodeSessionImpl(properties);
    }
}
