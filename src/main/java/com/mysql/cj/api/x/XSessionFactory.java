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

package com.mysql.cj.api.x;

import java.util.Properties;

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
public interface XSessionFactory {

    /**
     * Creates {@link XSession} by given URL.
     * 
     * @param url
     * @return {@link XSession}
     */
    XSession getSession(String url);

    /**
     * Creates {@link XSession} according to given properties.
     * 
     * @param properties
     * @return {@link XSession}
     */
    XSession getSession(Properties properties);

    // The mysqlx.getNodeSession() function can take a URL that specifies the connection information for a specific node or it can take a configuration provided by an XSession.
    NodeSession getNodeSession(String url);

    NodeSession getNodeSession(Properties properties);
}
