/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.xdevapi;

import java.util.List;
import java.util.Properties;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.InvalidConnectionAttributeException;

/**
 * SessionFactory is used for creation of sessions.
 *
 * <pre>
 * SessionFactory xFactory = new SessionFactory();
 *
 * {@link Session} session1 = xFactory.getSession("<b>mysqlx:</b>//[user1[:pwd1]@]host1[:port1]/db");
 * {@link Session} session2 = xFactory.getSession("<b>mysqlx:</b>//host2[:port2]/db?user=user2&amp;password=pwd2");
 * {@link Session} session3 = xFactory.getSession("<b>mysqlx+srv:</b>//[user1[:pwd1]@]service_name/db");
 * </pre>
 *
 */
public class SessionFactory {

    /**
     * Parses the connection string URL.
     *
     * @param url
     *            the connection string URL.
     * @return a {@link ConnectionUrl} instance containing the URL components.
     */
    protected ConnectionUrl parseUrl(String url) {
        ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, null);
        if (connUrl == null || connUrl.getType() != ConnectionUrl.Type.XDEVAPI_SESSION && connUrl.getType() != ConnectionUrl.Type.XDEVAPI_DNS_SRV_SESSION) {
            throw ExceptionFactory.createException(InvalidConnectionAttributeException.class, "Initialization via URL failed for \"" + url + "\"");
        }
        return connUrl;
    }

    /**
     * Creates {@link Session} by given URL.
     *
     * @param connUrl
     *            the session {@link ConnectionUrl}.
     * @return a {@link Session} instance.
     */
    protected Session getSession(ConnectionUrl connUrl) {
        CJException latestException = null;
        List<HostInfo> hostsList = connUrl.getHostsList();
        for (HostInfo hi : hostsList) {
            try {
                return new SessionImpl(hi);
            } catch (CJCommunicationsException e) {
                if (e.getCause() == null) {
                    throw e;
                }
                latestException = e;
            }
        }
        if (latestException != null) {
            throw ExceptionFactory.createException(CJCommunicationsException.class, Messages.getString("Session.Create.Failover.0"), latestException);
        }
        return null;
    }

    /**
     * Creates {@link Session} by given URL.
     *
     * @param url
     *            the session URL.
     * @return a {@link Session} instance.
     */
    public Session getSession(String url) {
        return getSession(parseUrl(url));
    }

    /**
     * Creates a {@link Session} using the information contained in the given properties.
     *
     * @param properties
     *            the {@link Properties} instance that contains the session components.
     * @return a {@link Session} instance.
     */
    public Session getSession(Properties properties) {
        if (properties.containsKey(PropertyKey.xdevapiDnsSrv.getKeyName()) && (Boolean) PropertyDefinitions.getPropertyDefinition(PropertyKey.xdevapiDnsSrv)
                .parseObject(properties.getProperty(PropertyKey.xdevapiDnsSrv.getKeyName()), null)) {

            ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(ConnectionUrl.Type.XDEVAPI_DNS_SRV_SESSION.getScheme(), properties);
            return getSession(connUrl);
        }

        ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(ConnectionUrl.Type.XDEVAPI_SESSION.getScheme(), properties);
        return new SessionImpl(connUrl.getMainHost());
    }

}
