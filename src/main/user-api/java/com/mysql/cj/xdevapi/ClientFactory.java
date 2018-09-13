/*
 * Copyright (c) 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.xdevapi;

import java.util.Properties;

/**
 * ClientFactory is used for creation of {@link Client} instances.
 * 
 * {@link Client} objects provide the means of creating {@link Session}s that use an internally managed connection pool.
 * 
 * <pre>
 * ClientFactory xClientFactory = new ClientFactory();
 * 
 * {@link Client} client1 = xClientFactory.getClient("<b>mysqlx:</b>//[user1[:pwd1]@]host1[:port1]/db", poolingProps);
 * {@link Client} client2 = xClientFactory.getClient("<b>mysqlx:</b>//host2[:port2]/db?user=user2&amp;password=pwd2", poolingProps);
 * </pre>
 *
 */
public class ClientFactory {
    /**
     * Creates a {@link Client} object which provides a Session pooling functionality.
     * 
     * @param url
     *            the session URL.
     * @param clientPropsJson
     *            JSON string representing a document that defines connection properties in a special format.
     *            For pooling configuration, it should contain an embedded document after the "pooling" key:
     * 
     *            <pre>
     * pooling : {
     *     enabled: true|false,
     *     maxSize: integer &gt; 0
     *     maxIdleTime: integer &ge; 0,
     *     queueTimeOut: integer &ge; 0
     * }
     *            </pre>
     * 
     * @return a {@link Client} instance
     */
    public Client getClient(String url, String clientPropsJson) {
        return new ClientImpl(url, clientPropsJson);
    }

    /**
     * Creates a {@link Client} object that provides a Session pooling functionality.
     * 
     * @param url
     *            the session URL.
     * @param clientProps
     *            the {@link Properties} instance that contains the connection properties. The keys in this {@link Properties} match with the path of each value
     *            in the JSON document from {@link #getClient(String, String)} (for example, <code>pooling.enabled</code> or <code>pooling.maxSize</code>).
     * @return a {@link Client} instance.
     */
    public Client getClient(String url, Properties clientProps) {
        return new ClientImpl(url, clientProps);
    }
}
