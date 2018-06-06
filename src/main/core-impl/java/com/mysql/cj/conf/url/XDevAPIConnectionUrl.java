/*
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.conf.url;

import static com.mysql.cj.util.StringUtils.isNullOrEmpty;
import static com.mysql.cj.util.StringUtils.safeTrim;

import java.util.Comparator;
import java.util.Map;
import java.util.Properties;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.conf.ConnectionUrlParser.Pair;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyDefinitions.PropertyKey;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;

public class XDevAPIConnectionUrl extends ConnectionUrl {
    private static final int DEFAULT_PORT = 33060;

    /**
     * Constructs an instance of {@link XDevAPIConnectionUrl}, performing all the required initializations.
     * 
     * @param connStrParser
     *            a {@link ConnectionUrlParser} instance containing the parsed version of the original connection string
     * @param info
     *            the connection arguments map
     */
    public XDevAPIConnectionUrl(ConnectionUrlParser connStrParser, Properties info) {
        super(connStrParser, info);
        this.type = Type.XDEVAPI_SESSION;

        /*
         * Validate the hosts list:
         * 1. Same user and password are required in all hosts.
         * 2. If the host property 'priority' is set for one host, then in needs to be set on all others too.
         * 3. 'Priority' value must be between 0 and 100.
         */
        boolean first = true;
        String user = null;
        String password = null;
        boolean hasPriority = false;
        for (HostInfo hi : this.hosts) {
            if (first) {
                first = false;
                user = hi.getUser();
                password = hi.getPassword();
                hasPriority = hi.getHostProperties().containsKey(PropertyKey.PRIORITY.getKeyName());
            } else {
                if (!user.equals(hi.getUser()) || !password.equals(hi.getPassword())) {
                    throw ExceptionFactory.createException(WrongArgumentException.class,
                            Messages.getString("ConnectionString.14", new Object[] { Type.XDEVAPI_SESSION.getScheme() }));
                }
                if (hasPriority ^ hi.getHostProperties().containsKey(PropertyKey.PRIORITY.getKeyName())) {
                    throw ExceptionFactory.createException(WrongArgumentException.class,
                            Messages.getString("ConnectionString.15", new Object[] { Type.XDEVAPI_SESSION.getScheme() }));
                }
            }
            if (hasPriority) {
                try {
                    int priority = Integer.parseInt(hi.getProperty(PropertyKey.PRIORITY.getKeyName()));
                    if (priority < 0 || priority > 100) {
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("ConnectionString.16", new Object[] { Type.XDEVAPI_SESSION.getScheme() }));
                    }
                } catch (NumberFormatException e) {
                    throw ExceptionFactory.createException(WrongArgumentException.class,
                            Messages.getString("ConnectionString.16", new Object[] { Type.XDEVAPI_SESSION.getScheme() }));
                }
            }
        }

        // Sort the hosts list according to their priority settings.
        if (hasPriority) {
            this.hosts.sort(
                    Comparator.<HostInfo, Integer> comparing(hi -> Integer.parseInt(hi.getHostProperties().get(PropertyKey.PRIORITY.getKeyName()))).reversed());
        }
    }

    @Override
    protected void processColdFusionAutoConfiguration() {
        // Not needed. Abort this operation.
    }

    @Override
    protected void preprocessPerTypeHostProperties(Map<String, String> hostProps) {
        if (hostProps.containsKey(PropertyKey.ADDRESS.getKeyName())) {
            String address = hostProps.get(PropertyKey.ADDRESS.getKeyName());
            Pair<String, Integer> hostPortPair = ConnectionUrlParser.parseHostPortPair(address);
            String host = safeTrim(hostPortPair.left);
            Integer port = hostPortPair.right;
            if (!isNullOrEmpty(host) && !hostProps.containsKey(PropertyKey.HOST.getKeyName())) {
                hostProps.put(PropertyKey.HOST.getKeyName(), host);
            }
            if (port != -1 && !hostProps.containsKey(PropertyKey.PORT.getKeyName())) {
                hostProps.put(PropertyKey.PORT.getKeyName(), port.toString());
            }
        }
    }

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    protected void fixProtocolDependencies(Map<String, String> hostProps) {
        // Not needed. Abort this operation.
    }
}
