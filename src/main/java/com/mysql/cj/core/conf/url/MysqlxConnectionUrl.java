/*
  Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.conf.url;

import static com.mysql.cj.core.conf.PropertyDefinitions.ADDRESS_PROPERTY_KEY;
import static com.mysql.cj.core.conf.PropertyDefinitions.HOST_PROPERTY_KEY;
import static com.mysql.cj.core.conf.PropertyDefinitions.PORT_PROPERTY_KEY;
import static com.mysql.cj.core.conf.PropertyDefinitions.PRIORITY_PROPERTY_KEY;
import static com.mysql.cj.core.util.StringUtils.isNullOrEmpty;
import static com.mysql.cj.core.util.StringUtils.safeTrim;

import java.util.Comparator;
import java.util.Map;
import java.util.Properties;

import com.mysql.cj.core.Messages;
import com.mysql.cj.core.conf.url.ConnectionUrlParser.Pair;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.WrongArgumentException;

public class MysqlxConnectionUrl extends ConnectionUrl {
    private static final int DEFAULT_PORT = 33060;

    /**
     * Constructs an instance of {@link MysqlxConnectionUrl}, performing all the required initializations.
     * 
     * @param connStrParser
     *            a {@link ConnectionUrlParser} instance containing the parsed version of the original connection string
     * @param info
     *            the connection arguments map
     */
    protected MysqlxConnectionUrl(ConnectionUrlParser connStrParser, Properties info) {
        super(connStrParser, info);
        this.type = Type.MYSQLX_SESSION;

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
                hasPriority = hi.getHostProperties().containsKey(PRIORITY_PROPERTY_KEY);
            } else {
                if (!user.equals(hi.getUser()) || !password.equals(hi.getPassword())) {
                    throw ExceptionFactory.createException(WrongArgumentException.class,
                            Messages.getString("ConnectionString.14", new Object[] { Type.MYSQLX_SESSION.getProtocol() }));
                }
                if (hasPriority ^ hi.getHostProperties().containsKey(PRIORITY_PROPERTY_KEY)) {
                    throw ExceptionFactory.createException(WrongArgumentException.class,
                            Messages.getString("ConnectionString.15", new Object[] { Type.MYSQLX_SESSION.getProtocol() }));
                }
            }
            if (hasPriority) {
                try {
                    int priority = Integer.parseInt(hi.getProperty(PRIORITY_PROPERTY_KEY));
                    if (priority < 0 || priority > 100) {
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("ConnectionString.16", new Object[] { Type.MYSQLX_SESSION.getProtocol() }));
                    }
                } catch (NumberFormatException e) {
                    throw ExceptionFactory.createException(WrongArgumentException.class,
                            Messages.getString("ConnectionString.16", new Object[] { Type.MYSQLX_SESSION.getProtocol() }));
                }
            }
        }

        // Sort the hosts list according to their priority settings.
        if (hasPriority) {
            this.hosts.sort(Comparator.<HostInfo, Integer> comparing(hi -> Integer.parseInt(hi.getHostProperties().get(PRIORITY_PROPERTY_KEY))).reversed());
        }
    }

    @Override
    protected void processColdFusionAutoConfiguration() {
        // Not needed. Abort this operation.
    }

    @Override
    protected Map<String, String> preprocessPerTypeHostProperties(Map<String, String> hostProps) {
        if (hostProps.containsKey(ADDRESS_PROPERTY_KEY)) {
            String address = hostProps.get(ADDRESS_PROPERTY_KEY);
            Pair<String, Integer> hostPortPair = ConnectionUrlParser.parseHostPortPair(address);
            String host = safeTrim(hostPortPair.left);
            Integer port = hostPortPair.right;
            if (!isNullOrEmpty(host) && !hostProps.containsKey(HOST_PROPERTY_KEY)) {
                hostProps.put(HOST_PROPERTY_KEY, host);
            }
            if (port != -1 && !hostProps.containsKey(PORT_PROPERTY_KEY)) {
                hostProps.put(PORT_PROPERTY_KEY, port.toString());
            }
        }
        return hostProps;
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
