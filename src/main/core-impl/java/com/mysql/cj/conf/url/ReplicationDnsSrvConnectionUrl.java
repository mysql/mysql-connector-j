/*
 * Copyright (c) 2019, Oracle and/or its affiliates. All rights reserved.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.BooleanPropertyDefinition;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.HostsListView;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.InvalidConnectionAttributeException;

public class ReplicationDnsSrvConnectionUrl extends ConnectionUrl {
    private static final String DEFAULT_HOST = "";
    private static final int DEFAULT_PORT = HostInfo.NO_PORT;
    private static final String TYPE_MASTER = "MASTER";
    private static final String TYPE_SLAVE = "SLAVE";

    private List<HostInfo> masterHosts = new ArrayList<>();
    private List<HostInfo> slaveHosts = new ArrayList<>();

    /**
     * Constructs an instance of {@link ReplicationDnsSrvConnectionUrl}, performing all the required initializations.
     * 
     * @param connStrParser
     *            a {@link ConnectionUrlParser} instance containing the parsed version of the original connection string
     * @param info
     *            the connection arguments map
     */
    public ReplicationDnsSrvConnectionUrl(ConnectionUrlParser connStrParser, Properties info) {
        super(connStrParser, info);
        this.type = Type.REPLICATION_DNS_SRV_CONNECTION;

        // Split masters and slaves:
        LinkedList<HostInfo> undefinedHosts = new LinkedList<>();
        for (HostInfo hi : this.hosts) {
            Map<String, String> hostProperties = hi.getHostProperties();
            if (hostProperties.containsKey(PropertyKey.TYPE.getKeyName())) {
                if (TYPE_MASTER.equalsIgnoreCase(hostProperties.get(PropertyKey.TYPE.getKeyName()))) {
                    this.masterHosts.add(hi);
                } else if (TYPE_SLAVE.equalsIgnoreCase(hostProperties.get(PropertyKey.TYPE.getKeyName()))) {
                    this.slaveHosts.add(hi);
                } else {
                    undefinedHosts.add(hi);
                }
            } else {
                undefinedHosts.add(hi);
            }
        }
        if (!undefinedHosts.isEmpty()) {
            if (this.masterHosts.isEmpty()) {
                this.masterHosts.add(undefinedHosts.removeFirst());
            }
            this.slaveHosts.addAll(undefinedHosts);
        }

        /*
         * Validate the hosts list:
         * 1. Exactly two hosts (SRV service name) must be provided.
         * 2. No more than one host (SRV service name) per type can be provided.
         * 3. No port can be provided, i.e., port number must be equals to DEFAULT_PORT.
         * 4. If property 'dnsSrv' is set then it cannot be "false".
         * 5. Property 'protocol' cannot be "PIPE".
         * 6. Property 'replicationConnectionGroup' cannot be set.
         */
        HostInfo srvHostMaster = this.masterHosts.isEmpty() ? null : this.masterHosts.get(0);
        Map<String, String> hostPropsMaster = srvHostMaster == null ? Collections.emptyMap() : srvHostMaster.getHostProperties();
        HostInfo srvHostSlave = this.slaveHosts.isEmpty() ? null : this.slaveHosts.get(0);
        Map<String, String> hostPropsSlave = srvHostSlave == null ? Collections.emptyMap() : srvHostSlave.getHostProperties();
        if (srvHostMaster == null || srvHostSlave == null || DEFAULT_HOST.equals(srvHostMaster.getHost()) || DEFAULT_HOST.equals(srvHostSlave.getHost())) {
            throw ExceptionFactory.createException(InvalidConnectionAttributeException.class, Messages.getString("ConnectionString.20"));
        }
        if (this.masterHosts.size() != 1 || this.slaveHosts.size() != 1) {
            throw ExceptionFactory.createException(InvalidConnectionAttributeException.class, Messages.getString("ConnectionString.21"));
        }
        if (srvHostMaster.getPort() != DEFAULT_PORT || srvHostSlave.getPort() != DEFAULT_PORT) {
            throw ExceptionFactory.createException(InvalidConnectionAttributeException.class, Messages.getString("ConnectionString.22"));
        }
        if (hostPropsMaster.containsKey(PropertyKey.dnsSrv.getKeyName()) || hostPropsSlave.containsKey(PropertyKey.dnsSrv.getKeyName())) {
            if (!BooleanPropertyDefinition.booleanFrom(PropertyKey.dnsSrv.getKeyName(), hostPropsMaster.get(PropertyKey.dnsSrv.getKeyName()), null)
                    || !BooleanPropertyDefinition.booleanFrom(PropertyKey.dnsSrv.getKeyName(), hostPropsSlave.get(PropertyKey.dnsSrv.getKeyName()), null)) {
                throw ExceptionFactory.createException(InvalidConnectionAttributeException.class,
                        Messages.getString("ConnectionString.23", new Object[] { PropertyKey.dnsSrv.getKeyName() }));
            }
        }
        if (hostPropsMaster.containsKey(PropertyKey.PROTOCOL.getKeyName()) && hostPropsMaster.get(PropertyKey.PROTOCOL.getKeyName()).equalsIgnoreCase("PIPE")
                || hostPropsSlave.containsKey(PropertyKey.PROTOCOL.getKeyName())
                        && hostPropsSlave.get(PropertyKey.PROTOCOL.getKeyName()).equalsIgnoreCase("PIPE")) {
            throw ExceptionFactory.createException(InvalidConnectionAttributeException.class, Messages.getString("ConnectionString.24"));
        }
        if (hostPropsMaster.containsKey(PropertyKey.replicationConnectionGroup.getKeyName())
                || hostPropsSlave.containsKey(PropertyKey.replicationConnectionGroup.getKeyName())) {
            throw ExceptionFactory.createException(InvalidConnectionAttributeException.class,
                    Messages.getString("ConnectionString.25", new Object[] { PropertyKey.replicationConnectionGroup.getKeyName() }));
        }
    }

    @Override
    public String getDefaultHost() {
        return DEFAULT_HOST;
    }

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    /**
     * Returns a hosts list built from the result of the DNS SRV lookup for the original host name.
     * 
     * @param view
     *            the type of the view to use in the returned list of hosts.
     * 
     * @return
     *         the hosts list from the result of the DNS SRV lookup, filtered for the given view.
     */
    @Override
    public List<HostInfo> getHostsList(HostsListView view) {
        switch (view) {
            case MASTERS:
                return getHostsListFromDnsSrv(this.masterHosts.get(0));
            case SLAVES:
                return getHostsListFromDnsSrv(this.slaveHosts.get(0));
            default:
                return super.getHostsList(HostsListView.ALL);
        }
    }
}
