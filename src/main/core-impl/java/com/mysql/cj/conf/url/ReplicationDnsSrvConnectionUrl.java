/*
 * Copyright (c) 2019, 2024, Oracle and/or its affiliates.
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
    private static final String TYPE_SOURCE = "SOURCE";
    private static final String TYPE_REPLICA = "REPLICA";

    private List<HostInfo> sourceHosts = new ArrayList<>();
    private List<HostInfo> replicaHosts = new ArrayList<>();

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

        // Split sources and replicas:
        LinkedList<HostInfo> undefinedHosts = new LinkedList<>();
        for (HostInfo hi : this.hosts) {
            Map<String, String> hostProperties = hi.getHostProperties();
            if (hostProperties.containsKey(PropertyKey.TYPE.getKeyName())) {
                if (TYPE_SOURCE.equalsIgnoreCase(hostProperties.get(PropertyKey.TYPE.getKeyName()))) {
                    this.sourceHosts.add(hi);
                } else if (TYPE_REPLICA.equalsIgnoreCase(hostProperties.get(PropertyKey.TYPE.getKeyName()))) {
                    this.replicaHosts.add(hi);
                } else {
                    undefinedHosts.add(hi);
                }
            } else {
                undefinedHosts.add(hi);
            }
        }
        if (!undefinedHosts.isEmpty()) {
            if (this.sourceHosts.isEmpty()) {
                this.sourceHosts.add(undefinedHosts.removeFirst());
            }
            this.replicaHosts.addAll(undefinedHosts);
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
        HostInfo srvHostSource = this.sourceHosts.isEmpty() ? null : this.sourceHosts.get(0);
        Map<String, String> hostPropsSource = srvHostSource == null ? Collections.emptyMap() : srvHostSource.getHostProperties();
        HostInfo srvHostReplica = this.replicaHosts.isEmpty() ? null : this.replicaHosts.get(0);
        Map<String, String> hostPropsReplica = srvHostReplica == null ? Collections.emptyMap() : srvHostReplica.getHostProperties();
        if (srvHostSource == null || srvHostReplica == null || DEFAULT_HOST.equals(srvHostSource.getHost()) || DEFAULT_HOST.equals(srvHostReplica.getHost())) {
            throw ExceptionFactory.createException(InvalidConnectionAttributeException.class, Messages.getString("ConnectionString.20"));
        }
        if (this.sourceHosts.size() != 1 || this.replicaHosts.size() != 1) {
            throw ExceptionFactory.createException(InvalidConnectionAttributeException.class, Messages.getString("ConnectionString.21"));
        }
        if (srvHostSource.getPort() != DEFAULT_PORT || srvHostReplica.getPort() != DEFAULT_PORT) {
            throw ExceptionFactory.createException(InvalidConnectionAttributeException.class, Messages.getString("ConnectionString.22"));
        }
        if (hostPropsSource.containsKey(PropertyKey.dnsSrv.getKeyName()) || hostPropsReplica.containsKey(PropertyKey.dnsSrv.getKeyName())) {
            if (!BooleanPropertyDefinition.booleanFrom(PropertyKey.dnsSrv.getKeyName(), hostPropsSource.get(PropertyKey.dnsSrv.getKeyName()), null)
                    || !BooleanPropertyDefinition.booleanFrom(PropertyKey.dnsSrv.getKeyName(), hostPropsReplica.get(PropertyKey.dnsSrv.getKeyName()), null)) {
                throw ExceptionFactory.createException(InvalidConnectionAttributeException.class,
                        Messages.getString("ConnectionString.23", new Object[] { PropertyKey.dnsSrv.getKeyName() }));
            }
        }
        if (hostPropsSource.containsKey(PropertyKey.PROTOCOL.getKeyName()) && hostPropsSource.get(PropertyKey.PROTOCOL.getKeyName()).equalsIgnoreCase("PIPE")
                || hostPropsReplica.containsKey(PropertyKey.PROTOCOL.getKeyName())
                        && hostPropsReplica.get(PropertyKey.PROTOCOL.getKeyName()).equalsIgnoreCase("PIPE")) {
            throw ExceptionFactory.createException(InvalidConnectionAttributeException.class, Messages.getString("ConnectionString.24"));
        }
        if (hostPropsSource.containsKey(PropertyKey.replicationConnectionGroup.getKeyName())
                || hostPropsReplica.containsKey(PropertyKey.replicationConnectionGroup.getKeyName())) {
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
            case SOURCES:
                return getHostsListFromDnsSrv(this.sourceHosts.get(0));
            case REPLICAS:
                return getHostsListFromDnsSrv(this.replicaHosts.get(0));
            default:
                return super.getHostsList(HostsListView.ALL);
        }
    }

}
