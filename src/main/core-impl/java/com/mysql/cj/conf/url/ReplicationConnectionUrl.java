/*
 * Copyright (c) 2016, 2020, Oracle and/or its affiliates.
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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.HostsListView;
import com.mysql.cj.conf.PropertyKey;

public class ReplicationConnectionUrl extends ConnectionUrl {
    private static final String TYPE_SOURCE = "SOURCE";
    private static final String TYPE_REPLICA = "REPLICA";
    @Deprecated
    private static final String TYPE_SOURCE_DEPRECATED = "MASTER";
    @Deprecated
    private static final String TYPE_REPLICA_DEPRECATED = "SLAVE";

    private List<HostInfo> sourceHosts = new ArrayList<>();
    private List<HostInfo> replicaHosts = new ArrayList<>();

    /**
     * Constructs an instance of {@link ReplicationConnectionUrl}, performing all the required initializations.
     * 
     * @param connStrParser
     *            a {@link ConnectionUrlParser} instance containing the parsed version of the original connection string
     * @param info
     *            the connection arguments map
     */
    public ReplicationConnectionUrl(ConnectionUrlParser connStrParser, Properties info) {
        super(connStrParser, info);
        this.type = Type.REPLICATION_CONNECTION;

        // Split sources and replicas:
        LinkedList<HostInfo> undefinedHosts = new LinkedList<>();
        for (HostInfo hi : this.hosts) {
            Map<String, String> hostProperties = hi.getHostProperties();
            if (hostProperties.containsKey(PropertyKey.TYPE.getKeyName())) {
                if (TYPE_SOURCE.equalsIgnoreCase(hostProperties.get(PropertyKey.TYPE.getKeyName()))
                        || TYPE_SOURCE_DEPRECATED.equalsIgnoreCase(hostProperties.get(PropertyKey.TYPE.getKeyName()))) {
                    this.sourceHosts.add(hi);
                } else if (TYPE_REPLICA.equalsIgnoreCase(hostProperties.get(PropertyKey.TYPE.getKeyName()))
                        || TYPE_REPLICA_DEPRECATED.equalsIgnoreCase(hostProperties.get(PropertyKey.TYPE.getKeyName()))) {
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

        // TODO: Validate the hosts list: there can't be any two hosts with same host:port.
        // Although this should be required, it also is incompatible with our current tests which are creating replication connections
        // using the same host configurations.
        //        Set<String> visitedHosts = new HashSet<>();
        //        for (List<HostInfo> hostsLists : Arrays.asList(this.sourceHosts, this.replicaHosts)) {
        //            for (HostInfo hi : hostsLists) {
        //                if (visitedHosts.contains(hi.getHostPortPair())) {
        //                    throw ExceptionFactory.createException(WrongArgumentException.class,
        //                            Messages.getString("ConnectionString.13", new Object[] { hi.getHostPortPair(), Type.REPLICATION_CONNECTION.getProtocol() }));
        //                }
        //                visitedHosts.add(hi.getHostPortPair());
        //            }
        //        }
    }

    /**
     * Constructs an instance of a {@link ReplicationConnectionUrl} based on a list of source hosts, a list of replica hosts and a global set of properties
     * instead of connection string parsing.
     * {@link ConnectionUrl} instances created by this process are not cached.
     * 
     * @param sources
     *            the source hosts list to use in this connection string
     * @param replicas
     *            the replica hosts list to use in this connection string
     * @param properties
     *            the properties common to all hosts
     */
    public ReplicationConnectionUrl(List<HostInfo> sources, List<HostInfo> replicas, Map<String, String> properties) {
        this.originalConnStr = ConnectionUrl.Type.REPLICATION_CONNECTION.getScheme() + "//**internally_generated**" + System.currentTimeMillis() + "**";
        this.originalDatabase = properties.containsKey(PropertyKey.DBNAME.getKeyName()) ? properties.get(PropertyKey.DBNAME.getKeyName()) : "";
        this.type = ConnectionUrl.Type.REPLICATION_CONNECTION;
        this.properties.putAll(properties);
        injectPerTypeProperties(this.properties);
        setupPropertiesTransformer(); // This is needed if new hosts come to be spawned in this connection URL.
        sources.stream().map(this::fixHostInfo).peek(this.sourceHosts::add).forEach(this.hosts::add); // Fix the hosts info based on the new properties before adding them.
        replicas.stream().map(this::fixHostInfo).peek(this.replicaHosts::add).forEach(this.hosts::add); // Fix the hosts info based on the new properties before adding them.
    }

    /**
     * Returns a list of the hosts in this connection URL, filtered for the given view.
     * 
     * @param view
     *            the type of the view to use in the returned list of hosts.
     * @return
     *         the hosts list from this connection URL, filtered for the given view.
     */
    @Override
    public List<HostInfo> getHostsList(HostsListView view) {
        switch (view) {
            case SOURCES:
                return Collections.unmodifiableList(this.sourceHosts);
            case REPLICAS:
                return Collections.unmodifiableList(this.replicaHosts);
            default:
                return super.getHostsList(HostsListView.ALL);
        }
    }

    /**
     * Returns an existing source host info with the same host:port part or spawns a new isolated host info based on this connection URL if none was found.
     * 
     * @param hostPortPair
     *            the host:port part to search for
     * @return the existing host info or a new independent one
     */
    public HostInfo getSourceHostOrSpawnIsolated(String hostPortPair) {
        return super.getHostOrSpawnIsolated(hostPortPair, this.sourceHosts);
    }

    /**
     * Returns a list of this connection URL source hosts in the form of host:port pairs.
     * 
     * @return a list of this connection URL source hosts in the form of host:port pairs
     */
    public List<String> getSourcesListAsHostPortPairs() {
        return this.sourceHosts.stream().map(hi -> hi.getHostPortPair()).collect(Collectors.toList());
    }

    /**
     * Returns the list of {@link HostInfo} instances that matches the given collection of host:port pairs in the corresponding hosts list. Isolated host info
     * elements are spawned for the missing elements.
     * 
     * @param hostPortPairs
     *            a list of host:port pairs
     * @return a list of {@link HostInfo} instances corresponding to the given host:port pairs
     */
    public List<HostInfo> getSourceHostsListFromHostPortPairs(Collection<String> hostPortPairs) {
        return hostPortPairs.stream().map(this::getSourceHostOrSpawnIsolated).collect(Collectors.toList());
    }

    /**
     * Returns an existing replica host info with the same host:port part or spawns a new isolated host info based on this connection URL if none was found.
     * 
     * @param hostPortPair
     *            the host:port part to search for
     * @return the existing host info or a new independent one
     */
    public HostInfo getReplicaHostOrSpawnIsolated(String hostPortPair) {
        return super.getHostOrSpawnIsolated(hostPortPair, this.replicaHosts);
    }

    /**
     * Returns a list of this connection URL replica hosts in the form of host:port pairs.
     * 
     * @return a list of this connection URL replica hosts in the form of host:port pairs
     */
    public List<String> getReplicasListAsHostPortPairs() {
        return this.replicaHosts.stream().map(hi -> hi.getHostPortPair()).collect(Collectors.toList());
    }

    /**
     * Returns the list of {@link HostInfo} instances that matches the given collection of host:port pairs in the corresponding hosts list. Isolated host info
     * elements are spawned for the missing elements.
     * 
     * @param hostPortPairs
     *            a list of host:port pairs
     * @return a list of {@link HostInfo} instances corresponding to the given host:port pairs
     */
    public List<HostInfo> getReplicaHostsListFromHostPortPairs(Collection<String> hostPortPairs) {
        return hostPortPairs.stream().map(this::getReplicaHostOrSpawnIsolated).collect(Collectors.toList());
    }
}
