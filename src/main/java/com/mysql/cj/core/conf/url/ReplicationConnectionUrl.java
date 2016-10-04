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

import static com.mysql.cj.core.conf.PropertyDefinitions.PNAME_useLocalSessionState;
import static com.mysql.cj.core.conf.PropertyDefinitions.TYPE_PROPERTY_KEY;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class ReplicationConnectionUrl extends ConnectionUrl {
    private static final String TYPE_MASTER = "MASTER";
    private static final String TYPE_SLAVE = "SLAVE";

    private List<HostInfo> masterHosts = new ArrayList<>();
    private List<HostInfo> slaveHosts = new ArrayList<>();

    /**
     * Constructs an instance of {@link ReplicationConnectionUrl}, performing all the required initializations.
     * 
     * @param connStrParser
     *            a {@link ConnectionUrlParser} instance containing the parsed version of the original connection string
     * @param info
     *            the connection arguments map
     */
    protected ReplicationConnectionUrl(ConnectionUrlParser connStrParser, Properties info) {
        super(connStrParser, info);
        this.type = Type.REPLICATION_CONNECTION;

        // Split masters and slaves:
        LinkedList<HostInfo> undefinedHosts = new LinkedList<>();
        for (HostInfo hi : this.hosts) {
            Map<String, String> hostProperties = hi.getHostProperties();
            if (hostProperties.containsKey(TYPE_PROPERTY_KEY)) {
                if (TYPE_MASTER.equalsIgnoreCase(hostProperties.get(TYPE_PROPERTY_KEY))) {
                    this.masterHosts.add(hi);
                } else if (TYPE_SLAVE.equalsIgnoreCase(hostProperties.get(TYPE_PROPERTY_KEY))) {
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

        // TODO: Validate the hosts list: there can't be any two hosts with same host:port.
        // Although this should be required, it also is incompatible with our current tests which are creating replication connections
        // using the same host configurations.
        //        Set<String> visitedHosts = new HashSet<>();
        //        for (List<HostInfo> hostsLists : Arrays.asList(this.masterHosts, this.slaveHosts)) {
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
     * Constructs an instance of a {@link ReplicationConnectionUrl} based on a list of master hosts, a list of slave hosts and a global set of properties
     * instead of connection string parsing.
     * {@link ConnectionUrl} instances created by this process are not cached.
     * 
     * @param masters
     *            the master hosts list to use in this connection string
     * @param slaves
     *            the slave hosts list to use in this connection string
     * @param properties
     *            the properties common to all hosts
     * @return an instance of a {@link LoadbalanceConnectionUrl}
     */
    public ReplicationConnectionUrl(List<HostInfo> masters, List<HostInfo> slaves, Map<String, String> properties) {
        this.originalConnStr = ConnectionUrl.Type.REPLICATION_CONNECTION.getProtocol() + "//**internally_generated**" + System.currentTimeMillis() + "**";
        this.type = ConnectionUrl.Type.REPLICATION_CONNECTION;
        this.hosts.addAll(masters);
        this.hosts.addAll(slaves);
        this.masterHosts.addAll(masters);
        this.slaveHosts.addAll(slaves);
        this.properties.putAll(properties);
        injectPerTypeProperties(this.properties);
        setupPropertiesTransformer(); // This is needed if new hosts come to be spawned in this connection URL.
    }

    /**
     * Injects additional properties into the connection arguments while it's being constructed.
     * 
     * @param props
     *            the properties already containing all known connection arguments
     */
    @Override
    protected void injectPerTypeProperties(Map<String, String> hostProps) {
        hostProps.put(PNAME_useLocalSessionState, "true");
    }

    /**
     * Returns an existing master host info with the same host:port part or spawns a new isolated host info based on this connection URL if none was found.
     * 
     * @param hostPortPair
     *            the host:port part to search for
     * @return the existing host info or a new independent one
     */
    public HostInfo getMasterHostOrSpawnIsolated(String hostPortPair) {
        return super.getHostOrSpawnIsolated(hostPortPair, this.masterHosts);
    }

    /**
     * Returns the list of master hosts.
     * 
     * @return the list of master hosts.
     */
    public List<HostInfo> getMastersList() {
        return Collections.unmodifiableList(this.masterHosts);
    }

    /**
     * Returns a list of this connection URL master hosts in the form of host:port pairs.
     * 
     * @return a list of this connection URL master hosts in the form of host:port pairs
     */
    public List<String> getMastersListAsHostPortPairs() {
        return this.masterHosts.stream().map(hi -> hi.getHostPortPair()).collect(Collectors.toList());
    }

    /**
     * Returns the list of {@link HostInfo} instances that matches the given collection of host:port pairs in the corresponding hosts list. Isolated host info
     * elements are spawned for the missing elements.
     * 
     * @param hostPortPairs
     *            a list of host:port pairs
     * @return a list of {@link HostInfo} instances corresponding to the given host:port pairs
     */
    public List<HostInfo> getMasterHostsListFromHostPortPairs(Collection<String> hostPortPairs) {
        return hostPortPairs.stream().map(this::getMasterHostOrSpawnIsolated).collect(Collectors.toList());
    }

    /**
     * Returns an existing slave host info with the same host:port part or spawns a new isolated host info based on this connection URL if none was found.
     * 
     * @param hostPortPair
     *            the host:port part to search for
     * @return the existing host info or a new independent one
     */
    public HostInfo getSlaveHostOrSpawnIsolated(String hostPortPair) {
        return super.getHostOrSpawnIsolated(hostPortPair, this.slaveHosts);
    }

    /**
     * Returns the list of slave hosts.
     * 
     * @return the list of slave hosts.
     */
    public List<HostInfo> getSlavesList() {
        return Collections.unmodifiableList(this.slaveHosts);
    }

    /**
     * Returns a list of this connection URL master hosts in the form of host:port pairs.
     * 
     * @return a list of this connection URL master hosts in the form of host:port pairs
     */
    public List<String> getSlavesListAsHostPortPairs() {
        return this.slaveHosts.stream().map(hi -> hi.getHostPortPair()).collect(Collectors.toList());
    }

    /**
     * Returns the list of {@link HostInfo} instances that matches the given collection of host:port pairs in the corresponding hosts list. Isolated host info
     * elements are spawned for the missing elements.
     * 
     * @param hostPortPairs
     *            a list of host:port pairs
     * @return a list of {@link HostInfo} instances corresponding to the given host:port pairs
     */
    public List<HostInfo> getSlaveHostsListFromHostPortPairs(Collection<String> hostPortPairs) {
        return hostPortPairs.stream().map(this::getSlaveHostOrSpawnIsolated).collect(Collectors.toList());
    }
}
