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

import static com.mysql.cj.core.conf.PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementThreshold;
import static com.mysql.cj.core.conf.PropertyDefinitions.PNAME_statementInterceptors;
import static com.mysql.cj.core.conf.PropertyDefinitions.PNAME_useLocalSessionState;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.jdbc.ha.LoadBalancedAutoCommitInterceptor;

public class LoadbalanceConnectionUrl extends ConnectionUrl {
    /**
     * Constructs an instance of {@link LoadbalanceConnectionUrl}, performing all the required initializations and validations. A loadbalance connection
     * cannot deal with multiple hosts with same host:port.
     * 
     * @param connStrParser
     *            a {@link ConnectionUrlParser} instance containing the parsed version of the original connection string
     * @param info
     *            the connection arguments map
     */
    protected LoadbalanceConnectionUrl(ConnectionUrlParser connStrParser, Properties info) {
        super(connStrParser, info);
        this.type = Type.LOADBALANCE_CONNECTION;

        // TODO: Validate the hosts list: there can't be any two hosts with same host:port.
        // Although this should be required, it also is incompatible with our current tests which are creating load-balanced connections
        // using the same host configurations.
        //        Set<String> visitedHosts = new HashSet<>();
        //        for (HostInfo hi : this.hosts) {
        //            if (visitedHosts.contains(hi.getHostPortPair())) {
        //                throw ExceptionFactory.createException(WrongArgumentException.class,
        //                        Messages.getString("ConnectionString.12", new Object[] { hi.getHostPortPair(), Type.LOADBALANCE_CONNECTION.getProtocol() }));
        //            }
        //            visitedHosts.add(hi.getHostPortPair());
        //        }
    }

    /**
     * Constructs an instance of a {@link LoadbalanceConnectionUrl} based on a list of hosts and a global set of properties instead of connection string
     * parsing.
     * {@link ConnectionUrl} instances created by this process are not cached.
     * 
     * @param hosts
     *            the hosts list to use in this connection URL
     * @param properties
     *            the properties common to all hosts
     * @return an instance of a {@link LoadbalanceConnectionUrl}
     */
    public LoadbalanceConnectionUrl(List<HostInfo> hosts, Map<String, String> properties) {
        this.originalConnStr = ConnectionUrl.Type.LOADBALANCE_CONNECTION.getProtocol() + "//**internally_generated**" + System.currentTimeMillis() + "**";
        this.type = ConnectionUrl.Type.LOADBALANCE_CONNECTION;
        this.hosts.addAll(hosts);
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
    protected void injectPerTypeProperties(Map<String, String> props) {
        props.put(PNAME_useLocalSessionState, "true");

        if (props.containsKey(PNAME_loadBalanceAutoCommitStatementThreshold)) {
            try {
                int autoCommitSwapThreshold = Integer.parseInt(props.get(PNAME_loadBalanceAutoCommitStatementThreshold));
                if (autoCommitSwapThreshold > 0) {
                    String statementInterceptors = props.get(PNAME_statementInterceptors);
                    if (StringUtils.isNullOrEmpty(statementInterceptors)) {
                        props.put(PNAME_statementInterceptors, LoadBalancedAutoCommitInterceptor.class.getName());
                    } else {
                        props.put(PNAME_statementInterceptors, statementInterceptors + "," + LoadBalancedAutoCommitInterceptor.class.getName());
                    }
                }
            } catch (Throwable t) {
                // Ignore, this will be handled later.
            }
        }
    }

    /**
     * Returns a list of this connection URL hosts in the form of host:port pairs.
     * 
     * @return a list of this connection URL hosts in the form of host:port pairs
     */
    public List<String> getHostInfoListAsHostPortPairs() {
        return this.hosts.stream().map(hi -> hi.getHostPortPair()).collect(Collectors.toList());
    }

    /**
     * Returns the list of {@link HostInfo} instances that matches the given collection of host:port pairs. Isolated host info elements are spawned for the
     * missing elements.
     * 
     * @param hostPortPairs
     *            a list of host:port pairs
     * @return a list of {@link HostInfo} instances corresponding to the given host:port pairs
     */
    public List<HostInfo> getHostInfoListFromHostPortPairs(Collection<String> hostPortPairs) {
        return hostPortPairs.stream().map(this::getHostOrSpawnIsolated).collect(Collectors.toList());
    }
}
