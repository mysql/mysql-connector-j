/*
 * Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.util.StringUtils;

public class LoadBalanceConnectionUrl extends ConnectionUrl {
    /**
     * Constructs an instance of {@link LoadBalanceConnectionUrl}, performing all the required initializations and validations. A load-balanced connection
     * cannot deal with multiple hosts with same host:port.
     * 
     * @param connStrParser
     *            a {@link ConnectionUrlParser} instance containing the parsed version of the original connection string
     * @param info
     *            the connection arguments map
     */
    public LoadBalanceConnectionUrl(ConnectionUrlParser connStrParser, Properties info) {
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
     * Constructs an instance of a {@link LoadBalanceConnectionUrl} based on a list of hosts and a global set of properties instead of connection string
     * parsing.
     * {@link ConnectionUrl} instances created by this process are not cached.
     * 
     * @param hosts
     *            the hosts list to use in this connection URL
     * @param properties
     *            the properties common to all hosts
     */
    public LoadBalanceConnectionUrl(List<HostInfo> hosts, Map<String, String> properties) {
        this.originalConnStr = ConnectionUrl.Type.LOADBALANCE_CONNECTION.getScheme() + "//**internally_generated**" + System.currentTimeMillis() + "**";
        this.originalDatabase = properties.containsKey(PropertyKey.DBNAME.getKeyName()) ? properties.get(PropertyKey.DBNAME.getKeyName()) : "";
        this.type = ConnectionUrl.Type.LOADBALANCE_CONNECTION;
        this.properties.putAll(properties);
        injectPerTypeProperties(this.properties);
        setupPropertiesTransformer(); // This is needed if new hosts come to be spawned in this connection URL.
        hosts.stream().map(this::fixHostInfo).forEach(this.hosts::add); // Fix the hosts info based on the new properties before adding them.
    }

    /**
     * Injects additional properties into the connection arguments while it's being constructed.
     * 
     * @param props
     *            the properties already containing all known connection arguments
     */
    @Override
    protected void injectPerTypeProperties(Map<String, String> props) {
        if (props.containsKey(PropertyKey.loadBalanceAutoCommitStatementThreshold.getKeyName())) {
            try {
                int autoCommitSwapThreshold = Integer.parseInt(props.get(PropertyKey.loadBalanceAutoCommitStatementThreshold.getKeyName()));
                if (autoCommitSwapThreshold > 0) {
                    String queryInterceptors = props.get(PropertyKey.queryInterceptors.getKeyName());
                    String lbi = "com.mysql.cj.jdbc.ha.LoadBalancedAutoCommitInterceptor";
                    if (StringUtils.isNullOrEmpty(queryInterceptors)) {
                        props.put(PropertyKey.queryInterceptors.getKeyName(), lbi);
                    } else {
                        props.put(PropertyKey.queryInterceptors.getKeyName(), queryInterceptors + "," + lbi);
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
