/*
 Copyright (C) 2007 MySQL AB

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 
 */
package com.mysql.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class RandomBalanceStrategy implements BalanceStrategy {

	public RandomBalanceStrategy() {
	}

	public void destroy() {
		// we don't have anything to clean up
	}

	public void init(Connection conn, Properties props) throws SQLException {
		// we don't have anything to initialize
	}

	public Connection pickConnection(LoadBalancingConnectionProxy proxy,
			List configuredHosts, Map liveConnections, long[] responseTimes,
			int numRetries) throws SQLException {
		int numHosts = configuredHosts.size();

		SQLException ex = null;

		Map whiteListMap = new HashMap(numHosts);
		List whiteList = new ArrayList(numHosts);
		whiteList.addAll(configuredHosts);

		for (int i = 0; i < numHosts; i++) {
			whiteListMap.put(whiteList.get(i), new Integer(i));
		}

		for (int attempts = 0; attempts < numRetries; attempts++) {
			int random = (int) (Math.random() * whiteList.size());

			if (random == whiteList.size()) {
				random--;
			}

			String hostPortSpec = (String) whiteList.get(random);

			Connection conn = (Connection) liveConnections.get(hostPortSpec);

			if (conn == null) {
				try {
					conn = proxy.createConnectionForHost(hostPortSpec);
				} catch (SQLException sqlEx) {
					ex = sqlEx;

					if (sqlEx instanceof CommunicationsException
							|| "08S01".equals(sqlEx.getSQLState())) {

						Integer whiteListIndex = (Integer) whiteListMap
								.get(hostPortSpec);

						// exclude this host from being picked again
						if (whiteListIndex != null) {
							whiteList.remove(whiteListIndex.intValue());
						}

						if (whiteList.size() == 0) {
							try {
								Thread.sleep(250);
							} catch (InterruptedException e) {
							}

							// start fresh
							whiteList.addAll(configuredHosts);
						}

						continue;
					} else {
						throw sqlEx;
					}
				}
			}

			return conn;
		}

		if (ex != null) {
			throw ex;
		}

		return null; // we won't get here, compiler can't tell
	}

}