/*
 Copyright  2007 MySQL AB, 2008 Sun Microsystems
 All rights reserved. Use is subject to license terms.

  The MySQL Connector/J is licensed under the terms of the GPL,
  like most MySQL Connectors. There are special exceptions to the
  terms and conditions of the GPL as it is applied to this software,
  see the FLOSS License Exception available on mysql.com.

  This program is free software; you can redistribute it and/or
  modify it under the terms of the GNU General Public License as
  published by the Free Software Foundation; version 2 of the
  License.

  This program is distributed in the hope that it will be useful,  
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  02110-1301 USA
 
 */
package com.mysql.jdbc;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class BestResponseTimeBalanceStrategy implements BalanceStrategy {

	/**
	 * @param loadBalancingConnectionProxy
	 */
	public BestResponseTimeBalanceStrategy() {
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
				
		Map blackList = proxy.getGlobalBlacklist();
				
		SQLException ex = null;

		for (int attempts = 0; attempts < numRetries; ) {
			long minResponseTime = Long.MAX_VALUE;

			int bestHostIndex = 0;

			// safety
			if (blackList.size() == configuredHosts.size()) {
				blackList = proxy.getGlobalBlacklist();
			}

			for (int i = 0; i < responseTimes.length; i++) {
				long candidateResponseTime = responseTimes[i];

				if (candidateResponseTime < minResponseTime
						&& !blackList.containsKey(configuredHosts.get(i))) {
					if (candidateResponseTime == 0) {
						bestHostIndex = i;

						break;
					}

					bestHostIndex = i;
					minResponseTime = candidateResponseTime;
				}
			}

			String bestHost = (String) configuredHosts.get(bestHostIndex);

			Connection conn = (Connection) liveConnections.get(bestHost);

			if (conn == null) {
				try {
					conn = proxy.createConnectionForHost(bestHost);
				} catch (SQLException sqlEx) {
					ex = sqlEx;

					if (sqlEx instanceof CommunicationsException
							|| "08S01".equals(sqlEx.getSQLState())) {
						proxy.addToGlobalBlacklist(bestHost);
						blackList.put(bestHost, null);


						if (blackList.size() == configuredHosts.size()) {
							attempts++;
							try {
								Thread.sleep(250);
							} catch (InterruptedException e) {
							}
							blackList = proxy.getGlobalBlacklist(); // try again after a little bit
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