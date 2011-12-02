/*
  Copyright (c) 2007, 2011, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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
package com.mysql.jdbc;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

	public ConnectionImpl pickConnection(LoadBalancingConnectionProxy proxy,
			List<String> configuredHosts, Map<String, ConnectionImpl> liveConnections, long[] responseTimes,
			int numRetries) throws SQLException {
				
		Map<String, Long> blackList = proxy.getGlobalBlacklist();
				
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

			String bestHost = configuredHosts.get(bestHostIndex);

			ConnectionImpl conn = liveConnections.get(bestHost);

			if (conn == null) {
				try {
					conn = proxy.createConnectionForHost(bestHost);
				} catch (SQLException sqlEx) {
					ex = sqlEx;

					if (proxy.shouldExceptionTriggerFailover(sqlEx)) {
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
					}
						
					throw sqlEx;
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