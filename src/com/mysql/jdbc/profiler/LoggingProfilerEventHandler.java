/*
  Copyright (c) 2007, 2014, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc.profiler;

import java.sql.SQLException;
import java.util.Properties;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.log.Log;

/**
 * A profile event handler that just logs to the standard
 * logging mechanism of the JDBC driver.
 *
 */
public class LoggingProfilerEventHandler implements ProfilerEventHandler {
	private Log log;
	
	public LoggingProfilerEventHandler() {}
	
	public void consumeEvent(ProfilerEvent evt) {
		if (evt.eventType == ProfilerEvent.TYPE_WARN) {
			this.log.logWarn(evt);
		} else {
			this.log.logInfo(evt);
		}
	}

	public void destroy() {
		this.log = null;
	}

	public void init(Connection conn, Properties props) throws SQLException {
		this.log = conn.getLog();
	}

}
