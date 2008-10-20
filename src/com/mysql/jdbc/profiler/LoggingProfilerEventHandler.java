/*
 Copyright  2007 MySQL AB, 2008 Sun Microsystems

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
