/*
 Copyright (C) 2002-2004 MySQL AB

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

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.log.Log;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mmatthew
 */
public class ProfileEventSink {

	private static final Map CONNECTIONS_TO_SINKS = new HashMap();

	private Connection ownerConnection = null;

	private Log log = null;

	/**
	 * Returns the ProfileEventSink that handles profiler events for the given
	 * connection.
	 * 
	 * @param conn
	 *            the connection to handle events for
	 * @return the ProfileEventSink that handles profiler events
	 */
	public static synchronized ProfileEventSink getInstance(Connection conn) {
		ProfileEventSink sink = (ProfileEventSink) CONNECTIONS_TO_SINKS
				.get(conn);

		if (sink == null) {
			sink = new ProfileEventSink(conn);
			CONNECTIONS_TO_SINKS.put(conn, sink);
		}

		return sink;
	}

	/**
	 * Process a profiler event
	 * 
	 * @param evt
	 *            the event to process
	 */
	public void consumeEvent(ProfilerEvent evt) {
		if (evt.eventType == ProfilerEvent.TYPE_WARN) {
			this.log.logWarn(evt);
		} else {
			this.log.logInfo(evt);
		}
	}

	public static synchronized void removeInstance(Connection conn) {
		CONNECTIONS_TO_SINKS.remove(conn);
	}

	private ProfileEventSink(Connection conn) {
		this.ownerConnection = conn;

		try {
			this.log = this.ownerConnection.getLog();
		} catch (SQLException sqlEx) {
			throw new RuntimeException("Unable to get logger from connection");
		}
	}

}
