/*
  Copyright (c) 2005, 2014, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc.log;

import com.mysql.jdbc.Util;
import com.mysql.jdbc.profiler.ProfilerEvent;

public class LogUtils {

    public static final String CALLER_INFORMATION_NOT_AVAILABLE = "Caller information not available";

    private static final String LINE_SEPARATOR = System
			.getProperty("line.separator");

	private static final int LINE_SEPARATOR_LENGTH = LINE_SEPARATOR.length();

	public static Object expandProfilerEventIfNecessary(
			Object possibleProfilerEvent) {

		if (possibleProfilerEvent instanceof ProfilerEvent) {
			StringBuffer msgBuf = new StringBuffer();

			ProfilerEvent evt = (ProfilerEvent) possibleProfilerEvent;

			String locationInformation = evt.getEventCreationPointAsString();

			if (locationInformation == null) {
				locationInformation = Util.stackTraceToString(new Throwable());
			}

			msgBuf.append("Profiler Event: [");

			switch (evt.getEventType()) {
			case ProfilerEvent.TYPE_EXECUTE:
				msgBuf.append("EXECUTE");

				break;

			case ProfilerEvent.TYPE_FETCH:
				msgBuf.append("FETCH");

				break;

			case ProfilerEvent.TYPE_OBJECT_CREATION:
				msgBuf.append("CONSTRUCT");

				break;

			case ProfilerEvent.TYPE_PREPARE:
				msgBuf.append("PREPARE");

				break;

			case ProfilerEvent.TYPE_QUERY:
				msgBuf.append("QUERY");

				break;

			case ProfilerEvent.TYPE_WARN:
				msgBuf.append("WARN");

				break;
				
			case ProfilerEvent.TYPE_SLOW_QUERY:
				msgBuf.append("SLOW QUERY");

				break;
				
			default:
				msgBuf.append("UNKNOWN");
			}

			msgBuf.append("] ");
			msgBuf.append(locationInformation);
			msgBuf.append(" duration: ");
			msgBuf.append(evt.getEventDuration());
			msgBuf.append(" ");
			msgBuf.append(evt.getDurationUnits());
			msgBuf.append(", connection-id: ");
			msgBuf.append(evt.getConnectionId());
			msgBuf.append(", statement-id: ");
			msgBuf.append(evt.getStatementId());
			msgBuf.append(", resultset-id: ");
			msgBuf.append(evt.getResultSetId());

			String evtMessage = evt.getMessage();

			if (evtMessage != null) {
				msgBuf.append(", message: ");
				msgBuf.append(evtMessage);
			}

			return msgBuf;
		}
		
		return possibleProfilerEvent;
	}
	
	public static String findCallingClassAndMethod(Throwable t) {
		String stackTraceAsString = Util.stackTraceToString(t);

		String callingClassAndMethod = CALLER_INFORMATION_NOT_AVAILABLE;

		int endInternalMethods = stackTraceAsString
				.lastIndexOf("com.mysql.jdbc");

		if (endInternalMethods != -1) {
			int endOfLine = -1;
			int compliancePackage = stackTraceAsString.indexOf(
					"com.mysql.jdbc.compliance", endInternalMethods);

			if (compliancePackage != -1) {
				endOfLine = compliancePackage - LINE_SEPARATOR_LENGTH;
			} else {
				endOfLine = stackTraceAsString.indexOf(LINE_SEPARATOR,
						endInternalMethods);
			}

			if (endOfLine != -1) {
				int nextEndOfLine = stackTraceAsString.indexOf(LINE_SEPARATOR,
						endOfLine + LINE_SEPARATOR_LENGTH);

				if (nextEndOfLine != -1) {
					callingClassAndMethod = stackTraceAsString.substring(
							endOfLine + LINE_SEPARATOR_LENGTH, nextEndOfLine);
				} else {
					callingClassAndMethod = stackTraceAsString
							.substring(endOfLine + LINE_SEPARATOR_LENGTH);
				}
			}
		}

		if (!callingClassAndMethod.startsWith("\tat ") && 
				!callingClassAndMethod.startsWith("at ")) {
			return "at " + callingClassAndMethod;
		}

		return callingClassAndMethod;
	}
}
