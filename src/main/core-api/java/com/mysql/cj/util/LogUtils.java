/*
 * Copyright (c) 2005, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.util;

import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.log.ProfilerEvent;

public class LogUtils {

    public static final String CALLER_INFORMATION_NOT_AVAILABLE = "Caller information not available";

    private static final String LINE_SEPARATOR = System.getProperty(PropertyDefinitions.SYSP_line_separator);

    private static final int LINE_SEPARATOR_LENGTH = LINE_SEPARATOR.length();

    public static Object expandProfilerEventIfNecessary(Object possibleProfilerEvent) {

        if (possibleProfilerEvent instanceof ProfilerEvent) {
            StringBuilder msgBuf = new StringBuilder();

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

        int endInternalMethods = Math.max(Math.max(stackTraceAsString.lastIndexOf("com.mysql.cj"), stackTraceAsString.lastIndexOf("com.mysql.cj.core")),
                stackTraceAsString.lastIndexOf("com.mysql.cj.jdbc"));

        if (endInternalMethods != -1) {
            int endOfLine = stackTraceAsString.indexOf(LINE_SEPARATOR, endInternalMethods);

            if (endOfLine != -1) {
                int nextEndOfLine = stackTraceAsString.indexOf(LINE_SEPARATOR, endOfLine + LINE_SEPARATOR_LENGTH);
                callingClassAndMethod = nextEndOfLine != -1 ? stackTraceAsString.substring(endOfLine + LINE_SEPARATOR_LENGTH, nextEndOfLine)
                        : stackTraceAsString.substring(endOfLine + LINE_SEPARATOR_LENGTH);
            }
        }

        if (!callingClassAndMethod.startsWith("\tat ") && !callingClassAndMethod.startsWith("at ")) {
            return "at " + callingClassAndMethod;
        }

        return callingClassAndMethod;
    }

}
