/*
 * Copyright (c) 2007, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.log;

import com.mysql.cj.Constants;
import com.mysql.cj.Query;
import com.mysql.cj.Session;
import com.mysql.cj.protocol.Resultset;

/**
 * A profile event handler that just logs to the standard logging mechanism of the driver.
 */
public class LoggingProfilerEventHandler implements ProfilerEventHandler {

    private Log logger;

    public LoggingProfilerEventHandler() {
    }

    @Override
    public void consumeEvent(ProfilerEvent evt) {
        switch (evt.getEventType()) {
            case ProfilerEvent.TYPE_USAGE:
                this.logger.logWarn(evt);
                break;

            default:
                this.logger.logInfo(evt);
                break;
        }
    }

    @Override
    public void destroy() {
        this.logger = null;
    }

    @Override
    public void init(Log log) {
        this.logger = log;
    }

    @Override
    public void processEvent(byte eventType, Session session, Query query, Resultset resultSet, long eventDuration, Throwable eventCreationPoint,
            String message) {
        consumeEvent(new ProfilerEventImpl(eventType, //
                session == null ? "" : session.getHostInfo().getHost(), //
                session == null ? "" : session.getHostInfo().getDatabase(), //
                session == null ? ProfilerEvent.NA : session.getThreadId(), //
                query == null ? ProfilerEvent.NA : query.getId(), //
                resultSet == null ? ProfilerEvent.NA : resultSet.getResultId(), //
                eventDuration, //
                session == null ? Constants.MILLIS_I18N : session.getQueryTimingUnits(), //
                eventCreationPoint, message));
    }

}
