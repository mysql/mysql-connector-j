/*
 * Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.core;

import com.mysql.cj.api.ProfilerEventHandler;
import com.mysql.cj.api.Session;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.core.log.NullLogger;

public abstract class AbstractSession implements Session {

    protected PropertySet propertySet;
    protected ExceptionInterceptor exceptionInterceptor;

    /** The event sink to use for profiling */
    private ProfilerEventHandler eventSink;

    /** The logger we're going to use */
    protected transient Log log;

    /** Null logger shared by all connections at startup */
    protected static final Log NULL_LOGGER = new NullLogger(Log.LOGGER_INSTANCE_NAME);

    @Override
    public PropertySet getPropertySet() {
        return this.propertySet;
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    public void setExceptionInterceptor(ExceptionInterceptor exceptionInterceptor) {
        this.exceptionInterceptor = exceptionInterceptor;
    }

    /**
     * Returns the log mechanism that should be used to log information from/for this Session.
     * 
     * @return the Log instance to use for logging messages.
     */
    public Log getLog() {
        return this.log;
    }

    public ProfilerEventHandler getProfilerEventHandler() {
        return this.eventSink;
    }

    public void setProfilerEventHandler(ProfilerEventHandler h) {
        this.eventSink = h;
    }

}
