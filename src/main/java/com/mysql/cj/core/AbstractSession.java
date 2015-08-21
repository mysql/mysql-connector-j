/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
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

    public void setLog(Log log) {
        this.log = log;
    }

    public ProfilerEventHandler getProfilerEventHandler() {
        return this.eventSink;
    }

    public void setProfilerEventHandler(ProfilerEventHandler h) {
        this.eventSink = h;
    }

}
