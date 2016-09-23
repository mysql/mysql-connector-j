/*
  Copyright (c) 2007, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.profiler;

import com.mysql.cj.api.ProfilerEvent;
import com.mysql.cj.api.ProfilerEventHandler;
import com.mysql.cj.api.log.Log;

/**
 * A profile event handler that just logs to the standard logging mechanism of the driver.
 */
public class LoggingProfilerEventHandler implements ProfilerEventHandler {
    private Log logger;

    public LoggingProfilerEventHandler() {
    }

    public void consumeEvent(ProfilerEvent evt) {
        if (evt.getEventType() == ProfilerEvent.TYPE_WARN) {
            this.logger.logWarn(evt);
        } else {
            this.logger.logInfo(evt);
        }
    }

    public void destroy() {
        this.logger = null;
    }

    public void init(Log log) {
        this.logger = log;
    }

}
