/*
  Copyright (c) 2011, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.cj.api.log.Log;

public class Slf4JLogger implements Log {
    private Logger log;

    public Slf4JLogger(String name) {
        this.log = LoggerFactory.getLogger(name);
    }

    public boolean isDebugEnabled() {
        return this.log.isDebugEnabled();
    }

    public boolean isErrorEnabled() {
        return this.log.isErrorEnabled();
    }

    public boolean isFatalEnabled() {
        return this.log.isErrorEnabled();
    }

    public boolean isInfoEnabled() {
        return this.log.isInfoEnabled();
    }

    public boolean isTraceEnabled() {
        return this.log.isTraceEnabled();
    }

    public boolean isWarnEnabled() {
        return this.log.isWarnEnabled();
    }

    public void logDebug(Object msg) {
        this.log.debug(msg.toString());
    }

    public void logDebug(Object msg, Throwable thrown) {
        this.log.debug(msg.toString(), thrown);
    }

    public void logError(Object msg) {
        this.log.error(msg.toString());
    }

    public void logError(Object msg, Throwable thrown) {
        this.log.error(msg.toString(), thrown);
    }

    public void logFatal(Object msg) {
        this.log.error(msg.toString());
    }

    public void logFatal(Object msg, Throwable thrown) {
        this.log.error(msg.toString(), thrown);
    }

    public void logInfo(Object msg) {
        this.log.info(msg.toString());
    }

    public void logInfo(Object msg, Throwable thrown) {
        this.log.info(msg.toString(), thrown);
    }

    public void logTrace(Object msg) {
        this.log.trace(msg.toString());
    }

    public void logTrace(Object msg, Throwable thrown) {
        this.log.trace(msg.toString(), thrown);
    }

    public void logWarn(Object msg) {
        this.log.warn(msg.toString());
    }

    public void logWarn(Object msg, Throwable thrown) {
        this.log.warn(msg.toString(), thrown);
    }

}
