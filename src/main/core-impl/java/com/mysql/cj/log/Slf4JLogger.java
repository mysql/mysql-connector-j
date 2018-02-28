/*
 * Copyright (c) 2011, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
