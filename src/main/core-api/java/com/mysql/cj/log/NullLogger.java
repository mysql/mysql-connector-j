/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates.
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

/**
 * A logger that does nothing. Used before the log is configured via the URL or properties.
 */
public class NullLogger implements Log {

    /**
     * Creates a new NullLogger with the given name
     * 
     * @param instanceName
     *            (ignored)
     */
    public NullLogger(String instanceName) {
    }

    public boolean isDebugEnabled() {
        return false;
    }

    public boolean isErrorEnabled() {
        return false;
    }

    public boolean isFatalEnabled() {
        return false;
    }

    public boolean isInfoEnabled() {
        return false;
    }

    public boolean isTraceEnabled() {
        return false;
    }

    public boolean isWarnEnabled() {
        return false;
    }

    public void logDebug(Object msg) {
    }

    public void logDebug(Object msg, Throwable thrown) {
    }

    public void logError(Object msg) {
    }

    public void logError(Object msg, Throwable thrown) {
    }

    public void logFatal(Object msg) {
    }

    public void logFatal(Object msg, Throwable thrown) {
    }

    public void logInfo(Object msg) {
    }

    public void logInfo(Object msg, Throwable thrown) {
    }

    public void logTrace(Object msg) {
    }

    public void logTrace(Object msg, Throwable thrown) {
    }

    public void logWarn(Object msg) {
    }

    public void logWarn(Object msg, Throwable thrown) {
    }

}
