/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
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
 * Unified interface to logging facilities on different platforms
 */
public interface Log {
    /** Logger instance name */
    static final String LOGGER_INSTANCE_NAME = "MySQL";

    /**
     * Is the 'debug' log level enabled?
     * 
     * @return true if so.
     */
    boolean isDebugEnabled();

    /**
     * Is the 'error' log level enabled?
     * 
     * @return true if so.
     */
    boolean isErrorEnabled();

    /**
     * Is the 'fatal' log level enabled?
     * 
     * @return true if so.
     */
    boolean isFatalEnabled();

    /**
     * Is the 'info' log level enabled?
     * 
     * @return true if so.
     */
    boolean isInfoEnabled();

    /**
     * Is the 'trace' log level enabled?
     * 
     * @return true if so.
     */
    boolean isTraceEnabled();

    /**
     * Is the 'warn' log level enabled?
     * 
     * @return true if so.
     */
    boolean isWarnEnabled();

    /**
     * Logs the given message instance using the 'debug' level
     * 
     * @param msg
     *            the message to log
     */
    void logDebug(Object msg);

    /**
     * Logs the given message and Throwable at the 'debug' level.
     * 
     * @param msg
     *            the message to log
     * @param thrown
     *            the throwable to log (may be null)
     */
    void logDebug(Object msg, Throwable thrown);

    /**
     * Logs the given message instance using the 'error' level
     * 
     * @param msg
     *            the message to log
     */
    void logError(Object msg);

    /**
     * Logs the given message and Throwable at the 'error' level.
     * 
     * @param msg
     *            the message to log
     * @param thrown
     *            the throwable to log (may be null)
     */
    void logError(Object msg, Throwable thrown);

    /**
     * Logs the given message instance using the 'fatal' level
     * 
     * @param msg
     *            the message to log
     */
    void logFatal(Object msg);

    /**
     * Logs the given message and Throwable at the 'fatal' level.
     * 
     * @param msg
     *            the message to log
     * @param thrown
     *            the throwable to log (may be null)
     */
    void logFatal(Object msg, Throwable thrown);

    /**
     * Logs the given message instance using the 'info' level
     * 
     * @param msg
     *            the message to log
     */
    void logInfo(Object msg);

    /**
     * Logs the given message and Throwable at the 'info' level.
     * 
     * @param msg
     *            the message to log
     * @param thrown
     *            the throwable to log (may be null)
     */
    void logInfo(Object msg, Throwable thrown);

    /**
     * Logs the given message instance using the 'trace' level
     * 
     * @param msg
     *            the message to log
     */
    void logTrace(Object msg);

    /**
     * Logs the given message and Throwable at the 'trace' level.
     * 
     * @param msg
     *            the message to log
     * @param thrown
     *            the throwable to log (may be null)
     */
    void logTrace(Object msg, Throwable thrown);

    /**
     * Logs the given message instance using the 'warn' level
     * 
     * @param msg
     *            the message to log
     */
    void logWarn(Object msg);

    /**
     * Logs the given message and Throwable at the 'warn' level.
     * 
     * @param msg
     *            the message to log
     * @param thrown
     *            the throwable to log (may be null)
     */
    void logWarn(Object msg, Throwable thrown);
}
