/*
 * Copyright (c) 2002, 2019, Oracle and/or its affiliates. All rights reserved.
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

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Logging functionality for JDK1.4
 */
public class Jdk14Logger implements Log {
    private static final Level DEBUG = Level.FINE;

    private static final Level ERROR = Level.SEVERE;

    private static final Level FATAL = Level.SEVERE;

    private static final Level INFO = Level.INFO;

    private static final Level TRACE = Level.FINEST;

    private static final Level WARN = Level.WARNING;

    /**
     * The underlying logger from JDK-1.4
     */
    protected Logger jdkLogger = null;

    /**
     * Creates a new Jdk14Logger object.
     * 
     * @param name
     *            logger name as per {@link Logger#getLogger(String)}
     */
    public Jdk14Logger(String name) {
        this.jdkLogger = Logger.getLogger(name);
    }

    public boolean isDebugEnabled() {
        return this.jdkLogger.isLoggable(Level.FINE);
    }

    public boolean isErrorEnabled() {
        return this.jdkLogger.isLoggable(Level.SEVERE);
    }

    public boolean isFatalEnabled() {
        return this.jdkLogger.isLoggable(Level.SEVERE);
    }

    public boolean isInfoEnabled() {
        return this.jdkLogger.isLoggable(Level.INFO);
    }

    public boolean isTraceEnabled() {
        return this.jdkLogger.isLoggable(Level.FINEST);
    }

    public boolean isWarnEnabled() {
        return this.jdkLogger.isLoggable(Level.WARNING);
    }

    /**
     * Logs the given message instance using the 'debug' level
     * 
     * @param message
     *            the message to log
     */
    public void logDebug(Object message) {
        logInternal(DEBUG, message, null);
    }

    /**
     * Logs the given message and Throwable at the 'debug' level.
     * 
     * @param message
     *            the message to log
     * @param exception
     *            the throwable to log (may be null)
     */
    public void logDebug(Object message, Throwable exception) {
        logInternal(DEBUG, message, exception);
    }

    /**
     * Logs the given message instance using the 'error' level
     * 
     * @param message
     *            the message to log
     */
    public void logError(Object message) {
        logInternal(ERROR, message, null);
    }

    /**
     * Logs the given message and Throwable at the 'error' level.
     * 
     * @param message
     *            the message to log
     * @param exception
     *            the throwable to log (may be null)
     */
    public void logError(Object message, Throwable exception) {
        logInternal(ERROR, message, exception);
    }

    /**
     * Logs the given message instance using the 'fatal' level
     * 
     * @param message
     *            the message to log
     */
    public void logFatal(Object message) {
        logInternal(FATAL, message, null);
    }

    /**
     * Logs the given message and Throwable at the 'fatal' level.
     * 
     * @param message
     *            the message to log
     * @param exception
     *            the throwable to log (may be null)
     */
    public void logFatal(Object message, Throwable exception) {
        logInternal(FATAL, message, exception);
    }

    /**
     * Logs the given message instance using the 'info' level
     * 
     * @param message
     *            the message to log
     */
    public void logInfo(Object message) {
        logInternal(INFO, message, null);
    }

    /**
     * Logs the given message and Throwable at the 'info' level.
     * 
     * @param message
     *            the message to log
     * @param exception
     *            the throwable to log (may be null)
     */
    public void logInfo(Object message, Throwable exception) {
        logInternal(INFO, message, exception);
    }

    /**
     * Logs the given message instance using the 'trace' level
     * 
     * @param message
     *            the message to log
     */
    public void logTrace(Object message) {
        logInternal(TRACE, message, null);
    }

    /**
     * Logs the given message and Throwable at the 'trace' level.
     * 
     * @param message
     *            the message to log
     * @param exception
     *            the throwable to log (may be null)
     */
    public void logTrace(Object message, Throwable exception) {
        logInternal(TRACE, message, exception);
    }

    /**
     * Logs the given message instance using the 'warn' level
     * 
     * @param message
     *            the message to log
     */
    public void logWarn(Object message) {
        logInternal(WARN, message, null);
    }

    /**
     * Logs the given message and Throwable at the 'warn' level.
     * 
     * @param message
     *            the message to log
     * @param exception
     *            the throwable to log (may be null)
     */
    public void logWarn(Object message, Throwable exception) {
        logInternal(WARN, message, exception);
    }

    private static final int findCallerStackDepth(StackTraceElement[] stackTrace) {
        int numFrames = stackTrace.length;

        for (int i = 0; i < numFrames; i++) {
            String callerClassName = stackTrace[i].getClassName();

            if (!(callerClassName.startsWith("com.mysql.cj") || callerClassName.startsWith("com.mysql.cj.core")
                    || callerClassName.startsWith("com.mysql.cj.jdbc"))) {
                return i;
            }
        }

        return 0;
    }

    private void logInternal(Level level, Object msg, Throwable exception) {
        //
        // only go through this exercise if the message will actually be logged.
        //

        if (this.jdkLogger.isLoggable(level)) {
            String messageAsString = null;
            String callerMethodName = "N/A";
            String callerClassName = "N/A";
            //int lineNumber = 0;
            //String fileName = "N/A";

            if (msg instanceof ProfilerEvent) {
                messageAsString = msg.toString();
            } else {
                Throwable locationException = new Throwable();
                StackTraceElement[] locations = locationException.getStackTrace();

                int frameIdx = findCallerStackDepth(locations);

                if (frameIdx != 0) {
                    callerClassName = locations[frameIdx].getClassName();
                    callerMethodName = locations[frameIdx].getMethodName();
                    //lineNumber = locations[frameIdx].getLineNumber();
                    //fileName = locations[frameIdx].getFileName();
                }

                messageAsString = String.valueOf(msg);
            }

            if (exception == null) {
                this.jdkLogger.logp(level, callerClassName, callerMethodName, messageAsString);
            } else {
                this.jdkLogger.logp(level, callerClassName, callerMethodName, messageAsString, exception);
            }
        }
    }
}
