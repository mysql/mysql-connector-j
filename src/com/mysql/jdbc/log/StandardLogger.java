/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc.log;

import java.util.Date;

import com.mysql.jdbc.Util;
import com.mysql.jdbc.profiler.ProfilerEvent;

/**
 * Provides logging facilities for those platforms that don't have built-in facilities. Simply logs messages to STDERR.
 */
public class StandardLogger implements Log {
    private static final int FATAL = 0;

    private static final int ERROR = 1;

    private static final int WARN = 2;

    private static final int INFO = 3;

    private static final int DEBUG = 4;

    private static final int TRACE = 5;

    private static StringBuffer bufferedLog = null;

    private boolean logLocationInfo = true;

    /**
     * Creates a new StandardLogger object.
     * 
     * @param name
     *            the name of the configuration to use -- ignored
     */
    public StandardLogger(String name) {
        this(name, false);
    }

    /**
     * @param name
     * @param logLocationInfo
     */
    public StandardLogger(String name, boolean logLocationInfo) {
        this.logLocationInfo = logLocationInfo;
    }

    public static void startLoggingToBuffer() {
        bufferedLog = new StringBuffer();
    }

    public static void dropBuffer() {
        bufferedLog = null;
    }

    public static Appendable getBuffer() {
        return bufferedLog;
    }

    /**
     * @see com.mysql.jdbc.log.Log#isDebugEnabled()
     */
    public boolean isDebugEnabled() {
        return true;
    }

    /**
     * @see com.mysql.jdbc.log.Log#isErrorEnabled()
     */
    public boolean isErrorEnabled() {
        return true;
    }

    /**
     * @see com.mysql.jdbc.log.Log#isFatalEnabled()
     */
    public boolean isFatalEnabled() {
        return true;
    }

    /**
     * @see com.mysql.jdbc.log.Log#isInfoEnabled()
     */
    public boolean isInfoEnabled() {
        return true;
    }

    /**
     * @see com.mysql.jdbc.log.Log#isTraceEnabled()
     */
    public boolean isTraceEnabled() {
        return true;
    }

    /**
     * @see com.mysql.jdbc.log.Log#isWarnEnabled()
     */
    public boolean isWarnEnabled() {
        return true;
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

    protected void logInternal(int level, Object msg, Throwable exception) {
        StringBuilder msgBuf = new StringBuilder();
        msgBuf.append(new Date().toString());
        msgBuf.append(" ");

        switch (level) {
            case FATAL:
                msgBuf.append("FATAL: ");

                break;

            case ERROR:
                msgBuf.append("ERROR: ");

                break;

            case WARN:
                msgBuf.append("WARN: ");

                break;

            case INFO:
                msgBuf.append("INFO: ");

                break;

            case DEBUG:
                msgBuf.append("DEBUG: ");

                break;

            case TRACE:
                msgBuf.append("TRACE: ");

                break;
        }

        if (msg instanceof ProfilerEvent) {
            msgBuf.append(LogUtils.expandProfilerEventIfNecessary(msg));

        } else {
            if (this.logLocationInfo && level != TRACE) {
                Throwable locationException = new Throwable();
                msgBuf.append(LogUtils.findCallingClassAndMethod(locationException));
                msgBuf.append(" ");
            }

            if (msg != null) {
                msgBuf.append(String.valueOf(msg));
            }
        }

        if (exception != null) {
            msgBuf.append("\n");
            msgBuf.append("\n");
            msgBuf.append("EXCEPTION STACK TRACE:");
            msgBuf.append("\n");
            msgBuf.append("\n");
            msgBuf.append(Util.stackTraceToString(exception));
        }

        String messageAsString = msgBuf.toString();

        System.err.println(messageAsString);

        if (bufferedLog != null) {
            bufferedLog.append(messageAsString);
        }
    }
}
