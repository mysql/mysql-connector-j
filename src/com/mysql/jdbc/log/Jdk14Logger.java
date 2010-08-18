/*
 Copyright (c) 2002, 2010, Oracle and/or its affiliates. All rights reserved.
 

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

import java.util.logging.Level;
import java.util.logging.Logger;

import com.mysql.jdbc.profiler.ProfilerEvent;

/**
 * Logging functionality for JDK1.4
 * 
 * @author Mark Matthews
 * 
 * @version $Id$
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
	 *            DOCUMENT ME!
	 */
	public Jdk14Logger(String name) {
		this.jdkLogger = Logger.getLogger(name);
	}

	/**
	 * @see com.mysql.jdbc.log.Log#isDebugEnabled()
	 */
	public boolean isDebugEnabled() {
		return this.jdkLogger.isLoggable(Level.FINE);
	}

	/**
	 * @see com.mysql.jdbc.log.Log#isErrorEnabled()
	 */
	public boolean isErrorEnabled() {
		return this.jdkLogger.isLoggable(Level.SEVERE);
	}

	/**
	 * @see com.mysql.jdbc.log.Log#isFatalEnabled()
	 */
	public boolean isFatalEnabled() {
		return this.jdkLogger.isLoggable(Level.SEVERE);
	}

	/**
	 * @see com.mysql.jdbc.log.Log#isInfoEnabled()
	 */
	public boolean isInfoEnabled() {
		return this.jdkLogger.isLoggable(Level.INFO);
	}

	/**
	 * @see com.mysql.jdbc.log.Log#isTraceEnabled()
	 */
	public boolean isTraceEnabled() {
		return this.jdkLogger.isLoggable(Level.FINEST);
	}

	/**
	 * @see com.mysql.jdbc.log.Log#isWarnEnabled()
	 */
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

			if (!callerClassName.startsWith("com.mysql.jdbc")
					|| callerClassName.startsWith("com.mysql.jdbc.compliance")) {
				return i;
			}
		}

		return 0;
	}

	private void logInternal(Level level, Object msg, Throwable exception) {
		//
		// only go through this exercise if the message will actually
		// be logged.
		//

		if (this.jdkLogger.isLoggable(level)) {
			String messageAsString = null;
			String callerMethodName = "N/A";
			String callerClassName = "N/A";
			int lineNumber = 0;
			String fileName = "N/A";

			if (msg instanceof ProfilerEvent) {
				messageAsString = LogUtils.expandProfilerEventIfNecessary(msg)
						.toString();
			} else {
				Throwable locationException = new Throwable();
				StackTraceElement[] locations = locationException
						.getStackTrace();

				int frameIdx = findCallerStackDepth(locations);

				if (frameIdx != 0) {
					callerClassName = locations[frameIdx].getClassName();
					callerMethodName = locations[frameIdx].getMethodName();
					lineNumber = locations[frameIdx].getLineNumber();
					fileName = locations[frameIdx].getFileName();
				}

				messageAsString = String.valueOf(msg);
			}

			if (exception == null) {
				this.jdkLogger.logp(level, callerClassName, callerMethodName,
						messageAsString);
			} else {
				this.jdkLogger.logp(level, callerClassName, callerMethodName,
						messageAsString, exception);
			}
		}
	}
}
