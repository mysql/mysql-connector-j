/*
 Copyright  2002-2004 MySQL AB, 2008 Sun Microsystems

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA



 */
package com.mysql.jdbc.log;

/**
 * Unified interface to logging facilities on different platforms
 * 
 * @author Mark Matthews
 * 
 * @version $Id$
 */
public interface Log {
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