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
 * A logger that does nothing. Used before the log is configured via the URL or
 * properties.
 * 
 * @author Mark Matthews
 * 
 * @version $Id$
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

	/**
	 * @see com.mysql.jdbc.log.Log#isDebugEnabled()
	 */
	public boolean isDebugEnabled() {
		// XXX Auto-generated method stub
		return false;
	}

	/**
	 * @see com.mysql.jdbc.log.Log#isErrorEnabled()
	 */
	public boolean isErrorEnabled() {
		// XXX Auto-generated method stub
		return false;
	}

	/**
	 * @see com.mysql.jdbc.log.Log#isFatalEnabled()
	 */
	public boolean isFatalEnabled() {
		// XXX Auto-generated method stub
		return false;
	}

	/**
	 * @see com.mysql.jdbc.log.Log#isInfoEnabled()
	 */
	public boolean isInfoEnabled() {
		// XXX Auto-generated method stub
		return false;
	}

	/**
	 * @see com.mysql.jdbc.log.Log#isTraceEnabled()
	 */
	public boolean isTraceEnabled() {
		// XXX Auto-generated method stub
		return false;
	}

	/**
	 * @see com.mysql.jdbc.log.Log#isWarnEnabled()
	 */
	public boolean isWarnEnabled() {
		// XXX Auto-generated method stub
		return false;
	}

	/**
	 * @see com.mysql.jdbc.log.Log#logDebug(java.lang.Object)
	 */
	public void logDebug(Object msg) {
		// XXX Auto-generated method stub

	}

	/**
	 * @see com.mysql.jdbc.log.Log#logDebug(java.lang.Object,
	 *      java.lang.Throwable)
	 */
	public void logDebug(Object msg, Throwable thrown) {
		// XXX Auto-generated method stub

	}

	/**
	 * @see com.mysql.jdbc.log.Log#logError(java.lang.Object)
	 */
	public void logError(Object msg) {
		// XXX Auto-generated method stub

	}

	/**
	 * @see com.mysql.jdbc.log.Log#logError(java.lang.Object,
	 *      java.lang.Throwable)
	 */
	public void logError(Object msg, Throwable thrown) {
		// XXX Auto-generated method stub

	}

	/**
	 * @see com.mysql.jdbc.log.Log#logFatal(java.lang.Object)
	 */
	public void logFatal(Object msg) {
		// XXX Auto-generated method stub

	}

	/**
	 * @see com.mysql.jdbc.log.Log#logFatal(java.lang.Object,
	 *      java.lang.Throwable)
	 */
	public void logFatal(Object msg, Throwable thrown) {
		// XXX Auto-generated method stub

	}

	/**
	 * @see com.mysql.jdbc.log.Log#logInfo(java.lang.Object)
	 */
	public void logInfo(Object msg) {
		// XXX Auto-generated method stub

	}

	/**
	 * @see com.mysql.jdbc.log.Log#logInfo(java.lang.Object,
	 *      java.lang.Throwable)
	 */
	public void logInfo(Object msg, Throwable thrown) {
		// XXX Auto-generated method stub

	}

	/**
	 * @see com.mysql.jdbc.log.Log#logTrace(java.lang.Object)
	 */
	public void logTrace(Object msg) {
		// XXX Auto-generated method stub

	}

	/**
	 * @see com.mysql.jdbc.log.Log#logTrace(java.lang.Object,
	 *      java.lang.Throwable)
	 */
	public void logTrace(Object msg, Throwable thrown) {
		// XXX Auto-generated method stub

	}

	/**
	 * @see com.mysql.jdbc.log.Log#logWarn(java.lang.Object)
	 */
	public void logWarn(Object msg) {
		// XXX Auto-generated method stub

	}

	/**
	 * @see com.mysql.jdbc.log.Log#logWarn(java.lang.Object,
	 *      java.lang.Throwable)
	 */
	public void logWarn(Object msg, Throwable thrown) {
		// XXX Auto-generated method stub

	}

}
