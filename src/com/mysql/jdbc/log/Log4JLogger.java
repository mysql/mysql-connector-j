/*
 Copyright  2002-2004 MySQL AB, 2008 Sun Microsystems
 All rights reserved. Use is subject to license terms.

  The MySQL Connector/J is licensed under the terms of the GPL,
  like most MySQL Connectors. There are special exceptions to the
  terms and conditions of the GPL as it is applied to this software,
  see the FLOSS License Exception available on mysql.com.

  This program is free software; you can redistribute it and/or
  modify it under the terms of the GNU General Public License as
  published by the Free Software Foundation; version 2 of the
  License.

  This program is distributed in the hope that it will be useful,  
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  02110-1301 USA



 */
package com.mysql.jdbc.log;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Implementation of log interface for Apache Log4j
 * 
 * @author Mark Matthews
 * 
 * @version $Id$
 */
public class Log4JLogger implements Log {

	private Logger logger;

	public Log4JLogger(String instanceName) {
		this.logger = Logger.getLogger(instanceName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.log.Log#isDebugEnabled()
	 */
	public boolean isDebugEnabled() {
		return this.logger.isDebugEnabled();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.log.Log#isErrorEnabled()
	 */
	public boolean isErrorEnabled() {
		return this.logger.isEnabledFor(Level.ERROR);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.log.Log#isFatalEnabled()
	 */
	public boolean isFatalEnabled() {
		return this.logger.isEnabledFor(Level.FATAL);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.log.Log#isInfoEnabled()
	 */
	public boolean isInfoEnabled() {
		return this.logger.isInfoEnabled();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.log.Log#isTraceEnabled()
	 */
	public boolean isTraceEnabled() {
		return this.logger.isDebugEnabled();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.log.Log#isWarnEnabled()
	 */
	public boolean isWarnEnabled() {
		return this.logger.isEnabledFor(Level.WARN);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.log.Log#logDebug(java.lang.Object)
	 */
	public void logDebug(Object msg) {
		this.logger.debug(LogUtils.expandProfilerEventIfNecessary(LogUtils
				.expandProfilerEventIfNecessary(msg)));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.log.Log#logDebug(java.lang.Object,
	 *      java.lang.Throwable)
	 */
	public void logDebug(Object msg, Throwable thrown) {
		this.logger.debug(LogUtils.expandProfilerEventIfNecessary(msg), thrown);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.log.Log#logError(java.lang.Object)
	 */
	public void logError(Object msg) {
		this.logger.error(LogUtils.expandProfilerEventIfNecessary(msg));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.log.Log#logError(java.lang.Object,
	 *      java.lang.Throwable)
	 */
	public void logError(Object msg, Throwable thrown) {
		this.logger.error(LogUtils.expandProfilerEventIfNecessary(msg), thrown);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.log.Log#logFatal(java.lang.Object)
	 */
	public void logFatal(Object msg) {
		this.logger.fatal(LogUtils.expandProfilerEventIfNecessary(msg));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.log.Log#logFatal(java.lang.Object,
	 *      java.lang.Throwable)
	 */
	public void logFatal(Object msg, Throwable thrown) {
		this.logger.fatal(LogUtils.expandProfilerEventIfNecessary(msg), thrown);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.log.Log#logInfo(java.lang.Object)
	 */
	public void logInfo(Object msg) {
		this.logger.info(LogUtils.expandProfilerEventIfNecessary(msg));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.log.Log#logInfo(java.lang.Object,
	 *      java.lang.Throwable)
	 */
	public void logInfo(Object msg, Throwable thrown) {
		this.logger.info(LogUtils.expandProfilerEventIfNecessary(msg), thrown);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.log.Log#logTrace(java.lang.Object)
	 */
	public void logTrace(Object msg) {
		this.logger.debug(LogUtils.expandProfilerEventIfNecessary(msg));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.log.Log#logTrace(java.lang.Object,
	 *      java.lang.Throwable)
	 */
	public void logTrace(Object msg, Throwable thrown) {
		this.logger.debug(LogUtils.expandProfilerEventIfNecessary(msg), thrown);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.log.Log#logWarn(java.lang.Object)
	 */
	public void logWarn(Object msg) {
		this.logger.warn(LogUtils.expandProfilerEventIfNecessary(msg));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.log.Log#logWarn(java.lang.Object,
	 *      java.lang.Throwable)
	 */
	public void logWarn(Object msg, Throwable thrown) {
		this.logger.warn(LogUtils.expandProfilerEventIfNecessary(msg), thrown);
	}
}
