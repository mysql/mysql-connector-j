/*
 Copyright  2006 MySQL AB, 2008 Sun Microsystems
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CommonsLogger implements com.mysql.jdbc.log.Log {
	private Log logger;
	
	public CommonsLogger(String instanceName) {
		logger = LogFactory.getLog(instanceName);
	}

	public boolean isDebugEnabled() {
		return this.logger.isInfoEnabled();
	}

	public boolean isErrorEnabled() {
		return this.logger.isErrorEnabled();
	}

	public boolean isFatalEnabled() {
		return this.logger.isFatalEnabled();
	}

	public boolean isInfoEnabled() {
		return this.logger.isInfoEnabled();
	}

	public boolean isTraceEnabled() {
		return this.logger.isTraceEnabled();
	}

	public boolean isWarnEnabled() {
		return this.logger.isWarnEnabled();
	}

	public void logDebug(Object msg) {
		this.logger.debug(LogUtils.expandProfilerEventIfNecessary(msg));
	}

	public void logDebug(Object msg, Throwable thrown) {
		this.logger.debug(LogUtils.expandProfilerEventIfNecessary(msg), thrown);
	}

	public void logError(Object msg) {
		this.logger.error(LogUtils.expandProfilerEventIfNecessary(msg));
	}

	public void logError(Object msg, Throwable thrown) {
		this.logger.fatal(LogUtils.expandProfilerEventIfNecessary(msg), thrown);
	}

	public void logFatal(Object msg) {
		this.logger.fatal(LogUtils.expandProfilerEventIfNecessary(msg));
	}

	public void logFatal(Object msg, Throwable thrown) {
		this.logger.fatal(LogUtils.expandProfilerEventIfNecessary(msg), thrown);
	}

	public void logInfo(Object msg) {
		this.logger.info(LogUtils.expandProfilerEventIfNecessary(msg));
	}

	public void logInfo(Object msg, Throwable thrown) {
		this.logger.info(LogUtils.expandProfilerEventIfNecessary(msg), thrown);
	}

	public void logTrace(Object msg) {
		this.logger.trace(LogUtils.expandProfilerEventIfNecessary(msg));
	}

	public void logTrace(Object msg, Throwable thrown) {
		this.logger.trace(LogUtils.expandProfilerEventIfNecessary(msg), thrown);
	}

	public void logWarn(Object msg) {
		this.logger.warn(LogUtils.expandProfilerEventIfNecessary(msg));
	}

	public void logWarn(Object msg, Throwable thrown) {
		this.logger.warn(LogUtils.expandProfilerEventIfNecessary(msg), thrown);
	}

}
