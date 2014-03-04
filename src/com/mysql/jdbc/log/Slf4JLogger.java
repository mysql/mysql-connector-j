/*
  Copyright (c) 2011, 2014, Oracle and/or its affiliates. All rights reserved.

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Slf4JLogger implements Log {
	private Logger log;
	
	public Slf4JLogger(String name) {
		log = LoggerFactory.getLogger(name);
	}
	
	public boolean isDebugEnabled() {
		return log.isDebugEnabled();
	}

	public boolean isErrorEnabled() {
		return log.isErrorEnabled();
	}

	public boolean isFatalEnabled() {
		return log.isErrorEnabled();
	}

	public boolean isInfoEnabled() {
		return log.isInfoEnabled();
	}

	public boolean isTraceEnabled() {
		return log.isTraceEnabled();
	}

	public boolean isWarnEnabled() {
		return log.isWarnEnabled();
	}

	public void logDebug(Object msg) {
		log.debug(msg.toString());
	}

	public void logDebug(Object msg, Throwable thrown) {
		log.debug(msg.toString(), thrown);
	}

	public void logError(Object msg) {
		log.error(msg.toString());
	}

	public void logError(Object msg, Throwable thrown) {
		log.error(msg.toString(), thrown);
	}

	public void logFatal(Object msg) {
		log.error(msg.toString());
	}

	public void logFatal(Object msg, Throwable thrown) {
		log.error(msg.toString(), thrown);
	}

	public void logInfo(Object msg) {
		log.info(msg.toString());
	}

	public void logInfo(Object msg, Throwable thrown) {
		log.info(msg.toString(), thrown);
	}

	public void logTrace(Object msg) {
		log.trace(msg.toString());
	}

	public void logTrace(Object msg, Throwable thrown) {
		log.trace(msg.toString(), thrown);
	}

	public void logWarn(Object msg) {
		log.warn(msg.toString());
	}

	public void logWarn(Object msg, Throwable thrown) {
		log.warn(msg.toString(), thrown);
	}

}
