/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

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

    /**
     * @see com.mysql.jdbc.log.Log#isDebugEnabled()
     */
    public boolean isDebugEnabled() {
        return false;
    }

    /**
     * @see com.mysql.jdbc.log.Log#isErrorEnabled()
     */
    public boolean isErrorEnabled() {
        return false;
    }

    /**
     * @see com.mysql.jdbc.log.Log#isFatalEnabled()
     */
    public boolean isFatalEnabled() {
        return false;
    }

    /**
     * @see com.mysql.jdbc.log.Log#isInfoEnabled()
     */
    public boolean isInfoEnabled() {
        return false;
    }

    /**
     * @see com.mysql.jdbc.log.Log#isTraceEnabled()
     */
    public boolean isTraceEnabled() {
        return false;
    }

    /**
     * @see com.mysql.jdbc.log.Log#isWarnEnabled()
     */
    public boolean isWarnEnabled() {
        return false;
    }

    /**
     * @see com.mysql.jdbc.log.Log#logDebug(java.lang.Object)
     */
    public void logDebug(Object msg) {
    }

    /**
     * @see com.mysql.jdbc.log.Log#logDebug(java.lang.Object, java.lang.Throwable)
     */
    public void logDebug(Object msg, Throwable thrown) {
    }

    /**
     * @see com.mysql.jdbc.log.Log#logError(java.lang.Object)
     */
    public void logError(Object msg) {
    }

    /**
     * @see com.mysql.jdbc.log.Log#logError(java.lang.Object, java.lang.Throwable)
     */
    public void logError(Object msg, Throwable thrown) {
    }

    /**
     * @see com.mysql.jdbc.log.Log#logFatal(java.lang.Object)
     */
    public void logFatal(Object msg) {
    }

    /**
     * @see com.mysql.jdbc.log.Log#logFatal(java.lang.Object, java.lang.Throwable)
     */
    public void logFatal(Object msg, Throwable thrown) {
    }

    /**
     * @see com.mysql.jdbc.log.Log#logInfo(java.lang.Object)
     */
    public void logInfo(Object msg) {
    }

    /**
     * @see com.mysql.jdbc.log.Log#logInfo(java.lang.Object, java.lang.Throwable)
     */
    public void logInfo(Object msg, Throwable thrown) {
    }

    /**
     * @see com.mysql.jdbc.log.Log#logTrace(java.lang.Object)
     */
    public void logTrace(Object msg) {
    }

    /**
     * @see com.mysql.jdbc.log.Log#logTrace(java.lang.Object, java.lang.Throwable)
     */
    public void logTrace(Object msg, Throwable thrown) {
    }

    /**
     * @see com.mysql.jdbc.log.Log#logWarn(java.lang.Object)
     */
    public void logWarn(Object msg) {
    }

    /**
     * @see com.mysql.jdbc.log.Log#logWarn(java.lang.Object, java.lang.Throwable)
     */
    public void logWarn(Object msg, Throwable thrown) {
    }

}
