/*
  Copyright (c) 2019, Oracle and/or its affiliates. All rights reserved.

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

package testsuite;

import com.mysql.jdbc.log.StandardLogger;

/**
 * Provides logging facilities for those platforms that don't have built-in facilities. Simply logs messages to STDERR.
 */
public class BufferingLogger extends StandardLogger {

    private static StringBuffer bufferedLog = null;

    public BufferingLogger(String name) {
        super(name);
    }

    public BufferingLogger(String name, boolean logLocationInfo) {
        super(name, logLocationInfo);
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

    @Override
    protected String logInternal(int level, Object msg, Throwable exception) {

        String messageAsString = super.logInternal(level, msg, exception);

        if (bufferedLog != null) {
            bufferedLog.append(messageAsString);
        }

        return messageAsString;
    }
}
