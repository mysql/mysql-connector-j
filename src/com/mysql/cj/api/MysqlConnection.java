/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api;

import java.util.Properties;
import java.util.TimeZone;

import com.mysql.cj.api.conf.ConnectionProperties;
import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.api.log.Log;

public interface MysqlConnection extends ConnectionProperties {

    void createNewIO(boolean isForReconnect);

    long getId();

    /**
     * Returns the log mechanism that should be used to log information from/for
     * this Connection.
     * 
     * @return the Log instance to use for logging messages.
     */
    public abstract Log getLog();

    /**
     * Returns the parsed and passed in properties for this connection.
     */
    public Properties getProperties();

    public String getProcessHost();

    public Protocol getIO();

    /**
     * Does the server this connection is connected to
     * meet or exceed the given version?
     */
    public boolean versionMeetsMinimum(int major, int minor, int subminor);

    public CharsetConverter getCharsetConverter(String javaEncodingName);

    Object getConnectionMutex();

    String getServerVariable(String variableName);

    ProfilerEventHandler getProfilerEventHandlerInstance();

    void setProfilerEventHandlerInstance(ProfilerEventHandler h);

    public abstract void initializeExtension(Extension ex);

    String getURL();

    String getUser();

    TimeZone getDefaultTimeZone();

    String getEncodingForIndex(int collationIndex);

    String getErrorMessageEncoding();

    int getMaxBytesPerChar(String javaCharsetName);

    int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName);

    int getNetBufferLength();

    SessionState getSessionState();
}
