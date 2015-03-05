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

package com.mysql.api;

import java.sql.SQLException;
import java.util.Properties;

import com.mysql.api.conf.ConnectionProperties;
import com.mysql.api.io.Protocol;
import com.mysql.api.log.Log;

public interface Connection extends ConnectionProperties {

    /**
     * Returns the log mechanism that should be used to log information from/for
     * this Connection.
     * 
     * @return the Log instance to use for logging messages.
     * @throws Exception
     *             if an error occurs
     */
    public abstract Log getLog() throws Exception;

    /**
     * Returns the parsed and passed in properties for this connection.
     */
    public Properties getProperties();

    public String getProcessHost() throws Exception;

    public Protocol getIO() throws Exception;

    public boolean versionMeetsMinimum(int major, int minor, int subminor) throws Exception;

    public CharsetConverter getCharsetConverter(String javaEncodingName) throws SQLException;

    Object getConnectionMutex();

    String getServerVariable(String variableName);

    ProfilerEventHandler getProfilerEventHandlerInstance();

    void setProfilerEventHandlerInstance(ProfilerEventHandler h);

    public abstract void initializeExtension(Extension ex) throws SQLException;

}
