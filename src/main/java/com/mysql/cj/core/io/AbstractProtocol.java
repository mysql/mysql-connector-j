/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.io;

import java.util.LinkedList;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.authentication.AuthenticationProvider;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.PacketReceivedTimeHolder;
import com.mysql.cj.api.io.PacketSentTimeHolder;
import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.api.io.SocketConnection;
import com.mysql.cj.api.log.Log;

public abstract class AbstractProtocol implements Protocol {

    protected MysqlConnection connection;
    protected SocketConnection socketConnection;

    protected PropertySet propertySet;

    /** The logger we're going to use */
    protected transient Log log;

    protected ExceptionInterceptor exceptionInterceptor;

    protected AuthenticationProvider authProvider;

    // Default until packet sender created
    private PacketSentTimeHolder packetSentTimeHolder = new PacketSentTimeHolder() {
        public long getLastPacketSentTime() {
            return 0;
        }
    };
    private PacketReceivedTimeHolder packetReceivedTimeHolder = new PacketReceivedTimeHolder() {
        public long getLastPacketReceivedTime() {
            return 0;
        }
    };

    protected LinkedList<StringBuilder> packetDebugRingBuffer = null;

    public MysqlConnection getConnection() {
        return this.connection;
    }

    public void setConnection(MysqlConnection connection) {
        this.connection = connection;
    }

    public SocketConnection getSocketConnection() {
        return this.socketConnection;
    }

    public AuthenticationProvider getAuthenticationProvider() {
        return this.authProvider;
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    public PacketSentTimeHolder getPacketSentTimeHolder() {
        return this.packetSentTimeHolder;
    }

    public void setPacketSentTimeHolder(PacketSentTimeHolder packetSentTimeHolder) {
        this.packetSentTimeHolder = packetSentTimeHolder;
    }

    public PacketReceivedTimeHolder getPacketReceivedTimeHolder() {
        return this.packetReceivedTimeHolder;
    }

    public void setPacketReceivedTimeHolder(PacketReceivedTimeHolder packetReceivedTimeHolder) {
        this.packetReceivedTimeHolder = packetReceivedTimeHolder;
    }

    public PropertySet getPropertySet() {
        return this.propertySet;
    }

    public void setPropertySet(PropertySet propertySet) {
        this.propertySet = propertySet;
    }

}
