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

package com.mysql.cj.core.io;

import java.util.LinkedList;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.SessionState;
import com.mysql.cj.api.authentication.AuthenticationFactory;
import com.mysql.cj.api.authentication.AuthenticationProvider;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exception.ExceptionInterceptor;
import com.mysql.cj.api.io.PacketSender;
import com.mysql.cj.api.io.PacketSentTimeHolder;
import com.mysql.cj.api.io.PhysicalConnection;
import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exception.CJException;
import com.mysql.cj.core.exception.ExceptionFactory;
import com.mysql.cj.core.exception.UnableToConnectException;

public abstract class AbstractProtocol implements Protocol {

    protected MysqlConnection connection;
    protected PhysicalConnection physicalConnection;

    protected PropertySet propertySet;

    protected ExceptionInterceptor exceptionInterceptor;

    protected long lastPacketSentTimeMs = 0;
    protected long lastPacketReceivedTimeMs = 0;

    protected long threadId = -1;
    protected boolean traceProtocol = false;
    protected boolean enablePacketDebug = false;
    protected PacketSender packetSender;

    protected AuthenticationProvider authProvider;
    protected SessionState sessionState;

    // Default until packet sender created
    protected PacketSentTimeHolder packetSentTimeHolder = new PacketSentTimeHolder() {
        public long getLastPacketSentTime() {
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

    public PhysicalConnection getPhysicalConnection() {
        return this.physicalConnection;
    }

    @Override
    public SessionState getSessionState() {
        return this.sessionState;
    }

    @Override
    public AuthenticationProvider getAuthenticationProvider() {
        return this.authProvider;
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    public long getLastPacketSentTimeMs() {
        return this.lastPacketSentTimeMs;
    }

    public long getLastPacketReceivedTimeMs() {
        return this.lastPacketReceivedTimeMs;
    }

    /**
     * Apply optional decorators to configured PacketSender.
     */
    protected void decoratePacketSender() {
        TimeTrackingPacketSender ttSender = new TimeTrackingPacketSender(this.packetSender);
        this.packetSentTimeHolder = ttSender;
        this.packetSender = ttSender;
        if (this.traceProtocol) {
            this.packetSender = new TracingPacketSender(this.packetSender, this.connection.getLog(), this.physicalConnection.getHost(), this.threadId);
        }
        if (this.enablePacketDebug) {
            this.packetSender = new DebugBufferingPacketSender(this.packetSender, this.packetDebugRingBuffer);
        }
    }

    protected AuthenticationFactory createAuthenticationFactory(String authenticationFactoryClassName) {
        try {
            if (authenticationFactoryClassName == null) {
                throw ExceptionFactory.createException(UnableToConnectException.class, Messages.getString("Session.2"), getExceptionInterceptor());
            }

            return (AuthenticationFactory) (Class.forName(authenticationFactoryClassName).newInstance());
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | CJException ex) {
            throw ExceptionFactory.createException(UnableToConnectException.class,
                    Messages.getString("Session.3", new String[] { authenticationFactoryClassName }), getExceptionInterceptor());
        }
    }
}
