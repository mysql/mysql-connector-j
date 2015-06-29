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

package com.mysql.cj.mysqla.io;

import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.ServerCapabilities;
import com.mysql.cj.core.ServerVersion;

public class MysqlaCapabilities implements ServerCapabilities {

    private Buffer initialHandshakePacket;

    private byte protocolVersion = 0;
    private ServerVersion serverVersion;
    private long threadId = -1;
    private String seed;
    private int capabilityFlags;
    private int serverCharsetIndex;
    private int statusFlags = 0;
    private int authPluginDataLength = 0;

    public MysqlaCapabilities() {
    }

    public Buffer getInitialHandshakePacket() {
        return this.initialHandshakePacket;
    }

    public void setInitialHandshakePacket(Buffer initialHandshakePacket, ExceptionInterceptor exceptionInterceptor) {
        this.initialHandshakePacket = initialHandshakePacket;

        // Get the protocol version
        setProtocolVersion(initialHandshakePacket.readByte());

        setServerVersion(ServerVersion.parseVersion(initialHandshakePacket.readString("ASCII")));

        // read connection id
        setThreadId(initialHandshakePacket.readLong());

        // read auth-plugin-data-part-1 (string[8])
        setSeed(initialHandshakePacket.readString("ASCII", 8));

        // read filler ([00])
        initialHandshakePacket.readByte();

        int capabilityFlags = 0;

        // read capability flags (lower 2 bytes)
        if (initialHandshakePacket.getPosition() < initialHandshakePacket.getBufLength()) {
            capabilityFlags = initialHandshakePacket.readInt();
        }

        // read character set (1 byte)
        setServerCharsetIndex(initialHandshakePacket.readByte() & 0xff);
        // read status flags (2 bytes)
        setStatusFlags(initialHandshakePacket.readInt());

        // read capability flags (upper 2 bytes)
        capabilityFlags |= initialHandshakePacket.readInt() << 16;

        setCapabilityFlags(capabilityFlags);

        if ((capabilityFlags & MysqlaServerSession.CLIENT_PLUGIN_AUTH) != 0) {
            // read length of auth-plugin-data (1 byte)
            this.authPluginDataLength = initialHandshakePacket.readByte() & 0xff;
        } else {
            // read filler ([00])
            initialHandshakePacket.readByte();
        }
        // next 10 bytes are reserved (all [00])
        initialHandshakePacket.setPosition(initialHandshakePacket.getPosition() + 10);

    }

    @Override
    public int getCapabilityFlags() {
        return this.capabilityFlags;
    }

    @Override
    public void setCapabilityFlags(int capabilityFlags) {
        this.capabilityFlags = capabilityFlags;
    }

    public byte getProtocolVersion() {
        return this.protocolVersion;
    }

    public void setProtocolVersion(byte protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public ServerVersion getServerVersion() {
        return this.serverVersion;
    }

    public void setServerVersion(ServerVersion serverVersion) {
        this.serverVersion = serverVersion;
    }

    public long getThreadId() {
        return this.threadId;
    }

    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    public String getSeed() {
        return this.seed;
    }

    public void setSeed(String seed) {
        this.seed = seed;
    }

    public int getServerCharsetIndex() {
        return this.serverCharsetIndex;
    }

    public void setServerCharsetIndex(int serverCharsetIndex) {
        this.serverCharsetIndex = serverCharsetIndex;
    }

    public int getStatusFlags() {
        return this.statusFlags;
    }

    public void setStatusFlags(int statusFlags) {
        this.statusFlags = statusFlags;
    }

    public int getAuthPluginDataLength() {
        return this.authPluginDataLength;
    }

    public void setAuthPluginDataLength(int authPluginDataLength) {
        this.authPluginDataLength = authPluginDataLength;
    }
}
