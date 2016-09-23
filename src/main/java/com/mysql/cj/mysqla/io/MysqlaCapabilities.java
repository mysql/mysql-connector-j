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

package com.mysql.cj.mysqla.io;

import com.mysql.cj.api.io.ServerCapabilities;
import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringLengthDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringSelfDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.core.ServerVersion;

public class MysqlaCapabilities implements ServerCapabilities {

    private PacketPayload initialHandshakePacket;

    private byte protocolVersion = 0;
    private ServerVersion serverVersion;
    private long threadId = -1;
    private String seed;
    private int capabilityFlags;
    private int serverDefaultCollationIndex;
    private int statusFlags = 0;
    private int authPluginDataLength = 0;

    public MysqlaCapabilities() {
    }

    public PacketPayload getInitialHandshakePacket() {
        return this.initialHandshakePacket;
    }

    public void setInitialHandshakePacket(PacketPayload initialHandshakePacket) {
        this.initialHandshakePacket = initialHandshakePacket;

        // Get the protocol version
        setProtocolVersion((byte) initialHandshakePacket.readInteger(IntegerDataType.INT1));

        setServerVersion(ServerVersion.parseVersion(initialHandshakePacket.readString(StringSelfDataType.STRING_TERM, "ASCII")));

        // read connection id
        setThreadId(initialHandshakePacket.readInteger(IntegerDataType.INT4));

        // read auth-plugin-data-part-1 (string[8])
        setSeed(initialHandshakePacket.readString(StringLengthDataType.STRING_FIXED, "ASCII", 8));

        // read filler ([00])
        initialHandshakePacket.readInteger(IntegerDataType.INT1);

        int flags = 0;

        // read capability flags (lower 2 bytes)
        if (initialHandshakePacket.getPosition() < initialHandshakePacket.getPayloadLength()) {
            flags = (int) initialHandshakePacket.readInteger(IntegerDataType.INT2);
        }

        // read character set (1 byte)
        setServerDefaultCollationIndex((int) initialHandshakePacket.readInteger(IntegerDataType.INT1));
        // read status flags (2 bytes)
        setStatusFlags((int) initialHandshakePacket.readInteger(IntegerDataType.INT2));

        // read capability flags (upper 2 bytes)
        flags |= (int) initialHandshakePacket.readInteger(IntegerDataType.INT2) << 16;

        setCapabilityFlags(flags);

        if ((flags & MysqlaServerSession.CLIENT_PLUGIN_AUTH) != 0) {
            // read length of auth-plugin-data (1 byte)
            this.authPluginDataLength = (int) initialHandshakePacket.readInteger(IntegerDataType.INT1);
        } else {
            // read filler ([00])
            initialHandshakePacket.readInteger(IntegerDataType.INT1);
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

    /**
     * 
     * @return Collation index which server provided in handshake greeting packet
     */
    public int getServerDefaultCollationIndex() {
        return this.serverDefaultCollationIndex;
    }

    /**
     * Stores collation index which server provided in handshake greeting packet.
     * 
     * @param serverDefaultCollationIndex
     */
    public void setServerDefaultCollationIndex(int serverDefaultCollationIndex) {
        this.serverDefaultCollationIndex = serverDefaultCollationIndex;
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
