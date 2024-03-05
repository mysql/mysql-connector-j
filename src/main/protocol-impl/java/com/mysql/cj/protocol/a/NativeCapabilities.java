/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.protocol.a;

import com.mysql.cj.Messages;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.UnableToConnectException;
import com.mysql.cj.protocol.ServerCapabilities;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;

public class NativeCapabilities implements ServerCapabilities {

    private NativePacketPayload initialHandshakePacket;

    private byte protocolVersion = 0;
    private ServerVersion serverVersion;
    private long threadId = -1;
    private String seed;
    private int capabilityFlags;
    private int serverDefaultCollationIndex;
    private int statusFlags = 0;
    private int authPluginDataLength = 0;
    private boolean serverHasFracSecsSupport = true;

    public NativeCapabilities(NativePacketPayload initialHandshakePacket) {
        this.initialHandshakePacket = initialHandshakePacket;

        // Get the protocol version
        this.protocolVersion = (byte) initialHandshakePacket.readInteger(IntegerDataType.INT1);

        try {
            this.serverVersion = ServerVersion.parseVersion(initialHandshakePacket.readString(StringSelfDataType.STRING_TERM, "ASCII"));

            // read connection id
            this.threadId = initialHandshakePacket.readInteger(IntegerDataType.INT4);

            // read auth-plugin-data-part-1 (string[8])
            this.seed = initialHandshakePacket.readString(StringLengthDataType.STRING_FIXED, "ASCII", 8);

            // read filler ([00])
            initialHandshakePacket.readInteger(IntegerDataType.INT1);

            int flags = 0;

            // read capability flags (lower 2 bytes)
            if (initialHandshakePacket.getPosition() < initialHandshakePacket.getPayloadLength()) {
                flags = (int) initialHandshakePacket.readInteger(IntegerDataType.INT2);
            }

            // read character set (1 byte)
            this.serverDefaultCollationIndex = (int) initialHandshakePacket.readInteger(IntegerDataType.INT1);
            // read status flags (2 bytes)
            this.statusFlags = (int) initialHandshakePacket.readInteger(IntegerDataType.INT2);

            // read capability flags (upper 2 bytes)
            flags |= (int) initialHandshakePacket.readInteger(IntegerDataType.INT2) << 16;

            setCapabilityFlags(flags);

            if ((flags & NativeServerSession.CLIENT_PLUGIN_AUTH) != 0) {
                // read length of auth-plugin-data (1 byte)
                this.authPluginDataLength = (int) initialHandshakePacket.readInteger(IntegerDataType.INT1);
            } else {
                // read filler ([00])
                initialHandshakePacket.readInteger(IntegerDataType.INT1);
            }
            // next 10 bytes are reserved (all [00])
            initialHandshakePacket.setPosition(initialHandshakePacket.getPosition() + 10);

            this.serverHasFracSecsSupport = this.serverVersion.meetsMinimum(new ServerVersion(5, 6, 4));
        } catch (Throwable t) {
            // Chances are that the other end is talking X Protocol instead of MySQL protocol.
            // X Protocol message type byte (NOTICE = 11) coincides with MySQL protocol version byte in the Initial Handshake Packet.
            if (this.protocolVersion == 11 && IndexOutOfBoundsException.class.isAssignableFrom(t.getClass())) {
                throw ExceptionFactory.createException(UnableToConnectException.class,
                        Messages.getString("NativeCapabilites.001", new Object[] { this.protocolVersion }));
            }

            throw t;
        }
    }

    public NativePacketPayload getInitialHandshakePacket() {
        return this.initialHandshakePacket;
    }

    @Override
    public int getCapabilityFlags() {
        return this.capabilityFlags;
    }

    @Override
    public void setCapabilityFlags(int capabilityFlags) {
        this.capabilityFlags = capabilityFlags;
    }

    @Override
    public ServerVersion getServerVersion() {
        return this.serverVersion;
    }

    @Override
    public long getThreadId() {
        return this.threadId;
    }

    @Override
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    public String getSeed() {
        return this.seed;
    }

    /**
     *
     * @return Collation index which server provided in handshake greeting packet
     */
    @Override
    public int getServerDefaultCollationIndex() {
        return this.serverDefaultCollationIndex;
    }

    public int getStatusFlags() {
        return this.statusFlags;
    }

    public int getAuthPluginDataLength() {
        return this.authPluginDataLength;
    }

    @Override
    public boolean serverSupportsFracSecs() {
        return this.serverHasFracSecsSupport;
    }

}
