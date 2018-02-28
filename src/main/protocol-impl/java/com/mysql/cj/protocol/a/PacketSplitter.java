/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.protocol.a;

/**
 * Keep track of splitting a large packet into multi-packets segments.
 */
public class PacketSplitter {
    private int totalSize;
    private int currentPacketLen = 0;
    private int offset = 0;

    public PacketSplitter(int totalSize) {
        this.totalSize = totalSize;
    }

    public int getPacketLen() {
        return this.currentPacketLen;
    }

    public int getOffset() {
        return this.offset;
    }

    public boolean nextPacket() {
        this.offset += this.currentPacketLen;
        // need a zero-len packet if final packet len is MAX_PACKET_SIZE
        if (this.currentPacketLen == NativeConstants.MAX_PACKET_SIZE && this.offset == this.totalSize) {
            this.currentPacketLen = 0;
            return true;
        }

        // allow empty packets
        if (this.totalSize == 0) {
            this.totalSize = -1; // to return `false' next iteration
            return true;
        }

        this.currentPacketLen = this.totalSize - this.offset;
        if (this.currentPacketLen > NativeConstants.MAX_PACKET_SIZE) {
            this.currentPacketLen = NativeConstants.MAX_PACKET_SIZE;
        }
        return this.offset < this.totalSize;
    }
}
