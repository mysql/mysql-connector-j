/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

import com.mysql.cj.mysqla.MysqlaConstants;

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
        if (this.currentPacketLen == MysqlaConstants.MAX_PACKET_SIZE && this.offset == this.totalSize) {
            this.currentPacketLen = 0;
            return true;
        }

        // allow empty packets
        if (this.totalSize == 0) {
            this.totalSize = -1; // to return `false' next iteration
            return true;
        }

        this.currentPacketLen = this.totalSize - this.offset;
        if (this.currentPacketLen > MysqlaConstants.MAX_PACKET_SIZE) {
            this.currentPacketLen = MysqlaConstants.MAX_PACKET_SIZE;
        }
        return this.offset < this.totalSize;
    }
}
