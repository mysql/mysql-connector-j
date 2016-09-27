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

import static org.junit.Assert.assertEquals;

import com.mysql.cj.api.mysqla.io.PacketSender;

/**
 * Common functionality for packet sender tests.
 */
public class PacketSenderTestBase {
    /**
     * Get a no-op packet sender that can be used when testing decorators.
     */
    protected PacketSender getNoopPacketSender() {
        return new PacketSender() {
            public void send(byte[] packet, int packetLen, byte packetSequence) throws java.io.IOException {
                // no-op
            }

            @Override
            public PacketSender undecorateAll() {
                return this;
            }

            @Override
            public PacketSender undecorate() {
                return this;
            }
        };
    }

    protected void fillPacketSequentially(byte[] packet) {
        for (int i = 0; i < packet.length; ++i) {
            packet[i] = (byte) i;
        }
    }

    protected void checkSequentiallyFilledPacket(byte[] packet, int offset, int len) {
        for (int i = 0; i < len; ++i) {
            assertEquals((byte) i, packet[offset + i]);
        }
    }
}
