/*
  Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api.mysqla.io;

import java.io.IOException;
import java.util.Optional;

public interface PacketReader {

    /**
     * Read MySQL packet header from input stream.
     * 
     * @return
     * @throws IOException
     */
    PacketHeader readHeader() throws IOException;

    /**
     * Read MySQL packet payload from input stream into to the given {@link PacketPayload} instance or into the new one if not present.
     * 
     * @param reuse
     *            {@link PacketPayload} to reuse
     * @param packetLength
     *            Expected length of packet
     * @return
     * @throws IOException
     */
    PacketPayload readPayload(Optional<PacketPayload> reuse, int packetLength) throws IOException;

    /**
     * Get last packet sequence number, as it was stored by {@link #readHeader(byte[], boolean)}.
     * 
     * @return
     */
    byte getPacketSequence();

    /**
     * Set stored packet sequence number to 0.
     */
    void resetPacketSequence();

    /**
     * Return a PacketReader instance free of decorators.
     * 
     * @return
     */
    PacketReader undecorateAll();

    /**
     * Return the previous PacketReader instance from the decorators chain or the current PacketReader
     * if it is the first entry in a chain.
     * 
     * @return
     */
    PacketReader undecorate();
}
