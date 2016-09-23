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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.LinkedList;
import java.util.regex.Pattern;

import org.junit.Test;

import com.mysql.cj.core.Messages;
import com.mysql.cj.core.conf.IntegerPropertyDefinition;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.conf.ReadableIntegerProperty;

/**
 * Tests for {@link DebugBufferingPacketSender}.
 */
public class DebugBufferingPacketSenderTest extends PacketSenderTestBase {
    @Test
    public void packetPushedToDebugBufferTest() throws IOException {
        LinkedList<StringBuilder> debugBuffer = new LinkedList<StringBuilder>();
        DebugBufferingPacketSender sender = new DebugBufferingPacketSender(getNoopPacketSender(), debugBuffer,
                new ReadableIntegerProperty(new IntegerPropertyDefinition(PropertyDefinitions.PNAME_packetDebugBufferSize, 20,
                        PropertyDefinitions.RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.packetDebugBufferSize"), "3.1.3",
                        PropertyDefinitions.CATEGORY_DEBUGING_PROFILING, 7, 0, Integer.MAX_VALUE)));
        byte packet[] = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7 };
        sender.send(packet, 8, (byte) 0);

        // check that packet was appended to the debug buffer
        String debugText = debugBuffer.get(0).toString();
        System.out.println("Debug text is: " + debugText);
        // simple best-effort to make sure we have something reasonable
        Pattern p = Pattern.compile("Packet payload:.*00 01 02 03 04 05 06 07", Pattern.DOTALL);
        assertTrue(p.matcher(debugText).find());
    }
}
