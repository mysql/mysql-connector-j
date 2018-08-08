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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.LinkedList;
import java.util.regex.Pattern;

import org.junit.Test;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.IntegerProperty;
import com.mysql.cj.conf.IntegerPropertyDefinition;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyKey;

/**
 * Tests for {@link DebugBufferingPacketSender}.
 */
public class DebugBufferingPacketSenderTest extends PacketSenderTestBase {
    @Test
    public void packetPushedToDebugBufferTest() throws IOException {
        LinkedList<StringBuilder> debugBuffer = new LinkedList<>();
        DebugBufferingPacketSender sender = new DebugBufferingPacketSender(getNoopPacketSender(), debugBuffer,
                new IntegerProperty(new IntegerPropertyDefinition(PropertyKey.packetDebugBufferSize, 20, PropertyDefinitions.RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.packetDebugBufferSize"), "3.1.3", PropertyDefinitions.CATEGORY_DEBUGING_PROFILING, 7, 0,
                        Integer.MAX_VALUE)));
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
