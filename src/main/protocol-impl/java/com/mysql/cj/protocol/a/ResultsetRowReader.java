/*
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
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

import java.io.IOException;
import java.util.Optional;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.ProtocolEntityReader;
import com.mysql.cj.protocol.ResultsetRow;

public class ResultsetRowReader implements ProtocolEntityReader<ResultsetRow, NativePacketPayload> {

    protected NativeProtocol protocol;

    protected PropertySet propertySet;

    protected RuntimeProperty<Integer> useBufferRowSizeThreshold;

    public ResultsetRowReader(NativeProtocol prot) {
        this.protocol = prot;

        this.propertySet = this.protocol.getPropertySet();
        this.useBufferRowSizeThreshold = this.propertySet.getMemorySizeProperty(PropertyKey.largeRowSizeThreshold);
    }

    /**
     * Retrieve one row from the MySQL server. Note: this method is not
     * thread-safe, but it is only called from methods that are guarded by
     * synchronizing on this object.
     * 
     * @param sf
     *            ProtocolEntityFactory
     * @throws IOException
     *             if an error occurs
     */
    @Override
    public ResultsetRow read(ProtocolEntityFactory<ResultsetRow, NativePacketPayload> sf) throws IOException {
        AbstractRowFactory rf = (AbstractRowFactory) sf;
        NativePacketPayload rowPacket = null;
        NativePacketHeader hdr = this.protocol.getPacketReader().readHeader();

        // read the entire packet(s)
        rowPacket = this.protocol.getPacketReader()
                .readMessage(rf.canReuseRowPacketForBufferRow() ? Optional.ofNullable(this.protocol.getReusablePacket()) : Optional.empty(), hdr);
        this.protocol.checkErrorMessage(rowPacket);
        // Didn't read an error, so re-position to beginning of packet in order to read result set data
        rowPacket.setPosition(rowPacket.getPosition() - 1);

        // exit early with null if there's an EOF packet
        if (!this.protocol.getServerSession().isEOFDeprecated() && rowPacket.isEOFPacket()
                || this.protocol.getServerSession().isEOFDeprecated() && rowPacket.isResultSetOKPacket()) {
            this.protocol.readServerStatusForResultSets(rowPacket, true);
            return null;
        }

        return sf.createFromMessage(rowPacket);
    }

}
