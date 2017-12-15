/*
 * Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.mysqla.io;

import java.io.IOException;
import java.util.ArrayList;

import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringSelfDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.io.ProtocolEntityFactory;
import com.mysql.cj.api.mysqla.io.ProtocolEntityReader;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.api.mysqla.result.Resultset;
import com.mysql.cj.api.mysqla.result.Resultset.Type;
import com.mysql.cj.api.mysqla.result.ResultsetRow;
import com.mysql.cj.api.mysqla.result.ResultsetRows;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.mysqla.result.OkPacket;
import com.mysql.cj.mysqla.result.ResultsetRowsCursor;
import com.mysql.cj.mysqla.result.ResultsetRowsStatic;
import com.mysql.cj.mysqla.result.ResultsetRowsStreaming;

public class BinaryResultsetReader implements ProtocolEntityReader<Resultset> {

    protected MysqlaProtocol protocol;

    public BinaryResultsetReader(MysqlaProtocol prot) {
        this.protocol = prot;
    }

    @Override
    public Resultset read(int maxRows, boolean streamResults, PacketPayload resultPacket, ColumnDefinition metadata,
            ProtocolEntityFactory<Resultset> resultSetFactory) throws IOException {

        Resultset rs = null;
        //try {
        long columnCount = resultPacket.readInteger(IntegerDataType.INT_LENENC);

        if (columnCount > 0) {
            // Build a result set with rows.

            // Read in the column information
            ColumnDefinition cdef = this.protocol.read(ColumnDefinition.class, new MergingColumnDefinitionFactory(columnCount, metadata));

            boolean isCursorPosible = this.protocol.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useCursorFetch).getValue()
                    && resultSetFactory.getResultSetType() == Type.FORWARD_ONLY && resultSetFactory.getFetchSize() > 0;

            // There is no EOF packet after fields when CLIENT_DEPRECATE_EOF is set;
            // if we asked to use cursor then there should be an OK packet here
            if (isCursorPosible || !this.protocol.getServerSession().isEOFDeprecated()) {
                this.protocol.readServerStatusForResultSets(this.protocol.readPacket(this.protocol.getReusablePacket()), true);
            }

            ResultsetRows rows = null;

            if (isCursorPosible && this.protocol.getServerSession().cursorExists()) {
                rows = new ResultsetRowsCursor(this.protocol, cdef);

            } else if (!streamResults) {
                BinaryRowFactory brf = new BinaryRowFactory(this.protocol, cdef, resultSetFactory.getResultSetConcurrency(), false);

                ArrayList<ResultsetRow> rowList = new ArrayList<ResultsetRow>();
                ResultsetRow row = this.protocol.read(ResultsetRow.class, brf);
                while (row != null) {
                    if ((maxRows == -1) || (rowList.size() < maxRows)) {
                        rowList.add(row);
                    }
                    row = this.protocol.read(ResultsetRow.class, brf);
                }

                rows = new ResultsetRowsStatic(rowList, cdef);

            } else {
                rows = new ResultsetRowsStreaming<Resultset>(this.protocol, cdef, true, resultSetFactory);
                this.protocol.setStreamingData(rows);
            }

            /*
             * Build ResultSet from ResultsetRows
             */
            rs = resultSetFactory.createFromProtocolEntity(rows);

        } else {
            // check for file request
            if (columnCount == PacketPayload.NULL_LENGTH) {
                String charEncoding = this.protocol.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue();
                String fileName = resultPacket.readString(StringSelfDataType.STRING_TERM, this.protocol.doesPlatformDbCharsetMatches() ? charEncoding : null);
                resultPacket = this.protocol.sendFileToServer(fileName);
            }

            /*
             * Build ResultSet with no ResultsetRows
             */

            // read and parse OK packet
            OkPacket ok = this.protocol.readServerStatusForResultSets(resultPacket, false); // oldStatus set in sendCommand()

            rs = resultSetFactory.createFromProtocolEntity(ok);
        }
        return rs;

    }
}
