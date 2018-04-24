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

import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.Resultset.Concurrency;
import com.mysql.cj.protocol.ResultsetRow;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.result.ByteArrayRow;
import com.mysql.cj.protocol.a.result.TextBufferRow;

public class TextRowFactory extends AbstractRowFactory implements ProtocolEntityFactory<ResultsetRow, NativePacketPayload> {

    public TextRowFactory(NativeProtocol protocol, ColumnDefinition colDefinition, Resultset.Concurrency resultSetConcurrency,
            boolean canReuseRowPacketForBufferRow) {
        this.columnDefinition = colDefinition;
        this.resultSetConcurrency = resultSetConcurrency;
        this.canReuseRowPacketForBufferRow = canReuseRowPacketForBufferRow;
        this.useBufferRowSizeThreshold = protocol.getPropertySet().getMemorySizeProperty(PropertyDefinitions.PNAME_largeRowSizeThreshold);
        this.exceptionInterceptor = protocol.getExceptionInterceptor();
        this.valueDecoder = new MysqlTextValueDecoder();
    }

    @Override
    public ResultsetRow createFromMessage(NativePacketPayload rowPacket) {

        // use a buffer row for reusable packets (streaming results), blobs and long strings
        // or if we're over the threshold
        boolean useBufferRow = this.canReuseRowPacketForBufferRow || this.columnDefinition.hasLargeFields()
                || rowPacket.getPayloadLength() >= this.useBufferRowSizeThreshold.getValue();

        if (this.resultSetConcurrency == Concurrency.UPDATABLE || !useBufferRow) {
            byte[][] rowBytes = new byte[this.columnDefinition.getFields().length][];

            for (int i = 0; i < this.columnDefinition.getFields().length; i++) {
                rowBytes[i] = rowPacket.readBytes(StringSelfDataType.STRING_LENENC);
            }

            return new ByteArrayRow(rowBytes, this.exceptionInterceptor);
        }

        return new TextBufferRow(rowPacket, this.columnDefinition, this.exceptionInterceptor, this.valueDecoder);
    }

    @Override
    public boolean canReuseRowPacketForBufferRow() {
        return this.canReuseRowPacketForBufferRow;
    }

}
