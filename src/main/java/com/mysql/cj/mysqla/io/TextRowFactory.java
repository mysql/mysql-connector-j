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

package com.mysql.cj.mysqla.io;

import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringSelfDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.io.ProtocolEntityFactory;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.api.mysqla.result.Resultset;
import com.mysql.cj.api.mysqla.result.Resultset.Concurrency;
import com.mysql.cj.api.mysqla.result.ResultsetRow;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.io.MysqlTextValueDecoder;
import com.mysql.cj.mysqla.result.ByteArrayRow;
import com.mysql.cj.mysqla.result.TextBufferRow;

public class TextRowFactory extends AbstractRowFactory implements ProtocolEntityFactory<ResultsetRow> {

    protected ColumnDefinition columnDefinition;
    protected Resultset.Concurrency resultSetConcurrency;
    protected boolean canReuseRowPacketForBufferRow;
    protected ReadableProperty<Integer> useBufferRowSizeThreshold;
    protected ExceptionInterceptor exceptionInterceptor;

    public TextRowFactory(MysqlaProtocol protocol, ColumnDefinition columnDefinition, Resultset.Concurrency resultSetConcurrency,
            boolean canReuseRowPacketForBufferRow) {
        this.columnDefinition = columnDefinition;
        this.resultSetConcurrency = resultSetConcurrency;
        this.canReuseRowPacketForBufferRow = canReuseRowPacketForBufferRow;
        this.useBufferRowSizeThreshold = protocol.getPropertySet().getMemorySizeReadableProperty(PropertyDefinitions.PNAME_largeRowSizeThreshold);
        this.exceptionInterceptor = protocol.getExceptionInterceptor();
        this.valueDecoder = new MysqlTextValueDecoder();
    }

    @Override
    public ResultsetRow createFromPacketPayload(PacketPayload rowPacket) {

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
