/*
 * Copyright (c) 2016, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.protocol.a;

import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.ProtocolEntity;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.Resultset.Concurrency;
import com.mysql.cj.protocol.Resultset.Type;
import com.mysql.cj.protocol.ResultsetRows;
import com.mysql.cj.protocol.a.result.NativeResultset;
import com.mysql.cj.protocol.a.result.OkPacket;

public class ResultsetFactory implements ProtocolEntityFactory<Resultset, NativePacketPayload> {

    private Type type = Type.FORWARD_ONLY;
    private Concurrency concurrency = Concurrency.READ_ONLY;

    public ResultsetFactory(Type type, Concurrency concurrency) {
        this.type = type;
        this.concurrency = concurrency;
    }

    @Override
    public Resultset.Type getResultSetType() {
        return this.type;
    }

    @Override
    public Resultset.Concurrency getResultSetConcurrency() {
        return this.concurrency;
    }

    @Override
    public Resultset createFromProtocolEntity(ProtocolEntity protocolEntity) {
        if (protocolEntity instanceof OkPacket) {
            return new NativeResultset((OkPacket) protocolEntity);

        } else if (protocolEntity instanceof ResultsetRows) {
            return new NativeResultset((ResultsetRows) protocolEntity);

        }
        throw ExceptionFactory.createException(WrongArgumentException.class, "Unknown ProtocolEntity class " + protocolEntity);
    }

}
