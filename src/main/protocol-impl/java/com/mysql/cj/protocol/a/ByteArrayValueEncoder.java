/*
 * Copyright (c) 2022, 2023, Oracle and/or its affiliates.
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

import java.nio.charset.StandardCharsets;

import com.mysql.cj.BindValue;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.util.StringUtils;

public class ByteArrayValueEncoder extends AbstractValueEncoder {

    protected RuntimeProperty<Integer> maxByteArrayAsHex;

    @Override
    public void init(PropertySet pset, ServerSession serverSess, ExceptionInterceptor excInterceptor) {
        super.init(pset, serverSess, excInterceptor);

        this.maxByteArrayAsHex = pset.getIntegerProperty(PropertyKey.maxByteArrayAsHex);
    }

    @Override
    public byte[] getBytes(BindValue binding) {
        if (binding.escapeBytesIfNeeded()) {
            return escapeBytesIfNeeded((byte[]) binding.getValue());
        }
        return (byte[]) binding.getValue();
    }

    @Override
    public String getString(BindValue binding) {
        if (binding.escapeBytesIfNeeded() && binding.getBinaryLength() <= this.maxByteArrayAsHex.getValue()) {
            return StringUtils.toString(escapeBytesIfNeeded((byte[]) binding.getValue()), StandardCharsets.US_ASCII);
        }
        return "** BYTE ARRAY DATA **";
    }

    @Override
    public void encodeAsBinary(Message msg, BindValue binding) {
        ((NativePacketPayload) msg).writeBytes(StringSelfDataType.STRING_LENENC, (byte[]) binding.getValue());
    }

}
