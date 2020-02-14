/*
 * Copyright (c) 2019, 2020, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.result;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.DataConversionException;
import com.mysql.cj.protocol.a.MysqlTextValueDecoder;
import com.mysql.cj.util.StringUtils;

public abstract class AbstractNumericValueFactory<T> extends DefaultValueFactory<T> {

    public AbstractNumericValueFactory(PropertySet pset) {
        super(pset);
    }

    @Override
    public T createFromBytes(byte[] bytes, int offset, int length, Field f) {
        if (length == 0 && this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).getValue()) {
            return createFromLong(0);
        }

        // TODO: Too expensive to convert from other charset to ASCII here? UTF-8 (e.g.) doesn't need any conversion before being sent to the decoder
        String s = StringUtils.toString(bytes, offset, length, f.getEncoding());
        byte[] newBytes = s.getBytes();

        if (s.contains("e") || s.contains("E") || s.matches("-?\\d*\\.\\d*")) {
            // floating point
            return createFromDouble(MysqlTextValueDecoder.getDouble(newBytes, 0, newBytes.length));
        } else if (s.matches("-?\\d+")) {
            // integer
            if (s.charAt(0) == '-' // TODO shouldn't we check the length as well?
                    || length <= (MysqlTextValueDecoder.MAX_SIGNED_LONG_LEN - 1) && newBytes[0] >= '0' && newBytes[0] <= '8') {
                return createFromLong(MysqlTextValueDecoder.getLong(newBytes, 0, newBytes.length));
            }
            return createFromBigInteger(MysqlTextValueDecoder.getBigInteger(newBytes, 0, newBytes.length));
        }
        throw new DataConversionException(Messages.getString("ResultSet.UnableToInterpretString", new Object[] { s }));
    }

    @Override
    public T createFromYear(long l) {
        return createFromLong(l);
    }
}
