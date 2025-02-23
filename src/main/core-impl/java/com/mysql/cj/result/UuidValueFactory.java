/*
 * Copyright (c) 2025, Oracle and/or its affiliates.
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

package com.mysql.cj.result;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.DataConversionException;
import com.mysql.cj.util.StringUtils;

public class UuidValueFactory extends DefaultValueFactory<UUID> {

    public UuidValueFactory(PropertySet pset) {
        super(pset);
    }

    @Override
    public UUID createFromBytes(byte[] bytes, int offset, int length, Field f) {
        if (f.isBinary()) {
            return getUuidFromBytes(bytes);
        }
        MysqlType mysqlType = f.getMysqlType();
        switch (mysqlType) {
            case CHAR:
            case VARCHAR:
            case TEXT:
            case TINYTEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                String s = StringUtils.toString(bytes, offset, length, f.getEncoding());
                try {
                    return UUID.fromString(s);
                } catch (IllegalArgumentException e) {
                    throw new DataConversionException(Messages.getString("ResultSet.UnableToConvertString", new Object[] { s, getTargetTypeName() }));
                }
            default:
                break;
        }
        throw new DataConversionException(Messages.getString("ResultSet.UnsupportedConversion", new Object[] { mysqlType.name(), getTargetTypeName() }));
    }

    @Override
    public String getTargetTypeName() {
        return UUID.class.getName();
    }

    private static UUID getUuidFromBytes(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long high = bb.getLong();
        long low = bb.getLong();

        return new UUID(high, low);
    }

}
