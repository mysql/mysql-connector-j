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
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.result.DefaultColumnDefinition;
import com.mysql.cj.result.Field;

// We need to merge metadata from COM_STMT_PREPARE and COM_STMT_EXECUTE:
// 1. some field flags do exist in metadata returned by COM_STMT_PREPARE but are missed after COM_STMT_EXECUTE
// 2. COM_STMT_EXECUTE returns metadata with actual field data types, they may mismatch those from COM_STMT_PREPARE
public class MergingColumnDefinitionFactory extends ColumnDefinitionFactory implements ProtocolEntityFactory<ColumnDefinition, NativePacketPayload> {

    public MergingColumnDefinitionFactory(long columnCount, ColumnDefinition columnDefinitionFromCache) {
        super(columnCount, columnDefinitionFromCache);
    }

    @Override
    public boolean mergeColumnDefinitions() {
        return true;
    }

    @Override
    public ColumnDefinition createFromFields(Field[] fields) {
        if (this.columnDefinitionFromCache != null) {
            if (fields.length != this.columnCount) {
                throw ExceptionFactory.createException(WrongArgumentException.class, "Wrong number of ColumnDefinition fields.");
            }
            Field[] f = this.columnDefinitionFromCache.getFields();
            for (int i = 0; i < fields.length; i++) {
                fields[i].setFlags(f[i].getFlags());
            }
        }
        return new DefaultColumnDefinition(fields);
    }

}
