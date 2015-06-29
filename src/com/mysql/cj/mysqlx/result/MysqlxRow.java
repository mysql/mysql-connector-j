/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

package com.mysql.cj.mysqlx.result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;

import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.Row;
// import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.RowFieldBytes;
// import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.RowFieldDatetime;
// import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.RowFieldDecimal;
// import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.RowFieldDouble;
// import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.RowFieldFloat;
// import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.RowFieldSignedInt;
// import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.RowFieldUnsignedInt;
// import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.RowFieldTime;

import com.mysql.cj.api.io.ValueFactory;
import com.mysql.cj.jdbc.Field;
import com.mysql.cj.mysqla.MysqlaConstants;

/**
 * @todo
 */
public class MysqlxRow implements com.mysql.cj.api.result.Row {
    /**
     * Mapping of MySQL type constant to the MySQL-X message parser for that type. This map does not contain a mapping for ALL of the types as some require
     * additional logic to make a decision for which type of message to parse.
     * @see {@link someMethodThatImplementsIt()}
     * @todo fix that link ^
     */
    private static final Map<Integer, Parser<? extends GeneratedMessage>> mysqlTypeToParser = new HashMap<>();
    static {
        // mysqlTypeToParser.put(MysqlaConstants.FIELD_TYPE_BIT, RowFieldUnsignedInt.getDefaultInstance().getParserForType());
        // mysqlTypeToParser.put(MysqlaConstants.FIELD_TYPE_DOUBLE, RowFieldDouble.getDefaultInstance().getParserForType());
        // mysqlTypeToParser.put(MysqlaConstants.FIELD_TYPE_DATETIME, RowFieldDatetime.getDefaultInstance().getParserForType());
        // mysqlTypeToParser.put(MysqlaConstants.FIELD_TYPE_DECIMAL, RowFieldDecimal.getDefaultInstance().getParserForType());
        // mysqlTypeToParser.put(MysqlaConstants.FIELD_TYPE_FLOAT, RowFieldFloat.getDefaultInstance().getParserForType());
        // mysqlTypeToParser.put(MysqlaConstants.FIELD_TYPE_GEOMETRY, RowFieldBytes.getDefaultInstance().getParserForType());
        // mysqlTypeToParser.put(MysqlaConstants.FIELD_TYPE_JSON, RowFieldBytes.getDefaultInstance().getParserForType());
        // // TODO: the spec says SET -> RowFieldBytes, but a RowFieldSet still exists
        // mysqlTypeToParser.put(MysqlaConstants.FIELD_TYPE_SET, RowFieldBytes.getDefaultInstance().getParserForType());
        // mysqlTypeToParser.put(MysqlaConstants.FIELD_TYPE_TIME, RowFieldTime.getDefaultInstance().getParserForType());
        // mysqlTypeToParser.put(MysqlaConstants.FIELD_TYPE_VARCHAR, RowFieldBytes.getDefaultInstance().getParserForType());
    }
    private ArrayList<Field> metadata;
    private Row rowMessage;

    public MysqlxRow(ArrayList<Field> metadata, Row rowMessage) {
        this.metadata = metadata;
        this.rowMessage = rowMessage;
    }

    private static Parser<? extends GeneratedMessage> getParser(Field f) {
        Parser<? extends GeneratedMessage> parser = mysqlTypeToParser.get(f.getMysqlType());
        if (parser == null) {
            if (f.getMysqlType() == MysqlaConstants.FIELD_TYPE_LONG) {
                if (f.isUnsigned()) {
                    // TODO: wait until Alfredo implements this
                    //return RowFieldUnsignedInt.getDefaultInstance().getParserForType();
                } else {
                    // TODO: wait until Alfredo implements this
                    //return RowFieldSignedInt.getDefaultInstance().getParserForType();
                }
            } else {
                // TODO: throw exception
                throw new NullPointerException("TODO: unknown type");
            }
        }
        return parser;
    }

    public <T> T getValue(int columnIndex, ValueFactory<T> vf) {
        // TODO: check BYTES for 0-length, then null
        // TODO: decode message in BYTES (type specific)
        // TODO: transform decoded message to type via valuefactory
        throw new NullPointerException("TODO");
    }

    public boolean getNull(int columnIndex) {
        throw new NullPointerException("TODO");
    }

    public boolean wasNull() {
        throw new NullPointerException("TODO");
    }
}
