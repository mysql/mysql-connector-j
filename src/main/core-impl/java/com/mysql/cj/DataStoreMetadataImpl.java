/*
 * Copyright (c) 2018, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.mysql.cj.result.LongValueFactory;
import com.mysql.cj.xdevapi.ExprUnparser;

public class DataStoreMetadataImpl implements DataStoreMetadata {

    private Session session;

    public DataStoreMetadataImpl(Session sess) {
        this.session = sess;
    }

    public boolean schemaExists(String schemaName) {
        StringBuilder stmt = new StringBuilder("select count(*) from information_schema.schemata where schema_name = '");
        // TODO: verify quoting rules
        stmt.append(schemaName.replaceAll("'", "\\'"));
        stmt.append("'");

        Function<com.mysql.cj.result.Row, Long> rowToLong = r -> r.getValue(0, new LongValueFactory(this.session.getPropertySet()));
        List<Long> counters = this.session.query(this.session.getMessageBuilder().buildSqlStatement(stmt.toString()), null, rowToLong, Collectors.toList());
        return 1 == counters.get(0);
    }

    public boolean tableExists(String schemaName, String tableName) {
        StringBuilder stmt = new StringBuilder("select count(*) from information_schema.tables where table_schema = '");
        // TODO: verify quoting rules
        stmt.append(schemaName.replaceAll("'", "\\'"));
        stmt.append("' and table_name = '");
        stmt.append(tableName.replaceAll("'", "\\'"));
        stmt.append("'");

        Function<com.mysql.cj.result.Row, Long> rowToLong = r -> r.getValue(0, new LongValueFactory(this.session.getPropertySet()));
        List<Long> counters = this.session.query(this.session.getMessageBuilder().buildSqlStatement(stmt.toString()), null, rowToLong, Collectors.toList());
        return 1 == counters.get(0);
    }

    @Override
    public long getTableRowCount(String schemaName, String tableName) {
        StringBuilder stmt = new StringBuilder("select count(*) from ");
        stmt.append(ExprUnparser.quoteIdentifier(schemaName));
        stmt.append(".");
        stmt.append(ExprUnparser.quoteIdentifier(tableName));

        Function<com.mysql.cj.result.Row, Long> rowToLong = r -> r.getValue(0, new LongValueFactory(this.session.getPropertySet()));
        List<Long> counters = this.session.query(this.session.getMessageBuilder().buildSqlStatement(stmt.toString()), null, rowToLong, Collectors.toList());
        return counters.get(0);
    }

}
