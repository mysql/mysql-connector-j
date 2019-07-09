/*
 * Copyright (c) 2019, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.xdevapi;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import com.mysql.cj.MysqlxSession;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ProtocolEntity;
import com.mysql.cj.protocol.ResultBuilder;
import com.mysql.cj.protocol.x.FetchDoneEntity;
import com.mysql.cj.protocol.x.Notice;
import com.mysql.cj.protocol.x.StatementExecuteOk;
import com.mysql.cj.protocol.x.StatementExecuteOkBuilder;
import com.mysql.cj.result.BufferedRowList;
import com.mysql.cj.result.DefaultColumnDefinition;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.Row;

/**
 * Result builder producing a {@link RowResult} instance.
 */
public class RowResultBuilder implements ResultBuilder<RowResult> {
    private ArrayList<Field> fields = new ArrayList<>();
    private ColumnDefinition metadata;
    private List<Row> rows = new ArrayList<>();
    private RowResult result;

    TimeZone defaultTimeZone;
    PropertySet pset;
    private StatementExecuteOkBuilder statementExecuteOkBuilder = new StatementExecuteOkBuilder();

    public RowResultBuilder(MysqlxSession sess) {
        this.defaultTimeZone = sess.getServerSession().getDefaultTimeZone();
        this.pset = sess.getPropertySet();
    }

    @Override
    public boolean addProtocolEntity(ProtocolEntity entity) {
        if (entity instanceof Field) {
            this.fields.add((Field) entity);
            return false;

        } else if (entity instanceof Row) {
            if (this.metadata == null) {
                this.metadata = new DefaultColumnDefinition(this.fields.toArray(new Field[] {}));
            }
            this.rows.add(((Row) entity).setMetadata(this.metadata));
            return false;

        } else if (entity instanceof Notice) {
            this.statementExecuteOkBuilder.addProtocolEntity(entity);
            return false;

        } else if (entity instanceof FetchDoneEntity) {
            return false;

        } else if (entity instanceof StatementExecuteOk) {
            return true;
        }
        throw ExceptionFactory.createException(WrongArgumentException.class, "Unexpected protocol entity " + entity);
    }

    @Override
    public RowResult build() {
        if (this.metadata == null) {
            this.metadata = new DefaultColumnDefinition(this.fields.toArray(new Field[] {}));
        }
        this.result = new RowResultImpl(this.metadata, this.defaultTimeZone, new BufferedRowList(this.rows), () -> this.statementExecuteOkBuilder.build(),
                this.pset);
        return this.result;
    }
}
