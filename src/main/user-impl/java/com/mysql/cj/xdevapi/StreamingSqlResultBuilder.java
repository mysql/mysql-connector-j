/*
 * Copyright (c) 2019, 2020, Oracle and/or its affiliates.
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.mysql.cj.MysqlxSession;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ProtocolEntity;
import com.mysql.cj.protocol.ResultBuilder;
import com.mysql.cj.protocol.x.Notice;
import com.mysql.cj.protocol.x.StatementExecuteOk;
import com.mysql.cj.protocol.x.StatementExecuteOkBuilder;
import com.mysql.cj.protocol.x.XProtocol;
import com.mysql.cj.protocol.x.XProtocolRowInputStream;
import com.mysql.cj.result.Field;

/**
 * Result builder producing a streaming {@link SqlResult} instance.
 */
public class StreamingSqlResultBuilder implements ResultBuilder<SqlResult> {
    TimeZone defaultTimeZone;
    PropertySet pset;
    XProtocol protocol;

    StatementExecuteOkBuilder statementExecuteOkBuilder = new StatementExecuteOkBuilder();
    boolean isRowResult = false;
    ProtocolEntity lastEntity = null;

    List<SqlSingleResult> resultSets = new ArrayList<>();
    private SqlResult result;

    public StreamingSqlResultBuilder(MysqlxSession sess) {
        this.defaultTimeZone = sess.getServerSession().getDefaultTimeZone();
        this.pset = sess.getPropertySet();
        this.protocol = sess.getProtocol();
    }

    @Override
    public boolean addProtocolEntity(ProtocolEntity entity) {

        if (entity instanceof Notice) {
            this.statementExecuteOkBuilder.addProtocolEntity(entity);
        } else {
            this.lastEntity = entity;
        }

        AtomicBoolean readLastResult = new AtomicBoolean(false);
        Supplier<ProtocolEntity> okReader = () -> {
            if (readLastResult.get()) {
                throw new CJCommunicationsException("Invalid state attempting to read ok packet");
            }
            if (this.protocol.hasMoreResults()) {
                StatementExecuteOk res = this.statementExecuteOkBuilder.build();
                this.statementExecuteOkBuilder = new StatementExecuteOkBuilder();
                return res;
            }
            readLastResult.set(true);
            return this.protocol.readQueryResult(this.statementExecuteOkBuilder);
        };
        Supplier<SqlResult> resultStream = () -> {
            if (readLastResult.get()) {
                return null;
            } else if (this.lastEntity != null && this.lastEntity instanceof Field || this.protocol.isSqlResultPending()) {
                ColumnDefinition cd;
                if (this.lastEntity != null && this.lastEntity instanceof Field) {
                    cd = this.protocol.readMetadata((Field) this.lastEntity, (n) -> {
                        this.statementExecuteOkBuilder.addProtocolEntity(n);
                    });
                    this.lastEntity = null;
                } else {
                    cd = this.protocol.readMetadata();
                }
                return new SqlSingleResult(cd, this.protocol.getServerSession().getDefaultTimeZone(), new XProtocolRowInputStream(cd, this.protocol, (n) -> {
                    this.statementExecuteOkBuilder.addProtocolEntity(n);
                }), okReader, this.pset);
            } else {
                readLastResult.set(true);
                SqlResultBuilder rb = new SqlResultBuilder(this.defaultTimeZone, this.pset);
                rb.addProtocolEntity(entity);
                return this.protocol.readQueryResult(rb);
            }
        };
        this.result = new SqlMultiResult(resultStream);

        return true;
    }

    @Override
    public SqlResult build() {
        return this.result;
    }
}
