/*
 * Copyright (c) 2009, 2023, Oracle and/or its affiliates.
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

import java.util.Properties;
import java.util.function.Supplier;

import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.ServerSession;

/**
 * Wraps query interceptors during driver startup so that they don't produce different result sets than we expect.
 */
public class NoSubInterceptorWrapper implements QueryInterceptor {

    private final QueryInterceptor underlyingInterceptor;

    public NoSubInterceptorWrapper(QueryInterceptor underlyingInterceptor) {
        if (underlyingInterceptor == null) {
            throw new RuntimeException(Messages.getString("NoSubInterceptorWrapper.0"));
        }

        this.underlyingInterceptor = underlyingInterceptor;
    }

    @Override
    public void destroy() {
        this.underlyingInterceptor.destroy();
    }

    @Override
    public boolean executeTopLevelOnly() {
        return this.underlyingInterceptor.executeTopLevelOnly();
    }

    @Override
    public QueryInterceptor init(MysqlConnection conn, Properties props, Log log) {
        this.underlyingInterceptor.init(conn, props, log);
        return this;
    }

    @Override
    public <T extends Resultset> T postProcess(Supplier<String> sql, Query interceptedQuery, T originalResultSet, ServerSession serverSession) {
        this.underlyingInterceptor.postProcess(sql, interceptedQuery, originalResultSet, serverSession);

        return null; // don't allow result set substitution
    }

    @Override
    public <T extends Resultset> T preProcess(Supplier<String> sql, Query interceptedQuery) {
        this.underlyingInterceptor.preProcess(sql, interceptedQuery);

        return null; // don't allow result set substitution
    }

    @Override
    public <M extends Message> M preProcess(M queryPacket) {
        this.underlyingInterceptor.preProcess(queryPacket);

        return null; // don't allow PacketPayload substitution
    }

    @Override
    public <M extends Message> M postProcess(M queryPacket, M originalResponsePacket) {
        this.underlyingInterceptor.postProcess(queryPacket, originalResponsePacket);

        return null; // don't allow PacketPayload substitution
    }

    public QueryInterceptor getUnderlyingInterceptor() {
        return this.underlyingInterceptor;
    }

}
