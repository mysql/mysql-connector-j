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

package com.mysql.cj.interceptors;

import java.util.Properties;
import java.util.function.Supplier;

import com.mysql.cj.MysqlConnection;
import com.mysql.cj.Query;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.ServerSession;

/**
 * Implement this interface to be placed "in between" query execution, so that you can influence it.
 *
 * QueryInterceptors are "chainable" when configured by the user, the results returned by the "current" interceptor will be passed on to the next on in the
 * chain, from left-to-right order, as specified by the user in the driver configuration property "queryInterceptors".
 */
public interface QueryInterceptor {

    /**
     * Called once per connection that wants to use the interceptor
     *
     * The properties are the same ones passed in in the URL or arguments to
     * Driver.connect() or DriverManager.getConnection().
     *
     * @param conn
     *            the connection for which this interceptor is being created
     * @param props
     *            configuration values as passed to the connection. Note that
     *            in order to support javax.sql.DataSources, configuration properties specific
     *            to an interceptor <strong>must</strong> be passed via setURL() on the
     *            DataSource. QueryInterceptor properties are not exposed via
     *            accessor/mutator methods on DataSources.
     * @param log
     *            logger
     * @return {@link QueryInterceptor}
     */
    QueryInterceptor init(MysqlConnection conn, Properties props, Log log);

    /**
     * Called before the given query is going to be sent to the server for processing.
     *
     * Interceptors are free to return a result set (which must implement the
     * interface {@link Resultset}), and if so,
     * the server will not execute the query, and the given result set will be
     * returned to the application instead.
     *
     * This method will be called while the connection-level mutex is held, so
     * it will only be called from one thread at a time.
     *
     * @param sql
     *            the Supplier for SQL representation of the query
     * @param interceptedQuery
     *            the actual {@link Query} instance being intercepted
     * @param <T>
     *            {@link Resultset} object
     *
     * @return a {@link Resultset} that should be returned to the application instead
     *         of results that are created from actual execution of the intercepted
     *         query.
     */
    <T extends Resultset> T preProcess(Supplier<String> sql, Query interceptedQuery);

    /**
     * Called before the given query packet is going to be sent to the server for processing.
     *
     * Interceptors are free to return a PacketPayload, and if so,
     * the server will not execute the query, and the given PacketPayload will be
     * returned to the application instead.
     *
     * This method will be called while the connection-level mutex is held, so
     * it will only be called from one thread at a time.
     *
     * @param queryPacket
     *            original {@link Message}
     * @param <M>
     *            {@link Message} object
     * @return processed {@link Message}
     */
    default <M extends Message> M preProcess(M queryPacket) {
        return null;
    }

    /**
     * Should the driver execute this interceptor only for the
     * "original" top-level query, and not put it in the execution
     * path for queries that may be executed from other interceptors?
     *
     * If an interceptor issues queries using the connection it was created for,
     * and does not return <code>true</code> for this method, it must ensure
     * that it does not cause infinite recursion.
     *
     * @return true if the driver should ensure that this interceptor is only
     *         executed for the top-level "original" query.
     */
    boolean executeTopLevelOnly();

    /**
     * Called by the driver when this extension should release any resources
     * it is holding and cleanup internally before the connection is
     * closed.
     */
    void destroy();

    /**
     * Called after the given query has been sent to the server for processing.
     *
     * Interceptors are free to inspect the "original" result set, and if a
     * different result set is returned by the interceptor, it is used in place
     * of the "original" result set.
     *
     * This method will be called while the connection-level mutex is held, so
     * it will only be called from one thread at a time.
     *
     * @param sql
     *            the Supplier for SQL representation of the query
     * @param interceptedQuery
     *            the actual {@link Query} instance being intercepted
     * @param originalResultSet
     *            a {@link Resultset} created from query execution
     * @param serverSession
     *            {@link ServerSession} object after the query execution
     * @param <T>
     *            {@link Resultset} object
     *
     * @return a {@link Resultset} that should be returned to the application instead
     *         of results that are created from actual execution of the intercepted
     *         query.
     */
    <T extends Resultset> T postProcess(Supplier<String> sql, Query interceptedQuery, T originalResultSet, ServerSession serverSession);

    /**
     * Called after the given query packet has been sent to the server for processing.
     *
     * Interceptors are free to return either a different PacketPayload than the originalResponsePacket or null.
     *
     * This method will be called while the connection-level mutex is held, so
     * it will only be called from one thread at a time.
     *
     * @param queryPacket
     *            query {@link Message}
     * @param originalResponsePacket
     *            response {@link Message}
     * @param <M>
     *            {@link Message} object
     * @return {@link Message}
     */
    default <M extends Message> M postProcess(M queryPacket, M originalResponsePacket) {
        return null;
    }

}
