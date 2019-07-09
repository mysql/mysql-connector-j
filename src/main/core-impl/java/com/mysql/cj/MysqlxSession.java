/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
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

import java.io.IOException;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.ResultBuilder;
import com.mysql.cj.protocol.x.StatementExecuteOkBuilder;
import com.mysql.cj.protocol.x.XProtocol;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.protocol.x.XProtocolRowInputStream;
import com.mysql.cj.result.Row;
import com.mysql.cj.xdevapi.PreparableStatement;

public class MysqlxSession extends CoreSession {

    public MysqlxSession(HostInfo hostInfo, PropertySet propSet) {
        super(hostInfo, propSet);

        // create protocol instance
        this.protocol = new XProtocol(hostInfo, propSet);

        this.messageBuilder = this.protocol.getMessageBuilder();

        this.protocol.connect(hostInfo.getUser(), hostInfo.getPassword(), hostInfo.getDatabase());
    }

    public MysqlxSession(XProtocol prot) {
        super(null, prot.getPropertySet());
        this.protocol = prot;
        this.messageBuilder = this.protocol.getMessageBuilder();
    }

    @Override
    public String getProcessHost() {
        return this.protocol.getSocketConnection().getHost();
    }

    public int getPort() {
        return this.protocol.getSocketConnection().getPort();
    }

    public XProtocol getProtocol() {
        return (XProtocol) this.protocol;
    }

    @Override
    public void quit() {
        try {
            this.protocol.close();
        } catch (IOException ex) {
            throw new CJCommunicationsException(ex);
        }
        super.quit();
    }

    public boolean isClosed() {
        return !((XProtocol) this.protocol).isOpen();
    }

    /**
     * Check if current session is using a MySQL server that supports prepared statements.
     * 
     * @return
     *         {@code true} if the MySQL server in use supports prepared statements
     */
    public boolean supportsPreparedStatements() {
        return ((XProtocol) this.protocol).supportsPreparedStatements();
    }

    /**
     * Check if enough statements were executed in the underlying MySQL server so that another prepare statement attempt should be done.
     * 
     * @return
     *         {@code true} if enough executions have been done since last time a prepared statement failed to be prepared
     */
    public boolean readyForPreparingStatements() {
        return ((XProtocol) this.protocol).readyForPreparingStatements();
    }

    /**
     * Return an id to be used as a client-managed prepared statement id.
     * 
     * @param preparableStatement
     *            {@link PreparableStatement}
     * @return a new identifier to be used as prepared statement id
     */
    public int getNewPreparedStatementId(PreparableStatement<?> preparableStatement) {
        return ((XProtocol) this.protocol).getNewPreparedStatementId(preparableStatement);
    }

    /**
     * Free a prepared statement id so that it can be reused.
     * 
     * @param preparedStatementId
     *            the prepared statement id to release
     */
    public void freePreparedStatementId(int preparedStatementId) {
        ((XProtocol) this.protocol).freePreparedStatementId(preparedStatementId);
    }

    /**
     * Propagate to the underlying protocol instance that preparing a statement on the connected server failed.
     * 
     * @param preparedStatementId
     *            the id of the prepared statement that failed to be prepared
     * @param e
     *            {@link XProtocolError}
     * @return
     *         {@code true} if the exception was properly handled
     */
    public boolean failedPreparingStatement(int preparedStatementId, XProtocolError e) {
        return ((XProtocol) this.protocol).failedPreparingStatement(preparedStatementId, e);
    }

    public <M extends Message, R, RES> RES query(M message, Predicate<Row> rowFilter, Function<Row, R> rowMapper, Collector<R, ?, RES> collector) {
        this.protocol.send(message, 0);
        ColumnDefinition metadata = this.protocol.readMetadata();
        Iterator<Row> ris = new XProtocolRowInputStream(metadata, (XProtocol) this.protocol, null);
        Stream<Row> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(ris, 0), false);
        if (rowFilter != null) {
            stream = stream.filter(rowFilter);
        }
        RES result = stream.map(rowMapper).collect(collector);
        this.protocol.readQueryResult(new StatementExecuteOkBuilder());
        return result;
    }

    public <M extends Message, R extends QueryResult> R query(M message, ResultBuilder<R> resultBuilder) {
        return ((XProtocol) this.protocol).query(message, resultBuilder);
    }

    public <M extends Message, R extends QueryResult> CompletableFuture<R> queryAsync(M message, ResultBuilder<R> resultBuilder) {
        return ((XProtocol) this.protocol).queryAsync(message, resultBuilder);
    }
}
