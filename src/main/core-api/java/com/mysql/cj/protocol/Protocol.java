/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol;

import java.io.IOException;
import java.io.InputStream;

import com.mysql.cj.MessageBuilder;
import com.mysql.cj.QueryResult;
import com.mysql.cj.Session;
import com.mysql.cj.TransactionEventHandler;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionInterceptor;

/**
 * A protocol provides the facilities to communicate with a MySQL server.
 * 
 * @param <M>
 *            Message type
 */
public interface Protocol<M extends Message> {

    /**
     * Init method takes the place of constructor.
     *
     * A constructor should be used unless the encapsulation of ProtocolFactory is necessary.
     * 
     * @param session
     *            {@link Session}
     * @param socketConnection
     *            {@link SocketConnection}
     * @param propertySet
     *            {@link PropertySet}
     * @param transactionManager
     *            {@link TransactionEventHandler}
     */
    void init(Session session, SocketConnection socketConnection, PropertySet propertySet, TransactionEventHandler transactionManager);

    PropertySet getPropertySet();

    void setPropertySet(PropertySet propertySet);

    MessageBuilder<M> getMessageBuilder();

    /**
     * Retrieve ServerCapabilities from server.
     * 
     * @return {@link ServerCapabilities}
     */
    ServerCapabilities readServerCapabilities();

    ServerSession getServerSession();

    SocketConnection getSocketConnection();

    AuthenticationProvider<M> getAuthenticationProvider();

    ExceptionInterceptor getExceptionInterceptor();

    PacketSentTimeHolder getPacketSentTimeHolder();

    void setPacketSentTimeHolder(PacketSentTimeHolder packetSentTimeHolder);

    PacketReceivedTimeHolder getPacketReceivedTimeHolder();

    void setPacketReceivedTimeHolder(PacketReceivedTimeHolder packetReceivedTimeHolder);

    /**
     * Create a new session. This generally happens once at the beginning of a connection.
     * 
     * @param user
     *            DB user name
     * @param password
     *            DB user password
     * @param database
     *            database name
     */
    void connect(String user, String password, String database);

    void negotiateSSLConnection();

    void beforeHandshake();

    void afterHandshake();

    void changeDatabase(String database);

    /**
     * Re-authenticates as the given user and password
     * 
     * @param user
     *            DB user name
     * @param password
     *            DB user password
     * @param database
     *            database name
     * 
     */
    void changeUser(String user, String password, String database);

    String getPasswordCharacterEncoding();

    boolean versionMeetsMinimum(int major, int minor, int subminor);

    /**
     * Read one message from the MySQL server into the reusable buffer if provided or into the new one.
     * 
     * @param reuse
     *            {@link Message} instance to read into, may be null
     * @return the message from the server.
     */
    M readMessage(M reuse);

    /**
     * Read one message from the MySQL server, checks for errors in it, and if none,
     * returns the message, ready for reading
     * 
     * @return a message ready for reading.
     */
    M checkErrorMessage();

    /**
     * @param message
     *            {@link Message} instance
     * @param packetLen
     *            length of header + payload
     */
    void send(Message message, int packetLen);

    ColumnDefinition readMetadata();

    /**
     * Send a command to the MySQL server.
     * 
     * @param queryPacket
     *            a packet pre-loaded with data for the protocol (eg.
     *            from a client-side prepared statement). The first byte of
     *            this packet is the MySQL protocol 'command' from MysqlDefs
     * @param skipCheck
     *            do not call checkErrorPacket() if true
     * @param timeoutMillis
     *            timeout
     * 
     * @return the response packet from the server
     * 
     * @throws CJException
     *             if an I/O error or SQL error occurs
     */

    M sendCommand(Message queryPacket, boolean skipCheck, int timeoutMillis);

    <T extends ProtocolEntity> T read(Class<T> requiredClass, ProtocolEntityFactory<T, M> protocolEntityFactory) throws IOException;

    /**
     * Read protocol entity.
     * 
     * @param requiredClass
     *            required Resultset class
     * @param maxRows
     *            the maximum number of rows to read (-1 means all rows)
     * @param streamResults
     *            should the driver leave the results on the wire,
     *            and read them only when needed?
     * @param resultPacket
     *            the first packet of information in the result set
     * @param isBinaryEncoded
     *            true if the binary protocol is used (for server prepared statements)
     * @param metadata
     *            use this metadata instead of the one provided on wire
     * @param protocolEntityFactory
     *            {@link ProtocolEntityFactory} instance
     * @param <T>
     *            object extending the {@link ProtocolEntity}
     * @return
     *         {@link ProtocolEntity} instance
     * @throws IOException
     *             if an error occurs
     */
    <T extends ProtocolEntity> T read(Class<Resultset> requiredClass, int maxRows, boolean streamResults, M resultPacket, boolean isBinaryEncoded,
            ColumnDefinition metadata, ProtocolEntityFactory<T, M> protocolEntityFactory) throws IOException;

    /**
     * Sets an InputStream instance that will be used to send data
     * to the MySQL server for a "LOAD DATA LOCAL INFILE" statement
     * rather than a FileInputStream or URLInputStream that represents
     * the path given as an argument to the statement.
     * 
     * This stream will be read to completion upon execution of a
     * "LOAD DATA LOCAL INFILE" statement, and will automatically
     * be closed by the driver, so it needs to be reset
     * before each call to execute*() that would cause the MySQL
     * server to request data to fulfill the request for
     * "LOAD DATA LOCAL INFILE".
     * 
     * If this value is set to NULL, the driver will revert to using
     * a FileInputStream or URLInputStream as required.
     * 
     * @param stream
     *            input stream
     */
    void setLocalInfileInputStream(InputStream stream);

    /**
     * Returns the InputStream instance that will be used to send
     * data in response to a "LOAD DATA LOCAL INFILE" statement.
     * 
     * This method returns NULL if no such stream has been set
     * via setLocalInfileInputStream().
     * 
     * @return input stream
     */
    InputStream getLocalInfileInputStream();

    /**
     * Returns the comment that will be prepended to all statements
     * sent to the server.
     * 
     * @return query comment string
     */
    String getQueryComment();

    /**
     * Sets the comment that will be prepended to all statements
     * sent to the server. Do not use slash-star or star-slash tokens
     * in the comment as these will be added by the driver itself.
     * 
     * @param comment
     *            query comment string
     */
    void setQueryComment(String comment);

    /**
     * Read messages from server and deliver them to resultBuilder.
     * 
     * @param resultBuilder
     *            {@link ResultBuilder} instance
     * @param <T>
     *            result type
     * @return {@link QueryResult}
     */
    <T extends QueryResult> T readQueryResult(ResultBuilder<T> resultBuilder);

    void close() throws IOException;

    void configureTimeZone();

    void initServerSession();

    /**
     * Return Protocol to its initial state right after successful connect.
     */
    void reset();

    String getQueryTimingUnits();
}
