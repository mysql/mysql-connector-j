/*
 * Copyright (c) 2015, 2023, Oracle and/or its affiliates.
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

import java.lang.ref.WeakReference;
import java.util.LinkedList;
import java.util.concurrent.CopyOnWriteArrayList;

import com.mysql.cj.MessageBuilder;
import com.mysql.cj.Messages;
import com.mysql.cj.Session;
import com.mysql.cj.TransactionEventHandler;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.Protocol.ProtocolEventHandler;
import com.mysql.cj.protocol.Protocol.ProtocolEventListener.EventType;
import com.mysql.cj.util.TimeUtil;

public abstract class AbstractProtocol<M extends Message> implements Protocol<M>, ProtocolEventHandler {

    protected Session session;
    protected SocketConnection socketConnection;

    protected PropertySet propertySet;

    protected TransactionEventHandler transactionManager;

    /** The logger we're going to use */
    protected transient Log log;

    protected ExceptionInterceptor exceptionInterceptor;

    protected AuthenticationProvider<M> authProvider;

    protected MessageBuilder<M> messageBuilder;

    // Default until packet sender created
    private PacketSentTimeHolder packetSentTimeHolder = new PacketSentTimeHolder() {
    };
    private PacketReceivedTimeHolder packetReceivedTimeHolder = new PacketReceivedTimeHolder() {
    };

    protected LinkedList<StringBuilder> packetDebugRingBuffer = null;

    protected boolean useNanosForElapsedTime;
    protected String queryTimingUnits;

    private CopyOnWriteArrayList<WeakReference<ProtocolEventListener>> listeners = new CopyOnWriteArrayList<>();

    @Override
    public void init(Session sess, SocketConnection phConnection, PropertySet propSet, TransactionEventHandler trManager) {
        this.session = sess;
        this.propertySet = propSet;

        this.socketConnection = phConnection;
        this.exceptionInterceptor = this.socketConnection.getExceptionInterceptor();

        this.transactionManager = trManager;

        this.useNanosForElapsedTime = this.propertySet.getBooleanProperty(PropertyKey.useNanosForElapsedTime).getValue() && TimeUtil.nanoTimeAvailable();
        this.queryTimingUnits = this.useNanosForElapsedTime ? Messages.getString("Nanoseconds") : Messages.getString("Milliseconds");
    }

    @Override
    public SocketConnection getSocketConnection() {
        return this.socketConnection;
    }

    @Override
    public AuthenticationProvider<M> getAuthenticationProvider() {
        return this.authProvider;
    }

    @Override
    public ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    @Override
    public PacketSentTimeHolder getPacketSentTimeHolder() {
        return this.packetSentTimeHolder;
    }

    @Override
    public void setPacketSentTimeHolder(PacketSentTimeHolder packetSentTimeHolder) {
        this.packetSentTimeHolder = packetSentTimeHolder;
    }

    @Override
    public PacketReceivedTimeHolder getPacketReceivedTimeHolder() {
        return this.packetReceivedTimeHolder;
    }

    @Override
    public void setPacketReceivedTimeHolder(PacketReceivedTimeHolder packetReceivedTimeHolder) {
        this.packetReceivedTimeHolder = packetReceivedTimeHolder;
    }

    @Override
    public PropertySet getPropertySet() {
        return this.propertySet;
    }

    @Override
    public void setPropertySet(PropertySet propertySet) {
        this.propertySet = propertySet;
    }

    @Override
    public MessageBuilder<M> getMessageBuilder() {
        return this.messageBuilder;
    }

    @Override
    public void reset() {
        // no-op
    }

    @Override
    public String getQueryTimingUnits() {
        return this.queryTimingUnits;
    }

    @Override
    public void addListener(ProtocolEventListener l) {
        this.listeners.addIfAbsent(new WeakReference<>(l));
    }

    @Override
    public void removeListener(ProtocolEventListener listener) {
        for (WeakReference<ProtocolEventListener> wr : this.listeners) {
            ProtocolEventListener l = wr.get();
            if (l == listener) {
                this.listeners.remove(wr);
                break;
            }
        }
    }

    @Override
    public void invokeListeners(EventType type, Throwable reason) {
        for (WeakReference<ProtocolEventListener> wr : this.listeners) {
            ProtocolEventListener l = wr.get();
            if (l != null) {
                l.handleEvent(type, this, reason);
            } else {
                this.listeners.remove(wr);
            }
        }
    }

}
