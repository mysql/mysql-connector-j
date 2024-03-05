/*
 * Copyright (c) 2018, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.xdevapi;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.BooleanPropertyDefinition;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.IntegerPropertyDefinition;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.Protocol.ProtocolEventListener;
import com.mysql.cj.protocol.x.XProtocol;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.util.StringUtils;

public class ClientImpl implements Client, ProtocolEventListener {

    private final PooledXProtocol poisonProtocolMarker = new PooledXProtocol();

    private boolean isClosed = false;

    private ConnectionUrl connUrl = null;

    private boolean poolingEnabled = true;
    private int maxSize = 25;
    private int maxIdleTime = 0;
    private int queueTimeout = 0;

    private Set<WeakReference<Session>> nonPooledSessions = null;

    private int demotedTimeout = 120_000;
    private ConcurrentMap<HostInfo, Long> demotedHosts = null;
    private Set<WeakReference<PooledXProtocol>> activeProtocols = null;
    private BlockingQueue<PooledXProtocol> idleProtocols = null;
    private Semaphore availableProtocols;

    private ReadWriteLock clientShutdownLock;

    private SessionFactory sessionFactory = new SessionFactory();

    public ClientImpl(String url, String clientPropsJson) {
        Properties clientProps = StringUtils.isNullOrEmpty(clientPropsJson) ? new Properties() : clientPropsFromJson(clientPropsJson);
        init(url, clientProps);
    }

    public ClientImpl(String url, Properties clientProps) {
        init(url, clientProps != null ? clientProps : new Properties());
    }

    private Properties clientPropsFromJson(String clientPropsJson) {
        Properties props = new Properties();
        DbDoc clientPropsDoc = JsonParser.parseDoc(clientPropsJson);

        JsonValue pooling = clientPropsDoc.remove("pooling");
        if (pooling != null) {
            if (!DbDoc.class.isAssignableFrom(pooling.getClass())) {
                throw new XDevAPIError(String.format("Client option 'pooling' does not support value '%s'.", pooling.toFormattedString()));
            }
            DbDoc poolingDoc = (DbDoc) pooling;
            JsonValue jsonVal;

            jsonVal = poolingDoc.remove("enabled");
            if (jsonVal != null) {
                if (JsonLiteral.class.isAssignableFrom(jsonVal.getClass())) {
                    JsonLiteral pe = (JsonLiteral) jsonVal;
                    if (pe != JsonLiteral.FALSE && pe != JsonLiteral.TRUE) {
                        throw new XDevAPIError(String.format("Client option '%s' does not support value '%s'.", ClientProperty.POOLING_ENABLED.getKeyName(),
                                jsonVal.toFormattedString()));
                    }
                    props.setProperty(ClientProperty.POOLING_ENABLED.getKeyName(), pe.value);
                } else if (JsonString.class.isAssignableFrom(jsonVal.getClass())) {
                    throw new XDevAPIError(String.format("Client option '%s' does not support value '%s'.", ClientProperty.POOLING_ENABLED.getKeyName(),
                            ((JsonString) jsonVal).getString()));
                } else {
                    throw new XDevAPIError(String.format("Client option '%s' does not support value '%s'.", ClientProperty.POOLING_ENABLED.getKeyName(),
                            jsonVal.toFormattedString()));
                }
            }
            jsonVal = poolingDoc.remove("maxSize");
            if (jsonVal != null) {
                if (JsonNumber.class.isAssignableFrom(jsonVal.getClass())) {
                    props.setProperty(ClientProperty.POOLING_MAX_SIZE.getKeyName(), ((JsonNumber) jsonVal).toString());
                } else if (JsonString.class.isAssignableFrom(jsonVal.getClass())) {
                    throw new XDevAPIError(String.format("Client option '%s' does not support value '%s'.", ClientProperty.POOLING_MAX_SIZE.getKeyName(),
                            ((JsonString) jsonVal).getString()));
                } else {
                    throw new XDevAPIError(String.format("Client option '%s' does not support value '%s'.", ClientProperty.POOLING_MAX_SIZE.getKeyName(),
                            jsonVal.toFormattedString()));
                }
            }
            jsonVal = poolingDoc.remove("maxIdleTime");
            if (jsonVal != null) {
                if (JsonNumber.class.isAssignableFrom(jsonVal.getClass())) {
                    props.setProperty(ClientProperty.POOLING_MAX_IDLE_TIME.getKeyName(), ((JsonNumber) jsonVal).toString());
                } else if (JsonString.class.isAssignableFrom(jsonVal.getClass())) {
                    throw new XDevAPIError(String.format("Client option '%s' does not support value '%s'.", ClientProperty.POOLING_MAX_IDLE_TIME.getKeyName(),
                            ((JsonString) jsonVal).getString()));
                } else {
                    throw new XDevAPIError(String.format("Client option '%s' does not support value '%s'.", ClientProperty.POOLING_MAX_IDLE_TIME.getKeyName(),
                            jsonVal.toFormattedString()));
                }
            }
            jsonVal = poolingDoc.remove("queueTimeout");
            if (jsonVal != null) {
                if (JsonNumber.class.isAssignableFrom(jsonVal.getClass())) {
                    props.setProperty(ClientProperty.POOLING_QUEUE_TIMEOUT.getKeyName(), ((JsonNumber) jsonVal).toString());
                } else if (JsonString.class.isAssignableFrom(jsonVal.getClass())) {
                    throw new XDevAPIError(String.format("Client option '%s' does not support value '%s'.", ClientProperty.POOLING_QUEUE_TIMEOUT.getKeyName(),
                            ((JsonString) jsonVal).getString()));
                } else {
                    throw new XDevAPIError(String.format("Client option '%s' does not support value '%s'.", ClientProperty.POOLING_QUEUE_TIMEOUT.getKeyName(),
                            jsonVal.toFormattedString()));
                }
            }
            if (poolingDoc.size() > 0) {
                String key = poolingDoc.keySet().stream().findFirst().get();
                throw new XDevAPIError(String.format("Client option 'pooling.%s' is not recognized as valid.", key));
            }
        }

        if (!clientPropsDoc.isEmpty()) {
            String key = clientPropsDoc.keySet().stream().findFirst().get();
            throw new XDevAPIError(String.format("Client option '%s' is not recognized as valid.", key));
        }

        return props;
    }

    private void validateAndInitializeClientProps(Properties clientProps) {
        String propKey = "";
        String propValue = "";
        propKey = ClientProperty.POOLING_ENABLED.getKeyName();
        if (clientProps.containsKey(propKey)) {
            propValue = clientProps.getProperty(propKey);
            try {
                this.poolingEnabled = BooleanPropertyDefinition.booleanFrom(propKey, propValue, null);
            } catch (CJException e) {
                throw new XDevAPIError(String.format("Client option '%s' does not support value '%s'.", propKey, propValue), e);
            }
        }

        propKey = ClientProperty.POOLING_MAX_SIZE.getKeyName();
        if (clientProps.containsKey(propKey)) {
            propValue = clientProps.getProperty(propKey);
            try {
                this.maxSize = IntegerPropertyDefinition.integerFrom(propKey, propValue, 1, null);
            } catch (WrongArgumentException e) {
                throw new XDevAPIError(String.format("Client option '%s' does not support value '%s'.", propKey, propValue), e);
            }
            if (this.maxSize <= 0) {
                throw new XDevAPIError(String.format("Client option '%s' does not support value '%s'.", propKey, propValue));
            }
        }

        propKey = ClientProperty.POOLING_MAX_IDLE_TIME.getKeyName();
        if (clientProps.containsKey(propKey)) {
            propValue = clientProps.getProperty(propKey);
            try {
                this.maxIdleTime = IntegerPropertyDefinition.integerFrom(propKey, propValue, 1, null);
            } catch (WrongArgumentException e) {
                throw new XDevAPIError(String.format("Client option '%s' does not support value '%s'.", propKey, propValue), e);
            }
            if (this.maxIdleTime < 0) {
                throw new XDevAPIError(String.format("Client option '%s' does not support value '%s'.", propKey, propValue));
            }
        }

        propKey = ClientProperty.POOLING_QUEUE_TIMEOUT.getKeyName();
        if (clientProps.containsKey(propKey)) {
            propValue = clientProps.getProperty(propKey);
            try {
                this.queueTimeout = IntegerPropertyDefinition.integerFrom(propKey, propValue, 1, null);
            } catch (WrongArgumentException e) {
                throw new XDevAPIError(String.format("Client option '%s' does not support value '%s'.", propKey, propValue), e);
            }
            if (this.queueTimeout < 0) {
                throw new XDevAPIError(String.format("Client option '%s' does not support value '%s'.", propKey, propValue));
            }
        }

        List<String> clientPropsAsString = Stream.of(ClientProperty.values()).map(ClientProperty::getKeyName).collect(Collectors.toList());
        propKey = (String) clientProps.keySet().stream().filter(k -> !clientPropsAsString.contains(k)).findFirst().orElse(null);
        if (propKey != null) {
            throw new XDevAPIError(String.format("Client option '%s' is not recognized as valid.", propKey));
        }
    }

    private void init(String url, Properties clientProps) {
        this.connUrl = this.sessionFactory.parseUrl(url);

        validateAndInitializeClientProps(clientProps);

        if (this.poolingEnabled) {
            this.demotedHosts = new ConcurrentHashMap<>();
            this.activeProtocols = new CopyOnWriteArraySet<>();
            this.idleProtocols = new LinkedBlockingQueue<>(this.maxSize);
            this.availableProtocols = new Semaphore(this.maxSize, true);
        } else {
            this.nonPooledSessions = new CopyOnWriteArraySet<>();
        }
        this.clientShutdownLock = new ReentrantReadWriteLock(true);
    }

    @Override
    public Session getSession() {
        return this.poolingEnabled ? getPooledSession() : getNonPooledSession();
    }

    private Session getNonPooledSession() {
        if (this.isClosed) {
            throw new XDevAPIError("Client is closed.");
        }

        this.clientShutdownLock.readLock().lock();
        try {
            // Remove nulled and closed session references from the nonPooledSessions set.
            for (WeakReference<Session> ws : this.nonPooledSessions) {
                Session s = ws.get();
                if (s == null || !s.isOpen()) {
                    this.nonPooledSessions.remove(ws);
                }
            }

            Session sess = this.sessionFactory.getSession(this.connUrl);
            this.nonPooledSessions.add(new WeakReference<>(sess));
            return sess;
        } finally {
            this.clientShutdownLock.readLock().unlock();
        }
    }

    private Session getPooledSession() {
        PooledXProtocol protocol = null;
        List<HostInfo> hostsList = this.connUrl.getHostsList();

        long startTime = System.currentTimeMillis();
        while (protocol == null) {
            if (this.isClosed) {
                throw new XDevAPIError("Client is closed.");
            }

            if (this.queueTimeout != 0 && System.currentTimeMillis() > startTime + this.queueTimeout) {
                // 0. Already waited queueTimeout for an idle Protocol. Check one last time if there are available slots in the pool and, if not, then fail.
                if (this.availableProtocols.tryAcquire()) {
                    protocol = newPooledXProtocol(hostsList);
                }
                if (protocol == null) {
                    throw new XDevAPIError("Session can not be obtained within " + this.queueTimeout + " milliseconds.");
                }
            }

            if ((protocol = this.idleProtocols.poll()) != null) {
                // 1. If there are idle Protocols then pick one and check if it is usable.
                protocol = validateAndResetPooledXProtocol(protocol, hostsList);

            } else if (this.availableProtocols.tryAcquire()) {
                // 2. No idle Protocols but the pool has free space. Create a new Protocol.
                protocol = newPooledXProtocol(hostsList);

            } else if (this.queueTimeout > 0) {
                // 3. No idle Protocols, no free space in the pool. Waiting up to queueTimeout milliseconds for an idle Protocol.
                long elapsedTime = System.currentTimeMillis() - startTime;
                long remainingTime = this.queueTimeout - elapsedTime;
                try {
                    if (remainingTime > 0) {
                        protocol = this.idleProtocols.poll(remainingTime, TimeUnit.MILLISECONDS);
                        protocol = validateAndResetPooledXProtocol(protocol, hostsList);
                    }
                } catch (InterruptedException e) {
                    throw new XDevAPIError("Session can not be obtained within " + this.queueTimeout + " milliseconds.", e);
                }

            } else {
                // 4. No idle Protocols, no free space in the pool, no queue timeout. Waiting indefinitely for an idle Protocol.
                try {
                    protocol = this.idleProtocols.take();
                    validateAndResetPooledXProtocol(protocol, hostsList);
                } catch (InterruptedException e) {
                    throw new XDevAPIError("Session can not be obtained.", e);
                }
            }
        }

        this.clientShutdownLock.readLock().lock();
        try {
            if (this.isClosed) {
                throw new XDevAPIError("Client is closed.");
            }
            this.activeProtocols.add(new WeakReference<>(protocol));
        } finally {
            this.clientShutdownLock.readLock().unlock();
        }

        SessionImpl sess = new SessionImpl(protocol);
        return sess;
    }

    private PooledXProtocol newPooledXProtocol(List<HostInfo> hostsList) {
        PooledXProtocol protocol = null;
        CJException latestException = null;
        List<HostInfo> hostsToRetryAfterwards = new ArrayList<>();

        for (HostInfo hi : hostsList) {
            if (this.demotedHosts.containsKey(hi)) {
                if (System.currentTimeMillis() - this.demotedHosts.get(hi) > this.demotedTimeout) {
                    this.demotedHosts.remove(hi);
                } else {
                    hostsToRetryAfterwards.add(hi);
                    continue;
                }
            }
            try {
                protocol = newPooledXProtocol(hi);
                break;
            } catch (CJCommunicationsException e) {
                if (e.getCause() == null) {
                    this.availableProtocols.release();
                    throw e;
                }
                latestException = e;
                this.demotedHosts.put(hi, System.currentTimeMillis());
            }
        }
        if (protocol == null) {
            // All non-demoted hosts have failed, let's try the ones that were previously demoted before calling it a failure.
            for (HostInfo hi : hostsToRetryAfterwards) {
                try {
                    protocol = newPooledXProtocol(hi);
                    // This host is fine now so let's promote it.
                    this.demotedHosts.remove(hi);
                    break;
                } catch (CJCommunicationsException e) {
                    if (e.getCause() == null) {
                        this.availableProtocols.release();
                        throw e;
                    }
                    latestException = e;
                    this.demotedHosts.put(hi, System.currentTimeMillis());
                }
            }
        }
        if (protocol == null) {
            this.availableProtocols.release();
            if (latestException != null) {
                throw ExceptionFactory.createException(CJCommunicationsException.class, Messages.getString("Session.Create.Failover.0"), latestException);
            }
        }

        return protocol;
    }

    private PooledXProtocol newPooledXProtocol(HostInfo hi) {
        PooledXProtocol protocol;
        PropertySet pset = new DefaultPropertySet();

        pset.initializeProperties(hi.exposeAsProperties());
        protocol = new PooledXProtocol(hi, pset, this.maxIdleTime);
        protocol.addListener(this);
        protocol.connect(hi.getUser(), hi.getPassword(), hi.getDatabase());

        return protocol;
    }

    private PooledXProtocol validateAndResetPooledXProtocol(PooledXProtocol protocol, List<HostInfo> hostsList) {
        if (protocol == null) {
            return null;
        }
        if (protocol == this.poisonProtocolMarker) {
            this.idleProtocols.add(this.poisonProtocolMarker);
            throw new XDevAPIError("Session can not be obtained. Client instance is closing.");
        }
        if (!protocol.isOpen()) { // If not open, ignore ant try next.
            this.availableProtocols.release();
            return null;
        }
        if (!protocol.isHostInfoValid(hostsList)) { // Protocol connected to a host that is not usable anymore. Clean up resources and try next.
            this.availableProtocols.release();
            this.demotedHosts.remove(protocol.getHostInfo());
            protocol.realClose();
            return null;
        }
        if (protocol.isIdleTimeoutReached()) { // Protocol expired. Clean up resources and try next.
            this.availableProtocols.release();
            protocol.realClose();
            return null;
        }
        try {
            protocol.reset();
        } catch (CJCommunicationsException | XProtocolError e) {
            // This Protocol is useless, let's try next one.
            this.availableProtocols.release();
            return null;
        }
        return protocol;
    }

    @Override
    public void close() {
        this.clientShutdownLock.writeLock().lock();
        try {
            if (!this.isClosed) {
                this.isClosed = true;
                if (this.poolingEnabled) {
                    this.availableProtocols.drainPermits();
                    this.idleProtocols.forEach(PooledXProtocol::realClose);
                    this.idleProtocols.clear();
                    this.idleProtocols.add(this.poisonProtocolMarker);
                    this.activeProtocols.stream().map(WeakReference::get).filter(Objects::nonNull).forEach(PooledXProtocol::realClose);
                    this.activeProtocols.clear();
                } else {
                    this.nonPooledSessions.stream().map(WeakReference::get).filter(Objects::nonNull).filter(Session::isOpen).forEach(Session::close);
                }
            }
        } finally {
            this.clientShutdownLock.writeLock().unlock();
        }
    }

    void idleProtocol(PooledXProtocol protocol) {
        if (!this.isClosed) {
            for (WeakReference<PooledXProtocol> protocolReference : this.activeProtocols) {
                PooledXProtocol referencedProtocol = protocolReference.get();
                if (referencedProtocol == null) {
                    if (this.activeProtocols.remove(protocolReference)) {
                        this.availableProtocols.release();
                    }
                } else if (referencedProtocol == protocol) {
                    this.activeProtocols.remove(protocolReference);
                    this.idleProtocols.add(referencedProtocol);
                }
            }
        }
    }

    @Override
    public void handleEvent(EventType type, Object info, Throwable reason) {
        switch (type) {
            case SERVER_SHUTDOWN:
                HostInfo hi = ((PooledXProtocol) info).getHostInfo();
                this.clientShutdownLock.writeLock().lock();
                try {
                    // Close and remove idle protocols connected to a host that is not usable anymore.
                    List<PooledXProtocol> toCloseAndRemove = this.idleProtocols.stream().filter(p -> p.getHostInfo().equalHostPortPair(hi))
                            .collect(Collectors.toList());
                    toCloseAndRemove.stream().peek(PooledXProtocol::realClose).peek(this.idleProtocols::remove).map(PooledXProtocol::getHostInfo).sequential()
                            .forEach(this.demotedHosts::remove);

                    removeActivePooledXProtocol((PooledXProtocol) info);
                } finally {
                    this.clientShutdownLock.writeLock().unlock();
                }
                break;

            case SERVER_CLOSED_SESSION:
                this.clientShutdownLock.writeLock().lock();
                try {
                    removeActivePooledXProtocol((PooledXProtocol) info);
                } finally {
                    this.clientShutdownLock.writeLock().unlock();
                }
                break;

            default:
                break;
        }
    }

    private void removeActivePooledXProtocol(PooledXProtocol protocol) {
        for (WeakReference<PooledXProtocol> protocolReference : this.activeProtocols) {
            PooledXProtocol referencedProtocol = protocolReference.get();
            if (referencedProtocol == protocol) {
                if (this.activeProtocols.remove(protocolReference)) {
                    this.availableProtocols.release();
                }
                protocol.realClose();
                return;
            }
        }
    }

    public class PooledXProtocol extends XProtocol {

        private int maxIdleTime = -1;
        private long idleSince = -1;
        private HostInfo hostInfo = null;

        PooledXProtocol() {
            super(null, null);
        }

        PooledXProtocol(HostInfo hostInfo, PropertySet propertySet, int maxIdleTime) {
            super(hostInfo, propertySet);
            this.hostInfo = hostInfo;
            this.maxIdleTime = maxIdleTime;
        }

        @Override
        public void close() {
            reset();
            this.idleSince = System.currentTimeMillis();
            idleProtocol(this);
        }

        HostInfo getHostInfo() {
            return this.hostInfo;
        }

        boolean isIdleTimeoutReached() {
            return this.maxIdleTime > 0 && this.idleSince > 0 && System.currentTimeMillis() > this.idleSince + this.maxIdleTime;
        }

        boolean isHostInfoValid(List<HostInfo> hostsList) {
            return hostsList.stream().filter(h -> h.equalHostPortPair(this.hostInfo)).findFirst().isPresent();
        }

        void realClose() {
            try {
                super.close();
            } catch (IOException e) {
                // There shouldn't be any. Throw anyway.
                throw new CJCommunicationsException(e);
            }
        }

    }

}
