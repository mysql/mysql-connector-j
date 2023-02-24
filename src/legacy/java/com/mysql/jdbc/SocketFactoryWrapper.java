/*
 * Copyright (c) 2018, 2023, Oracle and/or its affiliates.
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

package com.mysql.jdbc;

import java.io.Closeable;
import java.io.IOException;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.SocketConnection;
import com.mysql.cj.protocol.SocketFactory;
import com.mysql.cj.protocol.StandardSocketFactory;

/**
 * Wraps the legacy com.mysql.jdbc.SocketFactory implementations so they can be used as {@link SocketFactory}
 */
public class SocketFactoryWrapper extends StandardSocketFactory implements SocketFactory {

    @SuppressWarnings("deprecation")
    com.mysql.jdbc.SocketFactory socketFactory;

    @SuppressWarnings("deprecation")
    public SocketFactoryWrapper(com.mysql.jdbc.SocketFactory legacyFactory) {
        this.socketFactory = legacyFactory;
    }

    @SuppressWarnings({ "deprecation", "unchecked" })
    @Override
    public <T extends Closeable> T connect(String hostname, int portNumber, PropertySet pset, int loginTimeout) throws IOException {
        this.rawSocket = this.socketFactory.connect(hostname, portNumber, pset.exposeAsProperties());
        return (T) this.rawSocket;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Closeable> T performTlsHandshake(SocketConnection socketConnection, ServerSession serverSession, Log log) throws IOException {
        return (T) super.performTlsHandshake(socketConnection, serverSession, log);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void beforeHandshake() throws IOException {
        this.socketFactory.beforeHandshake();
    }

    @SuppressWarnings("deprecation")
    @Override
    public void afterHandshake() throws IOException {
        this.socketFactory.afterHandshake();
    }
}
