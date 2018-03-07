/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.protocol.a;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.AsynchronousSocketChannel;

import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.SSLParamsException;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.AbstractSocketConnection;
import com.mysql.cj.protocol.FullReadInputStream;
import com.mysql.cj.protocol.ReadAheadInputStream;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.SocketConnection;

public class NativeSocketConnection extends AbstractSocketConnection implements SocketConnection {

    @Override
    public void connect(String hostName, int portNumber, PropertySet propSet, ExceptionInterceptor excInterceptor, Log log, int loginTimeout) {

        // TODO we don't need both Properties and PropertySet in method params

        try {
            this.port = portNumber;
            this.host = hostName;
            this.propertySet = propSet;
            this.exceptionInterceptor = excInterceptor;

            this.socketFactory = createSocketFactory(propSet.getStringReadableProperty(PropertyDefinitions.PNAME_socketFactory).getStringValue());
            this.mysqlSocket = this.socketFactory.connect(this.host, this.port, propSet.exposeAsProperties(), loginTimeout);

            int socketTimeout = propSet.getIntegerReadableProperty(PropertyDefinitions.PNAME_socketTimeout).getValue();
            if (socketTimeout != 0) {
                try {
                    this.mysqlSocket.setSoTimeout(socketTimeout);
                } catch (Exception ex) {
                    /* Ignore if the platform does not support it */
                }
            }

            this.socketFactory.beforeHandshake();

            InputStream rawInputStream;
            if (propSet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useReadAheadInput).getValue()) {
                rawInputStream = new ReadAheadInputStream(this.mysqlSocket.getInputStream(), 16384,
                        propSet.getBooleanReadableProperty(PropertyDefinitions.PNAME_traceProtocol).getValue(), log);
            } else if (propSet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useUnbufferedInput).getValue()) {
                rawInputStream = this.mysqlSocket.getInputStream();
            } else {
                rawInputStream = new BufferedInputStream(this.mysqlSocket.getInputStream(), 16384);
            }

            this.mysqlInput = new FullReadInputStream(rawInputStream);
            this.mysqlOutput = new BufferedOutputStream(this.mysqlSocket.getOutputStream(), 16384);
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(propSet, null, 0, 0, ioEx, getExceptionInterceptor());
        }
    }

    @Override
    public void performTlsHandshake(ServerSession serverSession) throws SSLParamsException, FeatureNotAvailableException, IOException {

        this.mysqlSocket = this.socketFactory.performTlsHandshake(this, serverSession);

        this.mysqlInput = new FullReadInputStream(this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useUnbufferedInput).getValue()
                ? getMysqlSocket().getInputStream() : new BufferedInputStream(getMysqlSocket().getInputStream(), 16384));

        this.mysqlOutput = new BufferedOutputStream(getMysqlSocket().getOutputStream(), 16384);
        this.mysqlOutput.flush();

    }

    @Override
    public AsynchronousSocketChannel getAsynchronousSocketChannel() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }
}
