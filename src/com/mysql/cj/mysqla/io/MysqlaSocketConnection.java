/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */

package com.mysql.cj.mysqla.io;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.Properties;

import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.SocketConnection;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.io.AbstractSocketConnection;
import com.mysql.cj.core.io.ReadAheadInputStream;
import com.mysql.cj.core.io.FullReadInputStream;

public class MysqlaSocketConnection extends AbstractSocketConnection implements SocketConnection {

    @Override
    public void connect(String host, int port, Properties props, PropertySet propertySet, ExceptionInterceptor exceptionInterceptor, Log log, int loginTimeout) {

        // TODO we don't need both Properties and PropertySet in method params

        try {
            this.port = port;
            this.host = host;
            this.propertySet = propertySet;
            this.exceptionInterceptor = exceptionInterceptor;

            this.socketFactory = createSocketFactory(propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_socketFactory).getStringValue());
            this.mysqlSocket = this.socketFactory.connect(this.host, this.port, props, loginTimeout);

            int socketTimeout = propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_socketTimeout).getValue();
            if (socketTimeout != 0) {
                try {
                    this.mysqlSocket.setSoTimeout(socketTimeout);
                } catch (Exception ex) {
                    /* Ignore if the platform does not support it */
                }
            }

            this.mysqlSocket = this.socketFactory.beforeHandshake();

            InputStream rawInputStream;
            if (propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useReadAheadInput).getValue()) {
                rawInputStream = new ReadAheadInputStream(this.mysqlSocket.getInputStream(), 16384, propertySet.getBooleanReadableProperty(
                        PropertyDefinitions.PNAME_traceProtocol).getValue(), log);
            } else if (propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useUnbufferedInput).getValue()) {
                rawInputStream = this.mysqlSocket.getInputStream();
            } else {
                rawInputStream = new BufferedInputStream(this.mysqlSocket.getInputStream(), 16384);
            }

            this.mysqlInput = new FullReadInputStream(rawInputStream);
            this.mysqlOutput = new BufferedOutputStream(this.mysqlSocket.getOutputStream(), 16384);
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(propertySet, null, 0, 0, ioEx, getExceptionInterceptor());
        }
    }

}
