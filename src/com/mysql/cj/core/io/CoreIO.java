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

package com.mysql.cj.core.io;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.net.Socket;

import com.mysql.cj.api.ExceptionInterceptor;
import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.api.io.SocketFactory;

public abstract class CoreIO implements Protocol {

    protected String host = null;
    protected int port = 3306;
    protected InputStream mysqlInput = null;
    protected BufferedOutputStream mysqlOutput = null;
    protected SocketFactory socketFactory = null;

    /** The connection to the server */
    protected Socket mysqlSocket = null;

    protected MysqlConnection connection;
    protected ExceptionInterceptor exceptionInterceptor;

    protected long lastPacketSentTimeMs = 0;
    protected long lastPacketReceivedTimeMs = 0;

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public MysqlConnection getConnection() {
        return this.connection;
    }

    public void setConnection(MysqlConnection connection) {
        this.connection = connection;
    }

    public Socket getMysqlSocket() {
        return this.mysqlSocket;
    }

    public void setMysqlSocket(Socket mysqlSocket) {
        this.mysqlSocket = mysqlSocket;
    }

    public InputStream getMysqlInput() {
        return this.mysqlInput;
    }

    public void setMysqlInput(InputStream mysqlInput) {
        this.mysqlInput = mysqlInput;
    }

    public BufferedOutputStream getMysqlOutput() {
        return this.mysqlOutput;
    }

    public void setMysqlOutput(BufferedOutputStream mysqlOutput) {
        this.mysqlOutput = mysqlOutput;
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    public abstract boolean isSSLEstablished();

    public SocketFactory getSocketFactory() {
        return this.socketFactory;
    }

    public void setSocketFactory(SocketFactory socketFactory) {
        this.socketFactory = socketFactory;
    }

    public long getLastPacketSentTimeMs() {
        return this.lastPacketSentTimeMs;
    }

    public long getLastPacketReceivedTimeMs() {
        return this.lastPacketReceivedTimeMs;
    }

}
