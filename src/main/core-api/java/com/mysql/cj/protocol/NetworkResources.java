/*
 * Copyright (c) 2012, 2019, Oracle and/or its affiliates. All rights reserved.
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
import java.io.OutputStream;
import java.net.Socket;

public class NetworkResources {
    private final Socket mysqlConnection;
    private final InputStream mysqlInput;
    private final OutputStream mysqlOutput;

    public NetworkResources(Socket mysqlConnection, InputStream mysqlInput, OutputStream mysqlOutput) {
        this.mysqlConnection = mysqlConnection;
        this.mysqlInput = mysqlInput;
        this.mysqlOutput = mysqlOutput;
    }

    /**
     * Forcibly closes the underlying socket to MySQL.
     */
    public final void forceClose() {
        try {
            if (!ExportControlled.isSSLEstablished(this.mysqlConnection)) { // Fix for Bug#56979 does not apply to secure sockets.
                try {
                    if (this.mysqlInput != null) {
                        this.mysqlInput.close();
                    }
                } finally {
                    if (this.mysqlConnection != null && !this.mysqlConnection.isClosed() && !this.mysqlConnection.isInputShutdown()) {
                        try {
                            this.mysqlConnection.shutdownInput();
                        } catch (UnsupportedOperationException e) {
                            // Ignore, some sockets do not support this method.
                        }
                    }
                }
            }
        } catch (IOException e) {
            // Can't do anything constructive about this.
        }

        try {
            if (!ExportControlled.isSSLEstablished(this.mysqlConnection)) { // Fix for Bug#56979 does not apply to secure sockets.
                try {
                    if (this.mysqlOutput != null) {
                        this.mysqlOutput.close();
                    }
                } finally {
                    if (this.mysqlConnection != null && !this.mysqlConnection.isClosed() && !this.mysqlConnection.isOutputShutdown()) {
                        try {
                            this.mysqlConnection.shutdownOutput();
                        } catch (UnsupportedOperationException e) {
                            // Ignore, some sockets do not support this method.
                        }
                    }
                }
            }
        } catch (IOException e) {
            // Can't do anything constructive about this.
        }

        try {
            if (this.mysqlConnection != null) {
                this.mysqlConnection.close();
            }
        } catch (IOException e) {
            // Can't do anything constructive about this.
        }
    }
}
