/*
      Copyright (c) 2012, Oracle and/or its affiliates. All rights reserved.
      

      This program is free software; you can redistribute it and/or modify
      it under the terms of version 2 of the GNU General Public License as
      published by the Free Software Foundation.

      There are special exceptions to the terms and conditions of the GPL
      as it is applied to this software. View the full text of the
      exception in file EXCEPTIONS-CONNECTOR-J in the directory of this
      software distribution.

      This program is distributed in the hope that it will be useful,
      but WITHOUT ANY WARRANTY; without even the implied warranty of
      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
      GNU General Public License for more details.

      You should have received a copy of the GNU General Public License
      along with this program; if not, write to the Free Software
      Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA



 */

package com.mysql.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

class NetworkResources {
	private final Socket mysqlConnection;
	private final InputStream mysqlInput;
	private final OutputStream mysqlOutput;
	
	protected NetworkResources(Socket mysqlConnection, InputStream mysqlInput, OutputStream mysqlOutput) {
		this.mysqlConnection = mysqlConnection;
		this.mysqlInput = mysqlInput;
		this.mysqlOutput = mysqlOutput;
	}
	
	/**
     * Forcibly closes the underlying socket to MySQL.
     */
    protected final void forceClose() {	
        try {
        	try {
	            if (this.mysqlInput != null) {
	                this.mysqlInput.close();
	            }
        	} finally {
	            if (this.mysqlConnection != null && !this.mysqlConnection.isClosed() && !this.mysqlConnection.isInputShutdown()) {
	            	try {
	            		this.mysqlConnection.shutdownInput();
	            	} catch (UnsupportedOperationException ex) {
	            		// ignore, some sockets do not support this method
	            	}
	            }
        	}
        } catch (IOException ioEx) {
            // we can't do anything constructive about this
        }

        try {
        	try {
	            if (this.mysqlOutput != null) {
	                this.mysqlOutput.close();
	            }
        	} finally {
        		if (this.mysqlConnection != null && !this.mysqlConnection.isClosed() && !this.mysqlConnection.isOutputShutdown()) {
        			try {
        				this.mysqlConnection.shutdownOutput();
        			} catch (UnsupportedOperationException ex) {
	            		// ignore, some sockets do not support this method
	            	}
        		}
    		}
        } catch (IOException ioEx) {
            // we can't do anything constructive about this
        }

        try {
            if (this.mysqlConnection != null) {
                this.mysqlConnection.close();
            }
        } catch (IOException ioEx) {
            // we can't do anything constructive about this
        }
    }
}
