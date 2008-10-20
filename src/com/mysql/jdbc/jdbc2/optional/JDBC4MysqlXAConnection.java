/*
 Copyright  2007 MySQL AB, 2008 Sun Microsystems

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

package com.mysql.jdbc.jdbc2.optional;

import java.sql.SQLException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;

import com.mysql.jdbc.ConnectionImpl;

public class JDBC4MysqlXAConnection extends MysqlXAConnection {

	private Map<StatementEventListener, StatementEventListener> statementEventListeners;

	public JDBC4MysqlXAConnection(ConnectionImpl connection, boolean logXaCommands) throws SQLException {
		super(connection, logXaCommands);
		
		this.statementEventListeners = new HashMap<StatementEventListener, StatementEventListener>();
	}
	
	public synchronized void close() throws SQLException {
		super.close();
		
		if (this.statementEventListeners != null) {
			this.statementEventListeners.clear();
			
			this.statementEventListeners = null;
		}
	}
	
	
	/**
	 * Registers a <code>StatementEventListener</code> with this <code>PooledConnection</code> object.  Components that 
	 * wish to be notified when  <code>PreparedStatement</code>s created by the
     * connection are closed or are detected to be invalid may use this method 
     * to register a <code>StatementEventListener</code> with this <code>PooledConnection</code> object.
	 * <p>
	 * @param listener	an component which implements the <code>StatementEventListener</code> 
	 * 					interface that is to be registered with this <code>PooledConnection</code> object
	 * <p>
	 * @since 1.6
	 */
	public void addStatementEventListener(StatementEventListener listener) {
		synchronized (this.statementEventListeners) {
			this.statementEventListeners.put(listener, listener);
		}
	}
	
	/**
	 * Removes the specified <code>StatementEventListener</code> from the list of 
	 * components that will be notified when the driver detects that a 
	 * <code>PreparedStatement</code> has been closed or is invalid.
	 * <p> 
	 * @param listener	the component which implements the
	 * 					<code>StatementEventListener</code> interface that was previously 
	 * 					registered with this <code>PooledConnection</code> object
	 * <p>
	 * @since 1.6
	 */
	public void removeStatementEventListener(StatementEventListener listener) {
		synchronized (this.statementEventListeners) {
			this.statementEventListeners.remove(listener);
		}
	}
	
	void fireStatementEvent(StatementEvent event) throws SQLException {
		synchronized (this.statementEventListeners) {
			for (StatementEventListener listener : this.statementEventListeners.keySet()) {
				listener.statementClosed(event);
			}
		}
	}
}