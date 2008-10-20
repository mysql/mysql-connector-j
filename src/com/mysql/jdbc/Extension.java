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

package com.mysql.jdbc;

import java.sql.SQLException;
import java.util.Properties;

public interface Extension {

	/**
	 * Called once per connection that wants to use the extension
	 * 
	 * The properties are the same ones passed in in the URL or arguments to
	 * Driver.connect() or DriverManager.getConnection().
	 * 
	 * @param conn the connection for which this extension is being created
	 * @param props configuration values as passed to the connection. Note that
	 * in order to support javax.sql.DataSources, configuration properties specific
	 * to an interceptor <strong>must</strong> be passed via setURL() on the
	 * DataSource. Extension properties are not exposed via 
	 * accessor/mutator methods on DataSources.
	 * 
	 * @throws SQLException should be thrown if the the Extension
	 * can not initialize itself.
	 */
	
	public abstract void init(Connection conn, Properties props) throws SQLException;
	
	/**
	 * Called by the driver when this extension should release any resources
	 * it is holding and cleanup internally before the connection is
	 * closed.
	 */
	public abstract void destroy();
}
