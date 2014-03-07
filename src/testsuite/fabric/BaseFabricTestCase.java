/*
  Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.fabric;

import junit.framework.TestCase;

import com.mysql.fabric.jdbc.FabricMySQLDataSource;

public abstract class BaseFabricTestCase extends TestCase {
	protected String hostname;
	protected int port;
	protected String fabricUsername;
	protected String fabricPassword;
	protected String username;
	protected String password;
	protected String database;
	
	protected String fabricUrl;
	protected String baseJdbcUrl;

	public BaseFabricTestCase() throws Exception {
		super();
		Class.forName("com.mysql.fabric.jdbc.FabricMySQLDriver");
		this.hostname = System.getProperty("com.mysql.fabric.testsuite.hostname");
		this.port = Integer.valueOf(System.getProperty("com.mysql.fabric.testsuite.port"));
		this.fabricUsername = System.getProperty("com.mysql.fabric.testsuite.fabricUsername");
		this.fabricPassword = System.getProperty("com.mysql.fabric.testsuite.fabricPassword");
		this.username = System.getProperty("com.mysql.fabric.testsuite.username");
		this.password = System.getProperty("com.mysql.fabric.testsuite.password");
		this.database = System.getProperty("com.mysql.fabric.testsuite.database");

		this.fabricUrl = "http://" + hostname + ":" + port;
		this.baseJdbcUrl = "jdbc:mysql:fabric://" + hostname + ":" + port + "/" + database;
		this.baseJdbcUrl = this.baseJdbcUrl + "?fabricUsername=" + this.fabricUsername + "&fabricPassword=" + this.fabricPassword;
	}

	protected FabricMySQLDataSource getNewDefaultDataSource() {
		FabricMySQLDataSource ds = new FabricMySQLDataSource();
		ds.setServerName(this.hostname);
		ds.setPort(this.port);
		ds.setDatabaseName(this.database);
		ds.setFabricUsername(this.fabricUsername);
		ds.setFabricPassword(this.fabricPassword);
		return ds;
	}
}
