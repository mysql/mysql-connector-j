/*
 Copyright (c) 2002, 2012, Oracle and/or its affiliates. All rights reserved.
 

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

package testsuite.simple;

import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Properties;

import com.mysql.jdbc.ConnectionLifecycleInterceptor;

public class TestLifecycleInterceptor implements ConnectionLifecycleInterceptor {
	static int transactionsBegun = 0;
	static int transactionsCompleted = 0;

	public void close() throws SQLException {

	}

	public boolean commit() throws SQLException {

		return true;
	}

	public boolean rollback() throws SQLException {

		return true;
	}

	public boolean rollback(Savepoint s) throws SQLException {

		return true;
	}

	public boolean setAutoCommit(boolean flag) throws SQLException {

		return true;
	}

	public boolean setCatalog(String catalog) throws SQLException {

		return true;
	}

	public boolean transactionBegun() throws SQLException {
		transactionsBegun++;
		return true;
	}

	public boolean transactionCompleted() throws SQLException {
		transactionsCompleted++;
		return true;
	}

	public void destroy() {

	}

	public void init(com.mysql.jdbc.Connection conn, Properties props)
			throws SQLException {

	}

}