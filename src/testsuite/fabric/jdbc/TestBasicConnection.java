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

package testsuite.fabric.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import testsuite.fabric.BaseFabricTestCase;

import com.mysql.fabric.jdbc.FabricMySQLDataSource;

public class TestBasicConnection extends BaseFabricTestCase {

    public TestBasicConnection() throws Exception {
        super();
    }

    /**
     * Test that we can make a connection with a URL, given a server group name
     */
    public void testConnectionUrl() throws Exception {
        if (!this.isSetForFabricTest) {
            return;
        }
        String url = this.baseJdbcUrl + "&fabricServerGroup=fabric_test1_global";
        Connection c = DriverManager.getConnection(url, this.username, this.password);
        ResultSet rs = c.createStatement().executeQuery("select user()");
        rs.next();
        String userFromDb = rs.getString(1).split("@")[0];
        assertEquals(this.username, userFromDb);
        rs.close();
        c.close();
    }

    /**
     * Test that we can connect with the data source, given a server group name
     */
    public void testConnectionDataSource() throws Exception {
        if (!this.isSetForFabricTest) {
            return;
        }
        FabricMySQLDataSource ds = getNewDefaultDataSource();
        ds.setFabricServerGroup("fabric_test1_global");
        Connection c = ds.getConnection(this.username, this.password);
        // same as above
        ResultSet rs = c.createStatement().executeQuery("select user()");
        rs.next();
        String userFromDb = rs.getString(1).split("@")[0];
        assertEquals(this.username, userFromDb);
        rs.close();
        c.close();
    }
}
