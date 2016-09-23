/*
  Copyright (c) 2013, 2016, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
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

import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.fabric.jdbc.FabricMySQLDataSource;
import com.mysql.cj.jdbc.Driver;

import junit.framework.TestCase;

public abstract class BaseFabricTestCase extends TestCase {
    String hostname = System.getProperty(PropertyDefinitions.SYSP_testsuite_fabric_hostname);
    protected String portString = System.getProperty(PropertyDefinitions.SYSP_testsuite_fabric_port);
    protected String fabricUsername = System.getProperty(PropertyDefinitions.SYSP_testsuite_fabric_fabricUsername);
    protected String fabricPassword = System.getProperty(PropertyDefinitions.SYSP_testsuite_fabric_fabricPassword);
    protected String username = System.getProperty(PropertyDefinitions.SYSP_testsuite_fabric_username);
    protected String password = System.getProperty(PropertyDefinitions.SYSP_testsuite_fabric_password);
    protected String database = System.getProperty(PropertyDefinitions.SYSP_testsuite_fabric_database);

    protected String globalHost = System.getProperty(PropertyDefinitions.SYSP_testsuite_fabric_global_host);
    protected String globalPort = System.getProperty(PropertyDefinitions.SYSP_testsuite_fabric_global_port);
    protected String shard1Host = System.getProperty(PropertyDefinitions.SYSP_testsuite_fabric_shard1_host);
    protected String shard1Port = System.getProperty(PropertyDefinitions.SYSP_testsuite_fabric_shard1_port);
    protected String shard2Host = System.getProperty(PropertyDefinitions.SYSP_testsuite_fabric_shard2_host);
    protected String shard2Port = System.getProperty(PropertyDefinitions.SYSP_testsuite_fabric_shard2_port);

    protected int port = 0;
    protected String fabricUrl;
    protected String baseJdbcUrl;

    protected boolean isSetForFabricTest = false;

    public BaseFabricTestCase() throws Exception {
        super();

        this.isSetForFabricTest = (this.hostname != null && this.hostname.trim().length() > 0)
                || (this.portString != null && this.portString.trim().length() > 0) || (this.fabricUsername != null && this.fabricUsername.trim().length() > 0)
                || (this.fabricPassword != null && this.fabricPassword.trim().length() > 0) || (this.username != null && this.username.trim().length() > 0)
                || (this.password != null && this.password.trim().length() > 0) || (this.database != null && this.database.trim().length() > 0)
                || (this.globalHost != null && this.globalHost.trim().length() > 0) || (this.globalPort != null && this.globalPort.trim().length() > 0)
                || (this.shard1Host != null && this.shard1Host.trim().length() > 0) || (this.shard1Port != null && this.shard1Port.trim().length() > 0)
                || (this.shard2Host != null && this.shard2Host.trim().length() > 0) || (this.shard2Port != null && this.shard2Port.trim().length() > 0);

        if (this.isSetForFabricTest) {
            Class.forName(Driver.class.getName());

            if (this.portString != null && this.portString.trim().length() > 0) {
                this.port = Integer.valueOf(this.portString);
            }

            this.fabricUrl = "http://" + this.hostname + ":" + this.port;
            this.baseJdbcUrl = "jdbc:mysql:fabric://" + this.hostname + ":" + this.port + "/" + this.database;
            this.baseJdbcUrl = this.baseJdbcUrl + "?fabricUsername=" + this.fabricUsername + "&fabricPassword=" + this.fabricPassword;
        }

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
