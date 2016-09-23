/*
  Copyright (c) 2014, 2015, Oracle and/or its affiliates. All rights reserved.

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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import com.mysql.cj.core.conf.PropertyDefinitions;

public class SetupFabricTestsuite {

    public static void main(String args[]) throws Exception {
        String hostname = System.getProperty(PropertyDefinitions.SYSP_testsuite_fabric_global_host);
        String port = System.getProperty(PropertyDefinitions.SYSP_testsuite_fabric_global_port);
        String username = System.getProperty(PropertyDefinitions.SYSP_testsuite_fabric_username);
        String password = System.getProperty(PropertyDefinitions.SYSP_testsuite_fabric_password);

        // Create database employees
        Connection c = DriverManager.getConnection("jdbc:mysql://" + hostname + ":" + port + "/mysql", username, password);
        Statement statement = c.createStatement();
        statement.executeUpdate("create database if not exists employees");
        statement.close();
        c.close();
    }

}
