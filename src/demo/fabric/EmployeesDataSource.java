/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package demo.fabric;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import com.mysql.fabric.jdbc.FabricMySQLConnection;
import com.mysql.fabric.jdbc.FabricMySQLDataSource;

/**
 * Demonstrate working with employee data in MySQL Fabric with Connector/J and the JDBC APIs via a DataSource-created connection.
 */
public class EmployeesDataSource {
    public static void main(String args[]) throws Exception {
        String hostname = System.getProperty("com.mysql.fabric.testsuite.hostname");
        String port = System.getProperty("com.mysql.fabric.testsuite.port");
        String database = System.getProperty("com.mysql.fabric.testsuite.database");
        // credentials to authenticate with the SQL nodes
        String user = System.getProperty("com.mysql.fabric.testsuite.username");
        String password = System.getProperty("com.mysql.fabric.testsuite.password");
        // credentials to authenticate to the Fabric node
        String fabricUsername = System.getProperty("com.mysql.fabric.testsuite.fabricUsername");
        String fabricPassword = System.getProperty("com.mysql.fabric.testsuite.fabricPassword");

        // setup the Fabric datasource to create connections
        FabricMySQLDataSource ds = new FabricMySQLDataSource();
        ds.setServerName(hostname);
        ds.setPort(Integer.valueOf(port));
        ds.setDatabaseName(database);
        ds.setFabricUsername(fabricUsername);
        ds.setFabricPassword(fabricPassword);

        // Load the driver if running under Java 5
        if (!com.mysql.jdbc.Util.isJdbc4()) {
            Class.forName("com.mysql.fabric.jdbc.FabricMySQLDriver");
        }

        // 1. Create database and table for our demo
        ds.setDatabaseName("mysql"); // connect to the `mysql` database before creating our `employees` database
        ds.setFabricServerGroup("fabric_test1_global"); // connect to the global group
        Connection rawConnection = ds.getConnection(user, password);
        Statement statement = rawConnection.createStatement();
        statement.executeUpdate("create database if not exists employees");
        statement.close();
        rawConnection.close();

        // We should connect to the global group to run DDL statements, they will be replicated to the server groups for all shards.

        // The 1-st way is to set it's name explicitly via the "fabricServerGroup" datasource property
        ds.setFabricServerGroup("fabric_test1_global");
        rawConnection = ds.getConnection(user, password);
        statement = rawConnection.createStatement();
        statement.executeUpdate("create database if not exists employees");
        statement.close();
        rawConnection.close();

        // The 2-nd way is to get implicitly connected to global group when the shard key isn't provided, ie. set "fabricShardTable" connection property but 
        // don't set "fabricShardKey"
        ds.setFabricServerGroup(null); // clear the setting in the datasource for previous connections
        ds.setFabricShardTable("employee.employees");
        rawConnection = ds.getConnection(user, password);
        // At this point, we have a connection to the global group for  the `employees.employees' shard mapping.
        statement = rawConnection.createStatement();
        statement.executeUpdate("drop table if exists employees");
        statement.executeUpdate("create table employees (emp_no int not null, first_name varchar(50), last_name varchar(50), primary key (emp_no))");

        // 2. Insert data

        // Cast to a Fabric connection to have access to Fabric-specific methods
        FabricMySQLConnection connection = (FabricMySQLConnection) rawConnection;

        // example data used to create employee records
        Integer ids[] = new Integer[] { 1, 2, 10001, 10002 };
        String firstNames[] = new String[] { "John", "Jane", "Andy", "Alice" };
        String lastNames[] = new String[] { "Doe", "Doe", "Wiley", "Wein" };

        // insert employee data
        PreparedStatement ps = connection.prepareStatement("INSERT INTO employees.employees VALUES (?,?,?)");
        for (int i = 0; i < 4; ++i) {
            // choose the shard that handles the data we interested in
            connection.setShardKey(ids[i].toString());

            // perform insert in standard fashion
            ps.setInt(1, ids[i]);
            ps.setString(2, firstNames[i]);
            ps.setString(3, lastNames[i]);
            ps.executeUpdate();
        }

        // 3. Query the data from employees
        System.out.println("Querying employees");
        System.out.format("%7s | %-30s | %-30s%n", "emp_no", "first_name", "last_name");
        System.out.println("--------+--------------------------------+-------------------------------");
        ps = connection.prepareStatement("select emp_no, first_name, last_name from employees where emp_no = ?");
        for (int i = 0; i < 4; ++i) {

            // we need to specify the shard key before accessing the data
            connection.setShardKey(ids[i].toString());

            ps.setInt(1, ids[i]);
            ResultSet rs = ps.executeQuery();
            rs.next();
            System.out.format("%7d | %-30s | %-30s%n", rs.getInt(1), rs.getString(2), rs.getString(3));
            rs.close();
        }
        ps.close();

        // 4. Connect to the global group and clean up
        connection.setServerGroupName("fabric_test1_global");
        statement.executeUpdate("drop table if exists employees");
        statement.close();
        connection.close();
    }
}
