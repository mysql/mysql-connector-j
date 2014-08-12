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

package demo.fabric;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistryBuilder;

import com.mysql.fabric.hibernate.FabricMultiTenantConnectionProvider;

/**
 * Example using Hibernate 4 Multi-tenancy in DATABASE mode with Fabric.
 */
public class HibernateFabric {
    public static void main(String args[]) throws Exception {

        String hostname = System.getProperty("com.mysql.fabric.testsuite.hostname");
        String port = System.getProperty("com.mysql.fabric.testsuite.port");
        String user = System.getProperty("com.mysql.fabric.testsuite.username");
        String password = System.getProperty("com.mysql.fabric.testsuite.password");
        String database = System.getProperty("com.mysql.fabric.testsuite.database");
        String fabricUsername = System.getProperty("com.mysql.fabric.testsuite.fabricUsername");
        String fabricPassword = System.getProperty("com.mysql.fabric.testsuite.fabricPassword");

        // Using JDBC Fabric connection to create database and table
        Class.forName("com.mysql.fabric.jdbc.FabricMySQLDriver");
        Connection con = DriverManager.getConnection("jdbc:mysql:fabric://" + hostname + ":" + Integer.valueOf(port)
                + "/mysql?fabricServerGroup=fabric_test1_global&fabricUsername=" + fabricUsername + "&fabricPassword=" + fabricPassword, user, password);
        Statement stmt = con.createStatement();
        stmt.executeUpdate("create database if not exists employees");
        con.close();

        con = DriverManager.getConnection("jdbc:mysql:fabric://" + hostname + ":" + Integer.valueOf(port) + "/" + database
                + "?fabricServerGroup=fabric_test1_global&fabricUsername=" + fabricUsername + "&fabricPassword=" + fabricPassword, user, password);
        stmt = con.createStatement();
        stmt.executeUpdate("create database if not exists employees");
        stmt.executeUpdate("drop table if exists employees.employees");
        stmt.executeUpdate("create table employees.employees (emp_no INT PRIMARY KEY, first_name CHAR(40), last_name CHAR(40))");
        stmt.close();

        // we have to wait for replication ....
        Thread.sleep(2000);

        // Using Hibernate
        SessionFactory sf = createSessionFactory("http://" + hostname + ":" + port, user, password, fabricUsername, fabricPassword);

        // add some employees
        for (int i = 1; i < 11; ++i) {
            int j = i;
            // put a few in the other shard
            if ((j % 2) == 0) {
                j += 10000;
            }

            Session session = sf.withOptions().tenantIdentifier("" + j) // choose a db server
                    .openSession();

            // vanilla hibernate code
            session.beginTransaction();
            Employee e = new Employee();
            e.setId(j);
            e.setFirstName("First name of employee " + j);
            e.setLastName("Smith" + j);
            session.save(e);

            session.getTransaction().commit();
            session.close();
        }

        // clean up
        con.createStatement().executeUpdate("drop table employees.employees");
        con.close();

    }

    /**
     * Configuration of session factory with Fabric integration.
     */
    public static SessionFactory createSessionFactory(String fabricUrl, String username, String password, String fabricUser, String fabricPassword)
            throws Exception {
        // creating this here allows passing needed params to the constructor
        FabricMultiTenantConnectionProvider connProvider = new FabricMultiTenantConnectionProvider(fabricUrl, "employees", "employees", username, password,
                fabricUser, fabricPassword);
        ServiceRegistryBuilder srb = new ServiceRegistryBuilder();
        srb.addService(org.hibernate.service.jdbc.connections.spi.MultiTenantConnectionProvider.class, connProvider);
        srb.applySetting("hibernate.dialect", "org.hibernate.dialect.MySQLInnoDBDialect");

        Configuration config = new Configuration();
        config.setProperty("hibernate.multiTenancy", "DATABASE");
        config.addResource("com/mysql/fabric/demo/employee.hbm.xml");
        return config.buildSessionFactory(srb.buildServiceRegistry());
    }
}
