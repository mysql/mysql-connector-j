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

import com.mysql.fabric.proto.xmlrpc.XmlRpcClient;

/**
 * Basic usage client. print out a bunch of information we can ask for from Fabric
 */
public class Client1_Fabric {
    public static void main(String args[]) throws Exception {
        String hostname = System.getProperty("com.mysql.fabric.testsuite.hostname");
        String port = System.getProperty("com.mysql.fabric.testsuite.port");

        XmlRpcClient fabricClient = new XmlRpcClient("http://" + hostname + ":" + port, null, null);
        System.out.println("Fabrics: " + fabricClient.getFabricNames());
        System.out.println("Groups: " + fabricClient.getGroupNames());
        for (String groupName : fabricClient.getGroupNames()) {
            System.out.println("Group def for '" + groupName + "': " + fabricClient.getServerGroup(groupName).toString().replaceAll("Serv", "\n\tServ"));
        }
        System.out.println("Servers for employees.employees.50: " + fabricClient.getServersForKey("employees.employees", 50));
        System.out.println("Servers for employees.employees.10050: " + fabricClient.getServersForKey("employees.employees", 10050));
        System.out.flush();
        System.out.println("All servers: " + fabricClient.getServerGroups());
        //fabricClient.getGroup("NON_EXISTANT_GROUP");
    }
}
