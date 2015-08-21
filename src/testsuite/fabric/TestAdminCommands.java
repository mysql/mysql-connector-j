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

package testsuite.fabric;

import com.mysql.fabric.FabricCommunicationException;
import com.mysql.fabric.proto.xmlrpc.XmlRpcClient;

/**
 * Tests for admin commands.
 */
public class TestAdminCommands extends BaseFabricTestCase {

    private XmlRpcClient client;

    public TestAdminCommands() throws Exception {
        super();
    }

    @Override
    public void setUp() throws Exception {
        if (this.isSetForFabricTest) {
            this.client = new XmlRpcClient(this.fabricUrl, this.fabricUsername, this.fabricPassword);
        }
    }

    public void testCreateGroup() throws Exception {
        if (!this.isSetForFabricTest) {
            return;
        }
        String testGroupName = "CJ-testGroupName";
        try {
            this.client.destroyGroup(testGroupName);
        } catch (FabricCommunicationException ex) {
        }

        this.client.createGroup(testGroupName);
        // will throw an exception if the group wasn't created
        this.client.destroyGroup(testGroupName);
    }
}
