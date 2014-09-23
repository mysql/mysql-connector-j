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

import java.util.Map;

import com.mysql.fabric.proto.xmlrpc.DigestAuthentication;
import com.mysql.fabric.xmlrpc.Client;
import com.mysql.fabric.xmlrpc.base.MethodCall;
import com.mysql.fabric.xmlrpc.base.MethodResponse;
import com.mysql.fabric.xmlrpc.base.Param;
import com.mysql.fabric.xmlrpc.base.Params;
import com.mysql.fabric.xmlrpc.base.Value;

public class TestXmlRpcCore extends BaseFabricTestCase {

    public TestXmlRpcCore() throws Exception {
        super();
    }

    public void generateAuthHeaders(Client client) throws Exception {
        if ("".equals(this.fabricUsername)) {
            // no auth needed if no username
            return;
        }

        String authenticateHeader = DigestAuthentication.getChallengeHeader(this.fabricUrl);

        Map<String, String> digestChallenge = DigestAuthentication.parseDigestChallenge(authenticateHeader);

        String authorizationHeader = DigestAuthentication.generateAuthorizationHeader(digestChallenge, this.fabricUsername, this.fabricPassword);

        client.setHeader("Authorization", authorizationHeader);
    }

    public void testProtocol() throws Exception {
        if (!this.isSetForFabricTest) {
            return;
        }

        MethodCall mc = new MethodCall();
        MethodResponse resp = null;

        Client client = new Client(this.fabricUrl);

        Params pms = new Params();
        pms.addParam(new Param(new Value(0)));
        /*
         * pms.addParam(new Param(new Value("abc")));
         * pms.addParam(new Param(new Value(false)));
         * pms.addParam(new Param(new Value(-23.345D)));
         * pms.addParam(new Param(new Value((GregorianCalendar) GregorianCalendar.getInstance())));
         * // TODO base64
         * 
         * Struct s2 = new Struct();
         * s2.addMember(new Member("mem 2.1", new Value("qq")));
         * s2.addMember(new Member("mem 2.2", new Value(22.22)));
         * Struct s1 = new Struct();
         * s1.addMember(new Member("mem 1.1", new Value(false)));
         * s1.addMember(new Member("mem 1.2", new Value(22)));
         * s1.addMember(new Member("mem 1.3", new Value(s2)));
         * pms.addParam(new Param(new Value(s1)));
         * 
         * Array a = new Array();
         * a.addValue(new Value(true));
         * a.addValue(new Value(s1));
         * pms.addParam(new Param(new Value(a)));
         */
        mc.setMethodName("dump.servers");
        mc.setParams(pms);

        generateAuthHeaders(client);
        resp = client.execute(mc);
        System.out.println("Servers: " + resp.toString());
        System.out.println();

        mc = new MethodCall(); // to clean params
        mc.setMethodName("sharding.list_definitions");
        generateAuthHeaders(client);
        resp = client.execute(mc);
        System.out.println("Definitions: " + resp.toString());
        System.out.println();

        mc.setMethodName("dump.fabric_nodes");
        generateAuthHeaders(client);
        resp = client.execute(mc);
        System.out.println("Fabrics: " + resp.toString());
        System.out.println();

        mc.setMethodName("group.lookup_groups");
        generateAuthHeaders(client);
        resp = client.execute(mc);
        System.out.println("Groups: " + resp.toString());
        System.out.println();

        pms = new Params();
        pms.addParam(new Param(new Value("fabric_test1_global")));
        mc.setMethodName("group.lookup_servers");
        mc.setParams(pms);
        generateAuthHeaders(client);
        resp = client.execute(mc);
        System.out.println("Servers: " + resp.toString());
        System.out.println();

    }
}
