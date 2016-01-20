/*
  Copyright (c) 2014, 2016, Oracle and/or its affiliates. All rights reserved.

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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mysql.fabric.proto.xmlrpc.ResultSetParser;

import junit.framework.TestCase;

/**
 * Tests for Fabric XML-RPC ResultSetParser.
 */
public class TestResultSetParser extends TestCase {
    // Example response data represented in tests:
    // [1, 5ca1ab1e-a007-feed-f00d-cab3fe13249e, 0, , 
    // [
    //  {rows=
    //        [[5e26a7ab-de84-11e2-a885-df73a3d95316, fabric_test1_global, 127.0.0.1, 3401, 3, 3, 1.0],
    //         [07eee140-d466-11e3-abdf-dfb2de41aa92, fabric_test1_shard1, 127.0.0.1, 3402, 1, 2, 1.0]],
    //   info={names=[server_uuid, group_id, host, port, mode, status, weight]}}
    // ]]
    List<Map<String, ?>> exampleServersResultSet;

    public TestResultSetParser(String name) {
        super(name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setUp() throws Exception {
        final Map<String, ?> columns = new HashMap<String, List<String>>() {
            private static final long serialVersionUID = 1L;

            {
                put("names", Arrays.asList(new String[] { "server_uuid", "group_id", "host", "port", "mode", "status", "weight" }));
            }
        };
        final List<?> row1 = Arrays.asList(new Object[] { "5e26a7ab-de84-11e2-a885-df73a3d95316", "fabric_test1_global", "127.0.0.1", 3401, 3, 3, 1.0 });
        final List<?> row2 = Arrays.asList(new Object[] { "07eee140-d466-11e3-abdf-dfb2de41aa92", "fabric_test1_shard1", "127.0.0.1", 3402, 1, 2, 1.0 });
        final List<?> rows = Arrays.asList(new List[] { row1, row2 });
        Map<String, ?> resultData = new HashMap<String, Object>() {
            private static final long serialVersionUID = 1L;

            {
                put("info", columns);
                put("rows", rows);
            }
        };
        this.exampleServersResultSet = new ResultSetParser().parse((Map<String, ?>) resultData.get("info"), (List<List<Object>>) resultData.get("rows"));
    }

    public void testExampleData() throws Exception {
        Map<String, ?> row = this.exampleServersResultSet.get(0);
        assertEquals("5e26a7ab-de84-11e2-a885-df73a3d95316", row.get("server_uuid"));
        assertEquals("fabric_test1_global", row.get("group_id"));
        assertEquals("127.0.0.1", row.get("host"));
        assertEquals(3401, row.get("port"));
        assertEquals(3, row.get("mode"));
        assertEquals(3, row.get("status"));
        assertEquals(1.0, row.get("weight"));

        row = this.exampleServersResultSet.get(1);
        assertEquals("07eee140-d466-11e3-abdf-dfb2de41aa92", row.get("server_uuid"));
        assertEquals("fabric_test1_shard1", row.get("group_id"));
        assertEquals("127.0.0.1", row.get("host"));
        assertEquals(3402, row.get("port"));
        assertEquals(1, row.get("mode"));
        assertEquals(2, row.get("status"));
        assertEquals(1.0, row.get("weight"));

        assertEquals(2, this.exampleServersResultSet.size());
    }
}
