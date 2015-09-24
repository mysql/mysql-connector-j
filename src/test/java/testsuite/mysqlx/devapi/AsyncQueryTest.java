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

package testsuite.mysqlx.devapi;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.x.FetchedDocs;
import com.mysql.cj.api.x.Result;
import com.mysql.cj.api.x.Row;
import com.mysql.cj.api.x.Table;
import com.mysql.cj.x.json.JsonDoc;
import com.mysql.cj.x.json.JsonString;

public class AsyncQueryTest extends CollectionTest {
    @Before
    @Override
    public void setupCollectionTest() {
        super.setupCollectionTest();
    }

    @After
    @Override
    public void teardownCollectionTest() {
        super.teardownCollectionTest();
    }

    @Test
    public void basicAsyncQuery() throws Exception {
        String json = "{'firstName':'Frank', 'middleName':'Lloyd', 'lastName':'Wright'}".replaceAll("'", "\"");
        Result res = this.collection.add(json).execute();
        assertTrue(res.getLastDocumentId().matches("[a-f0-9]{32}"));

        CompletableFuture<FetchedDocs> docsF = this.collection.find("firstName like '%Fra%'").executeAsync();
        FetchedDocs docs = docsF.get();
        JsonDoc d = docs.next();
        JsonString val = (JsonString) d.get("lastName");
        assertEquals("Wright", val.getString());
    }

    @Test
    public void overlappedAsyncQueries() throws Exception {
        final int NUMBER_OF_QUERIES = 50;

        String json = "{'firstName':'Frank', 'middleName':'Lloyd', 'lastName':'Wright'}".replaceAll("'", "\"");
        Result res = this.collection.add(json).execute();
        assertTrue(res.getLastDocumentId().matches("[a-f0-9]{32}"));

        List<CompletableFuture<FetchedDocs>> futures = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_QUERIES; ++i) {
            futures.add(this.collection.find("firstName like '%Fra%'").executeAsync());
        }

        for (int i = 0; i < NUMBER_OF_QUERIES; ++i) {
            FetchedDocs docs = futures.get(i).get();
            JsonDoc d = docs.next();
            JsonString val = (JsonString) d.get("lastName");
            assertEquals("Wright", val.getString());
        }
    }

    @Test
    public void basicRowWiseAsync() throws Exception {
        sqlUpdate("drop table if exists rowwise");
        sqlUpdate("create table rowwise (age int)");
        sqlUpdate("insert into rowwise values (1), (1), (1)");

        Table table = this.schema.getTable("rowwise");
        CompletableFuture<Integer> sumF = table.select("age").executeAsync(1, (Integer r, Row row) -> r + row.getInt("age"));
        assertEquals(new Integer(4), sumF.get());
    }
}
