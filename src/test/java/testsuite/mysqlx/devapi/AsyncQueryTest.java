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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.x.FetchedDocs;
import com.mysql.cj.api.x.Result;
import com.mysql.cj.api.x.Row;
import com.mysql.cj.api.x.SqlResult;
import com.mysql.cj.api.x.Table;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.mysqlx.MysqlxError;
import com.mysql.cj.x.json.JsonDoc;
import com.mysql.cj.x.json.JsonNumber;
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

    @Test
    public void syntaxErrorRowWise() throws Exception {
        CompletableFuture<Integer> res = this.collection.find("NON_EXISTING_FUNCTION()").executeAsync(1, (acc, doc) -> 1);
        try {
            res.get();
            fail("Should fail due to non existing function");
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            assertEquals(MysqlxError.class, cause.getClass());
            assertEquals(MysqlErrorNumbers.ER_SP_DOES_NOT_EXIST, ((MysqlxError) cause).getErrorCode());
        }
    }

    @Test
    public void syntaxErrorEntireResult() throws Exception {
        CompletableFuture<FetchedDocs> res = this.collection.find("NON_EXISTING_FUNCTION()").executeAsync();
        try {
            res.get();
            fail("Should fail due to non existing function");
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            assertEquals(MysqlxError.class, cause.getClass());
            assertEquals(MysqlErrorNumbers.ER_SP_DOES_NOT_EXIST, ((MysqlxError) cause).getErrorCode());
        }
    }

    @Test
    public void insertDocs() throws Exception {
        String json = "{'firstName':'Frank', 'middleName':'Lloyd', 'lastName':'Wright'}".replaceAll("'", "\"");
        CompletableFuture<Result> resF = this.collection.add(json).executeAsync();
        CompletableFuture<FetchedDocs> docF = resF.thenCompose((Result res) -> {
                    assertTrue(res.getLastDocumentId().matches("[a-f0-9]{32}"));
                    return this.collection.find("firstName like '%Fra%'").executeAsync();
                });


        JsonDoc d = docF.thenApply((FetchedDocs docs) -> docs.next()).get(5, TimeUnit.SECONDS);
        JsonString val = (JsonString) d.get("lastName");
        assertEquals("Wright", val.getString());
    }

    @Test
    public void manyModifications() throws Exception {
        // we guarantee serial execution
        String json = "{'n':1}".replaceAll("'", "\"");
        this.collection.add(json).execute();

        CompletableFuture<Result> futures[] = new CompletableFuture[50];

        for (int i = 0; i < 50; ++i) {
            futures[i] = this.collection.modify().change("$.n", i).executeAsync();
        }

        // wait for them all to finish
        CompletableFuture.allOf(futures).get();

        JsonDoc jd = this.collection.find().execute().next();
        assertEquals(new Integer(49), ((JsonNumber) jd.get("n")).getInteger());
    }

    @Test
    public void sqlUpdate() throws Exception {
        CompletableFuture<SqlResult> resF = this.session.sql("set @cjTestVar = 1").executeAsync();
        resF.thenAccept(res -> {
                    assertFalse(res.hasData());
                    assertEquals(0, res.getAffectedItemsCount());
                    assertEquals(null, res.getLastInsertId());
                    assertEquals(0, res.getWarningsCount());
                    assertFalse(res.getWarnings().hasNext());
                }).get();
    }

    @Test
    public void sqlQuery() throws Exception {
        CompletableFuture<SqlResult> resF = this.session.sql("select 1,2,3 from dual").executeAsync();
        resF.thenAccept(res -> {
                    assertTrue(res.hasData());
                    Row r = res.next();
                    assertEquals("1", r.getString(0));
                    assertEquals("2", r.getString(1));
                    assertEquals("3", r.getString(2));
                    assertEquals("1", r.getString("1"));
                    assertEquals("2", r.getString("2"));
                    assertEquals("3", r.getString("3"));
                    assertFalse(res.hasNext());
                }).get();
    }
}
