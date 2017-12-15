/*
 * Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package testsuite.x.devapi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.mysql.cj.api.xdevapi.AddResult;
import com.mysql.cj.api.xdevapi.Collection;
import com.mysql.cj.api.xdevapi.DocResult;
import com.mysql.cj.api.xdevapi.Row;
import com.mysql.cj.api.xdevapi.SqlResult;
import com.mysql.cj.api.xdevapi.Table;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.x.core.XDevAPIError;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.JsonNumber;
import com.mysql.cj.xdevapi.JsonString;

@Category(testsuite.x.AsyncTests.class)
public class AsyncQueryTest extends BaseCollectionTestCase {

    @Test
    public void basicAsyncQuery() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        String json = "{'firstName':'Frank', 'middleName':'Lloyd', 'lastName':'Wright'}".replaceAll("'", "\"");
        AddResult res = this.collection.add(json).execute();
        assertTrue(res.getDocumentIds().get(0).matches("[a-f0-9]{32}"));
        assertTrue(res.getDocumentId().matches("[a-f0-9]{32}"));

        CompletableFuture<DocResult> docsF = this.collection.find("firstName like '%Fra%'").executeAsync();
        DocResult docs = docsF.get();
        DbDoc d = docs.next();
        JsonString val = (JsonString) d.get("lastName");
        assertEquals("Wright", val.getString());
    }

    @Test
    public void overlappedAsyncQueries() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        final int NUMBER_OF_QUERIES = 50;

        String json = "{'firstName':'Frank', 'middleName':'Lloyd', 'lastName':'Wright'}".replaceAll("'", "\"");
        AddResult res = this.collection.add(json).execute();
        assertTrue(res.getDocumentIds().get(0).matches("[a-f0-9]{32}"));
        assertTrue(res.getDocumentId().matches("[a-f0-9]{32}"));

        List<CompletableFuture<DocResult>> futures = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_QUERIES; ++i) {
            futures.add(this.collection.find("firstName like '%Fra%'").executeAsync());
        }

        for (int i = 0; i < NUMBER_OF_QUERIES; ++i) {
            DocResult docs = futures.get(i).get();
            DbDoc d = docs.next();
            JsonString val = (JsonString) d.get("lastName");
            assertEquals("Wright", val.getString());
        }
    }

    @Test
    public void basicRowWiseAsync() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        sqlUpdate("drop table if exists rowwise");
        sqlUpdate("create table rowwise (age int)");
        sqlUpdate("insert into rowwise values (1), (1), (1)");

        Table table = this.schema.getTable("rowwise");
        CompletableFuture<Integer> sumF = table.select("age").executeAsync(1, (Integer r, Row row) -> r + row.getInt("age"));
        assertEquals(new Integer(4), sumF.get());
    }

    @Test
    public void syntaxErrorRowWise() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        CompletableFuture<Integer> res = this.collection.find("NON_EXISTING_FUNCTION()").executeAsync(1, (acc, doc) -> 1);
        try {
            res.get();
            fail("Should fail due to non existing function");
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            assertEquals(XDevAPIError.class, cause.getClass());
            assertEquals(MysqlErrorNumbers.ER_SP_DOES_NOT_EXIST, ((XDevAPIError) cause).getErrorCode());
        }
    }

    @Test
    public void syntaxErrorEntireResult() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        CompletableFuture<DocResult> res = this.collection.find("NON_EXISTING_FUNCTION()").executeAsync();
        try {
            res.get();
            fail("Should fail due to non existing function");
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            assertEquals(XDevAPIError.class, cause.getClass());
            assertEquals(MysqlErrorNumbers.ER_SP_DOES_NOT_EXIST, ((XDevAPIError) cause).getErrorCode());
        }
    }

    @Test
    public void insertDocs() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        String json = "{'firstName':'Frank', 'middleName':'Lloyd', 'lastName':'Wright'}".replaceAll("'", "\"");
        CompletableFuture<AddResult> resF = this.collection.add(json).executeAsync();
        CompletableFuture<DocResult> docF = resF.thenCompose((AddResult res) -> {
            assertTrue(res.getDocumentIds().get(0).matches("[a-f0-9]{32}"));
            assertTrue(res.getDocumentId().matches("[a-f0-9]{32}"));
            return this.collection.find("firstName like '%Fra%'").executeAsync();
        });

        DbDoc d = docF.thenApply((DocResult docs) -> docs.next()).get(5, TimeUnit.SECONDS);
        JsonString val = (JsonString) d.get("lastName");
        assertEquals("Wright", val.getString());
    }

    @Test
    public void manyModifications() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        // we guarantee serial execution
        String json = "{'n':1}".replaceAll("'", "\"");
        this.collection.add(json).execute();

        @SuppressWarnings("rawtypes")
        CompletableFuture[] futures = new CompletableFuture[50];

        for (int i = 0; i < 50; ++i) {
            futures[i] = this.collection.modify("true").change("$.n", i).executeAsync();
        }

        // wait for them all to finish
        CompletableFuture.allOf(futures).get();

        DbDoc jd = this.collection.find().execute().next();
        assertEquals(new Integer(49), ((JsonNumber) jd.get("n")).getInteger());
    }

    @Test
    public void sqlUpdate() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        CompletableFuture<SqlResult> resF = this.session.sql("set @cjTestVar = 1").executeAsync();
        resF.thenAccept(res -> {
            assertFalse(res.hasData());
            assertEquals(0, res.getAffectedItemsCount());
            assertEquals(null, res.getAutoIncrementValue());
            assertEquals(0, res.getWarningsCount());
            assertFalse(res.getWarnings().hasNext());
        }).get();
    }

    @Test
    public void sqlQuery() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
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

    @Test
    public void sqlError() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        try {
            CompletableFuture<SqlResult> resF = this.session.sql("select x from dont_create_this_table").executeAsync();
            resF.get();
            fail("Should throw an exception");
        } catch (Exception ex) {
            // expected
        }
    }

    /**
     * This test addresses the "correlation" of messages to their proper async listeners.
     */
    @Test
    public void manyFutures() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        int MANY = 100000;
        Collection coll = this.collection;
        List<CompletableFuture<DocResult>> futures = new ArrayList<>();
        for (int i = 0; i < MANY; ++i) {
            //System.out.println("++++ Write " + i + " set " + i % 3 + " +++++");
            if (i % 3 == 0) {
                futures.add(coll.find("F1  like '%Field%-5'").fields("$._id as _id, $.F1 as F1, $.F2 as F2, $.F3 as F3").executeAsync());
            } else if (i % 3 == 1) {
                futures.add(coll.find("NON_EXISTING_FUNCTION()").fields("$._id as _id, $.F1 as F1, $.F2 as F2, $.F3 as F3").executeAsync()); // Expecting Error
            } else {
                futures.add(coll.find("F3 = ?").bind(106).executeAsync());
            }
        }
        DocResult docs;
        for (int i = 0; i < MANY; ++i) {
            //System.out.println("++++ Read " + i + " set " + i % 3 + " +++++");
            if (i % 3 == 0) {
                //Expect Success and check F1  is like  %Field%-5
                docs = futures.get(i).get();
                assertFalse(docs.hasNext());
            } else if (i % 3 == 1) {
                try {
                    //Expecting Error FUNCTION test.NON_EXISTING_FUNCTION does not exist
                    docs = futures.get(i).get();
                    fail("Expected error");
                } catch (ExecutionException ex) {
                    XDevAPIError err = (XDevAPIError) ex.getCause();
                    assertEquals(MysqlErrorNumbers.ER_SP_DOES_NOT_EXIST, err.getErrorCode());
                }
            } else {
                //Expect Success and check F3 is 106
                docs = futures.get(i).get();
                assertFalse(docs.hasNext());
            }
        }
        System.out.println("Done.");
    }
}
