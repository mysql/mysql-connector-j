/*
 * Copyright (c) 2015, 2021, Oracle and/or its affiliates.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.AddResult;
import com.mysql.cj.xdevapi.Collection;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.DocResult;
import com.mysql.cj.xdevapi.JsonNumber;
import com.mysql.cj.xdevapi.JsonString;
import com.mysql.cj.xdevapi.Row;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionFactory;
import com.mysql.cj.xdevapi.SqlResult;

@Tag("Async")
public class AsyncQueryTest extends BaseCollectionTestCase {

    @Test
    public void basicAsyncQuery() throws Exception {
        String json = "{'firstName':'Frank', 'middleName':'Lloyd', 'lastName':'Wright'}".replaceAll("'", "\"");
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            json = json.replace("{", "{\"_id\": \"1\", "); // Inject an _id.
        }
        AddResult res = this.collection.add(json).execute();
        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            assertTrue(res.getGeneratedIds().get(0).matches("[a-f0-9]{28}"));
        } else {
            assertEquals(0, res.getGeneratedIds().size());
        }

        CompletableFuture<DocResult> docsF = this.collection.find("firstName like '%Fra%'").executeAsync();
        DocResult docs = docsF.get();
        DbDoc d = docs.next();
        JsonString val = (JsonString) d.get("lastName");
        assertEquals("Wright", val.getString());
    }

    @Test
    public void overlappedAsyncQueries() throws Exception {
        final int NUMBER_OF_QUERIES = 1000;
        Session sess = null;

        try {
            sess = new SessionFactory().getSession(this.baseUrl);
            Collection coll = sess.getSchema(this.schema.getName()).getCollection(this.collection.getName());

            String json1 = "{'mode': 'sync'}".replaceAll("'", "\"");
            String json2 = "{'mode': 'async'}".replaceAll("'", "\"");
            if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
                // Inject an _id.
                json1 = json1.replace("{", "{\"_id\": \"1\", ");
                json2 = json2.replace("{", "{\"_id\": \"2\", ");
            }
            AddResult res = coll.add(json1).add(json2).execute();
            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
                assertTrue(res.getGeneratedIds().get(0).matches("[a-f0-9]{28}"));
                assertTrue(res.getGeneratedIds().get(1).matches("[a-f0-9]{28}"));
            } else {
                assertEquals(0, res.getGeneratedIds().size());
            }

            List<CompletableFuture<DocResult>> futures = new ArrayList<>();
            for (int i = 0; i < NUMBER_OF_QUERIES; ++i) {
                if (i % 5 == 0) {
                    futures.add(CompletableFuture.completedFuture(coll.find("mode = 'sync'").execute()));
                } else {
                    futures.add(coll.find("mode = 'async'").executeAsync());
                }
            }

            for (int i = 0; i < NUMBER_OF_QUERIES; ++i) {
                try {
                    DocResult docs = futures.get(i).get();
                    DbDoc d = docs.next();
                    JsonString mode = (JsonString) d.get("mode");
                    if (i % 5 == 0) {
                        assertEquals("sync", mode.getString(), "i = " + i);
                    } else {
                        assertEquals("async", mode.getString(), "i = " + i);
                    }
                } catch (Throwable t) {
                    throw new Exception("Error on i = " + i, t);
                }
            }
        } finally {
            if (sess != null) {
                sess.close();
                sess = null;
            }
        }
    }

    @Test
    public void syntaxErrorEntireResult() throws Exception {
        CompletableFuture<DocResult> res = this.collection.find("NON_EXISTING_FUNCTION()").executeAsync();
        try {
            res.get();
            fail("Should fail due to non existing function");
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            assertEquals(XProtocolError.class, cause.getClass());
            assertEquals(MysqlErrorNumbers.ER_SP_DOES_NOT_EXIST, ((XProtocolError) cause).getErrorCode());
        }
    }

    @Test
    public void insertDocs() throws Exception {
        String json = "{'firstName':'Frank', 'middleName':'Lloyd', 'lastName':'Wright'}".replaceAll("'", "\"");
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            json = json.replace("{", "{\"_id\": \"1\", "); // Inject an _id.
        }
        CompletableFuture<AddResult> resF = this.collection.add(json).executeAsync();
        CompletableFuture<DocResult> docF = resF.thenCompose((AddResult res) -> {
            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
                assertTrue(res.getGeneratedIds().get(0).matches("[a-f0-9]{28}"));
            } else {
                assertEquals(0, res.getGeneratedIds().size());
            }
            return this.collection.find("firstName like '%Fra%'").executeAsync();
        });

        DbDoc d = docF.thenApply((DocResult docs) -> docs.next()).get(5, TimeUnit.SECONDS);
        JsonString val = (JsonString) d.get("lastName");
        assertEquals("Wright", val.getString());
    }

    @Test
    public void manyModifications() throws Exception {
        // we guarantee serial execution
        String json = "{'n':1}".replaceAll("'", "\"");
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            json = json.replace("{", "{\"_id\": \"1\", "); // Inject an _id.
        }
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
        CompletableFuture<SqlResult> resF = this.session.sql("select x from dont_create_this_table").executeAsync();
        assertThrows(Exception.class, () -> resF.get());
    }

    /**
     * This test addresses the "correlation" of messages to their proper async listeners.
     * 
     * @throws Exception
     */
    @Test
    public void manyFutures() throws Exception {
        int MANY = 10;//100000;
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
                System.out.println("\nExpect Success and check F1  is like  %Field%-5");
                docs = futures.get(i).get();
                assertFalse(docs.hasNext());
                System.out.println(docs.fetchOne());
            } else if (i % 3 == 1) {
                try {
                    //Expecting Error FUNCTION test.NON_EXISTING_FUNCTION does not exist
                    docs = futures.get(i).get();
                    fail("Expected error");
                } catch (ExecutionException ex) {
                    XProtocolError err = (XProtocolError) ex.getCause();
                    assertEquals(MysqlErrorNumbers.ER_SP_DOES_NOT_EXIST, err.getErrorCode());
                }
            } else {
                //Expect Success and check F3 is 106
                System.out.println("\nExpect Success and check F3 is 106");
                docs = futures.get(i).get();
                assertFalse(docs.hasNext());
                System.out.println(docs.fetchOne());
            }
        }
        System.out.println("Done.");
    }
}
