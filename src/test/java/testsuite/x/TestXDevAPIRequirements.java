/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates. All rights reserved.
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

package testsuite.x;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Properties;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.xdevapi.Collection;
import com.mysql.cj.xdevapi.Schema;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionImpl;
import com.mysql.cj.xdevapi.Table;

/**
 * Tests for X DevAPI requirements.
 */
public class TestXDevAPIRequirements extends BaseXDevAPITestCase {
    /**
     * Session [11]
     * Session.Connect.Single [6]
     * Session.Connect.DataSource [7]
     * Session.Connect.Mysqls [8] [9] - not supported in first version
     * 
     * @throws Exception
     */
    @Test
    public void testSessionCreation() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        Session sess;

        String url = this.baseUrl;
        sess = getSession(url);
        sess.close();

        // TODO test different URLs

        ConnectionUrl conUrl = ConnectionUrl.getConnectionUrlInstance(url, null);

        Properties props = conUrl.getMainHost().exposeAsProperties();
        sess = getSession(props);
        sess.close();

        // test connection without port specification
        if (props.getProperty(PropertyKey.PORT.getKeyName()).equals("33060")) {
            props.remove(PropertyKey.PORT.getKeyName());
            sess = getSession(props);
            ConnectionUrl conUrl1 = ConnectionUrl.getConnectionUrlInstance(sess.getUri(), null);
            assertEquals("33060", conUrl1.getMainHost().exposeAsProperties().getProperty(PropertyKey.PORT.getKeyName()));
            sess.close();
        }
        // TODO test different properties
    }

    /**
     * Test session methods.
     * 
     * @throws Exception
     */
    @Test
    public void testSessionMethods() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        Session sess = getSession(this.baseUrl);
        assertNotNull(sess);
        assertTrue(sess instanceof SessionImpl);

        Schema sch = sess.getDefaultSchema();
        sch.getName();

        sess.getSchema(""); // TODO set name

        sess.close();

        // out of requirements
        //sess.createSchema("name"); // TODO set name
        //sess.dropSchema("name"); // TODO set name
        //sess.getSchemas();
        //sess.getUri();

        //String sql = ""; // TODO set query
        //sess.executeSql(sql);
    }

    /**
     * Schema browsing Schema.getCollections() [44]
     * Schema browsing Schema.getTables() [45]
     * Schema access Schema.getCollection() [47]
     * Schema access Schema.getCollectionAsTable() [50]
     * Schema access Schema.getTable() [48]
     * Schema - who am I? [51]
     * Schema - am I real? [52]
     * Schema - DDL create [55]
     * Schema.drop [53]
     * 
     * @throws Exception
     */
    @Test
    @Disabled("needs implemented")
    public void testSchemaMethods() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        // TODO fill in the next pattern

        Schema schema = getSession("").getDefaultSchema(); // TODO set URL

        // Schema browsing Schema.getCollections() [44]
        schema.getCollections();

        // Schema browsing Schema.getTables() [45]
        schema.getTables();

        // Schema access Schema.getCollection() [47]
        schema.getCollection(""); // TODO set name

        // Schema access Schema.getCollectionAsTable() [50]
        schema.getCollectionAsTable(""); // TODO set name

        // Schema access Schema.getTable() [48]
        schema.getTable(""); // TODO set name

        // Schema - who am I? [51]
        schema.getName();

        // Schema - am I real? [52]
        schema.existsInDatabase();

        // Schema - DDL create [55]
        schema.createCollection(""); // TODO set name

        // inherited
        schema.getSchema(); // "this" ???
        schema.getSession(); // ???
    }

    /**
     * Collection.createCollection [16]
     * Collection Index Creation [59]
     * Collection.getCollection [16]
     * Collection.add [17]
     * Collection.find basics [18]
     * Collection.modify (incl. all array_*) [21]
     * Collection.remove [22]
     * Collection.as [41]
     * Collection.count [43]
     * Collection - who am I? [51]
     * Collection - am I real? [52]
     * Collection.drop [53]
     * 
     * @throws Exception
     */
    @Test
    @Disabled("needs implemented")
    public void testCollectionMethods() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        // TODO fill in the next pattern

        // Collection.createCollection [16]
        Collection collection = getSession("").getDefaultSchema().createCollection(""); // TODO set URL and collection name

        // Collection Index Creation [59]
        // TODO spec in progress

        // Collection.getCollection [16]
        collection = getSession("").getDefaultSchema().getCollection(""); // TODO set URL and collection name

        // Collection.add [17]
        collection.add(new HashMap<String, String>()); // TODO set correct parameter
        collection.add("jsonString"); // TODO set correct parameter

        // Collection.find basics [18]
        collection.find("searchCondition"); // TODO set correct parameter

        // Collection.modify (incl. all array_*) [21]
        collection.modify("searchCondition"); // TODO set correct parameter

        // Collection.remove [22]
        collection.remove("searchCondition"); // TODO set correct parameter

        // Collection.as [41]
        // collection.as("alias"); // TODO set correct parameter

        // Collection.count [43]
        collection.count();

        // Collection - who am I? [51]
        collection.getName();

        // Collection - am I real? [52]
        collection.existsInDatabase();

        // inherited
        collection.getSchema();
        collection.getSession();

        // poor spec
        collection.newDoc();
    }

    /**
     * Table.createTable [26] - not supported in first version
     * Table Index Creation [60] - not supported in first version
     * Table.insert [28]
     * Table.select basics [27]
     * Table.update [29]
     * Table.delete [30]
     * Table.alter [31] - not supported in first version
     * Table.join (tables) [40] - not supported in first version
     * Table.as [42]
     * Table.count [43]
     * Table - who am I? [51]
     * Table - am I real? [52]
     * Table.drop [53] - not supported in first version
     * 
     * @throws Exception
     */
    @Test
    @Disabled("needs implemented")
    public void testTableMethods() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        // TODO fill in the next pattern

        Table table = getSession("").getDefaultSchema().getCollectionAsTable("name"); // TODO set URL and collection name

        // Table.insert [28]
        // Object fieldsAndValues = null;
        // table.insert(fieldsAndValues); // TODO set correct parameter, expand statements
        table.insert("fields"); // TODO set correct parameter, expand statements

        // Table.select basics [27]
        table.select("searchFields"); // TODO set correct parameter, expand statements

        // Table.update [29]
        table.update(); // TODO expand statements

        // Table.delete [30]
        table.delete(); // TODO expand statements

        // Table.as [42]
        // table.as("alias"); // TODO set correct parameter

        // Table.count [43]
        table.count();

        // Table - who am I? [51]
        table.getName();

        // Table - am I real? [52]
        table.existsInDatabase();

        // inherited
        table.getSchema();
        table.getSession();
    }

    /**
     * View.select [54]
     * View.count [43]
     * View - who am I? [51]
     * View - am I real? [52]
     * View.drop [53] - not supported in first version
     * 
     * @throws Exception
     */
    @Test
    @Disabled("needs implemented")
    public void testViewMethods() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        // TODO fill in the next pattern, Views are treated as Tables

        Table view = getSession("").getDefaultSchema().getTable("name");  // getView("name"); // TODO set URL and collection name

        view.isView();

        // View.select [54]
        view.select("searchFields"); // TODO set correct parameter, expand statements

        // View.count [43]
        view.count();

        // View - who am I? [51]
        view.getName();

        // View - am I real? [52]
        view.existsInDatabase();

        // inherited
        view.getSchema();
        view.getSession();
    }

    /**
     * Context.Session [33] - not supported in first version
     * Context.Transaction [34] - not supported in first version
     * Context.Batch.Collection [35] - not supported in first version
     * Context.Batch.Table [35] - not supported in first version
     * Context.Batch.SQL / executeSql() [35] - not supported in first version
     * Context Nesting [36] - not supported in first version
     * Context option Custom error handling [56] - not supported in first version
     * Context option Consistency [57] - not supported in first version
     * Context option Replication Factor [58] - not supported in first version
     * 
     * @throws Exception
     */
    @Test
    @Disabled("needs implemented")
    public void testExecutionContext() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
    }

    /**
     * Result.Basics [38]
     * Result client side buffering
     * Results.Multi Resultset [38]
     * 
     * @throws Exception
     */
    @Test
    @Disabled("needs implemented")
    public void testResultMethods() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
    }

    /**
     * CRUD.Synchronous execution [14]
     * CRUD.Asynchronous execution [14]
     * CRUD.Parameter Binding [15]
     * Document class DbDoc [25]
     * INSERT.Streaming [37]
     * 
     * @throws Exception
     */
    @Test
    @Disabled("needs implemented")
    public void testExecution() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
    }
}
