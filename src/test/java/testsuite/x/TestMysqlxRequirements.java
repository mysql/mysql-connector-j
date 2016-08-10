/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.x;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import com.mysql.cj.api.x.Collection;
import com.mysql.cj.api.x.NodeSession;
import com.mysql.cj.api.x.Schema;
import com.mysql.cj.api.x.Table;
import com.mysql.cj.api.x.XSession;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.conf.url.ConnectionUrl;
import com.mysql.cj.mysqlx.devapi.SessionImpl;

/**
 * Tests for MySQLx DevAPI requirements.
 */
public class TestMysqlxRequirements extends BaseMysqlxTestCase {

    /**
     * NodeSession [10]
     * NodeSession.Connect.Single [6]
     * NodeSession.Connect.DataSource [7]
     * NodeSession.Connect.Mysqls [8] [9] - not supported in first version
     * 
     * @throws Exception
     */
    @Test
    @Ignore("needs implemented")
    public void testNodeSessionCreation() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        // TODO fill in the next pattern

        NodeSession sess;

        String url = ""; // TODO test different URLs
        sess = getNodeSession(url);
        sess.close();

        Properties props = new Properties(); // TODO test different properties
        sess = getNodeSession(props);
        sess.close();
    }

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
        if (!this.isSetForMySQLxTests) {
            return;
        }
        XSession sess;

        String url = this.baseUrl;
        sess = getSession(url);
        sess.close();

        // TODO test different URLs

        ConnectionUrl conUrl = ConnectionUrl.getConnectionUrlInstance(url, null);

        Properties props = conUrl.getMainHost().exposeAsProperties();
        sess = getSession(props);
        sess.close();

        // test connection without port specification
        props.remove(PropertyDefinitions.PORT_PROPERTY_KEY);
        sess = getSession(props);
        ConnectionUrl conUrl1 = ConnectionUrl.getConnectionUrlInstance(sess.getUri(), null);
        assertEquals("33060", conUrl1.getMainHost().exposeAsProperties().getProperty(PropertyDefinitions.PORT_PROPERTY_KEY));
        sess.close();

        // TODO test different properties
    }

    /**
     * SQL incl. bind
     * 
     * @throws Exception
     */
    @Test
    @Ignore("needs implemented")
    public void testNodeSessionMethods() throws Exception {
        if (!this.isSetForMySQLxTests) {
            return;
        }

        // TODO fill in the next pattern
        NodeSession sess = getNodeSession(""); // TODO set URL

        sess.getDefaultSchema(); // according to spec it's available if NodeSession created with DataSource file

        String schema = ""; // TODO set name
        sess.getSchema(schema); // no-op, error or allowed?

        //String sql = ""; // TODO set query
        //sess.executeSql(sql);

        //sess.executeSql(sql, "v1", "v2"); // TODO test binding

        sess.close();

        // out of requirements
        sess.createSchema("name"); // TODO set name
        sess.dropSchema("name"); // TODO set name
        sess.getSchemas();
        sess.getUri();

    }

    /**
     * 
     * @throws Exception
     */
    @Test
    public void testSessionMethods() throws Exception {
        if (!this.isSetForMySQLxTests) {
            return;
        }

        XSession sess = getSession(this.baseUrl);
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
    @Ignore("needs implemented")
    public void testSchemaMethods() throws Exception {
        if (!this.isSetForMySQLxTests) {
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
    @Ignore("needs implemented")
    public void testCollectionMethods() throws Exception {
        if (!this.isSetForMySQLxTests) {
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
    @Ignore("needs implemented")
    public void testTableMethods() throws Exception {
        if (!this.isSetForMySQLxTests) {
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
    @Ignore("needs implemented")
    public void testViewMethods() throws Exception {
        if (!this.isSetForMySQLxTests) {
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
    @Ignore("needs implemented")
    public void testExecutionContext() throws Exception {
        if (!this.isSetForMySQLxTests) {
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
    @Ignore("needs implemented")
    public void testResultMethods() throws Exception {
        if (!this.isSetForMySQLxTests) {
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
    @Ignore("needs implemented")
    public void testExecution() throws Exception {
        if (!this.isSetForMySQLxTests) {
            return;
        }

    }

}
