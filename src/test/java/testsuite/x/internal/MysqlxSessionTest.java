/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

package testsuite.x.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.MysqlxSession;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.protocol.x.XMessage;
import com.mysql.cj.protocol.x.XMessageBuilder;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.result.IntegerValueFactory;
import com.mysql.cj.result.StringValueFactory;
import com.mysql.cj.result.ValueFactory;
import com.mysql.cj.xdevapi.DatabaseObject;
import com.mysql.cj.xdevapi.DatabaseObject.DbObjectType;
import com.mysql.cj.xdevapi.DocFilterParams;
import com.mysql.cj.xdevapi.DocResultImpl;
import com.mysql.cj.xdevapi.FilterParams;

/**
 * Tests for (internal) session-level APIs against X Plugin via X Protocol.
 */
public class MysqlxSessionTest extends InternalXBaseTestCase {
    private MysqlxSession session;

    @Before
    public void setupTestSession() {
        if (this.isSetForXTests) {
            this.session = createTestSession();
        }
    }

    @After
    public void destroyTestSession() {
        if (this.isSetForXTests && this.session != null) {
            this.session.quit();
        }
    }

    @Test
    public void testCreateDropCollection() {
        if (!this.isSetForXTests) {
            return;
        }
        String collName = "toBeCreatedAndDropped";
        XMessageBuilder builder = (XMessageBuilder) this.session.<XMessage> getMessageBuilder();
        try {
            this.session.sendMessage(builder.buildDropCollection(getTestDatabase(), collName));
        } catch (XProtocolError e) {
            if (e.getErrorCode() != MysqlErrorNumbers.ER_BAD_TABLE_ERROR) {
                throw e;
            }
        }
        assertFalse(this.session.getDataStoreMetadata().tableExists(getTestDatabase(), collName));
        this.session.sendMessage(builder.buildCreateCollection(getTestDatabase(), collName));
        assertTrue(this.session.getDataStoreMetadata().tableExists(getTestDatabase(), collName));
        this.session.sendMessage(builder.buildDropCollection(getTestDatabase(), collName));
        assertFalse(this.session.getDataStoreMetadata().tableExists(getTestDatabase(), collName));
        this.session.sendMessage(builder.buildCreateCollection(getTestDatabase(), collName));
        assertTrue(this.session.getDataStoreMetadata().tableExists(getTestDatabase(), collName));
        this.session.sendMessage(builder.buildDropCollection(getTestDatabase(), collName));
        assertFalse(this.session.getDataStoreMetadata().tableExists(getTestDatabase(), collName));
        this.session.sendMessage(builder.buildCreateCollection(getTestDatabase(), collName));
        assertTrue(this.session.getDataStoreMetadata().tableExists(getTestDatabase(), collName));
        this.session.sendMessage(builder.buildDropCollection(getTestDatabase(), collName));
        assertFalse(this.session.getDataStoreMetadata().tableExists(getTestDatabase(), collName));
    }

    @Test
    public void testGetObjects() {
        if (!this.isSetForXTests) {
            return;
        }
        XMessageBuilder builder = (XMessageBuilder) this.session.<XMessage> getMessageBuilder();
        ValueFactory<String> svf = new StringValueFactory();
        String collName = "test_get_objects";
        try {
            this.session.sendMessage(builder.buildDropCollection(getTestDatabase(), collName));
        } catch (XProtocolError e) {
            if (e.getErrorCode() != MysqlErrorNumbers.ER_BAD_TABLE_ERROR) {
                throw e;
            }
        }

        this.session.sendMessage(builder.buildCreateCollection(getTestDatabase(), collName));

        Set<String> strTypes = Arrays.stream(new DbObjectType[] { DbObjectType.COLLECTION }).map(DatabaseObject.DbObjectType::toString)
                .collect(Collectors.toSet());
        Predicate<com.mysql.cj.result.Row> rowFiler = r -> (strTypes).contains(r.getValue(1, svf));
        Function<com.mysql.cj.result.Row, String> rowToName = r -> r.getValue(0, svf);

        List<String> collNames = this.session.query(builder.buildListObjects(getTestDatabase(), null), rowFiler, rowToName, Collectors.toList());
        assertTrue(collNames.contains(collName));
        collNames = this.session.query(builder.buildListObjects(getTestDatabase(), "none%"), rowFiler, rowToName, Collectors.toList());
        assertFalse(collNames.contains(collName));
        collNames = this.session.query(builder.buildListObjects(getTestDatabase(), "%get_obj%"), rowFiler, rowToName, Collectors.toList());
        assertTrue(collNames.contains(collName));
        this.session.sendMessage(builder.buildDropCollection(getTestDatabase(), collName));
    }

    @Test
    public void testInterleavedResults() {
        if (!this.isSetForXTests) {
            return;
        }
        XMessageBuilder builder = (XMessageBuilder) this.session.<XMessage> getMessageBuilder();
        String collName = "testInterleavedResults";
        try {
            this.session.sendMessage(builder.buildDropCollection(getTestDatabase(), collName));
        } catch (XProtocolError e) {
            if (e.getErrorCode() != MysqlErrorNumbers.ER_BAD_TABLE_ERROR) {
                throw e;
            }
        }
        this.session.sendMessage(builder.buildCreateCollection(getTestDatabase(), collName));

        List<String> stringDocs = new ArrayList<>();
        stringDocs.add("{'_id':'0'}");
        stringDocs.add("{'_id':'1'}");
        stringDocs.add("{'_id':'2'}");
        stringDocs.add("{'_id':'3'}");
        stringDocs.add("{'_id':'4'}");
        stringDocs = stringDocs.stream().map(s -> s.replaceAll("'", "\"")).collect(Collectors.toList());
        this.session.sendMessage(builder.buildDocInsert(getTestDatabase(), collName, stringDocs, false));

        FilterParams filterParams = new DocFilterParams(getTestDatabase(), collName);
        filterParams.setOrder("$._id");

        DocResultImpl docs1 = this.session.find(filterParams, metadata -> (rows, task) -> new DocResultImpl(rows, task));
        DocResultImpl docs2 = this.session.find(filterParams, metadata -> (rows, task) -> new DocResultImpl(rows, task));
        DocResultImpl docs3 = this.session.find(filterParams, metadata -> (rows, task) -> new DocResultImpl(rows, task));
        DocResultImpl docs4 = this.session.find(filterParams, metadata -> (rows, task) -> new DocResultImpl(rows, task));
        DocResultImpl docs5 = this.session.find(filterParams, metadata -> (rows, task) -> new DocResultImpl(rows, task));
        assertTrue(docs5.hasNext());
        assertTrue(docs4.hasNext());
        assertTrue(docs3.hasNext());
        assertTrue(docs2.hasNext());
        assertTrue(docs1.hasNext());
        for (int i = 0; i < 5; ++i) {
            assertEquals("{\n\"_id\" : \"" + i + "\"\n}", docs1.next().toFormattedString());
            assertEquals("{\n\"_id\" : \"" + i + "\"\n}", docs2.next().toFormattedString());
            assertEquals("{\n\"_id\" : \"" + i + "\"\n}", docs3.next().toFormattedString());
            assertEquals("{\n\"_id\" : \"" + i + "\"\n}", docs4.next().toFormattedString());
            assertEquals("{\n\"_id\" : \"" + i + "\"\n}", docs5.next().toFormattedString());
        }
        assertFalse(docs5.hasNext());
        assertFalse(docs4.hasNext());
        assertFalse(docs3.hasNext());
        assertFalse(docs2.hasNext());
        assertFalse(docs1.hasNext());
        // let the session be closed with all of these "open"
    }

    @Test
    public void testGenericQuery() {
        if (!this.isSetForXTests) {
            return;
        }
        XMessageBuilder builder = (XMessageBuilder) this.session.<XMessage> getMessageBuilder();
        List<Integer> ints = this.session.query(builder.buildSqlStatement("select 2 union select 1"), null, r -> r.getValue(0, new IntegerValueFactory()),
                Collectors.toList());
        assertEquals(2, ints.size());
        assertEquals(new Integer(2), ints.get(0));
        assertEquals(new Integer(1), ints.get(1));
    }
}
