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

package testsuite.x.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mysql.cj.MysqlxSession;
import com.mysql.cj.conf.DefaultPropertySet;
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
import com.mysql.cj.xdevapi.DocResult;
import com.mysql.cj.xdevapi.FilterParams;
import com.mysql.cj.xdevapi.StreamingDocResultBuilder;
import com.mysql.cj.xdevapi.UpdateResultBuilder;

/**
 * Tests for (internal) session-level APIs against X Plugin via X Protocol.
 */
public class MysqlxSessionTest extends InternalXBaseTestCase {
    private MysqlxSession session;

    @BeforeEach
    public void setupTestSession() {
        if (this.isSetForXTests) {
            this.session = createTestSession();
        }
    }

    @AfterEach
    public void destroyTestSession() {
        if (this.isSetForXTests && this.session != null) {
            this.session.quit();
        }
    }

    @Test
    public void testCreateDropCollection() {
        assumeTrue(this.isSetForXTests);

        String collName = "toBeCreatedAndDropped";
        XMessageBuilder builder = (XMessageBuilder) this.session.<XMessage>getMessageBuilder();
        try {
            this.session.query(builder.buildDropCollection(getTestDatabase(), collName), new UpdateResultBuilder<>());
        } catch (XProtocolError e) {
            if (e.getErrorCode() != MysqlErrorNumbers.ER_BAD_TABLE_ERROR) {
                throw e;
            }
        }
        assertFalse(this.session.getDataStoreMetadata().tableExists(getTestDatabase(), collName));
        this.session.query(builder.buildCreateCollection(getTestDatabase(), collName), new UpdateResultBuilder<>());
        assertTrue(this.session.getDataStoreMetadata().tableExists(getTestDatabase(), collName));
        this.session.query(builder.buildDropCollection(getTestDatabase(), collName), new UpdateResultBuilder<>());
        assertFalse(this.session.getDataStoreMetadata().tableExists(getTestDatabase(), collName));
        this.session.query(builder.buildCreateCollection(getTestDatabase(), collName), new UpdateResultBuilder<>());
        assertTrue(this.session.getDataStoreMetadata().tableExists(getTestDatabase(), collName));
        this.session.query(builder.buildDropCollection(getTestDatabase(), collName), new UpdateResultBuilder<>());
        assertFalse(this.session.getDataStoreMetadata().tableExists(getTestDatabase(), collName));
        this.session.query(builder.buildCreateCollection(getTestDatabase(), collName), new UpdateResultBuilder<>());
        assertTrue(this.session.getDataStoreMetadata().tableExists(getTestDatabase(), collName));
        this.session.query(builder.buildDropCollection(getTestDatabase(), collName), new UpdateResultBuilder<>());
        assertFalse(this.session.getDataStoreMetadata().tableExists(getTestDatabase(), collName));
    }

    @Test
    public void testGetObjects() {
        assumeTrue(this.isSetForXTests);

        XMessageBuilder builder = (XMessageBuilder) this.session.<XMessage>getMessageBuilder();
        ValueFactory<String> svf = new StringValueFactory(new DefaultPropertySet());
        String collName = "test_get_objects";
        try {
            this.session.query(builder.buildDropCollection(getTestDatabase(), collName), new UpdateResultBuilder<>());
        } catch (XProtocolError e) {
            if (e.getErrorCode() != MysqlErrorNumbers.ER_BAD_TABLE_ERROR) {
                throw e;
            }
        }

        this.session.query(builder.buildCreateCollection(getTestDatabase(), collName), new UpdateResultBuilder<>());

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
        this.session.query(builder.buildDropCollection(getTestDatabase(), collName), new UpdateResultBuilder<>());
    }

    @Test
    public void testInterleavedResults() {
        assumeTrue(this.isSetForXTests);

        XMessageBuilder builder = (XMessageBuilder) this.session.<XMessage>getMessageBuilder();
        String collName = "testInterleavedResults";
        try {
            this.session.query(builder.buildDropCollection(getTestDatabase(), collName), new UpdateResultBuilder<>());
        } catch (XProtocolError e) {
            if (e.getErrorCode() != MysqlErrorNumbers.ER_BAD_TABLE_ERROR) {
                throw e;
            }
        }
        this.session.query(builder.buildCreateCollection(getTestDatabase(), collName), new UpdateResultBuilder<>());

        List<String> stringDocs = new ArrayList<>();
        stringDocs.add("{'_id':'0'}");
        stringDocs.add("{'_id':'1'}");
        stringDocs.add("{'_id':'2'}");
        stringDocs.add("{'_id':'3'}");
        stringDocs.add("{'_id':'4'}");
        stringDocs = stringDocs.stream().map(s -> s.replaceAll("'", "\"")).collect(Collectors.toList());
        this.session.query(builder.buildDocInsert(getTestDatabase(), collName, stringDocs, false), new UpdateResultBuilder<>());

        FilterParams filterParams = new DocFilterParams(getTestDatabase(), collName);
        filterParams.setOrder("$._id");

        DocResult docs1 = this.session.query(((XMessageBuilder) this.session.<XMessage>getMessageBuilder()).buildFind(filterParams),
                new StreamingDocResultBuilder(this.session));
        DocResult docs2 = this.session.query(((XMessageBuilder) this.session.<XMessage>getMessageBuilder()).buildFind(filterParams),
                new StreamingDocResultBuilder(this.session));
        DocResult docs3 = this.session.query(((XMessageBuilder) this.session.<XMessage>getMessageBuilder()).buildFind(filterParams),
                new StreamingDocResultBuilder(this.session));
        DocResult docs4 = this.session.query(((XMessageBuilder) this.session.<XMessage>getMessageBuilder()).buildFind(filterParams),
                new StreamingDocResultBuilder(this.session));
        DocResult docs5 = this.session.query(((XMessageBuilder) this.session.<XMessage>getMessageBuilder()).buildFind(filterParams),
                new StreamingDocResultBuilder(this.session));
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
        assumeTrue(this.isSetForXTests);

        XMessageBuilder builder = (XMessageBuilder) this.session.<XMessage>getMessageBuilder();
        List<Integer> ints = this.session.query(builder.buildSqlStatement("select 2 union select 1"), null,
                r -> r.getValue(0, new IntegerValueFactory(new DefaultPropertySet())), Collectors.toList());
        assertEquals(2, ints.size());
        assertEquals(new Integer(2), ints.get(0));
        assertEquals(new Integer(1), ints.get(1));
    }
}
