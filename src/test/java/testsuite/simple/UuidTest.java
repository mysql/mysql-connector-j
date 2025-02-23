/*
 * Copyright (c) 2025, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package testsuite.simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import testsuite.BaseTestCase;

public class UuidTest extends BaseTestCase {

    @Test
    public void testUuidGetObjectFromNull() throws SQLException {
        this.rs = this.stmt.executeQuery("select null col");
        assertTrue(this.rs.next());
        assertNull(this.rs.getObject(1, UUID.class));

        this.rs.close();
        this.rs = null;
    }

    @Test
    public void testUuidGetObjectFromEmpty() throws SQLException {
        this.rs = this.stmt.executeQuery("select '' col");
        assertTrue(this.rs.next());
        assertThrows(SQLException.class, "Cannot convert string '' to java.util.UUID value", () -> this.rs.getObject(1, UUID.class));

        this.rs.close();
        this.rs = null;
    }

    @Test
    public void testUuidGetObjectFromInvalidStr() throws SQLException {
        this.rs = this.stmt.executeQuery("select 'bogus uuid' col");
        assertTrue(this.rs.next());
        assertThrows(SQLException.class, "Cannot convert string 'bogus uuid' to java.util.UUID value", () -> this.rs.getObject(1, UUID.class));

        this.rs.close();
        this.rs = null;
    }

    @Test
    public void testUuidGetObjectFromNumber() throws SQLException {
        this.rs = this.stmt.executeQuery("select 1 col");
        assertTrue(this.rs.next());
        assertThrows(SQLException.class, "Unsupported conversion from LONG to java.util.UUID", () -> this.rs.getObject(1, UUID.class));

        this.rs.close();
        this.rs = null;
    }

    @Test
    public void testUuidGetObjectFromBinary() throws SQLException {
        testBinaryTypeColumns("binary(16)");
        testBinaryTypeColumns("varbinary(16)");
    }

    private void testBinaryTypeColumns(String colDef) throws SQLException {
        final UUID uuid = UUID.fromString("ed37122f-e13d-466e-b68e-38d3c06cc612");
        createTable("uuid_test", "(col " + colDef + ")");
        this.stmt.executeUpdate("insert into uuid_test (col) VALUES (uuid_to_bin('" + uuid.toString() + "'))");

        assertGetObjectResult(uuid);
    }

    @Test
    public void testUuidGetObjectFromChar() throws SQLException {
        testCharTypeColumns("char(36)");
        testCharTypeColumns("varchar(36)");
        testCharTypeColumns("text");
        testCharTypeColumns("tinytext");
        testCharTypeColumns("mediumtext");
        testCharTypeColumns("longtext");
    }

    private void testCharTypeColumns(String colDef) throws SQLException {
        final UUID uuid = UUID.fromString("ed37122f-e13d-466e-b68e-38d3c06cc612");
        createTable("uuid_test", "(col " + colDef + ")");
        this.stmt.executeUpdate("insert into uuid_test (col) VALUES ('" + uuid.toString() + "')");

        assertGetObjectResult(uuid);
    }

    private void assertGetObjectResult(UUID expected) throws SQLException {
        this.rs = this.stmt.executeQuery("select col from uuid_test");
        assertTrue(this.rs.next());
        assertEquals(expected, this.rs.getObject(1, UUID.class));

        this.rs.close();
        this.rs = null;
    }

}
