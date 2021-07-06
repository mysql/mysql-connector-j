/*
 * Copyright (c) 2002, 2021, Oracle and/or its affiliates.
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

package testsuite.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.lang.reflect.Method;
import java.sql.ResultSet;

import javax.sql.RowSet;

import org.junit.jupiter.api.Test;

import testsuite.BaseTestCase;

/**
 * Regression test cases for the ResultSet class.
 */
public class CachedRowsetTest extends BaseTestCase {
    /**
     * Tests fix for BUG#5188, CachedRowSet errors using PreparedStatement. Uses Sun's "com.sun.rowset.CachedRowSetImpl"
     * 
     * @throws Exception
     */
    @Test
    public void testBug5188() throws Exception {
        String implClass = "com.sun.rowset.CachedRowSetImpl";
        Class<?> c = null;
        Method populate;
        try {
            c = Class.forName(implClass);
        } catch (ClassNotFoundException e) {
            assumeFalse(true, "Requires: " + implClass);
        }
        populate = c.getMethod("populate", new Class<?>[] { ResultSet.class });

        createTable("testBug5188", "(ID int NOT NULL AUTO_INCREMENT, datafield VARCHAR(64), PRIMARY KEY(ID))");

        this.stmt.executeUpdate("INSERT INTO testBug5188(datafield) values('test data stuff !')");

        String sql = "SELECT * FROM testBug5188 where ID = ?";
        this.pstmt = this.conn.prepareStatement(sql);
        this.pstmt.setString(1, "1");
        this.rs = this.pstmt.executeQuery();

        // create a CachedRowSet and populate it
        RowSet cachedRowSet = (RowSet) c.newInstance();
        // cachedRowSet.populate(rs);
        populate.invoke(cachedRowSet, new Object[] { this.rs });

        // scroll through CachedRowSet ...
        assertTrue(cachedRowSet.next());
        assertEquals("1", cachedRowSet.getString("ID"));
        assertEquals("test data stuff !", cachedRowSet.getString("datafield"));
        assertFalse(cachedRowSet.next());
    }
}
