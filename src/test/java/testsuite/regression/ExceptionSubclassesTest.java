/*
 * Copyright (c) 2013, 2016, Oracle and/or its affiliates. All rights reserved.
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

import com.mysql.cj.jdbc.exceptions.SQLError;

import testsuite.BaseTestCase;

public class ExceptionSubclassesTest extends BaseTestCase {
    /**
     * Creates a new ExceptionSubclassesTest.
     * 
     * @param name
     *            the name of the test
     */
    public ExceptionSubclassesTest(String name) {
        super(name);
    }

    public void testBug17750877() throws Exception {

        assertEquals("java.sql.SQLTransientConnectionException", SQLError.createSQLException("test", "08000", 0, true, null).getClass().getCanonicalName());
        assertEquals("java.sql.SQLNonTransientConnectionException", SQLError.createSQLException("test", "08000", 0, false, null).getClass().getCanonicalName());
        assertEquals("java.sql.SQLSyntaxErrorException", SQLError.createSQLException("test", "42000", null).getClass().getCanonicalName());
        assertEquals("java.sql.SQLIntegrityConstraintViolationException", SQLError.createSQLException("test", "23000", null).getClass().getCanonicalName());
        assertEquals("com.mysql.cj.jdbc.exceptions.MySQLTransactionRollbackException",
                SQLError.createSQLException("test", "40000", null).getClass().getCanonicalName());
        assertEquals("com.mysql.cj.jdbc.exceptions.MySQLQueryInterruptedException",
                SQLError.createSQLException("test", "70100", null).getClass().getCanonicalName());

    }

}
