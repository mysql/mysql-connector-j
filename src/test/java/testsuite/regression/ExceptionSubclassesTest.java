/*
  Copyright (c) 2013, 2016, Oracle and/or its affiliates. All rights reserved.

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
