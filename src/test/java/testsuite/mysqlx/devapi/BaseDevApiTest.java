/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.mysqlx.devapi;

import testsuite.mysqlx.internal.BaseInternalMysqlxTest;

import com.mysql.cj.api.x.Schema;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.mysqlx.MysqlxError;
import com.mysql.cj.mysqlx.devapi.SessionImpl;

/**
 * Utilities for Dev API tests.
 */
public class BaseDevApiTest extends BaseInternalMysqlxTest {

    /**
     * Session for use in tests. This is a {@link SessionImpl} instance allowing access to the internal public APIs of the class.
     */
    SessionImpl session;
    Schema schema;

    public void setupTestSession() {
        this.session = new SessionImpl(this.testProperties);
        this.schema = this.session.getDefaultSchema();
    }

    public void destroyTestSession() {
        this.session.close();
        this.session = null;
    }

    protected void sqlUpdate(String sql) {
        this.session.getMysqlxSession().update(sql);
    }

    protected void dropCollection(String name) {
        try {
            this.schema.getCollection(name).drop();
        } catch (MysqlxError ex) {
            if (ex.getErrorCode() != MysqlErrorNumbers.ER_BAD_TABLE_ERROR) {
                throw ex;
            }
        }
    }
}
