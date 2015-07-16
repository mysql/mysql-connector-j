/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

import java.io.InputStream;
import java.util.Properties;

import com.mysql.cj.mysqlx.devapi.SessionImpl;
import com.mysql.cj.api.x.Collection;
import com.mysql.cj.api.x.Schema;
import com.mysql.cj.api.x.Session;

/**
 * Utilities for Dev API tests.
 */
public class BaseDevApiTest {

    Session session;
    Schema schema;

    private void initSession() {
        this.session = new SessionImpl(getTestHost(), getTestPort(), getTestUser(), getTestPassword(), getTestDatabase());
        this.schema = this.session.getDefaultSchema();
    }

    // BEGIN: duplicated from BaseInternalMysqlxTest
    // put this here to re-use the same properties file infrastructure. can be factored out somewhere else
    public Properties testProperties = new Properties();

    public BaseDevApiTest() throws Exception {
        InputStream propsFileStream = ClassLoader.getSystemResourceAsStream("test.mysqlx.properties");
        if (propsFileStream == null) {
            throw new Exception("Cannot load test.mysqlx.properties");
        }
        this.testProperties.load(propsFileStream);
        initSession();
    }

    public String getTestHost() {
        return this.testProperties.getProperty("com.mysql.mysqlx.testsuite.host");
    }

    public int getTestPort() {
        return Integer.valueOf(this.testProperties.getProperty("com.mysql.mysqlx.testsuite.port"));
    }

    public String getTestUser() {
        return this.testProperties.getProperty("com.mysql.mysqlx.testsuite.user");
    }

    public String getTestPassword() {
        return this.testProperties.getProperty("com.mysql.mysqlx.testsuite.password");
    }

    public String getTestDatabase() {
        return this.testProperties.getProperty("com.mysql.mysqlx.testsuite.database");
    }
    // END: duplicated from BaseInternalMysqlxTest
}
