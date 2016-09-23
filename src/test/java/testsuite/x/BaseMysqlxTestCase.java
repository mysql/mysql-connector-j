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

import java.util.Properties;

import com.mysql.cj.api.x.NodeSession;
import com.mysql.cj.api.x.XSession;
import com.mysql.cj.api.x.XSessionFactory;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.x.MysqlxSessionFactory;

public abstract class BaseMysqlxTestCase {
    protected String baseUrl = System.getProperty(PropertyDefinitions.SYSP_testsuite_url_mysqlx);
    protected boolean isSetForMySQLxTests = this.baseUrl != null && this.baseUrl.length() > 0;
    protected XSessionFactory f = new MysqlxSessionFactory();

    public BaseMysqlxTestCase() {
        super();
        // TODO create instance of XSessionFactory
    }

    protected NodeSession getNodeSession(String url) {

        NodeSession sess = this.f.getNodeSession(url);

        return sess;
    }

    protected NodeSession getNodeSession(Properties props) {

        NodeSession sess = this.f.getNodeSession(props);

        return sess;
    }

    protected XSession getSession(String url) {

        XSession sess = this.f.getSession(url);

        return sess;
    }

    protected XSession getSession(Properties props) {

        XSession sess = this.f.getSession(props);

        return sess;
    }
}
