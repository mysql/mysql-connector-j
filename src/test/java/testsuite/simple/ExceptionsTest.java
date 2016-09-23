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

package testsuite.simple;

import java.sql.SQLException;
import java.util.concurrent.Callable;

import com.mysql.cj.jdbc.NonRegisteringDriver;

import testsuite.BaseTestCase;

public class ExceptionsTest extends BaseTestCase {

    public ExceptionsTest(String name) {
        super(name);
    }

    public void testExceptionsTranslation() throws Exception {

        // java.sql.Driver methods
        assertThrows(SQLException.class,
                "Communications link failure\n\nThe last packet sent successfully to the server was 0 milliseconds ago. The driver has not received any packets from the server.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        new NonRegisteringDriver().connect("jdbc:mysql://wrongurl?user=usr", null);
                        return null;
                    }
                });
        assertThrows(SQLException.class, ".*Can't find configuration template named 'wrongvalue'", new Callable<Void>() {
            public Void call() throws Exception {
                new NonRegisteringDriver().connect(dbUrl + "&useConfigs=wrongvalue", null);
                return null;
            }
        });
        assertThrows(SQLException.class,
                "The connection property 'useSSL' only accepts values of the form: 'true', 'false', 'yes' or 'no'. The value 'wrongvalue' is not in this set.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        new NonRegisteringDriver().getPropertyInfo(dbUrl + "&useSSL=wrongvalue", null);
                        return null;
                    }
                });

    }

}
