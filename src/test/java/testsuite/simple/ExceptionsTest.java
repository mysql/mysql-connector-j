/*
 * Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.
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
                "The connection property 'useSSL' acceptable values are: 'TRUE', 'FALSE', 'YES' or 'NO'\\. The value 'wrongvalue' is not acceptable\\.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        new NonRegisteringDriver().getPropertyInfo(dbUrl + "&useSSL=wrongvalue", null);
                        return null;
                    }
                });

    }

}
