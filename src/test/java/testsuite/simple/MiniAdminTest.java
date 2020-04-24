/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates. All rights reserved.
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

import org.junit.jupiter.api.Test;

import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.jdbc.admin.MiniAdmin;

import testsuite.BaseTestCase;

/**
 * Testsuite for MiniAdmin functionality.
 */
public class MiniAdminTest extends BaseTestCase {
    /**
     * Tests whether or not you can shutdown the server with MiniAdmin.
     * 
     * Only runs if SHUTDOWN_PROP is defined.
     * 
     * @throws Exception
     */
    @Test
    public void testShutdown() throws Exception {
        if (runTestIfSysPropDefined(PropertyDefinitions.SYSP_testsuite_miniAdminTest_runShutdown)) {
            new MiniAdmin(this.conn).shutdown();
        }
    }

    /**
     * Tests whether or not you can construct a MiniAdmin with a JDBC URL.
     * 
     * @throws Exception
     */
    @Test
    public void testUrlConstructor() throws Exception {
        new MiniAdmin(dbUrl);
    }
}
