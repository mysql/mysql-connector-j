/*
  Copyright (c) 2005, 2016, Oracle and/or its affiliates. All rights reserved.

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

import java.sql.Connection;
import java.util.Properties;
import java.util.TimeZone;

import testsuite.BaseTestCase;

/**
 * Tests regressions w/ the Escape Processor code.
 */
public class EscapeProcessorRegressionTest extends BaseTestCase {

    public EscapeProcessorRegressionTest(String name) {
        super(name);
    }

    /**
     * Tests fix for BUG#11797 - Escape tokenizer doesn't respect stacked single
     * quotes for escapes.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug11797() throws Exception {
        assertEquals("select 'ESCAPED BY ''\\'' ON {tbl_name | * | *.* | db_name.*}'",
                this.conn.nativeSQL("select 'ESCAPED BY ''\\'' ON {tbl_name | * | *.* | db_name.*}'"));
    }

    /**
     * Tests fix for BUG#11498 - Escape processor didn't honor strings
     * demarcated with double quotes.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug11498() throws Exception {
        assertEquals(
                "replace into t1 (id, f1, f4) VALUES(1,\"\",\"tko { zna gdje se sakrio\"),(2,\"a\",\"sedmi { kontinentio\"),(3,\"a\",\"a } cigov si ti?\")",
                this.conn.nativeSQL(
                        "replace into t1 (id, f1, f4) VALUES(1,\"\",\"tko { zna gdje se sakrio\"),(2,\"a\",\"sedmi { kontinentio\"),(3,\"a\",\"a } cigov si ti?\")"));

    }

    /**
     * Tests fix for BUG#14909 - escape processor replaces quote character in
     * quoted string with string delimiter.
     * 
     * @throws Exception
     */
    public void testBug14909() throws Exception {
        assertEquals("select '{\"','}'", this.conn.nativeSQL("select '{\"','}'"));
    }

    /**
     * Tests fix for BUG#25399 - EscapeProcessor gets confused by multiple backslashes
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug25399() throws Exception {
        assertEquals("\\' {d}", getSingleValueWithQuery("SELECT '\\\\\\' {d}'"));
    }

    /**
     * Tests fix for BUG#63526 - Unhandled case of {data...}
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug63526() throws Exception {
        createTable("bug63526", "(`{123}` INT UNSIGNED NOT NULL)", "INNODB");
    }

    /**
     * Tests fix for BUG#60598 - nativeSQL() truncates fractional seconds
     * 
     * @throws Exception
     */
    public void testBug60598() throws Exception {

        String expected = versionMeetsMinimum(5, 6, 4) ? "SELECT '2001-02-03 04:05:06' , '2001-02-03 04:05:06.007' , '11:22:33.444'"
                : "SELECT '2001-02-03 04:05:06' , '2001-02-03 04:05:06' , '11:22:33'";

        Connection conn_nolegacy = null;
        Connection conn_legacy = null;
        Connection conn_legacy_tz = null;

        try {
            Properties props = new Properties();

            props.setProperty("serverTimezone", TimeZone.getDefault().getID() + "");
            props.setProperty("useLegacyDatetimeCode", "false");
            conn_nolegacy = getConnectionWithProps(props);

            props.setProperty("useLegacyDatetimeCode", "true");
            conn_legacy = getConnectionWithProps(props);

            props.setProperty("useLegacyDatetimeCode", "true");
            props.setProperty("useTimezone", "true");
            props.setProperty("useJDBCCompliantTimezoneShift", "true");
            conn_legacy_tz = getConnectionWithProps(props);

            String input = "SELECT {ts '2001-02-03 04:05:06' } , {ts '2001-02-03 04:05:06.007' } , {t '11:22:33.444' }";

            String output = conn_nolegacy.nativeSQL(input);
            assertEquals(expected, output);

            output = conn_legacy.nativeSQL(input);
            assertEquals(expected, output);

            output = conn_legacy_tz.nativeSQL(input);
            assertEquals(expected, output);

        } finally {
            if (conn_nolegacy != null) {
                conn_nolegacy.close();
            }
            if (conn_legacy != null) {
                conn_legacy.close();
            }
            if (conn_legacy_tz != null) {
                conn_legacy_tz.close();
            }
        }

    }

}
