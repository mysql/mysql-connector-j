/*
 * Copyright (c) 2005, 2023, Oracle and/or its affiliates.
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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;

import com.mysql.cj.MysqlConnection;
import com.mysql.cj.util.TimeUtil;

import testsuite.BaseTestCase;

/**
 * Tests regressions w/ the Escape Processor code.
 */
public class EscapeProcessorRegressionTest extends BaseTestCase {

    /**
     * Tests fix for BUG#11797 - Escape tokenizer doesn't respect stacked single quotes for escapes.
     *
     * @throws Exception
     */
    @Test
    public void testBug11797() throws Exception {
        assertEquals("select 'ESCAPED BY ''\\'' ON {tbl_name | * | *.* | db_name.*}'",
                this.conn.nativeSQL("select 'ESCAPED BY ''\\'' ON {tbl_name | * | *.* | db_name.*}'"));
    }

    /**
     * Tests fix for BUG#11498 - Escape processor didn't honor strings demarcated with double quotes.
     *
     * @throws Exception
     */
    @Test
    public void testBug11498() throws Exception {
        assertEquals(
                "replace into t1 (id, f1, f4) VALUES(1,\"\",\"tko { zna gdje se sakrio\"),(2,\"a\",\"sedmi { kontinentio\"),(3,\"a\",\"a } cigov si ti?\")",
                this.conn.nativeSQL(
                        "replace into t1 (id, f1, f4) VALUES(1,\"\",\"tko { zna gdje se sakrio\"),(2,\"a\",\"sedmi { kontinentio\"),(3,\"a\",\"a } cigov si ti?\")"));
    }

    /**
     * Tests fix for BUG#14909 - escape processor replaces quote character in quoted string with string delimiter.
     *
     * @throws Exception
     */
    @Test
    public void testBug14909() throws Exception {
        assertEquals("select '{\"','}'", this.conn.nativeSQL("select '{\"','}'"));
    }

    /**
     * Tests fix for BUG#25399 - EscapeProcessor gets confused by multiple backslashes
     *
     * @throws Exception
     */
    @Test
    public void testBug25399() throws Exception {
        assertEquals("\\' {d}", getSingleValueWithQuery("SELECT '\\\\\\' {d}'"));
    }

    /**
     * Tests fix for BUG#63526 - Unhandled case of {data...}
     *
     * @throws Exception
     */
    @Test
    public void testBug63526() throws Exception {
        createTable("bug63526", "(`{123}` INT UNSIGNED NOT NULL)", "INNODB");
    }

    /**
     * Tests fix for BUG#60598 - nativeSQL() truncates fractional seconds
     *
     * @throws Exception
     */
    @Test
    public void testBug60598() throws Exception {
        TimeZone sessionTz = ((MysqlConnection) this.conn).getSession().getServerSession().getSessionTimeZone();
        ZonedDateTime zdt = LocalDateTime.of(2001, 2, 3, 4, 5, 6, 7000000).atZone(ZoneId.systemDefault()).withZoneSameInstant(sessionTz.toZoneId());
        boolean withFract = versionMeetsMinimum(5, 6, 4);

        String expected = "SELECT '" + zdt.format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET) + "' , '"
                + zdt.format(withFract ? TimeUtil.DATETIME_FORMATTER_WITH_MILLIS_NO_OFFSET : TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET) + "' , '11:22:33"
                + (withFract ? ".444'" : "'");

        String input = "SELECT {ts '2001-02-03 04:05:06' } , {ts '2001-02-03 04:05:06.007' } , {t '11:22:33.444' }";

        String output = this.conn.nativeSQL(input);
        assertEquals(expected, output);
    }

}
