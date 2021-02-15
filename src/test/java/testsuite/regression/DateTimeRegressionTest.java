/*
 * Copyright (c) 2021, Oracle and/or its affiliates.
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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.PropertyKey;

import testsuite.BaseTestCase;

public class DateTimeRegressionTest extends BaseTestCase {

    /**
     * Tests fix for Bug#20391832, SETOBJECT() FOR TYPES.TIME RESULTS IN EXCEPTION WHEN VALUE HAS FRACTIONAL PART.
     * 
     * @throws Exception
     */
    @Test
    public void testBug20391832() throws Exception {
        createTable("testBug20391832", "(v varchar(40))");

        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions

        Properties props = new Properties();
        props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "LOCAL");
        for (boolean useSSPS : new boolean[] { false, true }) {
            for (boolean sendFr : new boolean[] { false, true }) {

                System.out.println("useServerPrepStmts=" + useSSPS + "; sendFractSeconds=" + sendFr);

                props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFr);
                Connection testConn = getConnectionWithProps(timeZoneFreeDbUrl, props);

                this.pstmt = testConn.prepareStatement("insert into testBug20391832 values(?)");

                for (MysqlType type : new MysqlType[] { MysqlType.DATETIME, MysqlType.TIMESTAMP }) {
                    subTestBug20391832(props, type, "2038-01-19", "2038-01-19 00:00:00");
                    subTestBug20391832(props, type, "38-01-19", "2038-01-19 00:00:00");
                    subTestBug20391832(props, type, "2038#01$19", "2038-01-19 00:00:00");
                    subTestBug20391832(props, type, "38#01$19", "2038-01-19 00:00:00");
                    subTestBug20391832(props, type, "20380119", "2038-01-19 00:00:00");
                    subTestBug20391832(props, type, "380119", "2038-01-19 00:00:00");
                    subTestBug20391832(props, type, "030417", "2003-04-17 00:00:00"); // resolved as a DATE literal

                    assertThrows(SQLException.class, ".* Conversion from java.time.Duration to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "12 1", null);
                        return null;
                    });
                    assertThrows(SQLException.class, ".* Conversion from java.time.Duration to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "12 13:04", null);
                        return null;
                    });
                    assertThrows(SQLException.class, ".* Conversion from java.time.Duration to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "12 1:4:1", null);
                        return null;
                    });
                    assertThrows(SQLException.class, ".* Conversion from java.time.Duration to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "12 1:04:17.123456789", null);
                        return null;
                    });
                    assertThrows(SQLException.class, ".* Conversion from java.time.Duration to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "-838:59:59", null);
                        return null;
                    });

                    assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "13:04:17", null);
                        return null;
                    });
                    assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "13:04", null);
                        return null;
                    });
                    assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "0417", null);
                        return null;
                    });
                    assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "03:14:07.012", null);
                        return null;
                    });
                    assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "031407.123", null);
                        return null;
                    });

                    assertThrows(SQLException.class, ".* There is no known date-time pattern for.*", () -> {
                        subTestBug20391832(props, type, "031407#12", null); // wrong delimiter
                        return null;
                    });

                    subTestBug20391832(props, type, "2038-01-19 03:14:07", "2038-01-19 03:14:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "2038-01-19 03:14:07.1", sendFr ? "2038-01-19 03:14:07.1" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038-01-19 03:14:07.01", sendFr ? "2038-01-19 03:14:07.01" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038-01-19 03:14:07.012", sendFr ? "2038-01-19 03:14:07.012" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038-01-19 03:14:07.0123", sendFr ? "2038-01-19 03:14:07.0123" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038-01-19 03:14:07.01234", sendFr ? "2038-01-19 03:14:07.01234" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038-01-19 03:14:07.012345", sendFr ? "2038-01-19 03:14:07.012345" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038-01-19 03:14:07.0123456", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038-01-19 03:14:07.01234567", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038-01-19 03:14:07.012345678", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                    }

                    subTestBug20391832(props, type, "38-01-19 03:14:07", "2038-01-19 03:14:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "38-01-19 03:14:07.1", sendFr ? "2038-01-19 03:14:07.1" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38-01-19 03:14:07.01", sendFr ? "2038-01-19 03:14:07.01" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38-01-19 03:14:07.012", sendFr ? "2038-01-19 03:14:07.012" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38-01-19 03:14:07.0123", sendFr ? "2038-01-19 03:14:07.0123" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38-01-19 03:14:07.01234", sendFr ? "2038-01-19 03:14:07.01234" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38-01-19 03:14:07.012345", sendFr ? "2038-01-19 03:14:07.012345" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38-01-19 03:14:07.0123456", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38-01-19 03:14:07.01234567", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38-01-19 03:14:07.012345678", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                    }

                    subTestBug20391832(props, type, "2038#01$19 03@14%07", "2038-01-19 03:14:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "2038#01$19 03@14%07.1", sendFr ? "2038-01-19 03:14:07.1" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19 03@14%07.01", sendFr ? "2038-01-19 03:14:07.01" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19 03@14%07.012", sendFr ? "2038-01-19 03:14:07.012" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19 03@14%07.0123", sendFr ? "2038-01-19 03:14:07.0123" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19 03@14%07.01234", sendFr ? "2038-01-19 03:14:07.01234" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19 03@14%07.012345", sendFr ? "2038-01-19 03:14:07.012345" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19 03@14%07.0123456", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19 03@14%07.01234567", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19 03@14%07.012345678", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                    }

                    subTestBug20391832(props, type, "38#01$19 03@14%07", "2038-01-19 03:14:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "38#01$19 03@14%07.1", sendFr ? "2038-01-19 03:14:07.1" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19 03@14%07.01", sendFr ? "2038-01-19 03:14:07.01" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19 03@14%07.012", sendFr ? "2038-01-19 03:14:07.012" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19 03@14%07.0123", sendFr ? "2038-01-19 03:14:07.0123" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19 03@14%07.01234", sendFr ? "2038-01-19 03:14:07.01234" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19 03@14%07.012345", sendFr ? "2038-01-19 03:14:07.012345" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19 03@14%07.0123456", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19 03@14%07.01234567", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19 03@14%07.012345678", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                    }

                    subTestBug20391832(props, type, "2038#01$19T03@14%07", "2038-01-19 03:14:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "2038#01$19T03@14%07.1", sendFr ? "2038-01-19 03:14:07.1" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19T03@14%07.01", sendFr ? "2038-01-19 03:14:07.01" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19T03@14%07.012", sendFr ? "2038-01-19 03:14:07.012" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19T03@14%07.0123", sendFr ? "2038-01-19 03:14:07.0123" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19T03@14%07.01234", sendFr ? "2038-01-19 03:14:07.01234" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19T03@14%07.012345", sendFr ? "2038-01-19 03:14:07.012345" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19T03@14%07.0123456", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19T03@14%07.01234567", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19T03@14%07.012345678", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                    }

                    subTestBug20391832(props, type, "38#01$19T03@14%07", "2038-01-19 03:14:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "38#01$19T03@14%07.1", sendFr ? "2038-01-19 03:14:07.1" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19T03@14%07.01", sendFr ? "2038-01-19 03:14:07.01" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19T03@14%07.012", sendFr ? "2038-01-19 03:14:07.012" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19T03@14%07.0123", sendFr ? "2038-01-19 03:14:07.0123" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19T03@14%07.01234", sendFr ? "2038-01-19 03:14:07.01234" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19T03@14%07.012345", sendFr ? "2038-01-19 03:14:07.012345" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19T03@14%07.0123456", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19T03@14%07.01234567", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19T03@14%07.012345678", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                    }

                    subTestBug20391832(props, type, "2038#1$19 3@14%7", "2038-01-19 03:14:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "2038#1$19T3@14%7.1", sendFr ? "2038-01-19 03:14:07.1" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#1$19 3@14%7.01", sendFr ? "2038-01-19 03:14:07.01" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#1$19T3@14%7.012", sendFr ? "2038-01-19 03:14:07.012" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#1$19 3@14%7.0123", sendFr ? "2038-01-19 03:14:07.0123" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#1$19T3@14%7.01234", sendFr ? "2038-01-19 03:14:07.01234" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#1$19 3@14%7.012345", sendFr ? "2038-01-19 03:14:07.012345" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#1$19T3@14%7.0123456", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#1$19 3@14%7.01234567", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#1$19T3@14%7.012345678", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                    }

                    subTestBug20391832(props, type, "38#1$9 3@4%7", "2038-01-09 03:04:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "38#1$9T3@4%7.1", sendFr ? "2038-01-09 03:04:07.1" : "2038-01-09 03:04:07");
                        subTestBug20391832(props, type, "38#1$9T3@4%7.01", sendFr ? "2038-01-09 03:04:07.01" : "2038-01-09 03:04:07");
                        subTestBug20391832(props, type, "38#1$9T3@4%7.012", sendFr ? "2038-01-09 03:04:07.012" : "2038-01-09 03:04:07");
                        subTestBug20391832(props, type, "38#1$9T3@4%7.0123", sendFr ? "2038-01-09 03:04:07.0123" : "2038-01-09 03:04:07");
                        subTestBug20391832(props, type, "38#1$9T3@4%7.01234", sendFr ? "2038-01-09 03:04:07.01234" : "2038-01-09 03:04:07");
                        subTestBug20391832(props, type, "38#1$9T3@4%7.012345", sendFr ? "2038-01-09 03:04:07.012345" : "2038-01-09 03:04:07");
                        subTestBug20391832(props, type, "38#1$9T3@4%7.0123456", sendFr ? "2038-01-09 03:04:07.012346" : "2038-01-09 03:04:07");
                        subTestBug20391832(props, type, "38#1$9T3@4%7.01234567", sendFr ? "2038-01-09 03:04:07.012346" : "2038-01-09 03:04:07");
                        subTestBug20391832(props, type, "38#1$9T3@4%7.012345678", sendFr ? "2038-01-09 03:04:07.012346" : "2038-01-09 03:04:07");
                    }

                    subTestBug20391832(props, type, "20380119031407", "2038-01-19 03:14:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "20380119031407.1", sendFr ? "2038-01-19 03:14:07.1" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "20380119031407.01", sendFr ? "2038-01-19 03:14:07.01" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "20380119031407.012", sendFr ? "2038-01-19 03:14:07.012" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "20380119031407.0123", sendFr ? "2038-01-19 03:14:07.0123" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "20380119031407.01234", sendFr ? "2038-01-19 03:14:07.01234" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "20380119031407.012345", sendFr ? "2038-01-19 03:14:07.012345" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "20380119031407.0123456", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "20380119031407.01234567", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "20380119031407.012345678", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                    }

                    subTestBug20391832(props, type, "380119031407", "2038-01-19 03:14:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "380119031407.1", sendFr ? "2038-01-19 03:14:07.1" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "380119031407.01", sendFr ? "2038-01-19 03:14:07.01" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "380119031407.012", sendFr ? "2038-01-19 03:14:07.012" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "380119031407.0123", sendFr ? "2038-01-19 03:14:07.0123" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "380119031407.01234", sendFr ? "2038-01-19 03:14:07.01234" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "380119031407.012345", sendFr ? "2038-01-19 03:14:07.012345" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "380119031407.0123456", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "380119031407.01234567", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "380119031407.012345678", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                    }
                }

                // ================================================

                subTestBug20391832(props, MysqlType.TIME, "12 1", "289:0:0");
                subTestBug20391832(props, MysqlType.TIME, "-12 1", "-289:0:0");
                subTestBug20391832(props, MysqlType.TIME, "12 12:04", "300:4:0");
                subTestBug20391832(props, MysqlType.TIME, "-12 12:04", "-300:4:0");
                subTestBug20391832(props, MysqlType.TIME, "12 1:4:1", "289:4:1");
                subTestBug20391832(props, MysqlType.TIME, "-12 1:4:7", "-289:4:7");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "5 1:04:17.123456789", sendFr ? "121:4:17.123457" : "121:4:17");
                    subTestBug20391832(props, MysqlType.TIME, "-5 1:04:17.123456789", sendFr ? "-121:4:17.123457" : "-121:4:17");
                }
                subTestBug20391832(props, MysqlType.TIME, "25:59:59", "25:59:59");
                subTestBug20391832(props, MysqlType.TIME, "-838:59:59", "-838:59:59");

                assertThrows(SQLException.class, ".* Conversion from java.time.LocalDate to TIME is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalDate to TIME is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalDate to TIME is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalDate to TIME is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalDate to TIME is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.TIME, "20380119", null);
                    return null;
                });
                subTestBug20391832(props, MysqlType.TIME, "120119", "12:01:19");
                subTestBug20391832(props, MysqlType.TIME, "13:04:17", "13:04:17");
                subTestBug20391832(props, MysqlType.TIME, "13:04", "13:04:00");
                subTestBug20391832(props, MysqlType.TIME, "130417", "13:04:17");
                subTestBug20391832(props, MysqlType.TIME, "0417", "00:04:17");
                subTestBug20391832(props, MysqlType.TIME, "17", "00:00:17");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "0417.1234567", sendFr ? "00:04:17.123457" : "00:04:17");
                    subTestBug20391832(props, MysqlType.TIME, "17.1234567", sendFr ? "00:00:17.123457" : "00:00:17");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:07.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:07.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:07.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:07.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "3:14:07.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:07.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:4:07.012345", sendFr ? "03:04:07.012345" : "03:04:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:07.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "3:4:7.0123456", sendFr ? "03:04:07.012346" : "03:04:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:7.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:07.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:07.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:07.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "031407.123", sendFr ? "03:14:07.123" : "03:14:07");
                }

                assertThrows(SQLException.class, ".* There is no known date-time pattern for.*", () -> {
                    subTestBug20391832(props, MysqlType.TIME, "031407#12", null); // wrong delimiter
                    return null;
                });

                subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07", "03:14:07");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                }

                subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07", "03:14:07");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                }

                subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07", "03:14:07");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                }

                subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07", "03:14:07");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                }

                subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07", "03:14:07");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                }

                subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07", "03:14:07");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                }

                subTestBug20391832(props, MysqlType.TIME, "2038#1$19 3@14%7", "03:14:07");
                subTestBug20391832(props, MysqlType.TIME, "38#1$19 3@14%7", "03:14:07");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "38#1$19T3@14%7.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#1$19 3@14%7.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#1$19T3@14%7.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#1$19T3@14%7.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#1$19T3@14%7.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#1$19 3@14%7.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#1$19T3@14%7.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#1$19T3@14%7.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#1$19 3@14%7.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                }

                subTestBug20391832(props, MysqlType.TIME, "20380119031407", "03:14:07");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "20380119031407.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "20380119031407.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "20380119031407.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "20380119031407.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "20380119031407.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "20380119031407.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "20380119031407.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "20380119031407.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "20380119031407.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                }

                subTestBug20391832(props, MysqlType.TIME, "380119031407", "03:14:07");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "380119031407.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "380119031407.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "380119031407.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "380119031407.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "380119031407.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "380119031407.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "380119031407.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "380119031407.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "380119031407.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                }

                // ================================================

                subTestBug20391832(props, MysqlType.DATE, "2038-01-19", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-1-9", "2038-01-09");
                subTestBug20391832(props, MysqlType.DATE, "38-1-9", "2038-01-09");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "030417", "2003-04-17");

                assertThrows(SQLException.class, ".* Conversion from java.time.Duration to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "12 1", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.Duration to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "12 13:04", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.Duration to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "12 1:4:1", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.Duration to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "12 1:04:17.123456789", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.Duration to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "-838:59:59", null);
                    return null;
                });

                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "13:04:17", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "13:04", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "0417", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "03:14:07.012", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "031407.123", null);
                    return null;
                });

                assertThrows(SQLException.class, ".* There is no known date-time pattern for.*", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "031407#12", null); // wrong delimiter
                    return null;
                });

                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07.1", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07.12", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07.012", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07.0123", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07.01234", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07.012345", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07.0123456", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07.01234567", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07.012345678", "2038-01-19");

                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07.1", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07.12", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07.012", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07.0123", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07.01234", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07.012345", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07.0123456", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07.01234567", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07.012345678", "2038-01-19");

                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07.1", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07.12", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07.012", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07.0123", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07.01234", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07.012345", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07.0123456", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07.01234567", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07.012345678", "2038-01-19");

                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07.1", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07.12", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07.012", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07.0123", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07.01234", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07.012345", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07.0123456", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07.01234567", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07.012345678", "2038-01-19");

                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07.1", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07.12", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07.012", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07.0123", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07.01234", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07.012345", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07.0123456", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07.01234567", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07.012345678", "2038-01-19");

                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07.1", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07.12", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07.012", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07.0123", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07.01234", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07.012345", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07.0123456", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07.01234567", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07.012345678", "2038-01-19");

                subTestBug20391832(props, MysqlType.DATE, "2038#1$19 3@14%7", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#1$9 3@4%7", "2038-01-09");

                subTestBug20391832(props, MysqlType.DATE, "20380119031407", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119031407.1", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119031407.01", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119031407.012", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119031407.0123", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119031407.01234", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119031407.012345", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119031407.0123456", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119031407.01234567", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119031407.012345678", "2038-01-19");

                subTestBug20391832(props, MysqlType.DATE, "380119031407", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119031407.1", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119031407.01", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119031407.012", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119031407.0123", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119031407.01234", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119031407.012345", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119031407.0123456", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119031407.01234567", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119031407.012345678", "2038-01-19");

                // ================================================

                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "030417", "2003");

                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to YEAR is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.YEAR, "13:04:17", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to YEAR is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.YEAR, "13:04", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to YEAR is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.YEAR, "0417", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to YEAR is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.YEAR, "03:14:07.012", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to YEAR is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.YEAR, "031407.123", null);
                    return null;
                });

                assertThrows(SQLException.class, ".* There is no known date-time pattern for.*", () -> {
                    subTestBug20391832(props, MysqlType.YEAR, "031407#12", null); // wrong delimiter
                    return null;
                });

                assertThrows(SQLException.class, ".* Conversion from java.time.Duration to YEAR is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.YEAR, "-838:59:59", null);
                    return null;
                });

                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.1", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.12", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.012", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.0123", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.01234", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.012345", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.0123456", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.01234567", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.012345678", "2038");

                assertThrows(SQLException.class, ".* There is no known date-time pattern for.*", () -> {
                    subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.0123456789", null); // nanos part is too long
                    return null;
                });

                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07.1", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07.12", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07.012", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07.0123", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07.01234", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07.012345", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07.0123456", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07.01234567", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07.012345678", "2038");

                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07.1", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07.12", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07.012", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07.0123", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07.01234", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07.012345", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07.0123456", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07.01234567", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07.012345678", "2038");

                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07.1", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07.12", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07.012", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07.0123", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07.01234", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07.012345", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07.0123456", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07.01234567", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07.012345678", "2038");

                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07.1", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07.12", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07.012", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07.0123", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07.01234", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07.012345", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07.0123456", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07.01234567", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07.012345678", "2038");

                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07.1", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07.12", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07.012", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07.0123", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07.01234", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07.012345", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07.0123456", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07.01234567", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07.012345678", "2038");

                subTestBug20391832(props, MysqlType.YEAR, "2038#1$19 3@14%7", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#1$9 3@4%7", "2038");

                subTestBug20391832(props, MysqlType.YEAR, "20380119031407", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119031407.1", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119031407.01", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119031407.012", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119031407.0123", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119031407.01234", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119031407.012345", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119031407.0123456", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119031407.01234567", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119031407.012345678", "2038");

                subTestBug20391832(props, MysqlType.YEAR, "380119031407", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119031407.1", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119031407.01", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119031407.012", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119031407.0123", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119031407.01234", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119031407.012345", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119031407.0123456", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119031407.01234567", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119031407.012345678", "2038");

                testConn.close();
            }
        }
    }

    private void subTestBug20391832(Properties props, MysqlType targetType, String val, String exp) throws Exception {
        boolean useSSPS = Boolean.parseBoolean(props.getProperty(PropertyKey.useServerPrepStmts.getKeyName()));

        this.pstmt.setObject(1, val, targetType);
        String query = this.pstmt.toString().replace(".0)", ")");

        assertEquals((useSSPS ? "com.mysql.cj.jdbc.ServerPreparedStatement[1]: " : "com.mysql.cj.jdbc.ClientPreparedStatement: ")
                + "insert into testBug20391832 values(" + (targetType == MysqlType.YEAR ? exp : "'" + exp + "'") + ")", query);
    }

}
