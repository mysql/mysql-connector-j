/*
 * Copyright (c) 2020, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.conf;

import com.mysql.cj.log.Log;
import com.mysql.cj.log.StandardLogger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.xml.parsers.ParserConfigurationException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class ConnectionPropertyMaxResultBufferParserTest {

    private final Log log = new StandardLogger(ConnectionPropertyMaxResultBufferParser.class.getName());

    @Parameterized.Parameter(0)
    public String valueToParse;

    @Parameterized.Parameter(1)
    public long expectedResult;

    @Parameterized.Parameters(name = "{index}: Test with valueToParse={0}, expectedResult={1}")
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][]{
                {"100", 100L},
                {"10K", 10L * 1000},
                {"25M", 25L * 1000 * 1000},
                //next two should be too big
                {"35G", (long) (0.90 * ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax())},
                {"1T", (long) (0.90 * ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax())},
                //percent test
                {"5p", (long) (0.05 * ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax())},
                {"10pct", (long) (0.10 * ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax())},
                {"15percent",
                        (long) (0.15 * ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax())},
                //for testing empty property
                {"", -1},
                {null, -1}
        };
        return Arrays.asList(data);
    }

    @Test
    public void testGetMaxResultBufferValue() {
        try {
            long result = ConnectionPropertyMaxResultBufferParser.parseProperty(valueToParse, log);
            assertEquals(expectedResult, result);
        } catch (ParserConfigurationException e) {
            //shouldn't occur
            fail();
        }
    }

    @Test(expected = ParserConfigurationException.class)
    public void testGetParserConfigurationException() throws ParserConfigurationException {
        long result = ConnectionPropertyMaxResultBufferParser.parseProperty("abc", log);
        fail();
    }

}