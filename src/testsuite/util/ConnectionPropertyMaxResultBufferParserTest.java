/*
  Copyright (c) 2020, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.util;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;

import javax.xml.parsers.ParserConfigurationException;

import src.com.mysql.jdbc.util.ConnectionPropertyMaxResultBufferParser;
import src.testsuite.BaseTestCase;

public class ConnectionPropertyMaxResultBufferParserTest extends BaseTestCase {

    public ConnectionPropertyMaxResultBufferParserTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     *
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(ConnectionPropertyMaxResultBufferParserTest.class);
    }

    public void testGetMaxResultBufferValue() {
        try {
            Collection<Object[]> data = data();
            for (Object[] item : data) {
                long result = ConnectionPropertyMaxResultBufferParser.parseProperty((String) item[0]);
                assertEquals("Expected :" + (Long) item[1] + " get: " + result, ((Long) item[1]).longValue(), result);
            }
        } catch (ParserConfigurationException e) {
            //shouldn't occur
            fail();
        }
    }

    public void testGetParserConfigurationException() {
        assertThrows(ParserConfigurationException.class, new Callable<Void>() {
            public Void call() throws Exception {
                ConnectionPropertyMaxResultBufferParser.parseProperty("abc");
                return null;
            }
        });
    }

    private Collection<Object[]> data() {
        Object[][] data = new Object[][] { { "100", 100L }, { "10K", 10L * 1000 }, { "25M", 25L * 1000 * 1000 },
                //next two should be too big
                { "35G", (long) (0.90 * ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax()) },
                { "1T", (long) (0.90 * ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax()) },
                //percent test
                { "5p", (long) (0.05 * ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax()) },
                { "10pct", (long) (0.10 * ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax()) },
                { "15percent", (long) (0.15 * ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax()) },
                //for testing empty property
                { "", -1 }, { null, -1 } };
        return Arrays.asList(data);
    }

}
