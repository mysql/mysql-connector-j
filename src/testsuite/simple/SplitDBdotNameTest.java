/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

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

import java.util.ArrayList;
import java.util.List;

import testsuite.BaseTestCase;

import com.mysql.jdbc.StringUtils;

/**
 * Tests new StringUtils functions in the driver: public static String sanitizeProcOrFuncName(String src) and public static List splitDBdotName(String src,
 * String cat, String quotId, boolean isNoBslashEscSet)
 * 
 * By the time sanitizeProcOrFuncName is called we should only have DB.SP as src, ie. SP/FUNC name is already sanitized during the process!
 */
public class SplitDBdotNameTest extends BaseTestCase {
    /**
     * Constructor for SplitDBdotNameTest.
     * 
     * @param name
     *            the name of the test to run.
     */
    public SplitDBdotNameTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(SplitDBdotNameTest.class);
    }

    /**
     * Tests sanitation and SplitDBdotName
     * 
     * @throws Exception
     *             if an error occurs
     */
    public void testSplit() throws Exception {
        String src = null;
        String resString = null;
        List<String> results = new ArrayList<String>();

        //Test 1.1, weird DB.SP name
        src = "`MyDatabase 1.0.1.0`.`Proc 1.v1`";
        resString = StringUtils.sanitizeProcOrFuncName(src);
        if ((resString != null)) {
            results = StringUtils.splitDBdotName(resString, null, "`", true);
            assertEquals(results.get(0), "MyDatabase 1.0.1.0");
            assertEquals(results.get(1), "Proc 1.v1");
        } else {
            fail("Test 1.1 returned null resString");
        }

        //Test 1.2, toggle isNoBslashEscSet
        src = "`MyDatabase 1.0.1.0`.`Proc 1.v1`";
        resString = StringUtils.sanitizeProcOrFuncName(src);
        if ((resString != null)) {
            results = StringUtils.splitDBdotName(resString, null, "`", false);
            assertEquals(results.get(0), "MyDatabase 1.0.1.0");
            assertEquals(results.get(1), "Proc 1.v1");
        } else {
            fail("Test 1.2 returned null resString");
        }

        //Test 2.1, weird SP name, no DB parameter
        src = "`Proc 1.v1`";
        resString = StringUtils.sanitizeProcOrFuncName(src);
        if ((resString != null)) {
            results = StringUtils.splitDBdotName(resString, null, "`", true);
            assertEquals(results.get(0), null);
            assertEquals(results.get(1), "Proc 1.v1");
        } else {
            fail("Test 2.1 returned null resString");
        }

        //Test 2.2, toggle isNoBslashEscSet
        src = "`Proc 1.v1`";
        resString = StringUtils.sanitizeProcOrFuncName(src);
        if ((resString != null)) {
            results = StringUtils.splitDBdotName(resString, null, "`", false);
            assertEquals(results.get(0), null);
            assertEquals(results.get(1), "Proc 1.v1");
        } else {
            fail("Test 2.2 returned null resString");
        }
    }
}