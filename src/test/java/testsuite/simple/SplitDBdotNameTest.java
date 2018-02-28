/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
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

import java.util.ArrayList;
import java.util.List;

import com.mysql.cj.util.StringUtils;

import testsuite.BaseTestCase;

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
        List<String> results = new ArrayList<>();

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
