/*
 * Copyright (c) 2002, 2019, Oracle and/or its affiliates. All rights reserved.
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

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import com.mysql.cj.util.LazyString;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.StringUtils.SearchMode;

import testsuite.BaseTestCase;

public class StringUtilsTest extends BaseTestCase {
    /**
     * Creates a new StringUtilsTest.
     * 
     * @param name
     *            the name of the test
     */
    public StringUtilsTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(StringUtilsTest.class);
    }

    /**
     * Tests StringUtil.indexOfIgnoreCase() methods
     * 
     * @throws Exception
     */
    public void testIndexOfIgnoreCase() throws Exception {
        final String markerStart = "\"'`(";
        final String markerEnd = "\"'`)";

        String searchIn;
        String[] searchInMulti;
        String searchFor;
        String[] searchForMulti;
        int[] expectedIdx;

        int pos;
        Set<SearchMode> searchMode;

        /*
         * A. test indexOfIgnoreCase(String searchIn, String searchFor)
         */
        // basic test set
        assertEquals(-1, StringUtils.indexOfIgnoreCase(null, null));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(null, "abc"));
        assertEquals(-1, StringUtils.indexOfIgnoreCase("abc", null));
        assertEquals(-1, StringUtils.indexOfIgnoreCase("abc", ""));
        assertEquals(-1, StringUtils.indexOfIgnoreCase("abc", "bcd"));
        assertEquals(0, StringUtils.indexOfIgnoreCase("abc", "abc"));
        assertEquals(3, StringUtils.indexOfIgnoreCase("abc d efg", " d "));

        // exhaustive test set
        searchIn = "A strange STRONG SsStRiNg to be searched in";
        searchForMulti = new String[] { "STR", "sstr", "Z", "a str", " in", "b" };
        expectedIdx = new int[] { 2, 18, -1, 0, 40, 29 };
        for (int i = 0; i < searchForMulti.length; i++) {
            assertEquals("Test A." + i, expectedIdx[i], StringUtils.indexOfIgnoreCase(searchIn, searchForMulti[i]));
        }

        /*
         * B. test indexOfIgnoreCase(int startingPosition, String searchIn, String searchFor)
         */
        // basic test set
        assertEquals(-1, StringUtils.indexOfIgnoreCase(3, null, null));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(3, null, "abc"));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(3, "abc", null));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(3, "abc", ""));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(3, "abc", "bcd"));
        assertEquals(3, StringUtils.indexOfIgnoreCase(3, "abcabc", "abc"));
        assertEquals(3, StringUtils.indexOfIgnoreCase("abc d efg", " d "));

        // exhaustive test set
        searchIn = "A strange STRONG SsStRiNg to be searched in";
        searchForMulti = new String[] { "STR", "sstr", "Z", "a str", " in", "b" };
        expectedIdx = new int[] { 10, 18, -1, -1, 40, 29 };
        for (int i = 0; i < searchForMulti.length; i++) {
            assertEquals("Test B." + i, expectedIdx[i], StringUtils.indexOfIgnoreCase(3, searchIn, searchForMulti[i]));
        }

        /*
         * C. test indexOfIgnoreCase(int startingPosition, String searchIn, String searchFor, String openingMarkers, String closingMarkers, Set<SearchMode>
         * searchMode) using search modes SEARCH_MODE__BSESC_MRK_WS or SEARCH_MODE__MRK_WS
         */
        // basic test set
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, null, (String) null, markerStart, markerEnd, StringUtils.SEARCH_MODE__BSESC_MRK_WS));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, null, "abc", markerStart, markerEnd, StringUtils.SEARCH_MODE__BSESC_MRK_WS));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "abc", (String) null, markerStart, markerEnd, StringUtils.SEARCH_MODE__BSESC_MRK_WS));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "abc", "", markerStart, markerEnd, StringUtils.SEARCH_MODE__BSESC_MRK_WS));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "abc", "bcd", markerStart, markerEnd, StringUtils.SEARCH_MODE__BSESC_MRK_WS));
        assertEquals(6, StringUtils.indexOfIgnoreCase(0, "ab -- abc", "abc", markerStart, markerEnd, StringUtils.SEARCH_MODE__BSESC_MRK_WS));
        assertEquals(5, StringUtils.indexOfIgnoreCase(0, "ab # abc", "abc", markerStart, markerEnd, StringUtils.SEARCH_MODE__BSESC_MRK_WS));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "ab/*/* /c", "abc", markerStart, markerEnd, StringUtils.SEARCH_MODE__BSESC_MRK_WS));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "ab/**/c", "abc", markerStart, markerEnd, StringUtils.SEARCH_MODE__BSESC_MRK_WS));
        assertEquals(5, StringUtils.indexOfIgnoreCase(0, "ab/* abc */c", "abc", markerStart, markerEnd, StringUtils.SEARCH_MODE__BSESC_MRK_WS));
        assertEquals(0, StringUtils.indexOfIgnoreCase(0, "abc", "abc", markerStart, markerEnd, StringUtils.SEARCH_MODE__BSESC_MRK_WS));
        assertEquals(3, StringUtils.indexOfIgnoreCase(0, "abc d efg", " d ", markerStart, markerEnd, StringUtils.SEARCH_MODE__BSESC_MRK_WS));

        // exhaustive test set
        searchInMulti = new String[] { "A \"strange \"STRONG SsStRiNg to be searched in", "A 'strange 'STRONG SsStRiNg to be searched in",
                "A `strange `STRONG SsStRiNg to be searched in", "A (strange )STRONG SsStRiNg to be searched in" };
        searchForMulti = new String[] { "STR", "sstr", "Z", "a str", " in", "b" };
        expectedIdx = new int[] { 12, 20, -1, -1, 42, 31 };
        for (int i = 0; i < searchForMulti.length; i++) {
            for (int j = 0; j < searchInMulti.length; j++) {
                // multiple markers
                assertEquals("Test C." + j + "." + i, expectedIdx[i],
                        StringUtils.indexOfIgnoreCase(0, searchInMulti[j], searchForMulti[i], markerStart, markerEnd, StringUtils.SEARCH_MODE__MRK_WS));
                assertEquals("Test C." + j + "." + i, expectedIdx[i],
                        StringUtils.indexOfIgnoreCase(0, searchInMulti[j], searchForMulti[i], markerStart, markerEnd, StringUtils.SEARCH_MODE__BSESC_MRK_WS));

                // single marker
                assertEquals("Test C." + j + "." + i, expectedIdx[i], StringUtils.indexOfIgnoreCase(0, searchInMulti[j], searchForMulti[i],
                        markerStart.substring(j, j + 1), markerEnd.substring(j, j + 1), StringUtils.SEARCH_MODE__MRK_WS));
                assertEquals("Test C." + j + "." + i, expectedIdx[i], StringUtils.indexOfIgnoreCase(0, searchInMulti[j], searchForMulti[i],
                        markerStart.substring(j, j + 1), markerEnd.substring(j, j + 1), StringUtils.SEARCH_MODE__BSESC_MRK_WS));
            }
        }

        searchIn = "A (`'\"strange \"'`)STRONG SsStRiNg to be searched in";
        searchForMulti = new String[] { "STR", "sstr", "Z", "a str", " in", "b" };
        expectedIdx = new int[] { 18, 26, -1, -1, 48, 37 };
        for (int i = 0; i < searchForMulti.length; i++) {
            // multiple markers
            assertEquals("Test C.4." + i, expectedIdx[i],
                    StringUtils.indexOfIgnoreCase(0, searchIn, searchForMulti[i], markerStart, markerEnd, StringUtils.SEARCH_MODE__BSESC_MRK_WS));
            // single marker
            assertEquals("Test C.5." + i, expectedIdx[i],
                    StringUtils.indexOfIgnoreCase(0, searchIn, searchForMulti[i], "'", "'", StringUtils.SEARCH_MODE__BSESC_MRK_WS));
        }

        searchIn = "A 'strange \\''STRONG \\`SsSTRING\\\" to be searched in";
        searchForMulti = new String[] { "STR", "sstr", "Z", "a str", " in", "b" };
        expectedIdx = new int[] { 14, 24, -1, -1, 48, 37 };
        for (int i = 0; i < searchForMulti.length; i++) {
            // multiple markers
            assertEquals("Test C.6." + i, expectedIdx[i],
                    StringUtils.indexOfIgnoreCase(0, searchIn, searchForMulti[i], markerStart, markerEnd, StringUtils.SEARCH_MODE__BSESC_MRK_WS));
            // single marker
            assertEquals("Test C.7." + i, expectedIdx[i],
                    StringUtils.indexOfIgnoreCase(0, searchIn, searchForMulti[i], "'", "'", StringUtils.SEARCH_MODE__BSESC_MRK_WS));
        }

        /*
         * D. test indexOfIgnoreCase(int startingPosition, String searchIn, String searchFor, String openingMarkers, String closingMarkers, Set<SearchMode>
         * searchMode) using combined and single search modes
         */
        // basic test set
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, null, (String) null, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, null, "abc", markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "abc", (String) null, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "abc", "", markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "abc", "bcd", markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "ab -- abc", "abc", markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "ab # abc", "abc", markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "ab/*/* /c", "abc", markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "ab/**/c", "abc", markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "ab/* abc */c", "abc", markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(0, StringUtils.indexOfIgnoreCase(0, "abc", "abc", markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(3, StringUtils.indexOfIgnoreCase(0, "abc d efg", " d ", markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));

        // exhaustive test set
        searchIn = "/* MySQL01 *//* MySQL02 */ \"MySQL03\" /* MySQL04 */-- MySQL05\n/* MySQL06 *//* MySQL07 */ 'MySQL08' /* MySQL09 */-- # MySQL10\r\n"
                + "/* MySQL11 *//* MySQL12 */ `MySQL13` /* MySQL14 */# MySQL15\r\n/* MySQL16 *//* MySQL17 */ (MySQL18) /* MySQL19 */# -- MySQL20 \n"
                + "/* MySQL21 *//* MySQL22 */ \\MySQL23--;/*! MySQL24 */ MySQL25 --";
        searchFor = "mYSql";

        // 1. different markers in method arguments
        pos = StringUtils.indexOfIgnoreCase(0, searchIn, searchFor, null, null, StringUtils.SEARCH_MODE__BSESC_COM_WS);
        assertEquals(3, testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));
        pos = StringUtils.indexOfIgnoreCase(0, searchIn, searchFor, "", "", StringUtils.SEARCH_MODE__ALL);
        assertEquals(3, testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));
        pos = StringUtils.indexOfIgnoreCase(0, searchIn, searchFor, "'`(", "'`)", StringUtils.SEARCH_MODE__ALL);
        assertEquals(3, testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));
        pos = StringUtils.indexOfIgnoreCase(0, searchIn, searchFor, "\"`(", "\"`)", StringUtils.SEARCH_MODE__ALL);
        assertEquals(8, testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));
        pos = StringUtils.indexOfIgnoreCase(0, searchIn, searchFor, "\"'(", "\"')", StringUtils.SEARCH_MODE__ALL);
        assertEquals(13, testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));
        pos = StringUtils.indexOfIgnoreCase(0, searchIn, searchFor, "\"'`", "\"'`", StringUtils.SEARCH_MODE__ALL);
        assertEquals(18, testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));

        // 2a. search mode: all but skip markers
        searchMode = StringUtils.SEARCH_MODE__BSESC_COM_WS;
        pos = 0;
        expectedIdx = new int[] { 3, 8, 13, 18, 24, 25, -1 };
        for (int i = 0; i < expectedIdx.length; i++, pos++) {
            pos = StringUtils.indexOfIgnoreCase(pos, searchIn, searchFor, markerStart, markerEnd, searchMode);
            assertEquals("Test D.2a." + i, expectedIdx[i], testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));
        }
        // 2b. search mode: only skip markers
        searchMode = EnumSet.of(SearchMode.SKIP_BETWEEN_MARKERS);
        pos = 0;
        expectedIdx = new int[] { 1, 2, 4, 5, 6, 7, 9, 10, 11, 12, 14, 15, 16, 17, 19, 20, 21, 22, 23, 24, 25, -1 };
        for (int i = 0; i < expectedIdx.length; i++, pos++) {
            pos = StringUtils.indexOfIgnoreCase(pos, searchIn, searchFor, markerStart, markerEnd, searchMode);
            assertEquals("Test D.2b." + i, expectedIdx[i], testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));
        }

        // 3a. search mode: all but skip line comments
        searchMode = EnumSet.of(SearchMode.ALLOW_BACKSLASH_ESCAPE, SearchMode.SKIP_BETWEEN_MARKERS, SearchMode.SKIP_BLOCK_COMMENTS,
                SearchMode.SKIP_WHITE_SPACE);
        pos = 0;
        expectedIdx = new int[] { 5, 10, 15, 20, 24, 25, -1 };
        for (int i = 0; i < expectedIdx.length; i++, pos++) {
            pos = StringUtils.indexOfIgnoreCase(pos, searchIn, searchFor, markerStart, markerEnd, searchMode);
            assertEquals("Test D.3a." + i, expectedIdx[i], testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));
        }
        // 3b. search mode: only skip line comments
        searchMode = EnumSet.of(SearchMode.SKIP_LINE_COMMENTS);
        pos = 0;
        expectedIdx = new int[] { 1, 2, 3, 4, 6, 7, 8, 9, 11, 12, 13, 14, 16, 17, 18, 19, 21, 22, 23, 24, 25, -1 };
        for (int i = 0; i < expectedIdx.length; i++, pos++) {
            pos = StringUtils.indexOfIgnoreCase(pos, searchIn, searchFor, markerStart, markerEnd, searchMode);
            assertEquals("Test D.3b." + i, expectedIdx[i], testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));
        }

        // 4a. search mode: all but skip block comments
        searchMode = EnumSet.of(SearchMode.ALLOW_BACKSLASH_ESCAPE, SearchMode.SKIP_BETWEEN_MARKERS, SearchMode.SKIP_LINE_COMMENTS, SearchMode.SKIP_WHITE_SPACE);
        pos = 0;
        expectedIdx = new int[] { 1, 2, 4, 6, 7, 9, 11, 12, 14, 16, 17, 19, 21, 22, 24, 25, -1 };
        for (int i = 0; i < expectedIdx.length; i++, pos++) {
            pos = StringUtils.indexOfIgnoreCase(pos, searchIn, searchFor, markerStart, markerEnd, searchMode);
            assertEquals("Test D.4a." + i, expectedIdx[i], testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));
        }
        // 4b. search mode: only skip block comments
        searchMode = EnumSet.of(SearchMode.SKIP_BLOCK_COMMENTS);
        pos = 0;
        expectedIdx = new int[] { 3, 5, 8, 10, 13, 15, 18, 20, 23, 24, 25, -1 };
        for (int i = 0; i < expectedIdx.length; i++, pos++) {
            pos = StringUtils.indexOfIgnoreCase(pos, searchIn, searchFor, markerStart, markerEnd, searchMode);
            assertEquals("Test D.4b." + i, expectedIdx[i], testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));
        }

        // 5a. search mode: all but allow backslash escape
        pos = StringUtils.indexOfIgnoreCase(0, searchIn, searchFor, markerStart, markerEnd, StringUtils.SEARCH_MODE__MRK_COM_WS);
        assertEquals(23, testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));
        // 5b. search mode: only allow backslash escape
        searchMode = EnumSet.of(SearchMode.ALLOW_BACKSLASH_ESCAPE);
        pos = 0;
        expectedIdx = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 24, 25, -1 };
        for (int i = 0; i < expectedIdx.length; i++, pos++) {
            pos = StringUtils.indexOfIgnoreCase(pos, searchIn, searchFor, markerStart, markerEnd, searchMode);
            assertEquals("Test D.5b." + i, expectedIdx[i], testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));
        }

        // 6. all together
        pos = 0;
        expectedIdx = new int[] { 24, 25, -1 };
        for (int i = 0; i < expectedIdx.length; i++, pos++) {
            pos = StringUtils.indexOfIgnoreCase(pos, searchIn, searchFor, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL);
            assertEquals("Test D.6." + i, expectedIdx[i], testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));
        }
        pos = StringUtils.indexOfIgnoreCase(0, searchIn, "YourSQL", markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL);
        assertEquals(-1, testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));

        // 7. none
        pos = 0;
        for (int i = 1; i <= 25; i++, pos++) {
            pos = StringUtils.indexOfIgnoreCase(pos, searchIn, searchFor, markerStart, markerEnd, StringUtils.SEARCH_MODE__NONE);
            assertEquals("Test D.7." + i, i, testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));
        }
        pos = StringUtils.indexOfIgnoreCase(pos + 1, searchIn, searchFor, markerStart, markerEnd, StringUtils.SEARCH_MODE__NONE);
        assertEquals(-1, testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));
        pos = StringUtils.indexOfIgnoreCase(0, searchIn, "YourSQL", markerStart, markerEnd, StringUtils.SEARCH_MODE__NONE);
        assertEquals(-1, testIndexOfIgnoreCaseMySQLIndexMarker(searchIn, pos));

        /*
         * E. test indexOfIgnoreCase(int startingPosition, String searchIn, String searchFor, String openingMarkers, String closingMarkers[, String
         * overridingMarkers], Set<SearchMode> searchMode) illegal markers arguments
         */
        assertThrows(IllegalArgumentException.class,
                "Illegal argument value null for openingMarkers and/or - for closingMarkers. These cannot be null and must have the same length.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        StringUtils.indexOfIgnoreCase(0, "abc", "abc", null, "-", EnumSet.of(SearchMode.SKIP_BETWEEN_MARKERS));
                        return null;
                    }
                });
        assertThrows(IllegalArgumentException.class,
                "Illegal argument value - for openingMarkers and/or null for closingMarkers. These cannot be null and must have the same length.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        StringUtils.indexOfIgnoreCase(0, "abc", "abc", "-", null, EnumSet.of(SearchMode.SKIP_BETWEEN_MARKERS));
                        return null;
                    }
                });
        assertThrows(IllegalArgumentException.class,
                "Illegal argument value null for openingMarkers and/or null for closingMarkers. These cannot be null and must have the same length.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        StringUtils.indexOfIgnoreCase(0, "abc", "abc", null, null, EnumSet.of(SearchMode.SKIP_BETWEEN_MARKERS));
                        return null;
                    }
                });
        assertThrows(IllegalArgumentException.class,
                "Illegal argument value - for openingMarkers and/or -! for closingMarkers. These cannot be null and must have the same length.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        StringUtils.indexOfIgnoreCase(0, "abc", "abc", "-", "-!", EnumSet.of(SearchMode.SKIP_BETWEEN_MARKERS));
                        return null;
                    }
                });
        assertThrows(IllegalArgumentException.class,
                "Illegal argument value null for overridingMarkers. These cannot be null and must be a sub-set of openingMarkers -!.", new Callable<Void>() {
                    public Void call() throws Exception {
                        StringUtils.indexOfIgnoreCase(0, "abc", "abc", "-!", "-!", null, EnumSet.of(SearchMode.SKIP_BETWEEN_MARKERS));
                        return null;
                    }
                });
        assertThrows(IllegalArgumentException.class,
                "Illegal argument value ' for overridingMarkers. These cannot be null and must be a sub-set of openingMarkers -!.", new Callable<Void>() {
                    public Void call() throws Exception {
                        StringUtils.indexOfIgnoreCase(0, "abc", "abc", "-!", "-!", "'", EnumSet.of(SearchMode.SKIP_BETWEEN_MARKERS));
                        return null;
                    }
                });

        /*
         * F. test indexOfIgnoreCase(int startingPosition, String searchIn, String searchFor, String openingMarkers, String closingMarkers, Set<SearchMode>
         * searchMode) special cases
         */
        // unclosed, unopened or nested block comments
        searchMode = EnumSet.of(SearchMode.SKIP_BLOCK_COMMENTS);
        searchIn = "one * /* two /* * / three /*/*/ four * /";
        searchForMulti = new String[] { "one", "two", "three", "four" };
        expectedIdx = new int[] { 0, -1, -1, 32 };
        for (int i = 0; i < searchForMulti.length; i++) {
            assertEquals("Test F.1." + i, expectedIdx[i], StringUtils.indexOfIgnoreCase(0, searchIn, searchForMulti[i], markerStart, markerEnd, searchMode));
        }
        searchMode = EnumSet.of(SearchMode.ALLOW_BACKSLASH_ESCAPE, SearchMode.SKIP_BETWEEN_MARKERS, SearchMode.SKIP_LINE_COMMENTS, SearchMode.SKIP_WHITE_SPACE);
        expectedIdx = new int[] { 0, 9, 20, 32 };
        for (int i = 0; i < searchForMulti.length; i++) {
            assertEquals("Test F.2." + i, expectedIdx[i], StringUtils.indexOfIgnoreCase(0, searchIn, searchForMulti[i], markerStart, markerEnd, searchMode));
        }

        // double quoted escapes, including some "noise" chars
        searchMode = EnumSet.of(SearchMode.SKIP_BETWEEN_MARKERS);
        searchIn = "one 'two\" ''three''' four\"";
        searchForMulti = new String[] { "one", "two", "three", "four" };
        expectedIdx = new int[] { 0, -1, -1, 21 };
        for (int i = 0; i < searchForMulti.length; i++) {
            assertEquals("Test F.3." + i, expectedIdx[i], StringUtils.indexOfIgnoreCase(0, searchIn, searchForMulti[i], markerStart, markerEnd, searchMode));
        }
        searchMode = StringUtils.SEARCH_MODE__BSESC_COM_WS;
        expectedIdx = new int[] { 0, 5, 12, 21 };
        for (int i = 0; i < searchForMulti.length; i++) {
            assertEquals("Test F.4." + i, expectedIdx[i], StringUtils.indexOfIgnoreCase(0, searchIn, searchForMulti[i], markerStart, markerEnd, searchMode));
        }

        // nested different opening/closing marker, including some "noise" chars
        searchMode = EnumSet.of(SearchMode.SKIP_BETWEEN_MARKERS);
        searchIn = "one (two\"( (three''')) )four)";
        searchForMulti = new String[] { "one", "two", "three", "four" };
        expectedIdx = new int[] { 0, -1, -1, 24 };
        for (int i = 0; i < searchForMulti.length; i++) {
            assertEquals("Test F.5." + i, expectedIdx[i], StringUtils.indexOfIgnoreCase(0, searchIn, searchForMulti[i], markerStart, markerEnd, searchMode));
        }
        searchMode = StringUtils.SEARCH_MODE__BSESC_COM_WS;
        expectedIdx = new int[] { 0, 5, 12, 24 };
        for (int i = 0; i < searchForMulti.length; i++) {
            assertEquals("Test F.6." + i, expectedIdx[i], StringUtils.indexOfIgnoreCase(0, searchIn, searchForMulti[i], markerStart, markerEnd, searchMode));
        }

        /*
         * G. test indexOfIgnoreCase(int startingPosition, String searchIn, String searchFor[], String openingMarkers, String closingMarkers, Set<SearchMode>
         * searchMode) using all combined search modes
         */
        // basic test set
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, null, (String[]) null, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, null, new String[] { "abc" }, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "abc", (String[]) null, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "abc", new String[] {}, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "abc", new String[] { "", "" }, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "abc -- d", new String[] { "c", "d" }, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(0, StringUtils.indexOfIgnoreCase(0, "abc", new String[] { "abc" }, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(-1,
                StringUtils.indexOfIgnoreCase(0, "abc d   efg h", new String[] { " d ", " efg" }, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(3, StringUtils.indexOfIgnoreCase(0, "abc d   efg h", new String[] { " d ", "efg" }, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));

        // exhaustive test set
        searchForMulti = new String[] { "ONE", "two", "ThrEE" };

        // 1. simple strings
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "onetwothee", searchForMulti, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(-1, StringUtils.indexOfIgnoreCase(0, "one one one one         two", searchForMulti, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(11, StringUtils.indexOfIgnoreCase(0, "onetwothee one  two  three", searchForMulti, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(20,
                StringUtils.indexOfIgnoreCase(0, "/* one two three */ one two three", searchForMulti, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(38, StringUtils.indexOfIgnoreCase(0, "/* one two three *//* one two three */one two three", searchForMulti, markerStart, markerEnd,
                StringUtils.SEARCH_MODE__ALL));
        assertEquals(7,
                StringUtils.indexOfIgnoreCase(0, "/*one*/one/*two*/two/*three*/three", searchForMulti, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(0,
                StringUtils.indexOfIgnoreCase(0, "one/*one*/two/*two*/three/*three*/", searchForMulti, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(16,
                StringUtils.indexOfIgnoreCase(0, "# one two three\none two three", searchForMulti, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(17,
                StringUtils.indexOfIgnoreCase(0, "-- one two three\none two three", searchForMulti, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(22,
                StringUtils.indexOfIgnoreCase(0, "/* one two three */--;one two three", searchForMulti, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(4,
                StringUtils.indexOfIgnoreCase(0, "/*! one two three */--;one two three", searchForMulti, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(9, StringUtils.indexOfIgnoreCase(0, "/*!50616 one two three */--;one two three", searchForMulti, markerStart, markerEnd,
                StringUtils.SEARCH_MODE__ALL));
        assertEquals(16,
                StringUtils.indexOfIgnoreCase(0, "\"one two three\" one two three", searchForMulti, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(16,
                StringUtils.indexOfIgnoreCase(0, "'one two three' one two three", searchForMulti, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(16,
                StringUtils.indexOfIgnoreCase(0, "`one two three` one two three", searchForMulti, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        assertEquals(16,
                StringUtils.indexOfIgnoreCase(0, "(one two three) one two three", searchForMulti, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));

        assertEquals(3, StringUtils.indexOfIgnoreCase(0, "/* one   two   three */ one two three", searchForMulti, markerStart, markerEnd,
                StringUtils.SEARCH_MODE__NONE));
        assertEquals(2,
                StringUtils.indexOfIgnoreCase(0, "# one   two   three\none two three", searchForMulti, markerStart, markerEnd, StringUtils.SEARCH_MODE__NONE));
        assertEquals(3,
                StringUtils.indexOfIgnoreCase(0, "-- one   two   three\none two three", searchForMulti, markerStart, markerEnd, StringUtils.SEARCH_MODE__NONE));

        // 2. complex string
        searchIn = "/* one  two  three *//* one two three */ one 'two' three -- \"one/* one */two  three\" one owt   three\n"
                + "onetwothree # 'one/* one */two  three' one owt   three\noneone /* two *//* three */ -- `one/* one */two  three` one two   three\n"
                + "two -- three\n\n\n/* three */threethree";

        printRuler(searchIn);
        // 2.1. skip all
        assertEquals(159, StringUtils.indexOfIgnoreCase(0, searchIn, searchForMulti, markerStart, markerEnd, StringUtils.SEARCH_MODE__ALL));
        // 2.2. search within block comments
        searchMode = EnumSet.of(SearchMode.ALLOW_BACKSLASH_ESCAPE, SearchMode.SKIP_BETWEEN_MARKERS, SearchMode.SKIP_LINE_COMMENTS, SearchMode.SKIP_WHITE_SPACE);
        assertEquals(3, StringUtils.indexOfIgnoreCase(0, searchIn, searchForMulti, markerStart, markerEnd, searchMode));
        assertEquals(3, StringUtils.indexOfIgnoreCase(0, searchIn, searchForMulti, markerStart, markerEnd, StringUtils.SEARCH_MODE__NONE));
        // 2.3. search within line comments and unidentified markers
        searchMode = EnumSet.of(SearchMode.ALLOW_BACKSLASH_ESCAPE, SearchMode.SKIP_BETWEEN_MARKERS, SearchMode.SKIP_BLOCK_COMMENTS,
                SearchMode.SKIP_WHITE_SPACE);
        assertEquals(61, StringUtils.indexOfIgnoreCase(0, searchIn, searchForMulti, "'`(", "'`)", searchMode));
        assertEquals(116, StringUtils.indexOfIgnoreCase(0, searchIn, searchForMulti, "\"`(", "\"`)", searchMode));
        assertEquals(188, StringUtils.indexOfIgnoreCase(0, searchIn, searchForMulti, "\"'(", "\"')", searchMode));
        assertEquals(212, StringUtils.indexOfIgnoreCase(0, searchIn, searchForMulti, markerStart, markerEnd, searchMode));

        /*
         * H. test indexOfIgnoreCase(int startingPosition, String searchIn, String searchFor[], String openingMarkers, String closingMarkers, Set<SearchMode>
         * searchMode) illegal markers arguments
         */
        assertThrows(IllegalArgumentException.class,
                "Illegal argument value null for openingMarkers and/or - for closingMarkers. These cannot be null and must have the same length.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        StringUtils.indexOfIgnoreCase(0, "abc", new String[] { "abc" }, null, "-", EnumSet.of(SearchMode.SKIP_BETWEEN_MARKERS));
                        return null;
                    }
                });
        assertThrows(IllegalArgumentException.class,
                "Illegal argument value - for openingMarkers and/or null for closingMarkers. These cannot be null and must have the same length.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        StringUtils.indexOfIgnoreCase(0, "abc", new String[] { "abc" }, "-", null, EnumSet.of(SearchMode.SKIP_BETWEEN_MARKERS));
                        return null;
                    }
                });
        assertThrows(IllegalArgumentException.class,
                "Illegal argument value null for openingMarkers and/or null for closingMarkers. These cannot be null and must have the same length.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        StringUtils.indexOfIgnoreCase(0, "abc", new String[] { "abc" }, null, null, EnumSet.of(SearchMode.SKIP_BETWEEN_MARKERS));
                        return null;
                    }
                });
        assertThrows(IllegalArgumentException.class,
                "Illegal argument value - for openingMarkers and/or -! for closingMarkers. These cannot be null and must have the same length.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        StringUtils.indexOfIgnoreCase(0, "abc", new String[] { "abc" }, "-", "-!", EnumSet.of(SearchMode.SKIP_BETWEEN_MARKERS));
                        return null;
                    }
                });
    }

    private int testIndexOfIgnoreCaseMySQLIndexMarker(String source, int pos) {
        return pos == -1 ? -1 : Integer.parseInt(source.substring(pos + 5, pos + 7));
    }

    private static void printRuler(String txt) {
        System.out.printf("      0    5   10   15   20   25   30   35   40   45   50   55   60   65   70   75   80   85   90   95  100%n");
        System.out.printf("      |----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|%n");

        int count = 0;
        for (String line : txt.split("\n")) {
            System.out.printf("%4d+ %s%n", count, line);
            count += line.length() + 1;
        }
    }

    /**
     * Tests StringUtil.indexOfQuoteDoubleAware() method
     * 
     * @throws Exception
     */
    public void testIndexOfQuoteDoubleAware() throws Exception {
        final String[] searchInDoubledQt = new String[] { "A 'strange' \"STRONG\" `SsStRiNg` to be searched in",
                "A ''strange'' \"\"STRONG\"\" ``SsStRiNg`` to be searched in" };

        assertEquals(-1, StringUtils.indexOfQuoteDoubleAware(null, null, 0));
        assertEquals(-1, StringUtils.indexOfQuoteDoubleAware(null, "'", 0));
        assertEquals(-1, StringUtils.indexOfQuoteDoubleAware("abc", null, 0));
        assertEquals(-1, StringUtils.indexOfQuoteDoubleAware("abc", "", 0));
        assertEquals(-1, StringUtils.indexOfQuoteDoubleAware("abc", "bcd", 0));
        assertEquals(0, StringUtils.indexOfQuoteDoubleAware("abc", "abc", 0));

        int qtPos = 0;
        assertEquals(2, qtPos = StringUtils.indexOfQuoteDoubleAware(searchInDoubledQt[0], "'", 0));
        assertEquals(10, qtPos = StringUtils.indexOfQuoteDoubleAware(searchInDoubledQt[0], "'", qtPos + 1));
        assertEquals(-1, StringUtils.indexOfQuoteDoubleAware(searchInDoubledQt[0], "'", qtPos + 1));
        assertEquals(12, qtPos = StringUtils.indexOfQuoteDoubleAware(searchInDoubledQt[0], "\"", 0));
        assertEquals(19, qtPos = StringUtils.indexOfQuoteDoubleAware(searchInDoubledQt[0], "\"", qtPos + 1));
        assertEquals(-1, StringUtils.indexOfQuoteDoubleAware(searchInDoubledQt[0], "\"", qtPos + 1));
        assertEquals(21, qtPos = StringUtils.indexOfQuoteDoubleAware(searchInDoubledQt[0], "`", 0));
        assertEquals(30, qtPos = StringUtils.indexOfQuoteDoubleAware(searchInDoubledQt[0], "`", qtPos + 1));
        assertEquals(-1, StringUtils.indexOfQuoteDoubleAware(searchInDoubledQt[0], "`", qtPos + 1));

        assertEquals(-1, StringUtils.indexOfQuoteDoubleAware(searchInDoubledQt[1], "'", 0));
        assertEquals(-1, StringUtils.indexOfQuoteDoubleAware(searchInDoubledQt[1], "\"", 0));
        assertEquals(-1, StringUtils.indexOfQuoteDoubleAware(searchInDoubledQt[1], "`", 0));
    }

    /**
     * Tests StringUtil.appendAsHex() methods.
     * 
     * @throws Exception
     */
    public void testAppendAsHex() throws Exception {
        final byte[] testBytes = new byte[256];
        final int[] testInts = new int[] { Integer.MIN_VALUE, -1023, 0, 511, 512, 0x100FF, 0x10000FF, Integer.MAX_VALUE };
        StringBuilder builder;

        for (int i = 0; i < 256; i++) {
            testBytes[i] = (byte) i;
        }

        // test StringUtils.appendAsHex(StringBuilder, byte[])
        builder = new StringBuilder(1024);
        builder.append("0x");
        for (byte b : testBytes) {
            builder.append(String.format("%02x", b));
        }
        String expected = builder.toString();

        builder = new StringBuilder(1024);
        StringUtils.appendAsHex(builder, testBytes);

        assertEquals("Wrong byte[] to HEX convertion", expected, builder.toString());

        // test StringUtils.appendAsHex(StringBuilder, int)
        for (int i : testInts) {
            builder = new StringBuilder(1024);
            StringUtils.appendAsHex(builder, i);
            assertEquals("Wrong int to HEX convertion", "0x" + Integer.toHexString(i), builder.toString());
        }
    }

    /**
     * Tests StringUtil.getBytes() methods.
     * 
     * @throws Exception
     */
    public void testGetBytes() throws Exception {
        final int offset = 8;
        final int length = 13;
        final String text = "MySQL ≈ 𝄞 for my ears";
        final String textPart = text.substring(offset, offset + length);
        final String textWrapped = "`MySQL ≈ 𝄞 for my ears`";
        final char[] textAsCharArray = text.toCharArray();

        byte[] asBytesFromString;
        byte[] asBytesFromStringUtils;

        asBytesFromString = text.getBytes();
        asBytesFromStringUtils = StringUtils.getBytes(text);
        assertByteArrayEquals("Default Charset: " + Charset.defaultCharset().name(), asBytesFromString, asBytesFromStringUtils);

        asBytesFromString = textPart.getBytes();
        asBytesFromStringUtils = StringUtils.getBytes(text, offset, length);
        assertByteArrayEquals("Default Charset: " + Charset.defaultCharset().name(), asBytesFromString, asBytesFromStringUtils);

        Map<String, Charset> charsetMap = Charset.availableCharsets();
        for (Charset cs : charsetMap.values()) {
            if (cs.canEncode()) {
                asBytesFromString = text.getBytes(cs.name());

                asBytesFromStringUtils = StringUtils.getBytes(text, cs.name());
                assertByteArrayEquals("Custom Charset: " + cs.name(), asBytesFromString, asBytesFromStringUtils);
                asBytesFromStringUtils = StringUtils.getBytes(textAsCharArray, cs.name());
                assertByteArrayEquals("Custom Charset: " + cs.name(), asBytesFromString, asBytesFromStringUtils);

                asBytesFromStringUtils = StringUtils.getBytes(text, cs.name());
                assertByteArrayEquals("Custom Charset: " + cs.name(), asBytesFromString, asBytesFromStringUtils);
                asBytesFromStringUtils = StringUtils.getBytes(textAsCharArray, cs.name());
                assertByteArrayEquals("Custom Charset: " + cs.name(), asBytesFromString, asBytesFromStringUtils);

                asBytesFromString = textPart.getBytes(cs.name());

                asBytesFromStringUtils = StringUtils.getBytes(text, offset, length, cs.name());
                assertByteArrayEquals("Custom Charset: " + cs.name(), asBytesFromString, asBytesFromStringUtils);
                asBytesFromStringUtils = StringUtils.getBytes(textAsCharArray, offset, length, cs.name());
                assertByteArrayEquals("Custom Charset: " + cs.name(), asBytesFromString, asBytesFromStringUtils);

                asBytesFromStringUtils = StringUtils.getBytes(text, offset, length, cs.name());
                assertByteArrayEquals("Custom Charset: " + cs.name(), asBytesFromString, asBytesFromStringUtils);
                asBytesFromStringUtils = StringUtils.getBytes(textAsCharArray, offset, length, cs.name());
                assertByteArrayEquals("Custom Charset: " + cs.name(), asBytesFromString, asBytesFromStringUtils);

                asBytesFromString = textWrapped.getBytes(cs.name());

                asBytesFromStringUtils = StringUtils.getBytesWrapped(text, '`', '`', cs.name());
                assertByteArrayEquals("Custom Charset: " + cs.name(), asBytesFromString, asBytesFromStringUtils);
            }
        }
    }

    /**
     * Tests StringUtil.quoteIdentifier() and StringUtil.unQuoteIdentifier() methods using back quote marks.
     * 
     * @throws Exception
     */
    public void testQuoteUnQuoteIdentifierWithBackQuote() throws Exception {
        // Base set of identifiers
        String[] identifiers = new String[] { "abcxyz", "abc`xyz", "abc``xyz", "abc```xyz", // 1..4
                "`abcxyz`", "`abc`xyz`", "`abc``xyz`", "`abc```xyz`",                       // 5..8
                "``abcxyz``", "``abc`xyz``", "``abc``xyz``", "``abc```xyz``",               // 9..12
                "```abcxyz```", "```abc`xyz```", "```abc``xyz```", "```abc```xyz```",       // 13..16
                "`abcxyz", "``abcxyz", "```abcxyz", "abcxyz`", "abcxyz``", "abcxyz```",     // 17..22
                "``abcxyz`", "``abc`xyz`", "``abc``xyz`", "``abc```xyz`",                   // 23..26
                "```abcxyz`", "```abc`xyz`", "```abc``xyz`", "```abc```xyz`",               // 27..30
                "`abcxyz``", "`abc`xyz``", "`abc``xyz``", "`abc```xyz``",                   // 31..34
                "`abcxyz```", "`abc`xyz```", "`abc``xyz```", "`abc```xyz```"                // 35..38
        };

        // Identifiers unquoted
        String[] identifiersUnQuoted = new String[] { "abcxyz", "abc`xyz", "abc``xyz", "abc```xyz", // 1..4
                "abcxyz", "`abc`xyz`", "abc`xyz", "`abc```xyz`",                                    // 5..8
                "``abcxyz``", "``abc`xyz``", "``abc``xyz``", "``abc```xyz``",                       // 9..12
                "`abcxyz`", "```abc`xyz```", "`abc`xyz`", "```abc```xyz```",                        // 13..16
                "`abcxyz", "``abcxyz", "```abcxyz", "abcxyz`", "abcxyz``", "abcxyz```",             // 17..22
                "``abcxyz`", "``abc`xyz`", "``abc``xyz`", "``abc```xyz`",                           // 23..26
                "`abcxyz", "```abc`xyz`", "`abc`xyz", "```abc```xyz`",                              // 27..30
                "`abcxyz``", "`abc`xyz``", "`abc``xyz``", "`abc```xyz``",                           // 31..34
                "abcxyz`", "`abc`xyz```", "abc`xyz`", "`abc```xyz```"                               // 35..38
        };

        // Identifiers quoted in non-pedantic mode
        String[] identifiersQuotedNonPedantic = new String[] { "`abcxyz`", "`abc``xyz`", "`abc````xyz`", "`abc``````xyz`", // 1..4
                "`abcxyz`", "```abc``xyz```", "`abc``xyz`", "```abc``````xyz```",                                          // 5..8
                "`````abcxyz`````", "`````abc``xyz`````", "`````abc````xyz`````", "`````abc``````xyz`````",                // 9..12
                "```abcxyz```", "```````abc``xyz```````", "```abc``xyz```", "```````abc``````xyz```````",                  // 13..16
                "```abcxyz`", "`````abcxyz`", "```````abcxyz`", "`abcxyz```", "`abcxyz`````", "`abcxyz```````",            // 17..22
                "`````abcxyz```", "`````abc``xyz```", "`````abc````xyz```", "`````abc``````xyz```",                        // 23..26
                "```abcxyz`", "```````abc``xyz```", "```abc``xyz`", "```````abc``````xyz```",                              // 27..30
                "```abcxyz`````", "```abc``xyz`````", "```abc````xyz`````", "```abc``````xyz`````",                        // 31..34
                "`abcxyz```", "```abc``xyz```````", "`abc``xyz```", "```abc``````xyz```````"                               // 35..38
        };

        // Identifiers quoted in pedantic mode
        String[] identifiersQuotedPedantic = new String[] { "`abcxyz`", "`abc``xyz`", "`abc````xyz`", "`abc``````xyz`",     // 1..4
                "```abcxyz```", "```abc``xyz```", "```abc````xyz```", "```abc``````xyz```",                                 // 5..8
                "`````abcxyz`````", "`````abc``xyz`````", "`````abc````xyz`````", "`````abc``````xyz`````",                 // 9..12
                "```````abcxyz```````", "```````abc``xyz```````", "```````abc````xyz```````", "```````abc``````xyz```````", // 13..16
                "```abcxyz`", "`````abcxyz`", "```````abcxyz`", "`abcxyz```", "`abcxyz`````", "`abcxyz```````",             // 17..22
                "`````abcxyz```", "`````abc``xyz```", "`````abc````xyz```", "`````abc``````xyz```",                         // 23..26
                "```````abcxyz```", "```````abc``xyz```", "```````abc````xyz```", "```````abc``````xyz```",                 // 27..30
                "```abcxyz`````", "```abc``xyz`````", "```abc````xyz`````", "```abc``````xyz`````",                         // 31..34
                "```abcxyz```````", "```abc``xyz```````", "```abc````xyz```````", "```abc``````xyz```````"                  // 35..38
        };

        // Quoting rules (non-pedantic mode):
        // * identifiers[n] --> identifiersQuotedNonPedantic[n]
        for (int i = 0; i < identifiers.length; i++) {
            assertEquals(i + 1 + ". " + identifiers[i] + ". non-pedantic quoting", identifiersQuotedNonPedantic[i],
                    StringUtils.quoteIdentifier(identifiers[i], "`", false));
            assertEquals(i + 1 + ". " + identifiers[i] + ". non-pedantic quoting", identifiersQuotedNonPedantic[i],
                    StringUtils.quoteIdentifier(identifiers[i], false));
        }

        // Quoting rules (pedantic mode):
        // * identifiers[n] --> identifiersQuotedPedantic[n]
        // * identifiersUnQuoted[n] --> identifiersQuotedNonPedantic[n]
        for (int i = 0; i < identifiers.length; i++) {
            assertEquals(i + 1 + ". " + identifiers[i] + ". pedantic quoting", identifiersQuotedPedantic[i],
                    StringUtils.quoteIdentifier(identifiers[i], "`", true));
            assertEquals(i + 1 + ". " + identifiers[i] + ". pedantic quoting", identifiersQuotedPedantic[i], StringUtils.quoteIdentifier(identifiers[i], true));

            assertEquals(i + 1 + ". " + identifiersUnQuoted[i] + ". pedantic quoting", identifiersQuotedNonPedantic[i],
                    StringUtils.quoteIdentifier(identifiersUnQuoted[i], "`", true));
            assertEquals(i + 1 + ". " + identifiersUnQuoted[i] + ". pedantic quoting", identifiersQuotedNonPedantic[i],
                    StringUtils.quoteIdentifier(identifiersUnQuoted[i], true));
        }

        // Unquoting rules:
        // * identifiers[n] --> identifiersUnQuoted[n]
        // * identifiersQuotedNonPedantic[n] --> identifiersUnQuoted[n]
        // * identifiersQuotedPedantic[n] --> identifiers[n]
        for (int i = 0; i < identifiers.length; i++) {
            assertEquals(i + 1 + ". " + identifiers[i] + ". unquoting", identifiersUnQuoted[i], StringUtils.unQuoteIdentifier(identifiers[i], "`"));
            assertEquals(i + 1 + ". " + identifiersQuotedNonPedantic[i] + ". non-pedantic unquoting", identifiersUnQuoted[i],
                    StringUtils.unQuoteIdentifier(identifiersQuotedNonPedantic[i], "`"));
            assertEquals(i + 1 + ". " + identifiersQuotedPedantic[i] + ". pedantic unquoting", identifiers[i],
                    StringUtils.unQuoteIdentifier(identifiersQuotedPedantic[i], "`"));
        }
    }

    /**
     * Tests StringUtil.quoteIdentifier() and StringUtil.unQuoteIdentifier() methods using double quote marks.
     * 
     * @throws Exception
     */
    public void testQuoteUnQuoteIdentifierWithDoubleQuote() throws Exception {
        // Base set of identifiers
        String[] identifiers = new String[] { "abcxyz", "abc\"xyz", "abc\"\"xyz", "abc\"\"\"xyz",                   // 1..4
                "\"abcxyz\"", "\"abc\"xyz\"", "\"abc\"\"xyz\"", "\"abc\"\"\"xyz\"",                                 // 5..8
                "\"\"abcxyz\"\"", "\"\"abc\"xyz\"\"", "\"\"abc\"\"xyz\"\"", "\"\"abc\"\"\"xyz\"\"",                 // 9..12
                "\"\"\"abcxyz\"\"\"", "\"\"\"abc\"xyz\"\"\"", "\"\"\"abc\"\"xyz\"\"\"", "\"\"\"abc\"\"\"xyz\"\"\"", // 13..16
                "\"abcxyz", "\"\"abcxyz", "\"\"\"abcxyz", "abcxyz\"", "abcxyz\"\"", "abcxyz\"\"\"",                 // 17..22
                "\"\"abcxyz\"", "\"\"abc\"xyz\"", "\"\"abc\"\"xyz\"", "\"\"abc\"\"\"xyz\"",                         // 23..26
                "\"\"\"abcxyz\"", "\"\"\"abc\"xyz\"", "\"\"\"abc\"\"xyz\"", "\"\"\"abc\"\"\"xyz\"",                 // 27..30
                "\"abcxyz\"\"", "\"abc\"xyz\"\"", "\"abc\"\"xyz\"\"", "\"abc\"\"\"xyz\"\"",                         // 31..34
                "\"abcxyz\"\"\"", "\"abc\"xyz\"\"\"", "\"abc\"\"xyz\"\"\"", "\"abc\"\"\"xyz\"\"\""                  // 35..38
        };

        // Identifiers unquoted
        String[] identifiersUnQuoted = new String[] { "abcxyz", "abc\"xyz", "abc\"\"xyz", "abc\"\"\"xyz", // 1..4
                "abcxyz", "\"abc\"xyz\"", "abc\"xyz", "\"abc\"\"\"xyz\"",                                 // 5..8
                "\"\"abcxyz\"\"", "\"\"abc\"xyz\"\"", "\"\"abc\"\"xyz\"\"", "\"\"abc\"\"\"xyz\"\"",       // 9..12
                "\"abcxyz\"", "\"\"\"abc\"xyz\"\"\"", "\"abc\"xyz\"", "\"\"\"abc\"\"\"xyz\"\"\"",         // 13..16
                "\"abcxyz", "\"\"abcxyz", "\"\"\"abcxyz", "abcxyz\"", "abcxyz\"\"", "abcxyz\"\"\"",       // 17..22
                "\"\"abcxyz\"", "\"\"abc\"xyz\"", "\"\"abc\"\"xyz\"", "\"\"abc\"\"\"xyz\"",               // 23..26
                "\"abcxyz", "\"\"\"abc\"xyz\"", "\"abc\"xyz", "\"\"\"abc\"\"\"xyz\"",                     // 27..30
                "\"abcxyz\"\"", "\"abc\"xyz\"\"", "\"abc\"\"xyz\"\"", "\"abc\"\"\"xyz\"\"",               // 31..34
                "abcxyz\"", "\"abc\"xyz\"\"\"", "abc\"xyz\"", "\"abc\"\"\"xyz\"\"\""                      // 35..38
        };

        // Identifiers quoted in non-pedantic mode
        String[] identifiersQuotedNonPedantic = new String[] { "\"abcxyz\"", "\"abc\"\"xyz\"", "\"abc\"\"\"\"xyz\"", "\"abc\"\"\"\"\"\"xyz\"",      // 1..4
                "\"abcxyz\"", "\"\"\"abc\"\"xyz\"\"\"", "\"abc\"\"xyz\"", "\"\"\"abc\"\"\"\"\"\"xyz\"\"\"",                                         // 5..8
                "\"\"\"\"\"abcxyz\"\"\"\"\"", "\"\"\"\"\"abc\"\"xyz\"\"\"\"\"",                                                                     // 9..
                "\"\"\"\"\"abc\"\"\"\"xyz\"\"\"\"\"", "\"\"\"\"\"abc\"\"\"\"\"\"xyz\"\"\"\"\"",                                                     //  ..12
                "\"\"\"abcxyz\"\"\"", "\"\"\"\"\"\"\"abc\"\"xyz\"\"\"\"\"\"\"",                                                                     // 13..
                "\"\"\"abc\"\"xyz\"\"\"", "\"\"\"\"\"\"\"abc\"\"\"\"\"\"xyz\"\"\"\"\"\"\"",                                                         //   ..16
                "\"\"\"abcxyz\"", "\"\"\"\"\"abcxyz\"", "\"\"\"\"\"\"\"abcxyz\"", "\"abcxyz\"\"\"", "\"abcxyz\"\"\"\"\"", "\"abcxyz\"\"\"\"\"\"\"", // 17..22
                "\"\"\"\"\"abcxyz\"\"\"", "\"\"\"\"\"abc\"\"xyz\"\"\"", "\"\"\"\"\"abc\"\"\"\"xyz\"\"\"", "\"\"\"\"\"abc\"\"\"\"\"\"xyz\"\"\"",     // 23..26
                "\"\"\"abcxyz\"", "\"\"\"\"\"\"\"abc\"\"xyz\"\"\"", "\"\"\"abc\"\"xyz\"", "\"\"\"\"\"\"\"abc\"\"\"\"\"\"xyz\"\"\"",                 // 27..30
                "\"\"\"abcxyz\"\"\"\"\"", "\"\"\"abc\"\"xyz\"\"\"\"\"", "\"\"\"abc\"\"\"\"xyz\"\"\"\"\"", "\"\"\"abc\"\"\"\"\"\"xyz\"\"\"\"\"",     // 31..34
                "\"abcxyz\"\"\"", "\"\"\"abc\"\"xyz\"\"\"\"\"\"\"", "\"abc\"\"xyz\"\"\"", "\"\"\"abc\"\"\"\"\"\"xyz\"\"\"\"\"\"\""                  // 35..38
        };

        // Identifiers quoted in pedantic mode
        String[] identifiersQuotedPedantic = new String[] { "\"abcxyz\"", "\"abc\"\"xyz\"", "\"abc\"\"\"\"xyz\"", "\"abc\"\"\"\"\"\"xyz\"",         // 1..4
                "\"\"\"abcxyz\"\"\"", "\"\"\"abc\"\"xyz\"\"\"", "\"\"\"abc\"\"\"\"xyz\"\"\"", "\"\"\"abc\"\"\"\"\"\"xyz\"\"\"",                     // 5..8
                "\"\"\"\"\"abcxyz\"\"\"\"\"", "\"\"\"\"\"abc\"\"xyz\"\"\"\"\"",                                                                     // 9..
                "\"\"\"\"\"abc\"\"\"\"xyz\"\"\"\"\"", "\"\"\"\"\"abc\"\"\"\"\"\"xyz\"\"\"\"\"",                                                     //  ..12
                "\"\"\"\"\"\"\"abcxyz\"\"\"\"\"\"\"", "\"\"\"\"\"\"\"abc\"\"xyz\"\"\"\"\"\"\"",                                                     // 13..
                "\"\"\"\"\"\"\"abc\"\"\"\"xyz\"\"\"\"\"\"\"", "\"\"\"\"\"\"\"abc\"\"\"\"\"\"xyz\"\"\"\"\"\"\"",                                     //   ..16
                "\"\"\"abcxyz\"", "\"\"\"\"\"abcxyz\"", "\"\"\"\"\"\"\"abcxyz\"", "\"abcxyz\"\"\"", "\"abcxyz\"\"\"\"\"", "\"abcxyz\"\"\"\"\"\"\"", // 17..22
                "\"\"\"\"\"abcxyz\"\"\"", "\"\"\"\"\"abc\"\"xyz\"\"\"", "\"\"\"\"\"abc\"\"\"\"xyz\"\"\"", "\"\"\"\"\"abc\"\"\"\"\"\"xyz\"\"\"",     // 23..26
                "\"\"\"\"\"\"\"abcxyz\"\"\"", "\"\"\"\"\"\"\"abc\"\"xyz\"\"\"",                                                                     // 27..
                "\"\"\"\"\"\"\"abc\"\"\"\"xyz\"\"\"", "\"\"\"\"\"\"\"abc\"\"\"\"\"\"xyz\"\"\"",                                                     //   ..30
                "\"\"\"abcxyz\"\"\"\"\"", "\"\"\"abc\"\"xyz\"\"\"\"\"", "\"\"\"abc\"\"\"\"xyz\"\"\"\"\"", "\"\"\"abc\"\"\"\"\"\"xyz\"\"\"\"\"",     // 31..34
                "\"\"\"abcxyz\"\"\"\"\"\"\"", "\"\"\"abc\"\"xyz\"\"\"\"\"\"\"",                                                                     // 35..
                "\"\"\"abc\"\"\"\"xyz\"\"\"\"\"\"\"", "\"\"\"abc\"\"\"\"\"\"xyz\"\"\"\"\"\"\""                                                      //   ..38
        };

        // Quoting rules (non-pedantic mode):
        // * identifiers[n] --> identifiersQuotedNonPedantic[n]
        for (int i = 0; i < identifiers.length; i++) {
            assertEquals(i + 1 + ". " + identifiers[i] + ". non-pedantic quoting", identifiersQuotedNonPedantic[i],
                    StringUtils.quoteIdentifier(identifiers[i], "\"", false));
        }

        // Quoting rules (pedantic mode):
        // * identifiers[n] --> identifiersQuotedPedantic[n]
        // * identifiersUnQuoted[n] --> identifiersQuotedNonPedantic[n]
        for (int i = 0; i < identifiers.length; i++) {
            assertEquals(i + 1 + ". " + identifiers[i] + ". pedantic quoting", identifiersQuotedPedantic[i],
                    StringUtils.quoteIdentifier(identifiers[i], "\"", true));

            assertEquals(i + 1 + ". " + identifiersUnQuoted[i] + ". pedantic quoting", identifiersQuotedNonPedantic[i],
                    StringUtils.quoteIdentifier(identifiersUnQuoted[i], "\"", true));
        }

        // Unquoting rules:
        // * identifiers[n] --> identifiersUnQuoted[n]
        // * identifiersQuotedNonPedantic[n] --> identifiersUnQuoted[n]
        // * identifiersQuotedPedantic[n] --> identifiers[n]
        for (int i = 0; i < identifiers.length; i++) {
            assertEquals(i + 1 + ". " + identifiers[i] + ". unquoting", identifiersUnQuoted[i], StringUtils.unQuoteIdentifier(identifiers[i], "\""));
            assertEquals(i + 1 + ". " + identifiersQuotedNonPedantic[i] + ". non-pedantic unquoting", identifiersUnQuoted[i],
                    StringUtils.unQuoteIdentifier(identifiersQuotedNonPedantic[i], "\""));
            assertEquals(i + 1 + ". " + identifiersQuotedPedantic[i] + ". pedantic unquoting", identifiers[i],
                    StringUtils.unQuoteIdentifier(identifiersQuotedPedantic[i], "\""));
        }
    }

    /**
     * Tests StringUtils.wildCompare() method.
     */
    public void testWildCompare() throws Exception {
        // Null values.
        assertFalse(StringUtils.wildCompareIgnoreCase(null, null));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", null));
        assertFalse(StringUtils.wildCompareIgnoreCase(null, "abcxyz"));
        assertFalse(StringUtils.wildCompareIgnoreCase(null, "_"));
        assertFalse(StringUtils.wildCompareIgnoreCase(null, "%"));
        assertFalse(StringUtils.wildCompareIgnoreCase(null, "_%"));
        assertFalse(StringUtils.wildCompareIgnoreCase(null, "%_"));

        // Empty values.
        assertTrue(StringUtils.wildCompareIgnoreCase("", ""));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", ""));
        assertFalse(StringUtils.wildCompareIgnoreCase("", "abcxyz"));

        // Different letter case.
        assertTrue(StringUtils.wildCompareIgnoreCase("ABCxyz", "abcxyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcXYZ", "abcxyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("ABCXYZ", "abcxyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "ABCxyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "abcXYZ"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "ABCXYZ"));
        assertTrue(StringUtils.wildCompareIgnoreCase("ABCxyz", "ab%YZ"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcXYZ", "AB%yz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("ABCxyz", "ab__YZ"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcXYZ", "AB__yz"));

        // Patterns without wildcards.
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "abcxyz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "zyxcba"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "a"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "abcxy"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "z"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "bcxyz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "aabcxyz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "abcxyzz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "abclmnxyz"));

        // Patterns with wildcard %.
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "a%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "a%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "abc%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "abc%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "abcxyz%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "abcxyz%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%z"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%z"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%abcxyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%abcxyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "a%z"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "a%%%z"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "abc%xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "abc%%%xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%cx%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%cx%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%abcxyz%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%abcxyz%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%b%y%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%b%%%y%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%a%b%c%x%y%z%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%a%%%b%%%c%%%x%%%y%%%z%%%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "abcxyz%z"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "abcxyz%%%z"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "a%abcxyz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "a%%%abcxyz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "ab%cx%cx%yz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "ab%%%cx%%%cx%%%yz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "abc%x"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "abc%%%x"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "c%xyz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "c%%%xyz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "%a%m%z%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%a%%%m%%%z%%%"));

        // Patterns with wildcard _.
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "abcxy_"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "abc___"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "a_____"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "_bcxyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "___xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "_____z"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "ab__yz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "a____z"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "_bcxy_"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "__cx__"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "a_c_y_"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "_b_x_z"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "______"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "abcxyz_"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "abc____"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "a______"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "_abcxyz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "____xyz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "______z"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "abc_xyz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "ab___yz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "a_____z"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "_bc_xy_"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "__c_x__"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "a_c_y__"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "__b_x_z"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "_______"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "abcx_"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "abc__"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "a____"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "_cxyz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "__xyz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "____z"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "ab_yz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "a___z"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "_cxy_"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "a_c__"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "__x_z"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "_____"));

        // Patterns with both wildcards.
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "abc_%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "abc_%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "abc___%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "abc___%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "abc%_"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "abc%%%_"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "abc%___"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "abc%%%___"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%_xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%_xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%___xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%___xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "_%xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "_%%%_xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "___%xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "___%%%xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%_cx_%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%_cx_%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%__cx__%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%__cx__%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "_%cx%_"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "_%%%cx%%%_"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "__%cx%__"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "__%%%cx%%%__"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "_b%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "_b%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "_____z%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "_____z%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%y_"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%y_"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%a_____"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%a_____"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%abc%_%_%_%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%abc%%%_%%%_%%%_%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%_%_%_%xyz%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%_%%%_%%%_%%%xyz%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%a%_%c%x%_%z%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%a%%%_%%%c%%%x%%%_%%%z%%%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "%_c%m%x_%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "%%_c%%%m%%%x_%%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "a%a_c%_yz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "a%%%a_c%%%_yz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "ab_%x_z%z"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "ab_%%%x_z%%%Z"));

        // Patterns with wildcards only.
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "_%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "_%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%_"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%_"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%_%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%_%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%___%___%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%___%%%___%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%_%_%_%_%_%_%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%_%%%_%%%_%%%_%%%_%%%_%%%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "%___%_%___%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "%%%___%%%_%%%___%%%"));

        // Escaped wildcards
        assertTrue(StringUtils.wildCompareIgnoreCase("abc%", "abc%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abc%%%", "abc%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abc%", "abc_"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abc%%%", "abc___"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abc%", "abc\\%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abc%%%", "abc\\%\\%\\%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abc%", "abc%\\%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abc%%%", "abc%\\%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abc%", "abc\\%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abc%%%", "abc\\%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abc_", "abc%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abc___", "abc%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abc_", "abc_"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abc___", "abc___"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abc_", "abc\\_"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abc___", "abc\\_\\_\\_"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abc___", "abc\\_\\__"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abc___", "abc\\__\\_"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abc___", "abc_\\_\\_"));
        assertTrue(StringUtils.wildCompareIgnoreCase("%xyz", "%xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("%%%xyz", "%xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("%xyz", "_xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("%%%xyz", "___xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("%xyz", "\\%xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("%%%xyz", "\\%\\%\\%xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("%xyz", "%\\%xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("%%%xyz", "%\\%xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("%xyz", "\\%%xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("%%%xyz", "\\%%xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("_xyz", "%xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("___xyz", "%xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("_xyz", "_xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("___xyz", "___xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("_xyz", "\\_xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("___xyz", "\\_\\_\\_xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("___xyz", "\\_\\__xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("___xyz", "\\__\\_xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("___xyz", "_\\_\\_xyz"));
        assertTrue(StringUtils.wildCompareIgnoreCase("%_%", "\\%\\_\\%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("_%_", "\\_\\%\\_"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "%abc\\%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "%abc%\\%%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "\\%xyz%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "%\\%%xyz%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "%abc\\%xyz%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "%abc%\\%%xyz%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "%abc\\_%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "%abc%\\_%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "%\\_xyz%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "%\\_%xyz%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "%abc\\_xyz%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyz", "%abc%\\_%xyz%"));

        // Values with repeated patterns.
        assertTrue(StringUtils.wildCompareIgnoreCase("abcabcabcabcabcabc", "%abc"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcabcabcabcabcabc", "%%%abc"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcabcabcabcabcabc", "abc%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcabcabcabcabcabc", "abc%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcabcabcabcabcabc", "%abc%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcabcabcabcabcabc", "%%%abc%%%"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcabcabcabcabcabc", "abc%abc"));
        assertTrue(StringUtils.wildCompareIgnoreCase("abcabcabcabcabcabc", "abc%%%abc"));
        assertFalse(StringUtils.wildCompareIgnoreCase("xyzxyzxyzxyzxyzabc", "%xyz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("xyzxyzxyzxyzxyzabc", "%%%xyz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyzxyzxyzxyzxyz", "xyz%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyzxyzxyzxyzxyz", "xyz%%%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcabcabcabcabcabc", "%xyz%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcabcabcabcabcabc", "%%%xyz%%%"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyzxyzxyzxyzabc", "abc%xyz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyzxyzxyzxyzabc", "abc%%%xyz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyzxyzxyzxyzabc", "xyz%abc"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyzxyzxyzxyzabc", "xyz%%%abc"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyzxyzxyzxyzabc", "xyz%xyz"));
        assertFalse(StringUtils.wildCompareIgnoreCase("abcxyzxyzxyzxyzabc", "xyz%%%xyz"));
    }

    /**
     * Tests StringUtils.split() methods.
     */
    public void testSplit() throws Exception {
        String testString = "  abstract, (contents, table of \"['figure''s'],(tables\"),  introduction  , \"methods(), ()results\", ['discussion'']', conclusion]   ";
        List<String> stringParts;

        // non existing split char, trim
        stringParts = StringUtils.split(testString, ";", true);
        assertEquals(1, stringParts.size());
        assertEquals(testString.trim(), stringParts.get(0));

        // non existing split char, don't trim
        stringParts = StringUtils.split(testString, ";", false);
        assertEquals(1, stringParts.size());
        assertEquals(testString, stringParts.get(0));

        // full split, trim
        stringParts = StringUtils.split(testString, ",", true);
        assertEquals(9, stringParts.size());
        assertEquals("abstract", stringParts.get(0));
        assertEquals("(contents", stringParts.get(1));
        assertEquals("table of \"['figure''s']", stringParts.get(2));
        assertEquals("(tables\")", stringParts.get(3));
        assertEquals("introduction", stringParts.get(4));
        assertEquals("\"methods()", stringParts.get(5));
        assertEquals("()results\"", stringParts.get(6));
        assertEquals("['discussion'']'", stringParts.get(7));
        assertEquals("conclusion]", stringParts.get(8));

        // full split, don't trim
        stringParts = StringUtils.split(testString, ",", false);
        assertEquals(9, stringParts.size());
        assertEquals("  abstract", stringParts.get(0));
        assertEquals(" (contents", stringParts.get(1));
        assertEquals(" table of \"['figure''s']", stringParts.get(2));
        assertEquals("(tables\")", stringParts.get(3));
        assertEquals("  introduction  ", stringParts.get(4));
        assertEquals(" \"methods()", stringParts.get(5));
        assertEquals(" ()results\"", stringParts.get(6));
        assertEquals(" ['discussion'']'", stringParts.get(7));
        assertEquals(" conclusion]   ", stringParts.get(8));

        // most common markers, trim
        stringParts = StringUtils.split(testString, ",", "'\"", "'\"", true);
        assertEquals(7, stringParts.size());
        assertEquals("abstract", stringParts.get(0));
        assertEquals("(contents", stringParts.get(1));
        assertEquals("table of \"['figure''s'],(tables\")", stringParts.get(2));
        assertEquals("introduction", stringParts.get(3));
        assertEquals("\"methods(), ()results\"", stringParts.get(4));
        assertEquals("['discussion'']'", stringParts.get(5));
        assertEquals("conclusion]", stringParts.get(6));

        // extended markers, trim
        stringParts = StringUtils.split(testString, ",", "'\"([{", "'\")]}", true);
        assertEquals(2, stringParts.size());
        assertEquals("abstract", stringParts.get(0));
        assertEquals("(contents, table of \"['figure''s'],(tables\"),  introduction  , \"methods(), ()results\", ['discussion'']', conclusion]",
                stringParts.get(1));

        // extended markers with overridable markers, trim
        stringParts = StringUtils.split(testString, ",", "'\"([{", "'\")]}", "'\"", true);
        assertEquals(5, stringParts.size());
        assertEquals("abstract", stringParts.get(0));
        assertEquals("(contents, table of \"['figure''s'],(tables\")", stringParts.get(1));
        assertEquals("introduction", stringParts.get(2));
        assertEquals("\"methods(), ()results\"", stringParts.get(3));
        assertEquals("['discussion'']', conclusion]", stringParts.get(4));
    }

    /**
     * Tests StringUtils.split() methods for corner cases.
     */
    public void testSplitCornerCases() throws Exception {
        List<String> stringParts;

        int c = 0;
        for (String s : new String[] { ",", "  ,", ",  ", "  ,  " }) {
            String testCase = "Case: >" + s + "<";

            // single delimiter, trim
            stringParts = StringUtils.split(s, ",", true);
            assertEquals(testCase, 2, stringParts.size());
            assertEquals(testCase, "", stringParts.get(0));
            assertEquals(testCase, "", stringParts.get(1));
            stringParts = StringUtils.split(s, ",", "([", ")]", true);
            assertEquals(testCase, 2, stringParts.size());
            assertEquals(testCase, "", stringParts.get(0));
            assertEquals(testCase, "", stringParts.get(1));

            // single delimiter, don't trim
            stringParts = StringUtils.split(s, ",", false);
            assertEquals(testCase, 2, stringParts.size());
            assertEquals(testCase, (c & 0x01) == 0 ? "" : "  ", stringParts.get(0));
            assertEquals(testCase, (c & 0x02) == 0 ? "" : "  ", stringParts.get(1));
            stringParts = StringUtils.split(s, ",", "([", ")]", false);
            assertEquals(testCase, 2, stringParts.size());
            assertEquals(testCase, (c & 0x01) == 0 ? "" : "  ", stringParts.get(0));
            assertEquals(testCase, (c & 0x02) == 0 ? "" : "  ", stringParts.get(1));
            c++;
        }

        // multiple delimiter (condensed)
        c = 0;
        for (String s : new String[] { ",,,", "  ,,,", ",,,  ", "  ,,,  " }) { // [empty|2sp], empty, empty, [empty|2sp]
            String testCase = "Case: >" + s + "<";

            // trim
            stringParts = StringUtils.split(s, ",", true);
            assertEquals(testCase, 4, stringParts.size());
            assertEquals(testCase, "", stringParts.get(0));
            assertEquals(testCase, "", stringParts.get(1));
            assertEquals(testCase, "", stringParts.get(2));
            assertEquals(testCase, "", stringParts.get(3));
            stringParts = StringUtils.split(s, ",", "([", ")]", true);
            assertEquals(testCase, 4, stringParts.size());
            assertEquals(testCase, "", stringParts.get(0));
            assertEquals(testCase, "", stringParts.get(1));
            assertEquals(testCase, "", stringParts.get(2));
            assertEquals(testCase, "", stringParts.get(3));

            // don't trim
            stringParts = StringUtils.split(s, ",", false);
            assertEquals(testCase, 4, stringParts.size());
            assertEquals(testCase, (c & 0x01) == 0 ? "" : "  ", stringParts.get(0)); // [empty|2sp]
            assertEquals(testCase, "", stringParts.get(1)); // empty
            assertEquals(testCase, "", stringParts.get(2)); // empty
            assertEquals(testCase, (c & 0x02) == 0 ? "" : "  ", stringParts.get(3)); // [empty|2sp]
            stringParts = StringUtils.split(s, ",", "([", ")]", false);
            assertEquals(testCase, 4, stringParts.size());
            assertEquals(testCase, (c & 0x01) == 0 ? "" : "  ", stringParts.get(0)); // [empty|2sp]
            assertEquals(testCase, "", stringParts.get(1)); // empty
            assertEquals(testCase, "", stringParts.get(2)); // empty
            assertEquals(testCase, (c & 0x02) == 0 ? "" : "  ", stringParts.get(3)); // [empty|2sp]
            c++;
        }

        // multiple delimiter (mixed)
        c = 0;
        for (String s : new String[] { ",, ,", "  ,, ,", ",, ,  ", "  ,, ,  " }) { // [empty|2sp], empty, 1sp, [empty|2sp]
            String testCase = "Case: >" + s + "<";

            // trim
            stringParts = StringUtils.split(s, ",", true);
            assertEquals(testCase, 4, stringParts.size());
            assertEquals(testCase, "", stringParts.get(0));
            assertEquals(testCase, "", stringParts.get(1));
            assertEquals(testCase, "", stringParts.get(2));
            assertEquals(testCase, "", stringParts.get(3));
            stringParts = StringUtils.split(s, ",", "([", ")]", true);
            assertEquals(testCase, 4, stringParts.size());
            assertEquals(testCase, "", stringParts.get(0));
            assertEquals(testCase, "", stringParts.get(1));
            assertEquals(testCase, "", stringParts.get(2));
            assertEquals(testCase, "", stringParts.get(3));

            // don't trim
            stringParts = StringUtils.split(s, ",", false);
            assertEquals(testCase, 4, stringParts.size());
            assertEquals(testCase, (c & 0x01) == 0 ? "" : "  ", stringParts.get(0)); // [empty|2sp]
            assertEquals(testCase, "", stringParts.get(1)); // empty
            assertEquals(testCase, " ", stringParts.get(2)); // 1sp
            assertEquals(testCase, (c & 0x02) == 0 ? "" : "  ", stringParts.get(3));// [empty|2sp]
            stringParts = StringUtils.split(s, ",", "([", ")]", false);
            assertEquals(testCase, 4, stringParts.size());
            assertEquals(testCase, (c & 0x01) == 0 ? "" : "  ", stringParts.get(0)); // [empty|2sp]
            assertEquals(testCase, "", stringParts.get(1)); // empty
            assertEquals(testCase, " ", stringParts.get(2)); // 1sp
            assertEquals(testCase, (c & 0x02) == 0 ? "" : "  ", stringParts.get(3));// [empty|2sp]
            c++;
        }

        // multiple delimiter (scattered)
        c = 0;
        for (String s : new String[] { ",   , ,", "  ,   , ,", ",   , ,  ", "  ,   , ,  " }) { // [empty|2sp], 3sp, 1sp, [empty|2sp]
            String testCase = "Case: >" + s + "<";

            // trim
            stringParts = StringUtils.split(s, ",", true);
            assertEquals(testCase, 4, stringParts.size());
            assertEquals(testCase, "", stringParts.get(0));
            assertEquals(testCase, "", stringParts.get(1));
            assertEquals(testCase, "", stringParts.get(2));
            assertEquals(testCase, "", stringParts.get(3));
            stringParts = StringUtils.split(s, ",", "([", ")]", true);
            assertEquals(testCase, 4, stringParts.size());
            assertEquals(testCase, "", stringParts.get(0));
            assertEquals(testCase, "", stringParts.get(1));
            assertEquals(testCase, "", stringParts.get(2));
            assertEquals(testCase, "", stringParts.get(3));

            // don't trim
            stringParts = StringUtils.split(s, ",", false);
            assertEquals(testCase, 4, stringParts.size());
            assertEquals(testCase, (c & 0x01) == 0 ? "" : "  ", stringParts.get(0)); // [empty|2sp]
            assertEquals(testCase, "   ", stringParts.get(1)); // 3sp
            assertEquals(testCase, " ", stringParts.get(2)); // 1sp
            assertEquals(testCase, (c & 0x02) == 0 ? "" : "  ", stringParts.get(3)); // [empty|2sp]
            stringParts = StringUtils.split(s, ",", "([", ")]", false);
            assertEquals(testCase, 4, stringParts.size());
            assertEquals(testCase, (c & 0x01) == 0 ? "" : "  ", stringParts.get(0)); // [empty|2sp]
            assertEquals(testCase, "   ", stringParts.get(1)); // 3sp
            assertEquals(testCase, " ", stringParts.get(2)); // 1sp
            assertEquals(testCase, (c & 0x02) == 0 ? "" : "  ", stringParts.get(3)); // [empty|2sp]
            c++;
        }
    }

    /**
     * Tests StringUtils.joinWithSerialComma().
     */
    public void testJoinWithSerialComma() throws Exception {
        assertEquals("", StringUtils.joinWithSerialComma(null));
        assertEquals("", StringUtils.joinWithSerialComma(Collections.emptyList()));

        // Using lists of Strings
        assertEquals("A", StringUtils.joinWithSerialComma(Arrays.asList("A")));
        assertEquals("A and B", StringUtils.joinWithSerialComma(Arrays.asList("A", "B")));
        assertEquals("A, B, and C", StringUtils.joinWithSerialComma(Arrays.asList("A", "B", "C")));

        // Using lists of objects other than Strings
        assertEquals("A", StringUtils.joinWithSerialComma(Arrays.asList(new LazyString("A"))));
        assertEquals("A and B", StringUtils.joinWithSerialComma(Arrays.asList(new LazyString("A"), new LazyString("B"))));
        assertEquals("A, B, and C", StringUtils.joinWithSerialComma(Arrays.asList(new LazyString("A"), new LazyString("B"), new LazyString("C"))));
    }

    public void testQuoteUnquoteBytes() throws Exception {

        byte[] origBytes = "ab'c\\''de".getBytes();
        byte[] expectedBytes = "'ab''c\\''''de'".getBytes();
        byte[] quotedBytes = StringUtils.quoteBytes(origBytes);

        for (int i = 0; i < quotedBytes.length; i++) {
            assertEquals(expectedBytes[i], quotedBytes[i]);
        }

        byte[] unquotedBytes = StringUtils.unquoteBytes(quotedBytes);

        for (int i = 0; i < unquotedBytes.length; i++) {
            assertEquals(origBytes[i], unquotedBytes[i]);
        }

    }
}
