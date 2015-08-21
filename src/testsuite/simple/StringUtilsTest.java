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

import java.nio.charset.Charset;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import testsuite.BaseTestCase;

import com.mysql.jdbc.StringUtils;
import com.mysql.jdbc.StringUtils.SearchMode;

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
        searchMode = EnumSet
                .of(SearchMode.ALLOW_BACKSLASH_ESCAPE, SearchMode.SKIP_BETWEEN_MARKERS, SearchMode.SKIP_BLOCK_COMMENTS, SearchMode.SKIP_WHITE_SPACE);
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
         * E. test indexOfIgnoreCase(int startingPosition, String searchIn, String searchFor, String openingMarkers, String closingMarkers, Set<SearchMode>
         * searchMode) illegal markers arguments
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
        searchMode = EnumSet
                .of(SearchMode.ALLOW_BACKSLASH_ESCAPE, SearchMode.SKIP_BETWEEN_MARKERS, SearchMode.SKIP_BLOCK_COMMENTS, SearchMode.SKIP_WHITE_SPACE);
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

                asBytesFromStringUtils = StringUtils.getBytes(text, null, cs.name(), null, true, null);
                assertByteArrayEquals("Custom Charset: " + cs.name(), asBytesFromString, asBytesFromStringUtils);
                asBytesFromStringUtils = StringUtils.getBytes(textAsCharArray, null, cs.name(), null, true, null);
                assertByteArrayEquals("Custom Charset: " + cs.name(), asBytesFromString, asBytesFromStringUtils);

                asBytesFromString = textPart.getBytes(cs.name());

                asBytesFromStringUtils = StringUtils.getBytes(text, offset, length, cs.name());
                assertByteArrayEquals("Custom Charset: " + cs.name(), asBytesFromString, asBytesFromStringUtils);
                asBytesFromStringUtils = StringUtils.getBytes(textAsCharArray, offset, length, cs.name());
                assertByteArrayEquals("Custom Charset: " + cs.name(), asBytesFromString, asBytesFromStringUtils);

                asBytesFromStringUtils = StringUtils.getBytes(text, null, cs.name(), null, offset, length, true, null);
                assertByteArrayEquals("Custom Charset: " + cs.name(), asBytesFromString, asBytesFromStringUtils);
                asBytesFromStringUtils = StringUtils.getBytes(textAsCharArray, null, cs.name(), null, offset, length, true, null);
                assertByteArrayEquals("Custom Charset: " + cs.name(), asBytesFromString, asBytesFromStringUtils);

                asBytesFromString = textWrapped.getBytes(cs.name());

                asBytesFromStringUtils = StringUtils.getBytesWrapped(text, '`', '`', null, cs.name(), null, true, null);
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
}
