/*
 * Copyright (c) 2022, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import testsuite.BaseTestCase;

public class StringInspectorTest extends BaseTestCase {

    @Test
    public void testIndexOfIgnoreCaseAndMatchesIgnoreCasePositioning() {
        final String markerStart = "\"'`(";
        final String markerEnd = "\"'`)";
        final String overrideMarkers = "";

        /*
         * Searching and matching in the same long string using different search strings and modes.
         */

        String source = "A matchable string/* nice */to-- \nMATCH sub-strings BuT nO /*!12345 matCH or NO */ matches to light \"no match!\"";
        StringInspector si = new StringInspector(source, 0, markerStart, markerEnd, overrideMarkers, SearchMode.__BSE_MRK_COM_MYM_HNT_WS);

        // Invalid matches.
        assertEquals(-1, si.matchesIgnoreCase(""));
        assertEquals(-1, si.matchesIgnoreCase(new String[] {}));
        assertEquals(-1, si.matchesIgnoreCase(new String[] { "", "" }));
        assertEquals(-1, si.matchesIgnoreCase((String) null));
        assertEquals(-1, si.matchesIgnoreCase((String[]) null));
        si.restart();

        // Single word matches.
        String toMatch = "mATch";
        assertEquals(-1, si.matchesIgnoreCase(toMatch));
        assertEquals(2, si.indexOfIgnoreCase(toMatch));
        assertEquals(7, si.matchesIgnoreCase(toMatch));
        assertEquals(2, si.indexOfIgnoreCase(toMatch));
        assertEquals(2, si.getPosition());
        assertEquals(3, si.incrementPosition());
        assertEquals(-1, si.matchesIgnoreCase(toMatch));
        assertEquals(34, si.indexOfIgnoreCase(toMatch));
        assertEquals(39, si.matchesIgnoreCase(toMatch));
        assertEquals(34, si.indexOfIgnoreCase(toMatch));
        assertEquals(34, si.getPosition());
        assertEquals(35, si.incrementPosition());
        assertEquals(-1, si.matchesIgnoreCase(toMatch));
        assertEquals(68, si.indexOfIgnoreCase(toMatch));
        assertEquals(73, si.matchesIgnoreCase(toMatch));
        assertEquals(68, si.indexOfIgnoreCase(toMatch));
        assertEquals(68, si.getPosition());
        assertEquals(69, si.incrementPosition());
        assertEquals(-1, si.matchesIgnoreCase(toMatch));
        assertEquals(83, si.indexOfIgnoreCase(toMatch));
        assertEquals(88, si.matchesIgnoreCase(toMatch));
        assertEquals(83, si.indexOfIgnoreCase(toMatch));
        assertEquals(83, si.getPosition());
        assertEquals(84, si.incrementPosition());
        assertEquals(-1, si.matchesIgnoreCase(toMatch));
        assertEquals(-1, si.indexOfIgnoreCase(toMatch));
        assertEquals(-1, si.matchesIgnoreCase(toMatch));
        assertEquals(-1, si.indexOfIgnoreCase(toMatch));
        assertEquals(111, si.getPosition()); // Skipped to the very last closing marker ("), otherwise would have stopped (toMatch.lengh() - 1) chars before.
        si.restart();

        // Single word matches, using multi word mathing method.
        String[] toMatchMulti1 = new String[] { toMatch };
        assertEquals(-1, si.matchesIgnoreCase(toMatchMulti1));
        assertEquals(2, si.indexOfIgnoreCase(toMatchMulti1));
        assertEquals(7, si.matchesIgnoreCase(toMatchMulti1));
        assertEquals(2, si.indexOfIgnoreCase(toMatchMulti1));
        assertEquals(2, si.getPosition());
        assertEquals(3, si.incrementPosition());
        assertEquals(-1, si.matchesIgnoreCase(toMatchMulti1));
        assertEquals(34, si.indexOfIgnoreCase(toMatchMulti1));
        assertEquals(39, si.matchesIgnoreCase(toMatchMulti1));
        assertEquals(34, si.indexOfIgnoreCase(toMatchMulti1));
        assertEquals(34, si.getPosition());
        assertEquals(35, si.incrementPosition());
        assertEquals(-1, si.matchesIgnoreCase(toMatchMulti1));
        assertEquals(68, si.indexOfIgnoreCase(toMatchMulti1));
        assertEquals(73, si.matchesIgnoreCase(toMatchMulti1));
        assertEquals(68, si.indexOfIgnoreCase(toMatchMulti1));
        assertEquals(68, si.getPosition());
        assertEquals(69, si.incrementPosition());
        assertEquals(-1, si.matchesIgnoreCase(toMatchMulti1));
        assertEquals(83, si.indexOfIgnoreCase(toMatchMulti1));
        assertEquals(88, si.matchesIgnoreCase(toMatchMulti1));
        assertEquals(83, si.indexOfIgnoreCase(toMatchMulti1));
        assertEquals(83, si.getPosition());
        assertEquals(84, si.incrementPosition());
        assertEquals(-1, si.matchesIgnoreCase(toMatchMulti1));
        assertEquals(-1, si.indexOfIgnoreCase(toMatchMulti1));
        assertEquals(-1, si.matchesIgnoreCase(toMatchMulti1));
        assertEquals(-1, si.indexOfIgnoreCase(toMatchMulti1));
        assertEquals(111, si.getPosition()); // Skipped to the very last closing marker ("), otherwise would have stopped (toMatch.lengh() - 1) chars before.
        si.restart();

        // Two words matches.
        String[] toMatchMulti2 = new String[] { "o", "MATch" };
        assertEquals(-1, si.matchesIgnoreCase(toMatchMulti2));
        assertEquals(29, si.indexOfIgnoreCase(toMatchMulti2));
        assertEquals(39, si.matchesIgnoreCase(toMatchMulti2));
        assertEquals(29, si.indexOfIgnoreCase(toMatchMulti2));
        assertEquals(29, si.getPosition());
        assertEquals(30, si.incrementPosition());
        assertEquals(-1, si.matchesIgnoreCase(toMatchMulti2));
        assertEquals(57, si.indexOfIgnoreCase(toMatchMulti2));
        assertEquals(73, si.matchesIgnoreCase(toMatchMulti2));
        assertEquals(57, si.indexOfIgnoreCase(toMatchMulti2));
        assertEquals(57, si.getPosition());
        assertEquals(58, si.incrementPosition());
        assertEquals(-1, si.matchesIgnoreCase(toMatchMulti2));
        assertEquals(78, si.indexOfIgnoreCase(toMatchMulti2));
        assertEquals(88, si.matchesIgnoreCase(toMatchMulti2));
        assertEquals(78, si.indexOfIgnoreCase(toMatchMulti2));
        assertEquals(78, si.getPosition());
        assertEquals(79, si.incrementPosition());
        assertEquals(-1, si.matchesIgnoreCase(toMatchMulti2));
        assertEquals(-1, si.indexOfIgnoreCase(toMatchMulti2));
        assertEquals(-1, si.matchesIgnoreCase(toMatchMulti2));
        assertEquals(-1, si.indexOfIgnoreCase(toMatchMulti2));
        assertEquals(111, si.getPosition()); // Skipped to the very last closing marker ("), otherwise would have stopped (toMatch.lengh() - 1) chars before.
        si.restart();

        // Multiple words matches.
        String[] toMatchMultiN = new String[] { "NO", "matches", "TO", "light" };
        assertEquals(-1, si.matchesIgnoreCase(toMatchMultiN));
        assertEquals(77, si.indexOfIgnoreCase(toMatchMultiN));
        assertEquals(99, si.matchesIgnoreCase(toMatchMultiN));
        assertEquals(77, si.indexOfIgnoreCase(toMatchMultiN));
        assertEquals(77, si.getPosition());
        assertEquals(78, si.incrementPosition());
        assertEquals(-1, si.matchesIgnoreCase(toMatchMultiN));
        assertEquals(-1, si.indexOfIgnoreCase(toMatchMultiN));
        assertEquals(-1, si.matchesIgnoreCase(toMatchMultiN));
        assertEquals(-1, si.indexOfIgnoreCase(toMatchMultiN));
        assertEquals(111, si.getPosition()); // Skipped to the very last closing marker ("), otherwise would have stopped (toMatch.lengh() - 1) chars before.
        si.restart();

        /*
         * Searching and matching the same sub-strings in different variations of the same generic string structure.
         */
        checkIndexOfAndMatches("select on duplicate key update and more", -1, 7, 30, -1, 31, 34);
        checkIndexOfAndMatches("on duplicate key update and more", 23, 0, 23, -1, 24, 27);
        checkIndexOfAndMatches("select on duplicate key update", -1, 7, 30, -1, -1, -1);
        checkIndexOfAndMatches("select on duplicate key update/* and more */", -1, 7, 30, -1, -1, -1);
        checkIndexOfAndMatches("selecton duplicate key updateandmore", -1, 6, 29, 32, 29, 32);
        checkIndexOfAndMatches("select on-- \n duplicate/* */key   update and more", -1, 7, 40, -1, 41, 44);
    }

    private void checkIndexOfAndMatches(String str, int m1, int i1, int m2, int m3, int i2, int m4) {
        final String markerStart = "\"'`(";
        final String markerEnd = "\"'`)";
        final String overrideMarkers = "";

        final char firstChar = 'o';
        final String[] find1 = new String[] { "ON", "DUPLICATE", "KEY", "UPDATE" };
        final String find2 = "AND";

        StringInspector si = new StringInspector(str, 0, markerStart, markerEnd, overrideMarkers, SearchMode.__BSE_MRK_COM_MYM_HNT_WS);
        assertEquals(m1, si.matchesIgnoreCase(find1));
        assertEquals(i1, si.indexOfIgnoreCase(find1));
        assertEquals(firstChar, si.getChar());
        int e;
        assertEquals(m2, e = si.matchesIgnoreCase(find1));
        assertEquals(i1, si.getPosition());
        assertEquals(firstChar, si.getChar());
        si.setStartPosition(e);
        assertEquals(m3, si.matchesIgnoreCase(find2));
        assertEquals(i2, si.indexOfIgnoreCase(find2));
        assertEquals(m4, si.matchesIgnoreCase(find2));
    }

}
