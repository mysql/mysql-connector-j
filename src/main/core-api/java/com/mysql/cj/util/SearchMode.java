/*
 * Copyright (c) 2021, 2024, Oracle and/or its affiliates.
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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Search mode flags enumeration. Primarily used by {@link StringInspector}.
 */
public enum SearchMode {

    /**
     * Allow backslash escapes.
     */
    ALLOW_BACKSLASH_ESCAPE,
    /**
     * Skip between markers (quoted text, quoted identifiers, text between parentheses).
     */
    SKIP_BETWEEN_MARKERS,
    /**
     * Skip between block comments ("/* text... *\/") but not between hint blocks.
     */
    SKIP_BLOCK_COMMENTS,
    /**
     * Skip line comments ("-- text...", "# text...").
     */
    SKIP_LINE_COMMENTS,
    /**
     * Skip MySQL specific markers ("/*![12345]" and "*\/") but not their contents.
     */
    SKIP_MYSQL_MARKERS,
    /**
     * Skip hint blocks ("/*+ text... *\/").
     */
    SKIP_HINT_BLOCKS,
    /**
     * Skip white space.
     */
    SKIP_WHITE_SPACE,
    /**
     * Dummy search mode. Does nothing.
     */
    VOID;

    /*
     * Convenience EnumSets for several SearchMode combinations
     */

    /**
     * Full search mode: allow backslash escape, skip between markers, skip block comments, skip line comments, skip MySQL markers, skip hint blocks and skip
     * white space.
     * This is technically equivalent to __BSE_MRK_COM_MYM_HNT_WS.
     */
    public static final Set<SearchMode> __FULL = Collections.unmodifiableSet(EnumSet.allOf(SearchMode.class));

    /**
     * Search mode: allow backslash escape, skip between markers, skip block comments, skip line comments, skip MySQL markers, skip hint blocks and skip
     * white space.
     */
    public static final Set<SearchMode> __BSE_MRK_COM_MYM_HNT_WS = Collections.unmodifiableSet(EnumSet.of(ALLOW_BACKSLASH_ESCAPE, SKIP_BETWEEN_MARKERS,
            SKIP_BLOCK_COMMENTS, SKIP_LINE_COMMENTS, SKIP_MYSQL_MARKERS, SKIP_HINT_BLOCKS, SKIP_WHITE_SPACE));

    /**
     * Search mode: skip between markers, skip block comments, skip line comments, skip MySQL markers, skip hint blocks and skip white space.
     */
    public static final Set<SearchMode> __MRK_COM_MYM_HNT_WS = Collections
            .unmodifiableSet(EnumSet.of(SKIP_BETWEEN_MARKERS, SKIP_BLOCK_COMMENTS, SKIP_LINE_COMMENTS, SKIP_MYSQL_MARKERS, SKIP_HINT_BLOCKS, SKIP_WHITE_SPACE));

    /**
     * Search mode: allow backslash escape, skip block comments, skip line comments, skip MySQL markers, skip hint blocks and skip white space.
     */
    public static final Set<SearchMode> __BSE_COM_MYM_HNT_WS = Collections.unmodifiableSet(
            EnumSet.of(ALLOW_BACKSLASH_ESCAPE, SKIP_BLOCK_COMMENTS, SKIP_LINE_COMMENTS, SKIP_MYSQL_MARKERS, SKIP_HINT_BLOCKS, SKIP_WHITE_SPACE));

    /**
     * Search mode: skip block comments, skip line comments, skip MySQL markers, skip hint blocks and skip white space.
     */
    public static final Set<SearchMode> __COM_MYM_HNT_WS = Collections
            .unmodifiableSet(EnumSet.of(SKIP_BLOCK_COMMENTS, SKIP_LINE_COMMENTS, SKIP_MYSQL_MARKERS, SKIP_HINT_BLOCKS, SKIP_WHITE_SPACE));

    /**
     * Search mode: allow backslash escape, skip between markers and skip white space.
     */
    public static final Set<SearchMode> __BSE_MRK_WS = Collections.unmodifiableSet(EnumSet.of(ALLOW_BACKSLASH_ESCAPE, SKIP_BETWEEN_MARKERS, SKIP_WHITE_SPACE));

    /**
     * Search mode: skip between markers and skip white space.
     */
    public static final Set<SearchMode> __MRK_WS = Collections.unmodifiableSet(EnumSet.of(SKIP_BETWEEN_MARKERS, SKIP_WHITE_SPACE));

    /**
     * Empty search mode.
     * There must be at least one element so that the Set may be later duplicated if needed.
     */
    public static final Set<SearchMode> __NONE = Collections.unmodifiableSet(EnumSet.of(VOID));

}
