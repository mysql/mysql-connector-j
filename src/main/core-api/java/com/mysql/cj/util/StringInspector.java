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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import com.mysql.cj.Messages;

/**
 * Utility class to inspect a MySQL string, typically a query string.
 *
 * Provides string searching and manipulation operations such as finding sub-strings, matching sub-strings or building a comments-free version of a string.
 *
 * This object keeps internal state that affect the operations, e.g., executing an indexOf operation after another causes the second to start the search form
 * where the previous one stopped.
 */
public class StringInspector {

    // Length of MySQL version reference in comments of type '/*![00000] */'.
    private static final int NON_COMMENTS_MYSQL_VERSION_REF_LENGTH = 5;

    private String source = null;
    private String openingMarkers = null;
    private String closingMarkers = null;
    private String overridingMarkers = null;
    private Set<SearchMode> defaultSearchMode = null;

    private int srcLen = 0;
    private int pos = 0;
    private int stopAt = 0;
    private boolean escaped = false;
    private boolean inMysqlBlock = false;

    private int markedPos = this.pos;
    private int markedStopAt = this.stopAt;
    private boolean markedEscape = this.escaped;
    private boolean markedInMysqlBlock = this.inMysqlBlock;

    /**
     * This object provides string searching and manipulation operations such as finding sub-strings, matching sub-strings or building a comments-free version
     * of a string.
     *
     * @param source
     *            the source string to process
     * @param openingMarkers
     *            the characters that can delimit the beginning of a quoted text block
     * @param closingMarkers
     *            the characters that can delimit the end of a of a quoted text block. The length of this parameter must match the length of th previous one
     * @param overridingMarkers
     *            the subset of <code>openingMarkers</code> that override the remaining markers, e.g., if <code>openingMarkers = "'("</code>,
     *            <code>closingMarkers = "')"</code> and <code>overridingMarkers = "'"</code> then the block between the outer parenthesis in <code>"start
     *            ('max('); end"</code> is strictly consumed, otherwise the suffix <code>"'); end"</code> would end up being consumed too in the process of
     *            handling the nested parenthesis.
     * @param searchMode
     *            a <code>Set</code>, ideally an <code>EnumSet</code>, containing the default flags from the enum {@link SearchMode} that determine the behavior
     *            of the search and manipulation operations. Note that some operation may override the search mode according to their needs, but the default one
     *            is not touched.
     */
    public StringInspector(String source, String openingMarkers, String closingMarkers, String overridingMarkers, Set<SearchMode> searchMode) {
        this(source, 0, openingMarkers, closingMarkers, overridingMarkers, searchMode);
    }

    /**
     * This object provides string searching and manipulation operations such as finding sub-strings, matching sub-strings or building a comments-free version
     * of a string.
     *
     * @param source
     *            the source string to process
     * @param startingPosition
     *            the position from where the source string will be processed
     * @param openingMarkers
     *            the characters that can delimit the beginning of a quoted text block
     * @param closingMarkers
     *            the characters that can delimit the end of a of a quoted text block. The length of this parameter must match the length of th previous one
     * @param overridingMarkers
     *            the subset of <code>openingMarkers</code> that override the remaining markers, e.g., if <code>openingMarkers = "'("</code>,
     *            <code>closingMarkers = "')"</code> and <code>overridingMarkers = "'"</code> then the block between the outer parenthesis in <code>"start
     *            ('max('); end"</code> is strictly consumed, otherwise the suffix <code>"'); end"</code> would end up being consumed too in the process of
     *            handling the nested parenthesis.
     * @param searchMode
     *            a <code>Set</code>, ideally an <code>EnumSet</code>, containing the default flags from the enum {@link SearchMode} that determine the behavior
     *            of the search and manipulation operations. Note that some operation may override the search mode according to their needs, but the default one
     *            is not touched.
     */
    public StringInspector(String source, int startingPosition, String openingMarkers, String closingMarkers, String overridingMarkers,
            Set<SearchMode> searchMode) {
        if (source == null) {
            throw new IllegalArgumentException(Messages.getString("StringInspector.1"));
        }

        this.source = source;
        this.openingMarkers = openingMarkers;
        this.closingMarkers = closingMarkers;
        this.overridingMarkers = overridingMarkers;
        this.defaultSearchMode = searchMode;

        if (this.defaultSearchMode.contains(SearchMode.SKIP_BETWEEN_MARKERS)) {
            if (this.openingMarkers == null || this.closingMarkers == null || this.openingMarkers.length() != this.closingMarkers.length()) {
                throw new IllegalArgumentException(Messages.getString("StringInspector.2", new String[] { this.openingMarkers, this.closingMarkers }));
            }
            if (this.overridingMarkers == null) {
                throw new IllegalArgumentException(Messages.getString("StringInspector.3", new String[] { this.overridingMarkers, this.openingMarkers }));
            }
            for (char c : this.overridingMarkers.toCharArray()) {
                if (this.openingMarkers.indexOf(c) == -1) {
                    throw new IllegalArgumentException(Messages.getString("StringInspector.3", new String[] { this.overridingMarkers, this.openingMarkers }));
                }
            }
        }

        this.srcLen = source.length();
        this.pos = 0;
        this.stopAt = this.srcLen;

        setStartPosition(startingPosition);
    }

    /**
     * Sets the position from where the source string will be processed from now on, taking into consideration the "escaped" status of current character, if
     * the mode {@link SearchMode#ALLOW_BACKSLASH_ESCAPE} is present in the default search mode.
     *
     * @param pos
     *            the position from where the source string will be processed
     * @return
     *         the previous current position value
     */
    public int setStartPosition(int pos) {
        if (pos < 0) {
            throw new IllegalArgumentException(Messages.getString("StringInspector.4"));
        }
        if (pos > this.stopAt) {
            throw new IllegalArgumentException(Messages.getString("StringInspector.5"));
        }
        int prevPos = this.pos;
        this.pos = pos;
        resetEscaped();
        this.inMysqlBlock = false;
        return prevPos;
    }

    /**
     * Sets the position where the source string processing will stop.
     *
     * @param pos
     *            the position where the source string will stop being processed
     * @return
     *         the previous stop position value
     */
    public int setStopPosition(int pos) {
        if (pos < 0) {
            throw new IllegalArgumentException(Messages.getString("StringInspector.6"));
        }
        if (pos > this.srcLen) {
            throw new IllegalArgumentException(Messages.getString("StringInspector.7"));
        }
        int prevPos = this.stopAt;
        this.stopAt = pos;
        return prevPos;
    }

    /**
     * Marks the current object's state. A subsequent call to the {@link #reset()} method restores the marked state.
     */
    public void mark() {
        this.markedPos = this.pos;
        this.markedStopAt = this.stopAt;
        this.markedEscape = this.escaped;
        this.markedInMysqlBlock = this.inMysqlBlock;
    }

    /**
     * Resets this object's state to previously mark state.
     */
    public void reset() {
        this.pos = this.markedPos;
        this.stopAt = this.markedStopAt;
        this.escaped = this.markedEscape;
        this.inMysqlBlock = this.markedInMysqlBlock;
    }

    /**
     * Resets this object's state to original values. Allows to reuse the object from a fresh start.
     */
    public void restart() {
        this.pos = 0;
        this.stopAt = this.srcLen;
        this.escaped = false;
        this.inMysqlBlock = false;
    }

    /**
     * Returns the character in the current position.
     *
     * @return
     *         the character in the current position
     */
    public char getChar() {
        if (this.pos >= this.stopAt) {
            return Character.MIN_VALUE;
        }
        return this.source.charAt(this.pos);
    }

    /**
     * Returns the current position.
     *
     * @return
     *         the current position
     */
    public int getPosition() {
        return this.pos;
    }

    /**
     * Increments the current position index, by one, taking into consideration the "escaped" status of current character, if the mode
     * {@link SearchMode#ALLOW_BACKSLASH_ESCAPE} is present in the default search mode.
     *
     * @return
     *         the new current position
     */
    public int incrementPosition() {
        return incrementPosition(this.defaultSearchMode);
    }

    /**
     * Increments the current position index, by one, taking into consideration the "escaped" status of current character, if the mode
     * {@link SearchMode#ALLOW_BACKSLASH_ESCAPE} is present in the search mode specified.
     *
     * @param searchMode
     *            the search mode to use in this operation
     * @return
     *         the new current position
     */
    public int incrementPosition(Set<SearchMode> searchMode) {
        if (this.pos >= this.stopAt) {
            return -1;
        }
        if (searchMode.contains(SearchMode.ALLOW_BACKSLASH_ESCAPE) && getChar() == '\\') {
            this.escaped = !this.escaped;
        } else if (this.escaped) {
            this.escaped = false;
        }
        return ++this.pos;
    }

    /**
     * Increments the current position index, by be given number, taking into consideration the "escaped" status of current character, if the mode
     * {@link SearchMode#ALLOW_BACKSLASH_ESCAPE} is present in the default search mode.
     *
     * @param by
     *            the number of positions to increment
     * @return
     *         the new current position
     */
    public int incrementPosition(int by) {
        return incrementPosition(by, this.defaultSearchMode);
    }

    /**
     * Increments the current position index, by be given number, taking into consideration the "escaped" status of current character, if the mode
     * {@link SearchMode#ALLOW_BACKSLASH_ESCAPE} is present in the specified search mode.
     *
     * @param by
     *            the number of positions to increment
     * @param searchMode
     *            the search mode to use in this operation
     * @return
     *         the new current position
     */
    public int incrementPosition(int by, Set<SearchMode> searchMode) {
        for (int i = 0; i < by; i++) {
            if (incrementPosition(searchMode) == -1) {
                return -1;
            }
        }
        return this.pos;
    }

    /**
     * Checks if the current character is escaped by analyzing previous characters, as long as the search mode {@link SearchMode#ALLOW_BACKSLASH_ESCAPE} is
     * present in the default search mode.
     */
    private void resetEscaped() {
        this.escaped = false;
        if (this.defaultSearchMode.contains(SearchMode.ALLOW_BACKSLASH_ESCAPE)) {
            for (int i = this.pos - 1; i >= 0; i--) {
                if (this.source.charAt(i) != '\\') {
                    break;
                }
                this.escaped = !this.escaped;
            }
        }
    }

    /**
     * Returns the position of the next valid character. This method does not increment the current position automatically, i.e., if already positioned in a
     * valid character then repeated calls return always the same index.
     * If the character in the current position matches one of the prefixes that determine a skipping block, then the position marker advances to the first
     * character after the block to skip.
     *
     * @return
     *         the position of the next valid character, or the current position if already on a valid character
     */
    public int indexOfNextChar() {
        return indexOfNextChar(this.defaultSearchMode);
    }

    /**
     * Returns the position of the next valid character using the given search mode instead of the default one. This method does not increment the current
     * position automatically, i.e., if already positioned in a valid character then repeated calls return always the same index.
     * If the character in the current position matches one of the prefixes that determine a skipping block, then the position marker advances to the first
     * character after the block to skip.
     *
     * @param searchMode
     *            the search mode to use in this operation
     * @return
     *         the position of the next valid character, or the current position if already on a valid character
     */
    private int indexOfNextChar(Set<SearchMode> searchMode) {
        if (this.source == null || this.pos >= this.stopAt) {
            return -1;
        }

        char c0 = Character.MIN_VALUE; // Current char.
        char c1 = this.source.charAt(this.pos); // Lookahead(1).
        char c2 = this.pos + 1 < this.srcLen ? this.source.charAt(this.pos + 1) : Character.MIN_VALUE; // Lookahead(2).

        for (; this.pos < this.stopAt; this.pos++) {
            c0 = c1;
            c1 = c2;
            c2 = this.pos + 2 < this.srcLen ? this.source.charAt(this.pos + 2) : Character.MIN_VALUE;

            boolean dashDashCommentImmediateEnd = false;
            boolean checkSkipConditions = !searchMode.contains(SearchMode.ALLOW_BACKSLASH_ESCAPE) || !this.escaped;

            if (checkSkipConditions && searchMode.contains(SearchMode.SKIP_BETWEEN_MARKERS) && this.openingMarkers.indexOf(c0) != -1) {
                // Opening marker found, skip until closing.
                indexOfClosingMarker(searchMode);
                if (this.pos >= this.stopAt) {
                    this.pos--; // Reached stop position. Correct position will be set by outer loop.
                } else {
                    // Reset lookahead.
                    c1 = this.pos + 1 < this.srcLen ? this.source.charAt(this.pos + 1) : Character.MIN_VALUE;
                    c2 = this.pos + 2 < this.srcLen ? this.source.charAt(this.pos + 2) : Character.MIN_VALUE;
                }

            } else if (checkSkipConditions && searchMode.contains(SearchMode.SKIP_BLOCK_COMMENTS) && c0 == '/' && c1 == '*' && c2 != '!' && c2 != '+') {
                // Comments block found, skip until end of block ("*/") (backslash escape doesn't work in comments).
                // Does not include hint blocks ("/*!" or "/*+").
                this.pos++; // Move to next char ('*').
                while (++this.pos < this.stopAt && (this.source.charAt(this.pos) != '*'
                        || (this.pos + 1 < this.srcLen ? this.source.charAt(this.pos + 1) : Character.MIN_VALUE) != '/')) {
                    // Continue
                }
                if (this.pos >= this.stopAt) {
                    this.pos--; // Reached stop position. Correct position will be set by outer loop.
                } else {
                    this.pos++; // Move to next char ('/').
                }

                // Reset lookahead.
                c1 = this.pos + 1 < this.srcLen ? this.source.charAt(this.pos + 1) : Character.MIN_VALUE;
                c2 = this.pos + 2 < this.srcLen ? this.source.charAt(this.pos + 2) : Character.MIN_VALUE;

            } else if (checkSkipConditions && searchMode.contains(SearchMode.SKIP_LINE_COMMENTS)
                    && (c0 == '-' && c1 == '-' && (Character.isWhitespace(c2) || (dashDashCommentImmediateEnd = c2 == ';') || c2 == Character.MIN_VALUE)
                            || c0 == '#')) {
                if (dashDashCommentImmediateEnd) {
                    // Comments line found but closed immediately by query delimiter marker.
                    this.pos++; // Move to next char ('-').
                    this.pos++; // Move to next char (';').
                    // Reset lookahead.
                    c1 = this.pos + 1 < this.srcLen ? this.source.charAt(this.pos + 1) : Character.MIN_VALUE;
                    c2 = this.pos + 2 < this.srcLen ? this.source.charAt(this.pos + 2) : Character.MIN_VALUE;
                } else {
                    // Comments line found, skip until EOL (backslash escape doesn't work on comments).
                    while (++this.pos < this.stopAt && (c0 = this.source.charAt(this.pos)) != '\n' && c0 != '\r') {
                        // Continue.
                    }
                    if (this.pos >= this.stopAt) {
                        this.pos--; // Reached stop position. Correct position will be set by outer loop.
                    } else {
                        // Reset lookahead.
                        c1 = this.pos + 1 < this.srcLen ? this.source.charAt(this.pos + 1) : Character.MIN_VALUE;
                        if (c0 == '\r' && c1 == '\n') {
                            // \r\n sequence found.
                            this.pos++; // Skip next char ('\n').
                            c1 = this.pos + 1 < this.srcLen ? this.source.charAt(this.pos + 1) : Character.MIN_VALUE;
                        }
                        c2 = this.pos + 2 < this.srcLen ? this.source.charAt(this.pos + 2) : Character.MIN_VALUE;
                    }
                }

            } else if (checkSkipConditions && searchMode.contains(SearchMode.SKIP_HINT_BLOCKS) && c0 == '/' && c1 == '*' && c2 == '+') {
                // Hints block found, skip until end of block ("*/") (backslash escape doesn't work in hints).
                this.pos++; // Move to next char ('*').
                this.pos++; // Move to next char ('+').
                while (++this.pos < this.stopAt && (this.source.charAt(this.pos) != '*'
                        || (this.pos + 1 < this.srcLen ? this.source.charAt(this.pos + 1) : Character.MIN_VALUE) != '/')) {
                    // Continue
                }
                if (this.pos >= this.stopAt) {
                    this.pos--; // Reached stop position. Correct position will be set by outer loop.
                } else {
                    this.pos++; // Move to next char ('/').
                }

                // Reset lookahead.
                c1 = this.pos + 1 < this.srcLen ? this.source.charAt(this.pos + 1) : Character.MIN_VALUE;
                c2 = this.pos + 2 < this.srcLen ? this.source.charAt(this.pos + 2) : Character.MIN_VALUE;

            } else if (checkSkipConditions && searchMode.contains(SearchMode.SKIP_MYSQL_MARKERS) && c0 == '/' && c1 == '*' && c2 == '!') {
                // MySQL specific block found, move to end of opening marker ("/*![12345]").
                this.pos++; // Move to next char ('*').
                this.pos++; // Move to next char ('!').
                if (c2 == '!') {
                    // Check if 5 digits MySQL version reference comes next, if so skip them.
                    int i = 0;
                    for (; i < NON_COMMENTS_MYSQL_VERSION_REF_LENGTH; i++) {
                        if (this.pos + 1 + i >= this.srcLen || !Character.isDigit(this.source.charAt(this.pos + 1 + i))) {
                            break;
                        }
                    }
                    if (i == NON_COMMENTS_MYSQL_VERSION_REF_LENGTH) {
                        this.pos += NON_COMMENTS_MYSQL_VERSION_REF_LENGTH;
                        if (this.pos >= this.stopAt) {
                            this.pos = this.stopAt - 1; // Reached stop position. Correct position will be set by outer loop.
                        }
                    }
                }

                // Reset lookahead.
                c1 = this.pos + 1 < this.srcLen ? this.source.charAt(this.pos + 1) : Character.MIN_VALUE;
                c2 = this.pos + 2 < this.srcLen ? this.source.charAt(this.pos + 2) : Character.MIN_VALUE;

                this.inMysqlBlock = true;

            } else if (this.inMysqlBlock && checkSkipConditions && searchMode.contains(SearchMode.SKIP_MYSQL_MARKERS) && c0 == '*' && c1 == '/') {
                // MySQL block closing marker ("*/") found.
                this.pos++; // move to next char ('/')
                // Reset lookahead.
                c1 = c2;
                c2 = this.pos + 2 < this.srcLen ? this.source.charAt(this.pos + 2) : Character.MIN_VALUE;

                this.inMysqlBlock = false;

            } else if (!searchMode.contains(SearchMode.SKIP_WHITE_SPACE) || !Character.isWhitespace(c0)) {
                // Whitespace is not affected by backslash escapes.
                return this.pos;
            }

            // Reaching here means that the position has incremented thus an 'escaped' status no longer holds.
            this.escaped = false;
        }

        return -1;
    }

    /**
     * Returns the position of the next closing marker corresponding to the opening marker in the current position.
     * If the current position is not an opening marker, then -1 is returned instead.
     *
     * @param searchMode
     *            the search mode to use in this operation
     * @return
     *         the position of the next closing marker corresponding to the opening marker in the current position
     */
    private int indexOfClosingMarker(Set<SearchMode> searchMode) {
        if (this.source == null || this.pos >= this.stopAt) {
            return -1;
        }

        char c0 = this.source.charAt(this.pos); // Current char, also the opening marker.
        int markerIndex = this.openingMarkers.indexOf(c0);
        if (markerIndex == -1) {
            // Not at an opening marker.
            return this.pos;
        }

        int nestedMarkersCount = 0;
        char openingMarker = c0;
        char closingMarker = this.closingMarkers.charAt(markerIndex);
        boolean outerIsAnOverridingMarker = this.overridingMarkers.indexOf(openingMarker) != -1;
        while (++this.pos < this.stopAt && ((c0 = this.source.charAt(this.pos)) != closingMarker || nestedMarkersCount != 0)) {
            if (!outerIsAnOverridingMarker && this.overridingMarkers.indexOf(c0) != -1) {
                // There is an overriding marker that needs to be consumed before returning to the previous marker.
                int overridingMarkerIndex = this.openingMarkers.indexOf(c0); // OverridingMarkers must be a sub-list of openingMarkers.
                int overridingNestedMarkersCount = 0;
                char overridingOpeningMarker = c0;
                char overridingClosingMarker = this.closingMarkers.charAt(overridingMarkerIndex);
                while (++this.pos < this.stopAt && ((c0 = this.source.charAt(this.pos)) != overridingClosingMarker || overridingNestedMarkersCount != 0)) {
                    // Do as in the outer loop, except that this marker cannot be overridden.
                    if (c0 == overridingOpeningMarker) {
                        overridingNestedMarkersCount++;
                    } else if (c0 == overridingClosingMarker) {
                        overridingNestedMarkersCount--;
                    } else if (searchMode.contains(SearchMode.ALLOW_BACKSLASH_ESCAPE) && c0 == '\\') {
                        this.pos++; // Next char is escaped, skip it.
                    }
                }

                if (this.pos >= this.stopAt) {
                    this.pos--; // Reached stop position. Correct position will be set by outer loop.
                }
            } else if (c0 == openingMarker) {
                nestedMarkersCount++;
            } else if (c0 == closingMarker) {
                nestedMarkersCount--;
            } else if (searchMode.contains(SearchMode.ALLOW_BACKSLASH_ESCAPE) && c0 == '\\') {
                this.pos++; // Next char is escaped, skip it.
            }
        }

        return this.pos;
    }

    /**
     * Returns the position of the next alphanumeric character regardless the default search mode originally specified. This method does not increment the
     * current position automatically, i.e., if already positioned in a valid character then repeated calls return always the same index.
     * If the character in the current position matches one of the prefixes that determine a skipping block, then the position marker advances to the first
     * alphanumeric character after the block to skip.
     *
     * @return
     *         the position of the next valid character, or the current position if already on a valid character
     */
    public int indexOfNextAlphanumericChar() {
        if (this.source == null || this.pos >= this.stopAt) {
            return -1;
        }

        Set<SearchMode> searchMode = this.defaultSearchMode;
        if (!this.defaultSearchMode.contains(SearchMode.SKIP_WHITE_SPACE)) {
            searchMode = EnumSet.copyOf(this.defaultSearchMode);
            searchMode.add(SearchMode.SKIP_WHITE_SPACE);
        }

        while (this.pos < this.stopAt) {
            int prevPos = this.pos;
            if (indexOfNextChar(searchMode) == -1) {
                return -1;
            }
            if (Character.isLetterOrDigit(this.source.charAt(this.pos))) {
                return this.pos;
            }
            if (this.pos == prevPos) {
                // Position didn't move but also not yet at an alphanumeric.
                incrementPosition(searchMode);
            }
        }
        return -1;
    }

    /**
     * Returns the position of the next non-whitespace character regardless the default search mode originally specified. This method does not increment the
     * current position automatically, i.e., if already positioned in a valid character then repeated calls return always the same index.
     * If the character in the current position matches one of the prefixes that determine a skipping block, then the position marker advances to the first
     * non-whitespace character after the block to skip.
     *
     * @return
     *         the position of the next valid character, or the current position if already on a valid character
     */
    public int indexOfNextNonWsChar() {
        if (this.source == null || this.pos >= this.stopAt) {
            return -1;
        }

        Set<SearchMode> searchMode = this.defaultSearchMode;
        if (!this.defaultSearchMode.contains(SearchMode.SKIP_WHITE_SPACE)) {
            searchMode = EnumSet.copyOf(this.defaultSearchMode);
            searchMode.add(SearchMode.SKIP_WHITE_SPACE);
        }

        return indexOfNextChar(searchMode);
    }

    /**
     * Returns the position of the next whitespace character regardless the default search mode originally specified. This method does not increment the
     * current position automatically, i.e., if already positioned in a valid character then repeated calls return always the same index.
     * If the character in the current position matches one of the prefixes that determine a skipping block, then the position marker advances to the first
     * whitespace character after the block to skip.
     *
     * @return
     *         the position of the next valid character, or the current position if already on a valid character
     */
    public int indexOfNextWsChar() {
        if (this.source == null || this.pos >= this.stopAt) {
            return -1;
        }

        Set<SearchMode> searchMode = this.defaultSearchMode;
        if (this.defaultSearchMode.contains(SearchMode.SKIP_WHITE_SPACE)) {
            searchMode = EnumSet.copyOf(this.defaultSearchMode);
            searchMode.remove(SearchMode.SKIP_WHITE_SPACE);
        }

        while (this.pos < this.stopAt) {
            int prevPos = this.pos;
            if (indexOfNextChar(searchMode) == -1) {
                return -1;
            }
            if (Character.isWhitespace(this.source.charAt(this.pos))) {
                return this.pos;
            }
            if (this.pos == prevPos) {
                // Position didn't move but also not yet at a white space.
                incrementPosition(searchMode);
            }
        }
        return -1;
    }

    /**
     * Finds the position of the given string within the source string, ignoring case, with the option to skip text delimited by the specified markers or inside
     * comment blocks.
     *
     * @param searchFor
     *            the sub-string to search for
     * @return
     *         the position where the sub-string is found, starting from the current position, or -1 if not found
     */
    public int indexOfIgnoreCase(String searchFor) {
        return indexOfIgnoreCase(searchFor, this.defaultSearchMode);
    }

    /**
     * Finds the position of the given string within the source string, ignoring case, with the option to skip text delimited by the specified markers or inside
     * comment blocks.
     *
     * @param searchFor
     *            the sub-string to search for
     * @param searchMode
     *            the search mode to use in this operation
     * @return
     *         the position where the sub-string is found, starting from the current position, or -1 if not found
     */
    public int indexOfIgnoreCase(String searchFor, Set<SearchMode> searchMode) {
        if (searchFor == null) {
            return -1;
        }

        int searchForLength = searchFor.length();
        int localStopAt = this.srcLen - searchForLength + 1;
        if (localStopAt > this.stopAt) {
            localStopAt = this.stopAt;
        }

        if (this.pos >= localStopAt || searchForLength == 0) {
            return -1;
        }

        // Some locales don't follow upper-case rule, so need to check both.
        char firstCharOfSearchForUc = Character.toUpperCase(searchFor.charAt(0));
        char firstCharOfSearchForLc = Character.toLowerCase(searchFor.charAt(0));

        Set<SearchMode> localSearchMode = searchMode;
        if (Character.isWhitespace(firstCharOfSearchForLc) && this.defaultSearchMode.contains(SearchMode.SKIP_WHITE_SPACE)) {
            // Can't skip white spaces if first searchFor char is one.
            localSearchMode = EnumSet.copyOf(this.defaultSearchMode);
            localSearchMode.remove(SearchMode.SKIP_WHITE_SPACE);
        }

        while (this.pos < localStopAt) {
            if (indexOfNextChar(localSearchMode) == -1) {
                return -1;
            }

            if (StringUtils.isCharEqualIgnoreCase(getChar(), firstCharOfSearchForUc, firstCharOfSearchForLc)
                    && StringUtils.regionMatchesIgnoreCase(this.source, this.pos, searchFor)) {
                return this.pos;
            }

            incrementPosition(localSearchMode);
        }

        return -1;
    }

    /**
     * Finds the position of the of the first of a consecutive sequence of strings within the source string, ignoring case, with the option to skip text
     * delimited by the specified markers or inside comment blocks.
     *
     * <p>
     * Independently of the <code>searchMode</code> specified, when searching for the second and following sub-strings {@link SearchMode#SKIP_WHITE_SPACE} will
     * be added and {@link SearchMode#SKIP_BETWEEN_MARKERS} removed.
     * </p>
     *
     * @param searchFor
     *            the sequence of sub-strings to search
     * @return
     *         the position where the first sub-string is found, starting from the current position, or -1 if not found
     */
    public int indexOfIgnoreCase(String... searchFor) {
        if (searchFor == null) {
            return -1;
        }

        int searchForLength = 0;
        for (String searchForPart : searchFor) {
            searchForLength += searchForPart.length();
        } // Minimum length for searchFor (without gaps between words).

        if (searchForLength == 0) {
            return -1;
        }

        int searchForWordsCount = searchFor.length;
        searchForLength += searchForWordsCount > 0 ? searchForWordsCount - 1 : 0; // Count gaps between words.
        int localStopAt = this.srcLen - searchForLength + 1;
        if (localStopAt > this.stopAt) {
            localStopAt = this.stopAt;
        }

        if (this.pos >= localStopAt) {
            return -1;
        }

        Set<SearchMode> searchMode1 = this.defaultSearchMode;
        if (Character.isWhitespace(searchFor[0].charAt(0)) && this.defaultSearchMode.contains(SearchMode.SKIP_WHITE_SPACE)) {
            // Cannot skip white spaces if first searchFor char is one.
            searchMode1 = EnumSet.copyOf(this.defaultSearchMode);
            searchMode1.remove(SearchMode.SKIP_WHITE_SPACE);
        }

        // searchMode used to search 2nd and following words cannot contain SearchMode.SKIP_BETWEEN_MARKERS and must contain SearchMode.SKIP_WHITE_SPACE.
        Set<SearchMode> searchMode2 = EnumSet.copyOf(this.defaultSearchMode);
        searchMode2.add(SearchMode.SKIP_WHITE_SPACE);
        searchMode2.remove(SearchMode.SKIP_BETWEEN_MARKERS);

        while (this.pos < localStopAt) {
            int positionOfFirstWord = indexOfIgnoreCase(searchFor[0], searchMode1);
            if (positionOfFirstWord == -1 || positionOfFirstWord >= localStopAt) {
                return -1;
            }
            mark();

            int startingPositionForNextWord = incrementPosition(searchFor[0].length(), searchMode2);
            int wc = 0;
            boolean match = true;
            while (++wc < searchForWordsCount && match) {
                if (indexOfNextChar(searchMode2) == -1 || startingPositionForNextWord == this.pos
                        || !StringUtils.regionMatchesIgnoreCase(this.source, this.pos, searchFor[wc])) {
                    // Either there are no more chars to search, there is no gap between words or match failed.
                    match = false;
                } else {
                    startingPositionForNextWord = incrementPosition(searchFor[wc].length(), searchMode2);
                }
            }

            if (match) {
                reset();
                return positionOfFirstWord;
            }
        }

        return -1;
    }

    /**
     * Checks if the given string matches the source string counting from the current position, ignoring case, with the option to skip text delimited by the
     * specified markers or inside comment blocks.
     *
     * @param toMatch
     *            the sub-string to match
     * @return
     *         the position where the sub-string match ended, or -1 if not matched
     */
    public int matchesIgnoreCase(String toMatch) {
        if (toMatch == null) {
            return -1;
        }

        int toMatchLength = toMatch.length();
        int localStopAt = this.srcLen - toMatchLength + 1;
        if (localStopAt > this.stopAt) {
            localStopAt = this.stopAt;
        }

        if (this.pos >= localStopAt || toMatchLength == 0) {
            return -1;
        }

        // Some locales don't follow upper-case rule, so need to check both.
        char firstCharOfToMatchUc = Character.toUpperCase(toMatch.charAt(0));
        char firstCharOfToMatchLc = Character.toLowerCase(toMatch.charAt(0));

        if (StringUtils.isCharEqualIgnoreCase(getChar(), firstCharOfToMatchUc, firstCharOfToMatchLc)
                && StringUtils.regionMatchesIgnoreCase(this.source, this.pos, toMatch)) {
            return this.pos + toMatchLength;
        }

        return -1;
    }

    /**
     * Checks if the given consecutive sequence of strings matches the source string counting from the current position, ignoring case, with the option to skip
     * text delimited by the specified markers or inside comment blocks.
     *
     * <p>
     * Independently of the <code>searchMode</code> specified, when matching the second and following sub-strings {@link SearchMode#SKIP_WHITE_SPACE} will be
     * added and {@link SearchMode#SKIP_BETWEEN_MARKERS} removed.
     * </p>
     *
     * @param toMatch
     *            the sequence of sub-strings to match
     * @return
     *         the position where the sequence of sub-strings match ended, or -1 if not matched
     */
    public int matchesIgnoreCase(String... toMatch) {
        if (toMatch == null) {
            return -1;
        }

        int toMatchLength = 0;
        for (String toMatchPart : toMatch) {
            toMatchLength += toMatchPart.length();
        } // Minimum length for searchFor (without gaps between words).

        if (toMatchLength == 0) {
            return -1;
        }

        int toMatchWordsCount = toMatch.length;
        toMatchLength += toMatchWordsCount > 0 ? toMatchWordsCount - 1 : 0; // Count gaps between words.
        int localStopAt = this.srcLen - toMatchLength + 1;
        if (localStopAt > this.stopAt) {
            localStopAt = this.stopAt;
        }

        if (this.pos >= localStopAt) {
            return -1;
        }

        // searchMode used to match 2nd and following words cannot contain SearchMode.SKIP_BETWEEN_MARKERS and must contain SearchMode.SKIP_WHITE_SPACE.
        Set<SearchMode> searchMode2 = EnumSet.copyOf(this.defaultSearchMode);
        searchMode2.add(SearchMode.SKIP_WHITE_SPACE);
        searchMode2.remove(SearchMode.SKIP_BETWEEN_MARKERS);

        mark();
        int endOfMatch = -1;
        int startingPositionForNextWord = -1;
        for (String searchForPart : toMatch) {
            if (getPosition() == startingPositionForNextWord) {
                // There is no gap between words.
                reset();
                return -1;
            }

            endOfMatch = matchesIgnoreCase(searchForPart);
            if (endOfMatch == -1) {
                reset();
                return -1;
            }

            startingPositionForNextWord = incrementPosition(searchForPart.length(), searchMode2);
            indexOfNextChar(searchMode2);
        }
        reset();
        return endOfMatch;
    }

    /**
     * Returns a copy of the source string stripped of all comments and hints.
     *
     * @return
     *         a comments-free string
     */
    public String stripCommentsAndHints() {
        restart();

        Set<SearchMode> searchMode = EnumSet.of(SearchMode.SKIP_BLOCK_COMMENTS, SearchMode.SKIP_LINE_COMMENTS, SearchMode.SKIP_HINT_BLOCKS);
        if (this.defaultSearchMode.contains(SearchMode.ALLOW_BACKSLASH_ESCAPE)) {
            searchMode.add(SearchMode.ALLOW_BACKSLASH_ESCAPE);
        }

        StringBuilder noCommsStr = new StringBuilder(this.source.length());
        while (this.pos < this.stopAt) {
            int prevPos = this.pos;
            if (indexOfNextChar(searchMode) == -1) {
                return noCommsStr.toString();
            }

            if (!this.escaped && this.openingMarkers.indexOf(getChar()) != -1) {
                // Characters between markers must be taken all at once otherwise indexOfNextChar() would skip them if they contain comments-like text.
                int idxOpMrkr = this.pos;
                if (indexOfClosingMarker(searchMode) < this.srcLen) {
                    incrementPosition(searchMode); // Include the closing marker.
                }
                noCommsStr.append(this.source, idxOpMrkr, this.pos);
            } else {
                if (this.pos - prevPos > 1) { // Non consecutive characters, there was a comment in between. Add a space if needed.
                    if (prevPos > 0 && !Character.isWhitespace(this.source.charAt(prevPos - 1)) && !Character.isWhitespace(this.source.charAt(this.pos))) {
                        noCommsStr.append(" ");
                    }
                }
                noCommsStr.append(getChar());
                incrementPosition(searchMode);
            }
        }

        return noCommsStr.toString();
    }

    /**
     * Splits the source string by the given delimiter. Consecutive delimiters result in empty string parts.
     *
     * @param delimiter
     *            the characters sequence where to split the source string
     * @param trim
     *            whether each one of the parts should be trimmed or not
     * @return
     *         a {@link List} containing all the string parts
     */
    public List<String> split(String delimiter, boolean trim) {
        if (delimiter == null) {
            throw new IllegalArgumentException(Messages.getString("StringInspector.8"));
        }

        restart();

        int startPos = 0;
        List<String> splitParts = new ArrayList<>();
        while (indexOfIgnoreCase(delimiter) != -1) {
            indexOfIgnoreCase(delimiter);
            String part = this.source.substring(startPos, this.pos);
            if (trim) {
                part = part.trim();
            }
            splitParts.add(part);
            startPos = incrementPosition(delimiter.length());
        }

        // Add last part.
        String token = this.source.substring(startPos);
        if (trim) {
            token = token.trim();
        }
        splitParts.add(token);

        return splitParts;
    }

}
