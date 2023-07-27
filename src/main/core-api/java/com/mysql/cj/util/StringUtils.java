/*
 * Copyright (c) 2002, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.util;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.mysql.cj.Messages;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;

/**
 * Various utility methods for converting to/from byte arrays in the platform encoding and several other String operations.
 */
public class StringUtils {

    private static final int WILD_COMPARE_MATCH = 0;
    private static final int WILD_COMPARE_CONTINUE_WITH_WILD = 1;
    private static final int WILD_COMPARE_NO_MATCH = -1;

    static final char WILDCARD_MANY = '%';
    static final char WILDCARD_ONE = '_';
    static final char WILDCARD_ESCAPE = '\\';

    private static final String VALID_ID_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIGKLMNOPQRSTUVWXYZ0123456789$_#@";

    /**
     * Returns the given bytes as a hex and ASCII dump (up to length bytes).
     *
     * @param byteBuffer
     *            the data to dump as hex
     * @param length
     *            the number of bytes to print
     *
     * @return a hex and ASCII dump
     */
    public static String dumpAsHex(byte[] byteBuffer, int length) {
        length = Math.min(length, byteBuffer.length);
        StringBuilder fullOutBuilder = new StringBuilder(length * 4);
        StringBuilder asciiOutBuilder = new StringBuilder(16);

        for (int p = 0, l = 0; p < length; l = 0) { // p: position in buffer (1..length); l: position in line (1..8)
            for (; l < 8 && p < length; p++, l++) {
                int asInt = byteBuffer[p] & 0xff;
                if (asInt < 0x10) {
                    fullOutBuilder.append("0");
                }
                fullOutBuilder.append(Integer.toHexString(asInt)).append(" ");
                asciiOutBuilder.append(" ").append(asInt >= 0x20 && asInt < 0x7f ? (char) asInt : ".");
            }
            for (; l < 8; l++) { // if needed, fill remaining of last line with spaces
                fullOutBuilder.append("   ");
            }
            fullOutBuilder.append("   ").append(asciiOutBuilder).append(System.lineSeparator());
            asciiOutBuilder.setLength(0);
        }
        return fullOutBuilder.toString();
    }

    /**
     * Converts the given byte array into Hex String, stopping at given length.
     *
     * @param byteBuffer
     *            the byte array to convert
     * @param length
     *            the number of bytes from the given array to convert
     * @return
     *         a String containing the Hex representation of the given bytes
     */
    public static String toHexString(byte[] byteBuffer, int length) {
        length = Math.min(length, byteBuffer.length);
        StringBuilder outputBuilder = new StringBuilder(length * 2);
        for (int i = 0; i < length; i++) {
            int asInt = byteBuffer[i] & 0xff;
            if (asInt < 0x10) {
                outputBuilder.append("0");
            }
            outputBuilder.append(Integer.toHexString(asInt));
        }
        return outputBuilder.toString();
    }

    private static boolean endsWith(byte[] dataFrom, String suffix) {
        for (int i = 1; i <= suffix.length(); i++) {
            int dfOffset = dataFrom.length - i;
            int suffixOffset = suffix.length() - i;
            if (dataFrom[dfOffset] != suffix.charAt(suffixOffset)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the first non-whitespace char, converted to upper case
     *
     * @param searchIn
     *            the string to search in
     *
     * @return the first non-whitespace character, upper cased.
     */
    public static char firstNonWsCharUc(String searchIn) {
        return firstNonWsCharUc(searchIn, 0);
    }

    public static char firstNonWsCharUc(String searchIn, int startAt) {
        if (searchIn == null) {
            return 0;
        }

        int length = searchIn.length();

        for (int i = startAt; i < length; i++) {
            char c = searchIn.charAt(i);

            if (!Character.isWhitespace(c)) {
                return Character.toUpperCase(c);
            }
        }

        return 0;
    }

    public static char firstAlphaCharUc(String searchIn, int startAt) {
        if (searchIn == null) {
            return 0;
        }

        int length = searchIn.length();

        for (int i = startAt; i < length; i++) {
            char c = searchIn.charAt(i);

            if (Character.isLetter(c)) {
                return Character.toUpperCase(c);
            }
        }

        return 0;
    }

    /**
     * Adds '+' to decimal numbers that are positive (MySQL doesn't understand
     * them otherwise
     *
     * @param dString
     *            The value as a string
     *
     * @return String the string with a '+' added (if needed)
     */
    public static String fixDecimalExponent(String dString) {
        int ePos = dString.indexOf('E');

        if (ePos == -1) {
            ePos = dString.indexOf('e');
        }

        if (ePos != -1) {
            if (dString.length() > ePos + 1) {
                char maybeMinusChar = dString.charAt(ePos + 1);

                if (maybeMinusChar != '-' && maybeMinusChar != '+') {
                    StringBuilder strBuilder = new StringBuilder(dString.length() + 1);
                    strBuilder.append(dString.substring(0, ePos + 1));
                    strBuilder.append('+');
                    strBuilder.append(dString.substring(ePos + 1, dString.length()));
                    dString = strBuilder.toString();
                }
            }
        }

        return dString;
    }

    /**
     * Returns the byte[] representation of the given string using the given encoding.
     *
     * @param s
     *            source string
     * @param encoding
     *            java encoding
     * @return bytes
     */
    public static byte[] getBytes(String s, String encoding) {
        if (s == null) {
            return new byte[0];
        }
        if (encoding == null) {
            return getBytes(s);
        }
        try {
            return s.getBytes(encoding);
        } catch (UnsupportedEncodingException uee) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("StringUtils.0", new Object[] { encoding }), uee);
        }
    }

    /**
     * Returns the byte[] representation of the given string properly wrapped between the given char delimiters using the given encoding.
     *
     * @param s
     *            source string
     * @param beginWrap
     *            opening char delimiter
     * @param endWrap
     *            closing char delimiter
     * @param encoding
     *            java encoding
     * @return bytes
     */
    public static byte[] getBytesWrapped(String s, char beginWrap, char endWrap, String encoding) {
        byte[] b;

        if (encoding == null) {
            StringBuilder strBuilder = new StringBuilder(s.length() + 2);
            strBuilder.append(beginWrap);
            strBuilder.append(s);
            strBuilder.append(endWrap);

            b = getBytes(strBuilder.toString());
        } else {
            StringBuilder strBuilder = new StringBuilder(s.length() + 2);
            strBuilder.append(beginWrap);
            strBuilder.append(s);
            strBuilder.append(endWrap);

            s = strBuilder.toString();
            b = getBytes(s, encoding);
        }

        return b;
    }

    /**
     * Finds the position of a substring within a string ignoring case.
     *
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the array of strings to search for
     * @return the position where <code>searchFor</code> is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    public static int indexOfIgnoreCase(String searchIn, String searchFor) {
        return indexOfIgnoreCase(0, searchIn, searchFor);
    }

    /**
     * Finds the position of a substring within a string ignoring case.
     *
     * @param startingPosition
     *            the position to start the search from
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the array of strings to search for
     * @return the position where <code>searchFor</code> is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    public static int indexOfIgnoreCase(int startingPosition, String searchIn, String searchFor) {
        if (searchIn == null || searchFor == null) {
            return -1;
        }

        int searchInLength = searchIn.length();
        int searchForLength = searchFor.length();
        int stopSearchingAt = searchInLength - searchForLength;

        if (startingPosition > stopSearchingAt || searchForLength == 0) {
            return -1;
        }

        // Some locales don't follow upper-case rule, so need to check both
        char firstCharOfSearchForUc = Character.toUpperCase(searchFor.charAt(0));
        char firstCharOfSearchForLc = Character.toLowerCase(searchFor.charAt(0));

        for (int i = startingPosition; i <= stopSearchingAt; i++) {
            if (isCharAtPosNotEqualIgnoreCase(searchIn, i, firstCharOfSearchForUc, firstCharOfSearchForLc)) {
                // find the first occurrence of the first character of searchFor in searchIn
                while (++i <= stopSearchingAt && isCharAtPosNotEqualIgnoreCase(searchIn, i, firstCharOfSearchForUc, firstCharOfSearchForLc)) {
                }
            }

            if (i <= stopSearchingAt && regionMatchesIgnoreCase(searchIn, i, searchFor)) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Finds the position of the first of a consecutive sequence of strings within a string, ignoring case, with the option to skip text delimited by given
     * markers or within comments.
     * <p>
     * Independently of the <code>searchMode</code> provided, when searching for the second and following strings <code>SearchMode.SKIP_WHITE_SPACE</code> will
     * be added and <code>SearchMode.SKIP_BETWEEN_MARKERS</code> removed.
     * </p>
     *
     * @param startingPosition
     *            the position to start the search from
     * @param searchIn
     *            the string to search in
     * @param searchForSequence
     *            searchForSequence
     * @param openingMarkers
     *            characters that delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters that delimit the end of a text block to skip
     * @param searchMode
     *            a <code>Set</code>, ideally an <code>EnumSet</code>, containing the flags from the enum <code>StringUtils.SearchMode</code> that determine the
     *            behavior of the search
     * @return the position where <code>searchFor</code> is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    public static int indexOfIgnoreCase(int startingPosition, String searchIn, String[] searchForSequence, String openingMarkers, String closingMarkers,
            Set<SearchMode> searchMode) {
        StringInspector strInspector = new StringInspector(searchIn, startingPosition, openingMarkers, closingMarkers, "", searchMode);
        return strInspector.indexOfIgnoreCase(searchForSequence);
    }

    /**
     * Finds the position of a substring within a string, ignoring case, with the option to skip text delimited by given markers or within comments.
     *
     * @param startingPosition
     *            the position to start the search from
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for
     * @param openingMarkers
     *            characters that delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters that delimit the end of a text block to skip
     * @param searchMode
     *            a <code>Set</code>, ideally an <code>EnumSet</code>, containing the flags from the enum <code>StringUtils.SearchMode</code> that determine the
     *            behavior of the search
     * @return the position where <code>searchFor</code> is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    public static int indexOfIgnoreCase(int startingPosition, String searchIn, String searchFor, String openingMarkers, String closingMarkers,
            Set<SearchMode> searchMode) {
        return indexOfIgnoreCase(startingPosition, searchIn, searchFor, openingMarkers, closingMarkers, "", searchMode);
    }

    /**
     * Finds the position of a substring within a string, ignoring case, with the option to skip text delimited by given markers or within comments.
     *
     * @param startingPosition
     *            the position to start the search from
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for
     * @param openingMarkers
     *            characters that delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters that delimit the end of a text block to skip
     * @param overridingMarkers
     *            the subset of <code>openingMarkers</code> that override the remaining markers, e.g., if <code>openingMarkers = "'("</code> and
     *            <code>overridingMarkers = "'"</code> then the block between the outer parenthesis in <code>"start ('max('); end"</code> is strictly consumed,
     *            otherwise the suffix <code>" end"</code> would end up being consumed too in the process of handling the nested parenthesis.
     * @param searchMode
     *            a <code>Set</code>, ideally an <code>EnumSet</code>, containing the flags from the enum <code>StringUtils.SearchMode</code> that determine the
     *            behavior of the search
     * @return the position where <code>searchFor</code> is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    public static int indexOfIgnoreCase(int startingPosition, String searchIn, String searchFor, String openingMarkers, String closingMarkers,
            String overridingMarkers, Set<SearchMode> searchMode) {
        StringInspector strInspector = new StringInspector(searchIn, startingPosition, openingMarkers, closingMarkers, overridingMarkers, searchMode);
        return strInspector.indexOfIgnoreCase(searchFor);
    }

    /**
     * Finds the position of the next alphanumeric character within a string, with the option to skip text delimited by given markers or within comments.
     *
     * @param startingPosition
     *            the position to start the search from
     * @param searchIn
     *            the string to search in
     * @param openingMarkers
     *            characters that delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters that delimit the end of a text block to skip
     * @param overridingMarkers
     *            the subset of <code>openingMarkers</code> that override the remaining markers, e.g., if <code>openingMarkers = "'("</code> and
     *            <code>overridingMarkers = "'"</code> then the block between the outer parenthesis in <code>"start ('max('); end"</code> is strictly consumed,
     *            otherwise the suffix <code>" end"</code> would end up being consumed too in the process of handling the nested parenthesis.
     * @param searchMode
     *            a <code>Set</code>, ideally an <code>EnumSet</code>, containing the flags from the enum <code>StringUtils.SearchMode</code> that determine the
     *            behavior of the search
     * @return the position where the next non-whitespace character is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    public static int indexOfNextAlphanumericChar(int startingPosition, String searchIn, String openingMarkers, String closingMarkers, String overridingMarkers,
            Set<SearchMode> searchMode) {
        StringInspector strInspector = new StringInspector(searchIn, startingPosition, openingMarkers, closingMarkers, overridingMarkers, searchMode);
        return strInspector.indexOfNextAlphanumericChar();
    }

    /**
     * Finds the position of the next non-whitespace character within a string, with the option to skip text delimited by given markers or within comments.
     *
     * @param startingPosition
     *            the position to start the search from
     * @param searchIn
     *            the string to search in
     * @param openingMarkers
     *            characters that delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters that delimit the end of a text block to skip
     * @param overridingMarkers
     *            the subset of <code>openingMarkers</code> that override the remaining markers, e.g., if <code>openingMarkers = "'("</code> and
     *            <code>overridingMarkers = "'"</code> then the block between the outer parenthesis in <code>"start ('max('); end"</code> is strictly consumed,
     *            otherwise the suffix <code>" end"</code> would end up being consumed too in the process of handling the nested parenthesis.
     * @param searchMode
     *            a <code>Set</code>, ideally an <code>EnumSet</code>, containing the flags from the enum <code>StringUtils.SearchMode</code> that determine the
     *            behavior of the search
     * @return the position where the next non-whitespace character is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    public static int indexOfNextNonWsChar(int startingPosition, String searchIn, String openingMarkers, String closingMarkers, String overridingMarkers,
            Set<SearchMode> searchMode) {
        StringInspector strInspector = new StringInspector(searchIn, startingPosition, openingMarkers, closingMarkers, overridingMarkers, searchMode);
        return strInspector.indexOfNextNonWsChar();
    }

    /**
     * Finds the position of the next whitespace character within a string, with the option to skip text delimited by given markers or within comments.
     *
     * @param startingPosition
     *            the position to start the search from
     * @param searchIn
     *            the string to search in
     * @param openingMarkers
     *            characters that delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters that delimit the end of a text block to skip
     * @param overridingMarkers
     *            the subset of <code>openingMarkers</code> that override the remaining markers, e.g., if <code>openingMarkers = "'("</code> and
     *            <code>overridingMarkers = "'"</code> then the block between the outer parenthesis in <code>"start ('max('); end"</code> is strictly consumed,
     *            otherwise the suffix <code>" end"</code> would end up being consumed too in the process of handling the nested parenthesis.
     * @param searchMode
     *            a <code>Set</code>, ideally an <code>EnumSet</code>, containing the flags from the enum <code>StringUtils.SearchMode</code> that determine the
     *            behavior of the search
     * @return the position where the next whitespace character is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    public static int indexOfNextWsChar(int startingPosition, String searchIn, String openingMarkers, String closingMarkers, String overridingMarkers,
            Set<SearchMode> searchMode) {
        StringInspector strInspector = new StringInspector(searchIn, startingPosition, openingMarkers, closingMarkers, overridingMarkers, searchMode);
        return strInspector.indexOfNextWsChar();
    }

    private static boolean isCharAtPosNotEqualIgnoreCase(String searchIn, int pos, char firstCharOfSearchForUc, char firstCharOfSearchForLc) {
        return Character.toLowerCase(searchIn.charAt(pos)) != firstCharOfSearchForLc && Character.toUpperCase(searchIn.charAt(pos)) != firstCharOfSearchForUc;
    }

    protected static boolean isCharEqualIgnoreCase(char charToCompare, char compareToCharUC, char compareToCharLC) {
        return Character.toLowerCase(charToCompare) == compareToCharLC || Character.toUpperCase(charToCompare) == compareToCharUC;
    }

    /**
     * Splits stringToSplit into a list, using the given delimiter
     *
     * @param stringToSplit
     *            the string to split
     * @param delimiter
     *            the string to split on
     * @param trim
     *            should the split strings be whitespace trimmed?
     *
     * @return the list of strings, split by delimiter
     *
     * @throws IllegalArgumentException
     *             if an error occurs
     */
    public static List<String> split(String stringToSplit, String delimiter, boolean trim) {
        if (stringToSplit == null) {
            return new ArrayList<>();
        }

        if (delimiter == null) {
            throw new IllegalArgumentException();
        }

        String[] tokens = stringToSplit.split(delimiter, -1);
        List<String> tokensList = Arrays.asList(tokens);
        if (trim) {
            tokensList = tokensList.stream().map(String::trim).collect(Collectors.toList());
        }
        return tokensList;
    }

    /**
     * Splits stringToSplit into a list, using the given delimiter and skipping all between the given markers.
     *
     * @param stringToSplit
     *            the string to split
     * @param delimiter
     *            the string to split on
     * @param openingMarkers
     *            characters that delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters that delimit the end of a text block to skip
     * @param trim
     *            should the split strings be whitespace trimmed?
     *
     * @return the list of strings, split by delimiter
     *
     * @throws IllegalArgumentException
     *             if an error occurs
     */
    public static List<String> split(String stringToSplit, String delimiter, String openingMarkers, String closingMarkers, boolean trim) {
        return split(stringToSplit, delimiter, openingMarkers, closingMarkers, "", trim);
    }

    /**
     * Splits stringToSplit into a list, using the given delimiter and skipping all between the given markers.
     *
     * @param stringToSplit
     *            the string to split
     * @param delimiter
     *            the string to split on
     * @param openingMarkers
     *            characters that delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters that delimit the end of a text block to skip
     * @param trim
     *            should the split strings be whitespace trimmed?
     * @param searchMode
     *            a <code>Set</code>, ideally an <code>EnumSet</code>, containing the flags from the enum <code>StringUtils.SearchMode</code> that determine the
     *            behaviour of the search
     *
     * @return the list of strings, split by delimiter
     *
     * @throws IllegalArgumentException
     *             if an error occurs
     */
    public static List<String> split(String stringToSplit, String delimiter, String openingMarkers, String closingMarkers, boolean trim,
            Set<SearchMode> searchMode) {
        return split(stringToSplit, delimiter, openingMarkers, closingMarkers, "", trim, searchMode);
    }

    /**
     * Splits stringToSplit into a list, using the given delimiter and skipping all between the given markers.
     *
     * @param stringToSplit
     *            the string to split
     * @param delimiter
     *            the string to split on
     * @param openingMarkers
     *            characters that delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters that delimit the end of a text block to skip
     * @param overridingMarkers
     *            the subset of <code>openingMarkers</code> that override the remaining markers, e.g., if <code>openingMarkers = "'("</code> and
     *            <code>overridingMarkers = "'"</code> then the block between the outer parenthesis in <code>"start ('max('); end"</code> is strictly consumed,
     *            otherwise the suffix <code>" end"</code> would end up being consumed too in the process of handling the nested parenthesis.
     * @param trim
     *            should the split strings be whitespace trimmed?
     *
     * @return the list of strings, split by delimiter
     *
     * @throws IllegalArgumentException
     *             if an error occurs
     */
    public static List<String> split(String stringToSplit, String delimiter, String openingMarkers, String closingMarkers, String overridingMarkers,
            boolean trim) {
        return split(stringToSplit, delimiter, openingMarkers, closingMarkers, overridingMarkers, trim, SearchMode.__MRK_COM_MYM_HNT_WS);
    }

    /**
     * Splits stringToSplit into a list, using the given delimiter and skipping all between the given markers.
     *
     * @param stringToSplit
     *            the string to split
     * @param delimiter
     *            the string to split on
     * @param openingMarkers
     *            characters that delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters that delimit the end of a text block to skip
     * @param overridingMarkers
     *            the subset of <code>openingMarkers</code> that override the remaining markers, e.g., if <code>openingMarkers = "'("</code> and
     *            <code>overridingMarkers = "'"</code> then the block between the outer parenthesis in <code>"start ('max('); end"</code> is strictly consumed,
     *            otherwise the suffix <code>" end"</code> would end up being consumed too in the process of handling the nested parenthesis.
     * @param trim
     *            should the split strings be whitespace trimmed?
     * @param searchMode
     *            a <code>Set</code>, ideally an <code>EnumSet</code>, containing the flags from the enum <code>StringUtils.SearchMode</code> that determine the
     *            behaviour of the search
     *
     * @return the list of strings, split by delimiter
     *
     * @throws IllegalArgumentException
     *             if an error occurs
     */
    public static List<String> split(String stringToSplit, String delimiter, String openingMarkers, String closingMarkers, String overridingMarkers,
            boolean trim, Set<SearchMode> searchMode) {
        StringInspector strInspector = new StringInspector(stringToSplit, openingMarkers, closingMarkers, overridingMarkers, searchMode);
        return strInspector.split(delimiter, trim);
    }

    private static boolean startsWith(byte[] dataFrom, String chars) {
        int charsLength = chars.length();

        if (dataFrom.length < charsLength) {
            return false;
        }
        for (int i = 0; i < charsLength; i++) {
            if (dataFrom[i] != chars.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Determines whether or not the string 'searchIn' contains the string 'searchFor', disregarding case and starting at 'startAt'. Shorthand for a
     * String.regionMatch(...)
     *
     * @param searchIn
     *            the string to search in
     * @param startAt
     *            the position to start at
     * @param searchFor
     *            the string to search for
     *
     * @return whether searchIn starts with searchFor, ignoring case
     */
    public static boolean regionMatchesIgnoreCase(String searchIn, int startAt, String searchFor) {
        return searchIn.regionMatches(true, startAt, searchFor, 0, searchFor.length());
    }

    /**
     * Determines whether or not the string 'searchIn' starts with the string 'searchFor', dis-regarding case. Shorthand for a String.regionMatch(...)
     *
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for
     *
     * @return whether searchIn starts with searchFor, ignoring case
     */
    public static boolean startsWithIgnoreCase(String searchIn, String searchFor) {
        return regionMatchesIgnoreCase(searchIn, 0, searchFor);
    }

    /**
     * Determines whether or not the string 'searchIn' starts with the string 'searchFor', disregarding case,leading whitespace and non-alphanumeric characters.
     *
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for
     *
     * @return true if the string starts with 'searchFor' ignoring whitespace
     */
    public static boolean startsWithIgnoreCaseAndNonAlphaNumeric(String searchIn, String searchFor) {
        if (searchIn == null) {
            return searchFor == null;
        }

        int beginPos = 0;
        int inLength = searchIn.length();

        for (; beginPos < inLength; beginPos++) {
            char c = searchIn.charAt(beginPos);
            if (Character.isLetterOrDigit(c)) {
                break;
            }
        }

        return regionMatchesIgnoreCase(searchIn, beginPos, searchFor);
    }

    /**
     * Determines whether or not the string 'searchIn' starts with the string 'searchFor', disregarding case and leading whitespace
     *
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for
     *
     * @return true if the string starts with 'searchFor' ignoring whitespace
     */
    public static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor) {
        return startsWithIgnoreCaseAndWs(searchIn, searchFor, 0);
    }

    /**
     * Determines whether or not the string 'searchIn' contains the string 'searchFor', disregarding case and leading whitespace
     *
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for
     * @param beginPos
     *            where to start searching
     *
     * @return true if the string starts with 'searchFor' ignoring whitespace
     */

    public static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor, int beginPos) {
        if (searchIn == null) {
            return searchFor == null;
        }

        for (; beginPos < searchIn.length(); beginPos++) {
            if (!Character.isWhitespace(searchIn.charAt(beginPos))) {
                break;
            }
        }

        return regionMatchesIgnoreCase(searchIn, beginPos, searchFor);
    }

    /**
     * Determines whether or not the string 'searchIn' starts with one of the strings in 'searchFor', disregarding case and leading whitespace
     *
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string array to search for
     *
     * @return the 'searchFor' array index that matched or -1 if none matches
     */
    public static int startsWithIgnoreCaseAndWs(String searchIn, String[] searchFor) {
        for (int i = 0; i < searchFor.length; i++) {
            if (startsWithIgnoreCaseAndWs(searchIn, searchFor[i], 0)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Determines whether or not the string 'searchIn' ends with the string 'searchFor', dis-regarding case starting at 'startAt' Shorthand for a
     * String.regionMatch(...)
     *
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for
     *
     * @return whether searchIn ends with searchFor, ignoring case
     */
    public static boolean endsWithIgnoreCase(String searchIn, String searchFor) {
        int len = searchFor.length();
        return searchIn.regionMatches(true, searchIn.length() - len, searchFor, 0, len);
    }

    /**
     * @param source
     *            bytes to strip
     * @param prefix
     *            prefix
     * @param suffix
     *            suffix
     * @return result bytes
     */
    public static byte[] stripEnclosure(byte[] source, String prefix, String suffix) {
        if (source.length >= prefix.length() + suffix.length() && startsWith(source, prefix) && endsWith(source, suffix)) {

            int totalToStrip = prefix.length() + suffix.length();
            int enclosedLength = source.length - totalToStrip;
            byte[] enclosed = new byte[enclosedLength];

            int startPos = prefix.length();
            int numToCopy = enclosed.length;
            System.arraycopy(source, startPos, enclosed, 0, numToCopy);

            return enclosed;
        }
        return source;
    }

    /**
     * Returns the bytes as an ASCII String.
     *
     * @param buffer
     *            the bytes representing the string
     *
     * @return The ASCII String.
     */
    public static String toAsciiString(byte[] buffer) {
        return toAsciiString(buffer, 0, buffer.length);
    }

    /**
     * Returns the bytes as an ASCII String.
     *
     * @param buffer
     *            the bytes to convert
     * @param startPos
     *            the position to start converting
     * @param length
     *            the length of the string to convert
     *
     * @return the ASCII string
     */
    public static String toAsciiString(byte[] buffer, int startPos, int length) {
        return new String(toAsciiCharArray(buffer, startPos, length));
    }

    /**
     * Returns the bytes as an ASCII String.
     *
     * @param buffer
     *            the bytes to convert
     * @param startPos
     *            the position to start converting
     * @param length
     *            the length of the string to convert
     *
     * @return the ASCII char array
     */
    public static char[] toAsciiCharArray(byte[] buffer, int startPos, int length) {
        char[] charArray = new char[length];
        int readpoint = startPos;

        for (int i = 0; i < length; i++) {
            charArray[i] = (char) buffer[readpoint];
            readpoint++;
        }
        return charArray;
    }

    /**
     * Compares searchIn against searchForWildcard with wildcards, in a case insensitive manner.
     *
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for, using the 'standard' SQL wildcard chars of '%' and '_'
     * @return true if matches
     */
    public static boolean wildCompareIgnoreCase(String searchIn, String searchFor) {
        return wildCompareInternal(searchIn, searchFor) == WILD_COMPARE_MATCH;
    }

    /**
     * Compares searchIn against searchForWildcard with wildcards (heavily borrowed from strings/ctype-simple.c in the server sources)
     *
     * This method does a single passage matching for normal characters and WILDCARD_ONE (_), and recursive matching for WILDCARD_MANY (%) which may be repeated
     * for as many anchor chars are found.
     *
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for, using the 'standard' SQL wildcard chars of '%' and '_'
     *
     * @return WILD_COMPARE_MATCH if matched, WILD_COMPARE_NO_MATCH if not matched, WILD_COMPARE_CONTINUE_WITH_WILD if not matched yet, but it may in one of
     *         following recursion rounds
     */
    private static int wildCompareInternal(String searchIn, String searchFor) {
        if (searchIn == null || searchFor == null) {
            return WILD_COMPARE_NO_MATCH;
        }

        if (searchFor.equals("%")) {
            return WILD_COMPARE_MATCH;
        }

        int searchForPos = 0;
        int searchForEnd = searchFor.length();

        int searchInPos = 0;
        int searchInEnd = searchIn.length();

        int result = WILD_COMPARE_NO_MATCH; /* Not found, using wildcards */

        while (searchForPos != searchForEnd) {
            while (searchFor.charAt(searchForPos) != WILDCARD_MANY && searchFor.charAt(searchForPos) != WILDCARD_ONE) {
                if (searchFor.charAt(searchForPos) == WILDCARD_ESCAPE && searchForPos + 1 != searchForEnd) {
                    searchForPos++;
                }

                if (searchInPos == searchInEnd
                        || Character.toUpperCase(searchFor.charAt(searchForPos++)) != Character.toUpperCase(searchIn.charAt(searchInPos++))) {
                    return WILD_COMPARE_CONTINUE_WITH_WILD; /* No match */
                }

                if (searchForPos == searchForEnd) {
                    return searchInPos != searchInEnd ? WILD_COMPARE_CONTINUE_WITH_WILD : WILD_COMPARE_MATCH; /* Match if both are at end */
                }

                result = WILD_COMPARE_CONTINUE_WITH_WILD; /* Found an anchor char */
            }

            if (searchFor.charAt(searchForPos) == WILDCARD_ONE) {
                do {
                    if (searchInPos == searchInEnd) { /* Skip one char if possible */
                        return result;
                    }
                    searchInPos++;
                } while (++searchForPos < searchForEnd && searchFor.charAt(searchForPos) == WILDCARD_ONE);

                if (searchForPos == searchForEnd) {
                    break;
                }
            }

            if (searchFor.charAt(searchForPos) == WILDCARD_MANY) { /* Found w_many */
                searchForPos++;

                /* Remove any '%' and '_' from the wild search string */
                for (; searchForPos != searchForEnd; searchForPos++) {
                    if (searchFor.charAt(searchForPos) == WILDCARD_MANY) {
                        continue;
                    }

                    if (searchFor.charAt(searchForPos) == WILDCARD_ONE) {
                        if (searchInPos == searchInEnd) { /* Skip one char if possible */
                            return WILD_COMPARE_NO_MATCH;
                        }
                        searchInPos++;
                        continue;
                    }

                    break; /* Not a wild character */
                }

                if (searchForPos == searchForEnd) {
                    return WILD_COMPARE_MATCH; /* Ok if w_many is last */
                }

                if (searchInPos == searchInEnd) {
                    return WILD_COMPARE_NO_MATCH;
                }

                char cmp;
                if ((cmp = searchFor.charAt(searchForPos)) == WILDCARD_ESCAPE && searchForPos + 1 != searchForEnd) {
                    cmp = searchFor.charAt(++searchForPos);
                }

                searchForPos++;

                do {
                    while (searchInPos != searchInEnd && Character.toUpperCase(searchIn.charAt(searchInPos)) != Character.toUpperCase(cmp)) {
                        searchInPos++;
                    } /* Searches for an anchor char */

                    if (searchInPos++ == searchInEnd) {
                        return WILD_COMPARE_NO_MATCH;
                    }

                    int tmp = wildCompareInternal(searchIn.substring(searchInPos), searchFor.substring(searchForPos));
                    if (tmp <= 0) {
                        return tmp;
                    }

                } while (searchInPos != searchInEnd);

                return WILD_COMPARE_NO_MATCH;
            }
        }

        return searchInPos != searchInEnd ? WILD_COMPARE_CONTINUE_WITH_WILD : WILD_COMPARE_MATCH;
    }

    public static int lastIndexOf(byte[] s, char c) {
        if (s == null) {
            return -1;
        }

        for (int i = s.length - 1; i >= 0; i--) {
            if (s[i] == c) {
                return i;
            }
        }

        return -1;
    }

    public static int indexOf(byte[] s, char c) {
        if (s == null) {
            return -1;
        }

        int length = s.length;

        for (int i = 0; i < length; i++) {
            if (s[i] == c) {
                return i;
            }
        }

        return -1;
    }

    public static boolean isNullOrEmpty(String str) {
        return str == null || str.isEmpty();
    }

    /**
     * Two given strings are considered equal if both are null or if they have the same string value.
     *
     * @param str1
     *            first string to compare
     * @param str2
     *            fecond string to compare
     * @return
     *         <code>true</code> if both strings are null or have the same value
     */
    public static boolean nullSafeEqual(String str1, String str2) {
        return str1 == null && str2 == null || str1 != null && str1.equals(str2);
    }

    /**
     * Removes comments and hints from the given string.
     *
     * @param source
     *            the query string to clean up.
     * @param openingMarkers
     *            characters that delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters that delimit the end of a text block to skip
     * @param allowBackslashEscapes
     *            whether or not backslash escapes are allowed
     * @return the query string with all comment-delimited data removed
     */
    public static String stripCommentsAndHints(final String source, final String openingMarkers, final String closingMarkers,
            final boolean allowBackslashEscapes) {
        StringInspector strInspector = new StringInspector(source, openingMarkers, closingMarkers, "",
                allowBackslashEscapes ? SearchMode.__BSE_MRK_COM_MYM_HNT_WS : SearchMode.__MRK_COM_MYM_HNT_WS);
        return strInspector.stripCommentsAndHints();
    }

    /**
     * Next two functions are to help DBMD check if the given string is in form of database.name and return it as "database";"name" with comments removed.
     * If string is NULL or wildcard (%), returns null and exits.
     *
     * First, we sanitize...
     *
     * @param src
     *            the source string
     * @return the input string with all comment-delimited data removed
     */
    public static String sanitizeProcOrFuncName(String src) {
        if (src == null || src.equals("%")) {
            return null;
        }

        return src;
    }

    /**
     * Splits an entity identifier into its parts (database and entity name) and returns a list containing the two elements. If the identifier doesn't contain
     * the database part then the argument <code>db</code> is used in its place and <code>source</code> corresponds to the full entity name.
     * If argument <code>source</code> is NULL or wildcard (%), returns an empty list.
     *
     * @param source
     *            the source string
     * @param db
     *            database, if available
     * @param quoteId
     *            quote character as defined on server
     * @param isNoBslashEscSet
     *            is our connection in no BackSlashEscape mode
     * @return the input string with all comment-delimited data removed
     */
    public static List<String> splitDBdotName(String source, String db, String quoteId, boolean isNoBslashEscSet) {
        if (source == null || source.equals("%")) {
            return Collections.emptyList();
        }

        int dotIndex = -1;
        if (" ".equals(quoteId)) {
            dotIndex = source.indexOf(".");
        } else {
            dotIndex = indexOfIgnoreCase(0, source, ".", quoteId, quoteId, isNoBslashEscSet ? SearchMode.__MRK_WS : SearchMode.__BSE_MRK_WS);
        }

        String database = db;
        String entityName;
        if (dotIndex != -1) {
            database = unQuoteIdentifier(source.substring(0, dotIndex), quoteId);
            entityName = unQuoteIdentifier(source.substring(dotIndex + 1), quoteId);
        } else {
            entityName = unQuoteIdentifier(source, quoteId);
        }

        return Arrays.asList(database, entityName);
    }

    /**
     * Builds and returns a fully qualified name, quoted if necessary, for the given database entity.
     *
     * @param db
     *            database name
     * @param entity
     *            identifier
     * @param quoteId
     *            quote character as defined on server
     * @param isPedantic
     *            are we in pedantic mode
     * @return fully qualified name
     */
    public static String getFullyQualifiedName(String db, String entity, String quoteId, boolean isPedantic) {
        StringBuilder fullyQualifiedName = new StringBuilder(StringUtils.quoteIdentifier(db == null ? "" : db, quoteId, isPedantic));
        fullyQualifiedName.append('.');
        fullyQualifiedName.append(StringUtils.quoteIdentifier(entity, quoteId, isPedantic));
        return fullyQualifiedName.toString();
    }

    public static boolean isEmptyOrWhitespaceOnly(String str) {
        if (str == null || str.length() == 0) {
            return true;
        }

        int length = str.length();

        for (int i = 0; i < length; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    public static String escapeQuote(String str, String quotChar) {
        if (str == null) {
            return null;
        }

        str = StringUtils.toString(stripEnclosure(str.getBytes(), quotChar, quotChar));

        int lastNdx = str.indexOf(quotChar);
        String tmpSrc;
        String tmpRest;

        tmpSrc = str.substring(0, lastNdx);
        tmpSrc = tmpSrc + quotChar + quotChar;

        tmpRest = str.substring(lastNdx + 1, str.length());

        lastNdx = tmpRest.indexOf(quotChar);
        while (lastNdx > -1) {

            tmpSrc = tmpSrc + tmpRest.substring(0, lastNdx);
            tmpSrc = tmpSrc + quotChar + quotChar;
            tmpRest = tmpRest.substring(lastNdx + 1, tmpRest.length());

            lastNdx = tmpRest.indexOf(quotChar);
        }

        tmpSrc = tmpSrc + tmpRest;
        str = tmpSrc;

        return str;
    }

    /**
     * Surrounds identifier with quoteChar and duplicates these symbols inside the identifier.
     *
     * @param quoteChar
     *            ` or "
     * @param identifier
     *            in pedantic mode (connection property pedantic=true) identifier is treated as unquoted
     *            (as it is stored in the database) even if it starts and ends with quoteChar;
     *            in non-pedantic mode if identifier starts and ends with quoteChar method treats it as already quoted and doesn't modify.
     * @param isPedantic
     *            are we in pedantic mode
     *
     * @return
     *         With quoteChar="`":<br>
     *         <ul>
     *         <li>null {@code ->} null</li>
     *         <li>abc {@code ->} `abc`</li>
     *         <li>ab`c {@code ->} `ab``c`</li>
     *         <li>ab"c {@code ->} `ab"c`</li>
     *         <li>`ab``c` {@code ->} `ab``c` in non-pedantic mode or ```ab````c``` in pedantic mode</li>
     *         </ul>
     *         With quoteChar="\"":<br>
     *         <ul>
     *         <li>null {@code ->} null</li>
     *         <li>abc {@code ->} "abc"</li>
     *         <li>ab`c {@code ->} "ab`c"</li>
     *         <li>ab"c {@code ->} "ab""c"</li>
     *         <li>"ab""c" {@code ->} "ab""c" in non-pedantic mode or """ab""""c""" in pedantic mode</li>
     *         </ul>
     */
    public static String quoteIdentifier(String identifier, String quoteChar, boolean isPedantic) {
        if (identifier == null) {
            return null;
        }

        identifier = identifier.trim();

        int quoteCharLength = quoteChar.length();
        if (quoteCharLength == 0) {
            return identifier;
        }

        // Check if the identifier is correctly quoted and if quotes within are correctly escaped. If not, quote and escape it.
        if (!isPedantic && identifier.startsWith(quoteChar) && identifier.endsWith(quoteChar)) {
            // Trim outermost quotes from the identifier.
            String identifierQuoteTrimmed = identifier.substring(quoteCharLength, identifier.length() - quoteCharLength);

            // Check for pairs of quotes.
            int quoteCharPos = identifierQuoteTrimmed.indexOf(quoteChar);
            while (quoteCharPos >= 0) {
                int quoteCharNextExpectedPos = quoteCharPos + quoteCharLength;
                int quoteCharNextPosition = identifierQuoteTrimmed.indexOf(quoteChar, quoteCharNextExpectedPos);

                if (quoteCharNextPosition == quoteCharNextExpectedPos) {
                    quoteCharPos = identifierQuoteTrimmed.indexOf(quoteChar, quoteCharNextPosition + quoteCharLength);
                } else {
                    // Not a pair of quotes!
                    break;
                }
            }

            if (quoteCharPos < 0) {
                return identifier;
            }
        }

        return quoteChar + identifier.replaceAll(quoteChar, quoteChar + quoteChar) + quoteChar;
    }

    /**
     * Surrounds identifier with "`" and duplicates these symbols inside the identifier.
     *
     * @param identifier
     *            in pedantic mode (connection property pedantic=true) identifier is treated as unquoted (as it is stored in the database) even if it starts and
     *            ends with "`";
     *            in non-pedantic mode if identifier starts and ends with "`" method treats it as already quoted and doesn't modify.
     * @param isPedantic
     *            are we in pedantic mode
     *
     * @return
     *         <ul>
     *         <li>null {@code ->} null</li>
     *         <li>abc {@code ->} `abc`</li>
     *         <li>ab`c {@code ->} `ab``c`</li>
     *         <li>ab"c {@code ->} `ab"c`</li>
     *         <li>`ab``c` {@code ->} `ab``c` in non-pedantic mode or ```ab````c``` in pedantic mode</li>
     *         </ul>
     */
    public static String quoteIdentifier(String identifier, boolean isPedantic) {
        return quoteIdentifier(identifier, "`", isPedantic);
    }

    /**
     * Trims the identifier, removes quote chars from first and last positions and replaces double occurrences of quote char from entire identifier, i.e.
     * converts quoted identifier into the form as it is stored in database.
     *
     * @param identifier
     *            identifier
     * @param quoteChar
     *            ` or "
     * @return
     *         <ul>
     *         <li>null {@code ->} null</li>
     *         <li>abc {@code ->} abc</li>
     *         <li>`abc` {@code ->} abc</li>
     *         <li>`ab``c` {@code ->} ab`c</li>
     *         <li>`"ab`c"` {@code ->} "ab`c"</li>
     *         <li>`ab"c` {@code ->} ab"c</li>
     *         <li>"abc" {@code ->} abc</li>
     *         <li>"`ab""c`" {@code ->} `ab"c`</li>
     *         <li>"ab`c" {@code ->} ab`c</li>
     *         </ul>
     */
    public static String unQuoteIdentifier(String identifier, String quoteChar) {
        if (identifier == null) {
            return null;
        }

        identifier = identifier.trim();

        int quoteCharLength = quoteChar.length();
        if (quoteCharLength == 0) {
            return identifier;
        }

        // Check if the identifier is really quoted or if it simply contains quote chars in it (assuming that the value is a valid identifier).
        if (identifier.startsWith(quoteChar) && identifier.endsWith(quoteChar)) {
            // Trim outermost quotes from the identifier.
            String identifierQuoteTrimmed = identifier.substring(quoteCharLength, identifier.length() - quoteCharLength);

            // Check for pairs of quotes.
            int quoteCharPos = identifierQuoteTrimmed.indexOf(quoteChar);
            while (quoteCharPos >= 0) {
                int quoteCharNextExpectedPos = quoteCharPos + quoteCharLength;
                int quoteCharNextPosition = identifierQuoteTrimmed.indexOf(quoteChar, quoteCharNextExpectedPos);

                if (quoteCharNextPosition == quoteCharNextExpectedPos) {
                    quoteCharPos = identifierQuoteTrimmed.indexOf(quoteChar, quoteCharNextPosition + quoteCharLength);
                } else {
                    // Not a pair of quotes! Return as it is...
                    return identifier;
                }
            }

            return identifier.substring(quoteCharLength, identifier.length() - quoteCharLength).replaceAll(quoteChar + quoteChar, quoteChar);
        }

        return identifier;
    }

    public static int indexOfQuoteDoubleAware(String searchIn, String quoteChar, int startFrom) {
        if (searchIn == null || quoteChar == null || quoteChar.length() == 0 || startFrom > searchIn.length()) {
            return -1;
        }

        int lastIndex = searchIn.length() - 1;

        int beginPos = startFrom;
        int pos = -1;

        boolean next = true;
        while (next) {
            pos = searchIn.indexOf(quoteChar, beginPos);
            if (pos == -1 || pos == lastIndex || !searchIn.startsWith(quoteChar, pos + 1)) {
                next = false;
            } else {
                beginPos = pos + 2;
            }
        }

        return pos;
    }

    public static String toString(byte[] value, int offset, int length, String encoding) {
        if (encoding == null || "null".equalsIgnoreCase(encoding)) {
            return new String(value, offset, length);
        }
        try {
            return new String(value, offset, length, encoding);
        } catch (UnsupportedEncodingException uee) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("StringUtils.0", new Object[] { encoding }), uee);
        }
    }

    public static String toString(byte[] value, String encoding) {
        if (encoding == null) {
            return new String(value);
        }
        try {
            return new String(value, encoding);
        } catch (UnsupportedEncodingException uee) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("StringUtils.0", new Object[] { encoding }), uee);
        }
    }

    public static String toString(byte[] value, Charset charset) {
        return new String(value, charset);
    }

    public static String toString(byte[] value, int offset, int length) {
        return new String(value, offset, length);
    }

    public static String toString(byte[] value) {
        return new String(value);
    }

    /**
     * Returns the byte[] representation of subset of the given char[] using the default/platform encoding.
     *
     * @param value
     *            chars
     * @return bytes
     */
    public static byte[] getBytes(char[] value) {
        return getBytes(value, 0, value.length);
    }

    /**
     * Returns the byte[] representation of subset of the given char[] using the given encoding.
     *
     * @param c
     *            chars
     * @param encoding
     *            java encoding
     * @return bytes
     */
    public static byte[] getBytes(char[] c, String encoding) {
        return getBytes(c, 0, c.length, encoding);
    }

    public static byte[] getBytes(char[] value, int offset, int length) {
        return getBytes(value, offset, length, null);
    }

    /**
     * Returns the byte[] representation of subset of the given char[] using the given encoding.
     *
     * @param value
     *            chars
     * @param offset
     *            offset
     * @param length
     *            length
     * @param encoding
     *            java encoding
     * @return bytes
     */
    public static byte[] getBytes(char[] value, int offset, int length, String encoding) {
        Charset cs;
        try {
            if (encoding == null) {
                cs = Charset.defaultCharset();
            } else {
                cs = Charset.forName(encoding);
            }
        } catch (UnsupportedCharsetException ex) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("StringUtils.0", new Object[] { encoding }), ex);
        }
        ByteBuffer buf = cs.encode(CharBuffer.wrap(value, offset, length));

        // can't simply .array() this to get the bytes especially with variable-length charsets the buffer is sometimes larger than the actual encoded data
        int encodedLen = buf.limit();
        byte[] asBytes = new byte[encodedLen];
        buf.get(asBytes, 0, encodedLen);

        return asBytes;
    }

    public static byte[] getBytes(String value) {
        return value.getBytes();
    }

    public static byte[] getBytes(String value, int offset, int length) {
        return value.substring(offset, offset + length).getBytes();
    }

    public static byte[] getBytes(String value, int offset, int length, String encoding) {
        if (encoding == null) {
            return getBytes(value, offset, length);
        }

        try {
            return value.substring(offset, offset + length).getBytes(encoding);
        } catch (UnsupportedEncodingException uee) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("StringUtils.0", new Object[] { encoding }), uee);
        }
    }

    public static final boolean isValidIdChar(char c) {
        return VALID_ID_CHARS.indexOf(c) != -1;
    }

    private static final char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    /**
     * Used to escape binary data with hex
     *
     * @param buf
     *            source bytes
     * @param size
     *            number of bytes to read
     * @param bc
     *            consumer for low and high bits of each byte
     */
    public static final void hexEscapeBlock(byte[] buf, int size, BiConsumer<Byte, Byte> bc) {
        for (int i = 0; i < size; i++) {
            bc.accept((byte) HEX_DIGITS[buf[i] >>> 4 & 0xF], (byte) HEX_DIGITS[buf[i] & 0xF]);
        }
    }

    public static void appendAsHex(StringBuilder builder, byte[] bytes) {
        builder.append("0x");
        for (byte b : bytes) {
            builder.append(HEX_DIGITS[b >>> 4 & 0xF]).append(HEX_DIGITS[b & 0xF]);
        }
    }

    public static void appendAsHex(StringBuilder builder, int value) {
        if (value == 0) {
            builder.append("0x0");
            return;
        }

        int shift = 32;
        byte nibble;
        boolean nonZeroFound = false;

        builder.append("0x");
        do {
            shift -= 4;
            nibble = (byte) (value >>> shift & 0xF);
            if (nonZeroFound) {
                builder.append(HEX_DIGITS[nibble]);
            } else if (nibble != 0) {
                builder.append(HEX_DIGITS[nibble]);
                nonZeroFound = true;
            }
        } while (shift != 0);
    }

    public static byte[] getBytesNullTerminated(String value, String encoding) {
        Charset cs = Charset.forName(encoding);
        ByteBuffer buf = cs.encode(value);
        int encodedLen = buf.limit();
        byte[] asBytes = new byte[encodedLen + 1];
        buf.get(asBytes, 0, encodedLen);
        asBytes[encodedLen] = 0;

        return asBytes;
    }

    public static boolean canHandleAsServerPreparedStatementNoCache(String sql, ServerVersion serverVersion, boolean allowMultiQueries,
            boolean noBackslashEscapes, boolean useAnsiQuotes) {
        // Can't use server-side prepare for CALL
        if (startsWithIgnoreCaseAndNonAlphaNumeric(sql, "CALL")) {
            return false;
        }

        boolean canHandleAsStatement = true;

        boolean allowBackslashEscapes = !noBackslashEscapes;
        String quoteChar = useAnsiQuotes ? "\"" : "'";

        if (allowMultiQueries) {
            if (StringUtils.indexOfIgnoreCase(0, sql, ";", quoteChar, quoteChar,
                    allowBackslashEscapes ? SearchMode.__BSE_MRK_COM_MYM_HNT_WS : SearchMode.__MRK_COM_MYM_HNT_WS) != -1) {
                canHandleAsStatement = false;
            }
        } else if (startsWithIgnoreCaseAndWs(sql, "XA ")) {
            canHandleAsStatement = false;
        } else if (startsWithIgnoreCaseAndWs(sql, "CREATE TABLE")) {
            canHandleAsStatement = false;
        } else if (startsWithIgnoreCaseAndWs(sql, "DO")) {
            canHandleAsStatement = false;
        } else if (startsWithIgnoreCaseAndWs(sql, "SET")) {
            canHandleAsStatement = false;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(sql, "SHOW WARNINGS") && serverVersion.meetsMinimum(ServerVersion.parseVersion("5.7.2"))) {
            canHandleAsStatement = false;
        } else if (sql.startsWith("/* ping */")) {
            canHandleAsStatement = false;
        }

        return canHandleAsStatement;
    }

    final static char[] EMPTY_SPACE = new char[255];
    static {
        for (int i = 0; i < EMPTY_SPACE.length; i++) {
            EMPTY_SPACE[i] = ' ';
        }
    }

    public static String padString(String stringVal, int requiredLength) {
        int currentLength = stringVal.length();
        int difference = requiredLength - currentLength;

        if (difference > 0) {
            StringBuilder paddedBuf = new StringBuilder(requiredLength);
            paddedBuf.append(stringVal);
            paddedBuf.append(EMPTY_SPACE, 0, difference);
            return paddedBuf.toString();
        }

        return stringVal;
    }

    public static int safeIntParse(String intAsString) {
        try {
            return Integer.parseInt(intAsString);
        } catch (NumberFormatException nfe) {
            return 0;
        }
    }

    /**
     * Checks is the CharSequence contains digits only. No leading sign and thousands or decimal separators are allowed.
     *
     * @param cs
     *            The CharSequence to check.
     * @return
     *         {@code true} if the CharSequence not empty and contains only digits, {@code false} otherwise.
     */
    public static boolean isStrictlyNumeric(CharSequence cs) {
        if (cs == null || cs.length() == 0) {
            return false;
        }
        for (int i = 0; i < cs.length(); i++) {
            if (!Character.isDigit(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static String safeTrim(String toTrim) {
        return isNullOrEmpty(toTrim) ? toTrim : toTrim.trim();
    }

    /**
     * Constructs a String containing all the elements in the String array bounded and joined by the provided concatenation elements. The last element uses a
     * different delimiter.
     *
     * @param elems
     *            the String array from where to take the elements.
     * @param prefix
     *            the prefix of the resulting String.
     * @param midDelimiter
     *            the delimiter to be used between the N-1 elements
     * @param lastDelimiter
     *            the delimiter to be used before the last element.
     * @param suffix
     *            the suffix of the resulting String.
     * @return
     *         a String built from the provided String array and concatenation elements.
     */
    public static String stringArrayToString(String[] elems, String prefix, String midDelimiter, String lastDelimiter, String suffix) {
        StringBuilder valuesString = new StringBuilder();
        if (elems.length > 1) {
            valuesString.append(Arrays.stream(elems).limit(elems.length - 1).collect(Collectors.joining(midDelimiter, prefix, lastDelimiter)));
        } else {
            valuesString.append(prefix);
        }
        valuesString.append(elems[elems.length - 1]).append(suffix);

        return valuesString.toString();
    }

    /**
     * Does the string contain wildcard symbols ('%' or '_'). Used in DatabaseMetaData.
     *
     * @param src
     *            string
     * @return true if src contains wildcard symbols
     */
    public static boolean hasWildcards(String src) {
        return indexOfIgnoreCase(0, src, "%") > -1 || indexOfIgnoreCase(0, src, "_") > -1;
    }

    public static String getUniqueSavepointId() {
        String uuid = UUID.randomUUID().toString();
        return uuid.replaceAll("-", "_"); // for safety
    }

    /**
     * Joins all elements of the given list using serial comma (Oxford comma) rules.
     * E.g.:
     * - "A"
     * - "A and B"
     * - "A, B, and C"
     *
     * @param elements
     *            the elements to join
     * @return
     *         the String with all elements, joined by commas and "and".
     */
    public static String joinWithSerialComma(List<?> elements) {
        if (elements == null || elements.size() == 0) {
            return "";
        }
        if (elements.size() == 1) {
            return elements.get(0).toString();
        }
        if (elements.size() == 2) {
            return elements.get(0) + " and " + elements.get(1);
        }
        return elements.subList(0, elements.size() - 1).stream().map(Object::toString).collect(Collectors.joining(", ", "", ", and "))
                + elements.get(elements.size() - 1).toString();
    }

    public static byte[] unquoteBytes(byte[] bytes) {
        if (bytes[0] == '\'' && bytes[bytes.length - 1] == '\'') {

            byte[] valNoQuotes = new byte[bytes.length - 2];
            int j = 0;
            int quoteCnt = 0;

            for (int i = 1; i < bytes.length - 1; i++) {
                if (bytes[i] == '\'') {
                    quoteCnt++;
                } else {
                    quoteCnt = 0;
                }

                if (quoteCnt == 2) {
                    quoteCnt = 0;
                } else {
                    valNoQuotes[j++] = bytes[i];
                }
            }

            byte[] res = new byte[j];
            System.arraycopy(valNoQuotes, 0, res, 0, j);

            return res;
        }
        return bytes;
    }

    public static byte[] quoteBytes(byte[] bytes) {
        byte[] withQuotes = new byte[bytes.length * 2 + 2];
        int j = 0;
        withQuotes[j++] = '\'';
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == '\'') {
                withQuotes[j++] = '\'';
            }
            withQuotes[j++] = bytes[i];
        }
        withQuotes[j++] = '\'';

        byte[] res = new byte[j];
        System.arraycopy(withQuotes, 0, res, 0, j);
        return res;
    }

    public static StringBuilder escapeString(StringBuilder buf, String x, boolean useAnsiQuotedIdentifiers, CharsetEncoder charsetEncoder) {
        int stringLength = x.length();

        buf.append('\'');

        //
        // Note: buf.append(char) is _faster_ than appending in blocks, because the block append requires a System.arraycopy().... go figure...
        //

        for (int i = 0; i < stringLength; ++i) {
            char c = x.charAt(i);

            switch (c) {
                case 0: /* Must be escaped for 'mysql' */
                    buf.append('\\');
                    buf.append('0');
                    break;
                case '\n': /* Must be escaped for logs */
                    buf.append('\\');
                    buf.append('n');
                    break;
                case '\r':
                    buf.append('\\');
                    buf.append('r');
                    break;
                case '\\':
                    buf.append('\\');
                    buf.append('\\');
                    break;
                case '\'':
                    buf.append('\'');
                    buf.append('\'');
                    break;
                case '"': /* Better safe than sorry */
                    if (useAnsiQuotedIdentifiers) {
                        buf.append('\\');
                    }
                    buf.append('"');
                    break;
                case '\032': /* This gives problems on Win32 */
                    buf.append('\\');
                    buf.append('Z');
                    break;
                case '\u00a5':
                case '\u20a9':
                    // escape characters interpreted as backslash by mysql
                    if (charsetEncoder != null) {
                        CharBuffer cbuf = CharBuffer.allocate(1);
                        ByteBuffer bbuf = ByteBuffer.allocate(1);
                        cbuf.put(c);
                        cbuf.position(0);
                        charsetEncoder.encode(cbuf, bbuf, true);
                        if (bbuf.get(0) == '\\') {
                            buf.append('\\');
                        }
                    }
                    buf.append(c);
                    break;

                default:
                    buf.append(c);
            }
        }

        buf.append('\'');

        return buf;
    }

    public static void escapeBytes(ByteArrayOutputStream bOut, byte[] x) {
        int numBytes = x.length;
        for (int i = 0; i < numBytes; ++i) {
            byte b = x[i];

            switch (b) {
                case 0: /* Must be escaped for 'mysql' */
                    bOut.write('\\');
                    bOut.write('0');
                    break;
                case '\n': /* Must be escaped for logs */
                    bOut.write('\\');
                    bOut.write('n');
                    break;
                case '\r':
                    bOut.write('\\');
                    bOut.write('r');
                    break;
                case '\\':
                    bOut.write('\\');
                    bOut.write('\\');
                    break;
                case '\'':
                    bOut.write('\\');
                    bOut.write('\'');
                    break;
                case '"': /* Better safe than sorry */
                    bOut.write('\\');
                    bOut.write('"');
                    break;
                case '\032': /* This gives problems on Win32 */
                    bOut.write('\\');
                    bOut.write('Z');
                    break;
                default:
                    bOut.write(b);
            }
        }
    }

    /**
     * URL-encode the given string.
     *
     * @param stringToEncode
     *            the string to encode
     * @return
     *         the encoded string
     */
    public static String urlEncode(String stringToEncode) {
        try {
            return URLEncoder.encode(stringToEncode, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            // Won't happen.
            return null;
        }
    }

}
