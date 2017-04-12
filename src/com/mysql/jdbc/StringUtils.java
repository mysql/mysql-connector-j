/*
  Copyright (c) 2002, 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Various utility methods for converting to/from byte arrays in the platform encoding
 */
public class StringUtils {
    public enum SearchMode {
        ALLOW_BACKSLASH_ESCAPE, SKIP_BETWEEN_MARKERS, SKIP_BLOCK_COMMENTS, SKIP_LINE_COMMENTS, SKIP_WHITE_SPACE;
    }

    /*
     * Convenience EnumSets for several SearchMode combinations
     */

    /**
     * Full search mode: allow backslash escape, skip between markers, skip block comments, skip line comments and skip white space.
     */
    public static final Set<SearchMode> SEARCH_MODE__ALL = Collections.unmodifiableSet(EnumSet.allOf(SearchMode.class));

    /**
     * Search mode: skip between markers, skip block comments, skip line comments and skip white space.
     */
    public static final Set<SearchMode> SEARCH_MODE__MRK_COM_WS = Collections.unmodifiableSet(
            EnumSet.of(SearchMode.SKIP_BETWEEN_MARKERS, SearchMode.SKIP_BLOCK_COMMENTS, SearchMode.SKIP_LINE_COMMENTS, SearchMode.SKIP_WHITE_SPACE));

    /**
     * Search mode: allow backslash escape, skip block comments, skip line comments and skip white space.
     */
    public static final Set<SearchMode> SEARCH_MODE__BSESC_COM_WS = Collections.unmodifiableSet(
            EnumSet.of(SearchMode.ALLOW_BACKSLASH_ESCAPE, SearchMode.SKIP_BLOCK_COMMENTS, SearchMode.SKIP_LINE_COMMENTS, SearchMode.SKIP_WHITE_SPACE));

    /**
     * Search mode: allow backslash escape, skip between markers and skip white space.
     */
    public static final Set<SearchMode> SEARCH_MODE__BSESC_MRK_WS = Collections
            .unmodifiableSet(EnumSet.of(SearchMode.ALLOW_BACKSLASH_ESCAPE, SearchMode.SKIP_BETWEEN_MARKERS, SearchMode.SKIP_WHITE_SPACE));

    /**
     * Search mode: skip block comments, skip line comments and skip white space.
     */
    public static final Set<SearchMode> SEARCH_MODE__COM_WS = Collections
            .unmodifiableSet(EnumSet.of(SearchMode.SKIP_BLOCK_COMMENTS, SearchMode.SKIP_LINE_COMMENTS, SearchMode.SKIP_WHITE_SPACE));

    /**
     * Search mode: skip between markers and skip white space.
     */
    public static final Set<SearchMode> SEARCH_MODE__MRK_WS = Collections
            .unmodifiableSet(EnumSet.of(SearchMode.SKIP_BETWEEN_MARKERS, SearchMode.SKIP_WHITE_SPACE));

    /**
     * Empty search mode.
     */
    public static final Set<SearchMode> SEARCH_MODE__NONE = Collections.unmodifiableSet(EnumSet.noneOf(SearchMode.class));

    // length of MySQL version reference in comments of type '/*![00000] */'
    private static final int NON_COMMENTS_MYSQL_VERSION_REF_LENGTH = 5;

    private static final int BYTE_RANGE = (1 + Byte.MAX_VALUE) - Byte.MIN_VALUE;

    private static byte[] allBytes = new byte[BYTE_RANGE];

    private static char[] byteToChars = new char[BYTE_RANGE];

    private static Method toPlainStringMethod;

    private static final int WILD_COMPARE_MATCH = 0;
    private static final int WILD_COMPARE_CONTINUE_WITH_WILD = 1;
    private static final int WILD_COMPARE_NO_MATCH = -1;

    static final char WILDCARD_MANY = '%';
    static final char WILDCARD_ONE = '_';
    static final char WILDCARD_ESCAPE = '\\';

    private static final ConcurrentHashMap<String, Charset> charsetsByAlias = new ConcurrentHashMap<String, Charset>();

    private static final String platformEncoding = System.getProperty("file.encoding");

    private static final String VALID_ID_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIGKLMNOPQRSTUVWXYZ0123456789$_#@";

    static Charset findCharset(String alias) throws UnsupportedEncodingException {
        try {
            Charset cs = charsetsByAlias.get(alias);

            if (cs == null) {
                cs = Charset.forName(alias);
                Charset oldCs = charsetsByAlias.putIfAbsent(alias, cs);
                if (oldCs != null) {
                    // if the previous value was recently set by another thread we return it instead of value we found here
                    cs = oldCs;
                }
            }

            return cs;

            // We re-throw these runtimes for compatibility with java.io
        } catch (UnsupportedCharsetException uce) {
            throw new UnsupportedEncodingException(alias);
        } catch (IllegalCharsetNameException icne) {
            throw new UnsupportedEncodingException(alias);
        } catch (IllegalArgumentException iae) {
            throw new UnsupportedEncodingException(alias);
        }
    }

    static {
        for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
            allBytes[i - Byte.MIN_VALUE] = (byte) i;
        }

        String allBytesString = new String(allBytes, 0, Byte.MAX_VALUE - Byte.MIN_VALUE);

        int allBytesStringLen = allBytesString.length();

        for (int i = 0; (i < (Byte.MAX_VALUE - Byte.MIN_VALUE)) && (i < allBytesStringLen); i++) {
            byteToChars[i] = allBytesString.charAt(i);
        }

        try {
            toPlainStringMethod = BigDecimal.class.getMethod("toPlainString", new Class[0]);
        } catch (NoSuchMethodException nsme) {
            // that's okay, we fallback to .toString()
        }
    }

    /**
     * Takes care of the fact that Sun changed the output of
     * BigDecimal.toString() between JDK-1.4 and JDK 5
     * 
     * @param decimal
     *            the big decimal to stringify
     * 
     * @return a string representation of 'decimal'
     */
    public static String consistentToString(BigDecimal decimal) {
        if (decimal == null) {
            return null;
        }

        if (toPlainStringMethod != null) {
            try {
                return (String) toPlainStringMethod.invoke(decimal, (Object[]) null);
            } catch (InvocationTargetException invokeEx) {
                // that's okay, we fall-through to decimal.toString()
            } catch (IllegalAccessException accessEx) {
                // that's okay, we fall-through to decimal.toString()
            }
        }

        return decimal.toString();
    }

    /**
     * Dumps the given bytes to STDOUT as a hex dump (up to length bytes).
     * 
     * @param byteBuffer
     *            the data to print as hex
     * @param length
     *            the number of bytes to print
     * 
     * @return ...
     */
    public static String dumpAsHex(byte[] byteBuffer, int length) {
        StringBuilder outputBuilder = new StringBuilder(length * 4);

        int p = 0;
        int rows = length / 8;

        for (int i = 0; (i < rows) && (p < length); i++) {
            int ptemp = p;

            for (int j = 0; j < 8; j++) {
                String hexVal = Integer.toHexString(byteBuffer[ptemp] & 0xff);

                if (hexVal.length() == 1) {
                    hexVal = "0" + hexVal;
                }

                outputBuilder.append(hexVal + " ");
                ptemp++;
            }

            outputBuilder.append("    ");

            for (int j = 0; j < 8; j++) {
                int b = 0xff & byteBuffer[p];

                if (b > 32 && b < 127) {
                    outputBuilder.append((char) b + " ");
                } else {
                    outputBuilder.append(". ");
                }

                p++;
            }

            outputBuilder.append("\n");
        }

        int n = 0;

        for (int i = p; i < length; i++) {
            String hexVal = Integer.toHexString(byteBuffer[i] & 0xff);

            if (hexVal.length() == 1) {
                hexVal = "0" + hexVal;
            }

            outputBuilder.append(hexVal + " ");
            n++;
        }

        for (int i = n; i < 8; i++) {
            outputBuilder.append("   ");
        }

        outputBuilder.append("    ");

        for (int i = p; i < length; i++) {
            int b = 0xff & byteBuffer[i];

            if (b > 32 && b < 127) {
                outputBuilder.append((char) b + " ");
            } else {
                outputBuilder.append(". ");
            }
        }

        outputBuilder.append("\n");

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
     * Unfortunately, SJIS has 0x5c as a high byte in some of its double-byte
     * characters, so we need to escape it.
     * 
     * @param origBytes
     *            the original bytes in SJIS format
     * @param origString
     *            the string that had .getBytes() called on it
     * 
     * @return byte[] with 0x5c escaped
     */
    public static byte[] escapeEasternUnicodeByteStream(byte[] origBytes, String origString) {
        if (origBytes == null) {
            return null;
        }
        if (origBytes.length == 0) {
            return new byte[0];
        }

        int bytesLen = origBytes.length;
        int bufIndex = 0;
        int strIndex = 0;

        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream(bytesLen);

        while (true) {
            if (origString.charAt(strIndex) == '\\') {
                // write it out as-is
                bytesOut.write(origBytes[bufIndex++]);

                // bytesOut.write(origBytes[bufIndex++]);
            } else {
                // Grab the first byte
                int loByte = origBytes[bufIndex];

                if (loByte < 0) {
                    loByte += 256; // adjust for signedness/wrap-around
                }

                // We always write the first byte
                bytesOut.write(loByte);

                //
                // The codepage characters in question exist between
                // 0x81-0x9F and 0xE0-0xFC...
                //
                // See:
                //
                // http://www.microsoft.com/GLOBALDEV/Reference/dbcs/932.htm
                //
                // Problematic characters in GBK
                //
                // U+905C : CJK UNIFIED IDEOGRAPH
                //
                // Problematic characters in Big5
                //
                // B9F0 = U+5C62 : CJK UNIFIED IDEOGRAPH
                //
                if (loByte >= 0x80) {
                    if (bufIndex < (bytesLen - 1)) {
                        int hiByte = origBytes[bufIndex + 1];

                        if (hiByte < 0) {
                            hiByte += 256; // adjust for signedness/wrap-around
                        }

                        // write the high byte here, and increment the index for the high byte
                        bytesOut.write(hiByte);
                        bufIndex++;

                        // escape 0x5c if necessary
                        if (hiByte == 0x5C) {
                            bytesOut.write(hiByte);
                        }
                    }
                } else if (loByte == 0x5c) {
                    if (bufIndex < (bytesLen - 1)) {
                        int hiByte = origBytes[bufIndex + 1];

                        if (hiByte < 0) {
                            hiByte += 256; // adjust for signedness/wrap-around
                        }

                        if (hiByte == 0x62) {
                            // we need to escape the 0x5c
                            bytesOut.write(0x5c);
                            bytesOut.write(0x62);
                            bufIndex++;
                        }
                    }
                }

                bufIndex++;
            }

            if (bufIndex >= bytesLen) {
                // we're done
                break;
            }

            strIndex++;
        }

        return bytesOut.toByteArray();
    }

    /**
     * Returns the first non whitespace char, converted to upper case
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
            if (dString.length() > (ePos + 1)) {
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
     * Returns the byte[] representation of the given char[] (re)using the given charset converter, and the given
     * encoding.
     */
    public static byte[] getBytes(char[] c, SingleByteCharsetConverter converter, String encoding, String serverEncoding, boolean parserKnowsUnicode,
            ExceptionInterceptor exceptionInterceptor) throws SQLException {
        try {
            byte[] b;

            if (converter != null) {
                b = converter.toBytes(c);
            } else if (encoding == null) {
                b = getBytes(c);
            } else {
                b = getBytes(c, encoding);

                if (!parserKnowsUnicode && CharsetMapping.requiresEscapeEasternUnicode(encoding)) {

                    if (!encoding.equalsIgnoreCase(serverEncoding)) {
                        b = escapeEasternUnicodeByteStream(b, new String(c));
                    }
                }
            }

            return b;
        } catch (UnsupportedEncodingException uee) {
            throw SQLError.createSQLException(Messages.getString("StringUtils.0") + encoding + Messages.getString("StringUtils.1"),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
        }
    }

    /**
     * Returns the byte[] representation of subset of the given char[] (re)using the given charset converter, and the
     * given encoding.
     */
    public static byte[] getBytes(char[] c, SingleByteCharsetConverter converter, String encoding, String serverEncoding, int offset, int length,
            boolean parserKnowsUnicode, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        try {
            byte[] b;

            if (converter != null) {
                b = converter.toBytes(c, offset, length);
            } else if (encoding == null) {
                b = getBytes(c, offset, length);
            } else {
                b = getBytes(c, offset, length, encoding);

                if (!parserKnowsUnicode && CharsetMapping.requiresEscapeEasternUnicode(encoding)) {

                    if (!encoding.equalsIgnoreCase(serverEncoding)) {
                        b = escapeEasternUnicodeByteStream(b, new String(c, offset, length));
                    }
                }
            }

            return b;
        } catch (UnsupportedEncodingException uee) {
            throw SQLError.createSQLException(Messages.getString("StringUtils.0") + encoding + Messages.getString("StringUtils.1"),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
        }
    }

    /**
     * Returns the byte[] representation of the given char[] (re)using a cached charset converter, and the given
     * encoding.
     */
    public static byte[] getBytes(char[] c, String encoding, String serverEncoding, boolean parserKnowsUnicode, MySQLConnection conn,
            ExceptionInterceptor exceptionInterceptor) throws SQLException {
        try {
            SingleByteCharsetConverter converter = conn != null ? conn.getCharsetConverter(encoding) : SingleByteCharsetConverter.getInstance(encoding, null);

            return getBytes(c, converter, encoding, serverEncoding, parserKnowsUnicode, exceptionInterceptor);
        } catch (UnsupportedEncodingException uee) {
            throw SQLError.createSQLException(Messages.getString("StringUtils.0") + encoding + Messages.getString("StringUtils.1"),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
        }
    }

    /**
     * Returns the byte[] representation of the given string (re)using the given charset converter, and the given
     * encoding.
     */
    public static byte[] getBytes(String s, SingleByteCharsetConverter converter, String encoding, String serverEncoding, boolean parserKnowsUnicode,
            ExceptionInterceptor exceptionInterceptor) throws SQLException {
        try {
            byte[] b;

            if (converter != null) {
                b = converter.toBytes(s);
            } else if (encoding == null) {
                b = getBytes(s);
            } else {
                b = getBytes(s, encoding);

                if (!parserKnowsUnicode && CharsetMapping.requiresEscapeEasternUnicode(encoding)) {

                    if (!encoding.equalsIgnoreCase(serverEncoding)) {
                        b = escapeEasternUnicodeByteStream(b, s);
                    }
                }
            }

            return b;
        } catch (UnsupportedEncodingException uee) {
            throw SQLError.createSQLException(Messages.getString("StringUtils.5") + encoding + Messages.getString("StringUtils.6"),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
        }
    }

    /**
     * Returns the byte[] representation of a substring of the given string (re)using the given charset converter, and
     * the given encoding.
     */
    public static byte[] getBytes(String s, SingleByteCharsetConverter converter, String encoding, String serverEncoding, int offset, int length,
            boolean parserKnowsUnicode, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        try {
            byte[] b;

            if (converter != null) {
                b = converter.toBytes(s, offset, length);
            } else if (encoding == null) {
                b = getBytes(s, offset, length);
            } else {
                s = s.substring(offset, offset + length);
                b = getBytes(s, encoding);

                if (!parserKnowsUnicode && CharsetMapping.requiresEscapeEasternUnicode(encoding)) {

                    if (!encoding.equalsIgnoreCase(serverEncoding)) {
                        b = escapeEasternUnicodeByteStream(b, s);
                    }
                }
            }

            return b;
        } catch (UnsupportedEncodingException uee) {
            throw SQLError.createSQLException(Messages.getString("StringUtils.5") + encoding + Messages.getString("StringUtils.6"),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
        }
    }

    /**
     * Returns the byte[] representation of the given string (re)using a cached charset converter, and the given
     * encoding.
     */
    public static byte[] getBytes(String s, String encoding, String serverEncoding, boolean parserKnowsUnicode, MySQLConnection conn,
            ExceptionInterceptor exceptionInterceptor) throws SQLException {
        try {
            SingleByteCharsetConverter converter = conn != null ? conn.getCharsetConverter(encoding) : SingleByteCharsetConverter.getInstance(encoding, null);

            return getBytes(s, converter, encoding, serverEncoding, parserKnowsUnicode, exceptionInterceptor);
        } catch (UnsupportedEncodingException uee) {
            throw SQLError.createSQLException(Messages.getString("StringUtils.5") + encoding + Messages.getString("StringUtils.6"),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
        }
    }

    /**
     * Returns the byte[] representation of a substring of the given string (re)using a cached charset converter, and
     * the given encoding.
     */
    public static final byte[] getBytes(String s, String encoding, String serverEncoding, int offset, int length, boolean parserKnowsUnicode,
            MySQLConnection conn, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        try {
            SingleByteCharsetConverter converter = conn != null ? conn.getCharsetConverter(encoding) : SingleByteCharsetConverter.getInstance(encoding, null);

            return getBytes(s, converter, encoding, serverEncoding, offset, length, parserKnowsUnicode, exceptionInterceptor);
        } catch (UnsupportedEncodingException uee) {
            throw SQLError.createSQLException(Messages.getString("StringUtils.5") + encoding + Messages.getString("StringUtils.6"),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
        }
    }

    /**
     * Returns the byte[] representation of the given string properly wrapped between the given char delimiters,
     * (re)using the given charset converter, and the given encoding.
     */
    public static byte[] getBytesWrapped(String s, char beginWrap, char endWrap, SingleByteCharsetConverter converter, String encoding, String serverEncoding,
            boolean parserKnowsUnicode, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        try {
            byte[] b;

            if (converter != null) {
                b = converter.toBytesWrapped(s, beginWrap, endWrap);
            } else if (encoding == null) {
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

                if (!parserKnowsUnicode && CharsetMapping.requiresEscapeEasternUnicode(encoding)) {

                    if (!encoding.equalsIgnoreCase(serverEncoding)) {
                        b = escapeEasternUnicodeByteStream(b, s);
                    }
                }
            }

            return b;
        } catch (UnsupportedEncodingException uee) {
            throw SQLError.createSQLException(Messages.getString("StringUtils.10") + encoding + Messages.getString("StringUtils.11"),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
        }
    }

    public static int getInt(byte[] buf) throws NumberFormatException {
        return getInt(buf, 0, buf.length);
    }

    public static int getInt(byte[] buf, int offset, int endPos) throws NumberFormatException {
        int base = 10;

        int s = offset;

        /* Skip white space. */
        while (s < endPos && Character.isWhitespace((char) buf[s])) {
            ++s;
        }

        if (s == endPos) {
            throw new NumberFormatException(StringUtils.toString(buf));
        }

        /* Check for a sign. */
        boolean negative = false;

        if ((char) buf[s] == '-') {
            negative = true;
            ++s;
        } else if ((char) buf[s] == '+') {
            ++s;
        }

        /* Save the pointer so we can check later if anything happened. */
        int save = s;

        int cutoff = Integer.MAX_VALUE / base;
        int cutlim = (Integer.MAX_VALUE % base);

        if (negative) {
            cutlim++;
        }

        boolean overflow = false;

        int i = 0;

        for (; s < endPos; s++) {
            char c = (char) buf[s];

            if (Character.isDigit(c)) {
                c -= '0';
            } else if (Character.isLetter(c)) {
                c = (char) (Character.toUpperCase(c) - 'A' + 10);
            } else {
                break;
            }

            if (c >= base) {
                break;
            }

            /* Check for overflow. */
            if ((i > cutoff) || ((i == cutoff) && (c > cutlim))) {
                overflow = true;
            } else {
                i *= base;
                i += c;
            }
        }

        if (s == save) {
            throw new NumberFormatException(StringUtils.toString(buf));
        }

        if (overflow) {
            throw new NumberFormatException(StringUtils.toString(buf));
        }

        /* Return the result of the appropriate sign. */
        return (negative ? (-i) : i);
    }

    public static long getLong(byte[] buf) throws NumberFormatException {
        return getLong(buf, 0, buf.length);
    }

    public static long getLong(byte[] buf, int offset, int endpos) throws NumberFormatException {
        int base = 10;

        int s = offset;

        /* Skip white space. */
        while (s < endpos && Character.isWhitespace((char) buf[s])) {
            ++s;
        }

        if (s == endpos) {
            throw new NumberFormatException(StringUtils.toString(buf));
        }

        /* Check for a sign. */
        boolean negative = false;

        if ((char) buf[s] == '-') {
            negative = true;
            ++s;
        } else if ((char) buf[s] == '+') {
            ++s;
        }

        /* Save the pointer so we can check later if anything happened. */
        int save = s;

        long cutoff = Long.MAX_VALUE / base;
        long cutlim = (int) (Long.MAX_VALUE % base);

        if (negative) {
            cutlim++;
        }

        boolean overflow = false;
        long i = 0;

        for (; s < endpos; s++) {
            char c = (char) buf[s];

            if (Character.isDigit(c)) {
                c -= '0';
            } else if (Character.isLetter(c)) {
                c = (char) (Character.toUpperCase(c) - 'A' + 10);
            } else {
                break;
            }

            if (c >= base) {
                break;
            }

            /* Check for overflow. */
            if ((i > cutoff) || ((i == cutoff) && (c > cutlim))) {
                overflow = true;
            } else {
                i *= base;
                i += c;
            }
        }

        if (s == save) {
            throw new NumberFormatException(StringUtils.toString(buf));
        }

        if (overflow) {
            throw new NumberFormatException(StringUtils.toString(buf));
        }

        /* Return the result of the appropriate sign. */
        return (negative ? (-i) : i);
    }

    public static short getShort(byte[] buf) throws NumberFormatException {
        return getShort(buf, 0, buf.length);
    }

    public static short getShort(byte[] buf, int offset, int endpos) throws NumberFormatException {
        short base = 10;

        int s = offset;

        /* Skip white space. */
        while (s < endpos && Character.isWhitespace((char) buf[s])) {
            ++s;
        }

        if (s == endpos) {
            throw new NumberFormatException(StringUtils.toString(buf));
        }

        /* Check for a sign. */
        boolean negative = false;

        if ((char) buf[s] == '-') {
            negative = true;
            ++s;
        } else if ((char) buf[s] == '+') {
            ++s;
        }

        /* Save the pointer so we can check later if anything happened. */
        int save = s;

        short cutoff = (short) (Short.MAX_VALUE / base);
        short cutlim = (short) (Short.MAX_VALUE % base);

        if (negative) {
            cutlim++;
        }

        boolean overflow = false;
        short i = 0;

        for (; s < endpos; s++) {
            char c = (char) buf[s];

            if (Character.isDigit(c)) {
                c -= '0';
            } else if (Character.isLetter(c)) {
                c = (char) (Character.toUpperCase(c) - 'A' + 10);
            } else {
                break;
            }

            if (c >= base) {
                break;
            }

            /* Check for overflow. */
            if ((i > cutoff) || ((i == cutoff) && (c > cutlim))) {
                overflow = true;
            } else {
                i *= base;
                i += c;
            }
        }

        if (s == save) {
            throw new NumberFormatException(StringUtils.toString(buf));
        }

        if (overflow) {
            throw new NumberFormatException(StringUtils.toString(buf));
        }

        /* Return the result of the appropriate sign. */
        return (negative ? (short) -i : (short) i);
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
        if ((searchIn == null) || (searchFor == null)) {
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
                while (++i <= stopSearchingAt && (isCharAtPosNotEqualIgnoreCase(searchIn, i, firstCharOfSearchForUc, firstCharOfSearchForLc))) {
                }
            }

            if (i <= stopSearchingAt && startsWithIgnoreCase(searchIn, i, searchFor)) {
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
     * @param searchFor
     *            the array of strings to search for
     * @param openingMarkers
     *            characters which delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters which delimit the end of a text block to skip
     * @param searchMode
     *            a <code>Set</code>, ideally an <code>EnumSet</code>, containing the flags from the enum <code>StringUtils.SearchMode</code> that determine the
     *            behavior of the search
     * @return the position where <code>searchFor</code> is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    public static int indexOfIgnoreCase(int startingPosition, String searchIn, String[] searchForSequence, String openingMarkers, String closingMarkers,
            Set<SearchMode> searchMode) {
        if ((searchIn == null) || (searchForSequence == null)) {
            return -1;
        }

        int searchInLength = searchIn.length();
        int searchForLength = 0;
        for (String searchForPart : searchForSequence) {
            searchForLength += searchForPart.length();
        } // minimum length for searchFor (without gaps between words)

        if (searchForLength == 0) {
            return -1;
        }

        int searchForWordsCount = searchForSequence.length;
        searchForLength += searchForWordsCount > 0 ? searchForWordsCount - 1 : 0; // add gaps between words
        int stopSearchingAt = searchInLength - searchForLength;

        if (startingPosition > stopSearchingAt) {
            return -1;
        }

        if (searchMode.contains(SearchMode.SKIP_BETWEEN_MARKERS)
                && (openingMarkers == null || closingMarkers == null || openingMarkers.length() != closingMarkers.length())) {
            throw new IllegalArgumentException(Messages.getString("StringUtils.15", new String[] { openingMarkers, closingMarkers }));
        }

        if (Character.isWhitespace(searchForSequence[0].charAt(0)) && searchMode.contains(SearchMode.SKIP_WHITE_SPACE)) {
            // Can't skip white spaces if first searchFor char is one
            searchMode = EnumSet.copyOf(searchMode);
            searchMode.remove(SearchMode.SKIP_WHITE_SPACE);
        }

        // searchMode set used to search 2nd and following words can't contain SearchMode.SKIP_BETWEEN_MARKERS and must
        // contain SearchMode.SKIP_WHITE_SPACE
        Set<SearchMode> searchMode2 = EnumSet.of(SearchMode.SKIP_WHITE_SPACE);
        searchMode2.addAll(searchMode);
        searchMode2.remove(SearchMode.SKIP_BETWEEN_MARKERS);

        for (int positionOfFirstWord = startingPosition; positionOfFirstWord <= stopSearchingAt; positionOfFirstWord++) {
            positionOfFirstWord = indexOfIgnoreCase(positionOfFirstWord, searchIn, searchForSequence[0], openingMarkers, closingMarkers, searchMode);

            if (positionOfFirstWord == -1 || positionOfFirstWord > stopSearchingAt) {
                return -1;
            }

            int startingPositionForNextWord = positionOfFirstWord + searchForSequence[0].length();
            int wc = 0;
            boolean match = true;
            while (++wc < searchForWordsCount && match) {
                int positionOfNextWord = indexOfNextChar(startingPositionForNextWord, searchInLength - 1, searchIn, null, null, null, searchMode2);
                if (startingPositionForNextWord == positionOfNextWord || !startsWithIgnoreCase(searchIn, positionOfNextWord, searchForSequence[wc])) {
                    // either no gap between words or match failed
                    match = false;
                } else {
                    startingPositionForNextWord = positionOfNextWord + searchForSequence[wc].length();
                }
            }

            if (match) {
                return positionOfFirstWord;
            }
        }

        return -1;
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
     *            characters which delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters which delimit the end of a text block to skip
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
     *            characters which delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters which delimit the end of a text block to skip
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
        if (searchIn == null || searchFor == null) {
            return -1;
        }

        int searchInLength = searchIn.length();
        int searchForLength = searchFor.length();
        int stopSearchingAt = searchInLength - searchForLength;

        if (startingPosition > stopSearchingAt || searchForLength == 0) {
            return -1;
        }

        if (searchMode.contains(SearchMode.SKIP_BETWEEN_MARKERS)) {
            if (openingMarkers == null || closingMarkers == null || openingMarkers.length() != closingMarkers.length()) {
                throw new IllegalArgumentException(Messages.getString("StringUtils.15", new String[] { openingMarkers, closingMarkers }));
            }
            if (overridingMarkers == null) {
                throw new IllegalArgumentException(Messages.getString("StringUtils.16", new String[] { overridingMarkers, openingMarkers }));
            }
            for (char c : overridingMarkers.toCharArray()) {
                if (openingMarkers.indexOf(c) == -1) {
                    throw new IllegalArgumentException(Messages.getString("StringUtils.16", new String[] { overridingMarkers, openingMarkers }));
                }
            }
        }

        // Some locales don't follow upper-case rule, so need to check both
        char firstCharOfSearchForUc = Character.toUpperCase(searchFor.charAt(0));
        char firstCharOfSearchForLc = Character.toLowerCase(searchFor.charAt(0));

        if (Character.isWhitespace(firstCharOfSearchForLc) && searchMode.contains(SearchMode.SKIP_WHITE_SPACE)) {
            // Can't skip white spaces if first searchFor char is one
            searchMode = EnumSet.copyOf(searchMode);
            searchMode.remove(SearchMode.SKIP_WHITE_SPACE);
        }

        for (int i = startingPosition; i <= stopSearchingAt; i++) {
            i = indexOfNextChar(i, stopSearchingAt, searchIn, openingMarkers, closingMarkers, overridingMarkers, searchMode);

            if (i == -1) {
                return -1;
            }

            char c = searchIn.charAt(i);

            if (isCharEqualIgnoreCase(c, firstCharOfSearchForUc, firstCharOfSearchForLc) && startsWithIgnoreCase(searchIn, i, searchFor)) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Finds the position the next character from a string, possibly skipping white space, comments and text between markers.
     * 
     * @param startingPosition
     *            the position to start the search from
     * @param stopPosition
     *            the position where to stop the search (inclusive)
     * @param searchIn
     *            the string to search in
     * @param openingMarkers
     *            characters which delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters which delimit the end of a text block to skip
     * @param searchMode
     *            a <code>Set</code>, ideally an <code>EnumSet</code>, containing the flags from the enum <code>StringUtils.SearchMode</code> that determine the
     *            behavior of the search
     * @return the position where <code>searchFor</code> is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    private static int indexOfNextChar(int startingPosition, int stopPosition, String searchIn, String openingMarkers, String closingMarkers,
            String overridingMarkers, Set<SearchMode> searchMode) {
        if (searchIn == null) {
            return -1;
        }

        int searchInLength = searchIn.length();

        if (startingPosition >= searchInLength) {
            return -1;
        }

        char c0 = Character.MIN_VALUE; // current char
        char c1 = searchIn.charAt(startingPosition); // lookahead(1)
        char c2 = startingPosition + 1 < searchInLength ? searchIn.charAt(startingPosition + 1) : Character.MIN_VALUE; // lookahead(2)

        for (int i = startingPosition; i <= stopPosition; i++) {
            c0 = c1;
            c1 = c2;
            c2 = i + 2 < searchInLength ? searchIn.charAt(i + 2) : Character.MIN_VALUE;

            boolean dashDashCommentImmediateEnd = false;
            int markerIndex = -1;

            if (searchMode.contains(SearchMode.ALLOW_BACKSLASH_ESCAPE) && c0 == '\\') {
                i++; // next char is escaped, skip it
                // reset lookahead
                c1 = c2;
                c2 = i + 2 < searchInLength ? searchIn.charAt(i + 2) : Character.MIN_VALUE;

            } else if (searchMode.contains(SearchMode.SKIP_BETWEEN_MARKERS) && (markerIndex = openingMarkers.indexOf(c0)) != -1) {
                // marker found, skip until closing, while being aware of nested markers if opening and closing markers are distinct
                int nestedMarkersCount = 0;
                char openingMarker = c0;
                char closingMarker = closingMarkers.charAt(markerIndex);
                boolean outerIsAnOverridingMarker = overridingMarkers.indexOf(openingMarker) != -1;
                while (++i <= stopPosition && ((c0 = searchIn.charAt(i)) != closingMarker || nestedMarkersCount != 0)) {
                    if (!outerIsAnOverridingMarker && overridingMarkers.indexOf(c0) != -1) {
                        // there is an overriding marker that needs to be consumed before returning to the previous marker
                        int overridingMarkerIndex = openingMarkers.indexOf(c0); // overridingMarkers must be a sub-list of openingMarkers
                        int overridingNestedMarkersCount = 0;
                        char overridingOpeningMarker = c0;
                        char overridingClosingMarker = closingMarkers.charAt(overridingMarkerIndex);
                        while (++i <= stopPosition && ((c0 = searchIn.charAt(i)) != overridingClosingMarker || overridingNestedMarkersCount != 0)) {
                            // do as before, but this marker can't be overridden
                            if (c0 == overridingOpeningMarker) {
                                overridingNestedMarkersCount++;
                            } else if (c0 == overridingClosingMarker) {
                                overridingNestedMarkersCount--;
                            } else if (searchMode.contains(SearchMode.ALLOW_BACKSLASH_ESCAPE) && c0 == '\\') {
                                i++; // next char is escaped, skip it
                            }
                        }
                    } else if (c0 == openingMarker) {
                        nestedMarkersCount++;
                    } else if (c0 == closingMarker) {
                        nestedMarkersCount--;
                    } else if (searchMode.contains(SearchMode.ALLOW_BACKSLASH_ESCAPE) && c0 == '\\') {
                        i++; // next char is escaped, skip it
                    }
                }
                // reset lookahead
                c1 = i + 1 < searchInLength ? searchIn.charAt(i + 1) : Character.MIN_VALUE;
                c2 = i + 2 < searchInLength ? searchIn.charAt(i + 2) : Character.MIN_VALUE;

            } else if (searchMode.contains(SearchMode.SKIP_BLOCK_COMMENTS) && c0 == '/' && c1 == '*') {
                if (c2 != '!') {
                    // comments block found, skip until end of block ("*/") (backslash escape doesn't work on comments)
                    i++; // move to next char ('*')
                    while (++i <= stopPosition
                            && (searchIn.charAt(i) != '*' || (i + 1 < searchInLength ? searchIn.charAt(i + 1) : Character.MIN_VALUE) != '/')) {
                        // continue
                    }
                    i++; // move to next char ('/')

                } else {
                    // special non-comments block found, move to end of opening marker ("/*![12345]")
                    i++; // move to next char ('*')
                    i++; // move to next char ('!')
                    // check if a 5 digits MySQL version reference follows, if so skip them
                    int j = 1;
                    for (; j <= NON_COMMENTS_MYSQL_VERSION_REF_LENGTH; j++) {
                        if (i + j >= searchInLength || !Character.isDigit(searchIn.charAt(i + j))) {
                            break;
                        }
                    }
                    if (j == NON_COMMENTS_MYSQL_VERSION_REF_LENGTH) {
                        i += NON_COMMENTS_MYSQL_VERSION_REF_LENGTH;
                    }
                }
                // reset lookahead
                c1 = i + 1 < searchInLength ? searchIn.charAt(i + 1) : Character.MIN_VALUE;
                c2 = i + 2 < searchInLength ? searchIn.charAt(i + 2) : Character.MIN_VALUE;

            } else if (searchMode.contains(SearchMode.SKIP_BLOCK_COMMENTS) && c0 == '*' && c1 == '/') {
                // special non-comments block closing marker ("*/") found - assume that if we get it here it's because it
                // belongs to a non-comments block ("/*!"), otherwise the query should be misspelled as nesting comments isn't allowed.
                i++; // move to next char ('/')
                // reset lookahead
                c1 = c2;
                c2 = i + 2 < searchInLength ? searchIn.charAt(i + 2) : Character.MIN_VALUE;

            } else if (searchMode.contains(SearchMode.SKIP_LINE_COMMENTS)
                    && ((c0 == '-' && c1 == '-' && (Character.isWhitespace(c2) || (dashDashCommentImmediateEnd = c2 == ';') || c2 == Character.MIN_VALUE))
                            || c0 == '#')) {
                if (dashDashCommentImmediateEnd) {
                    // comments line found but closed immediately by query delimiter marker
                    i++; // move to next char ('-')
                    i++; // move to next char (';')
                    // reset lookahead
                    c1 = i + 1 < searchInLength ? searchIn.charAt(i + 1) : Character.MIN_VALUE;
                    c2 = i + 2 < searchInLength ? searchIn.charAt(i + 2) : Character.MIN_VALUE;
                } else {
                    // comments line found, skip until eol (backslash escape doesn't work on comments)
                    while (++i <= stopPosition && (c0 = searchIn.charAt(i)) != '\n' && c0 != '\r') {
                        // continue
                    }
                    // reset lookahead
                    c1 = i + 1 < searchInLength ? searchIn.charAt(i + 1) : Character.MIN_VALUE;
                    if (c0 == '\r' && c1 == '\n') {
                        // \r\n sequence found
                        i++; // skip next char ('\n')
                        c1 = i + 1 < searchInLength ? searchIn.charAt(i + 1) : Character.MIN_VALUE;
                    }
                    c2 = i + 2 < searchInLength ? searchIn.charAt(i + 2) : Character.MIN_VALUE;
                }

            } else if (!searchMode.contains(SearchMode.SKIP_WHITE_SPACE) || !Character.isWhitespace(c0)) {
                return i;
            }
        }

        return -1;
    }

    private static boolean isCharAtPosNotEqualIgnoreCase(String searchIn, int pos, char firstCharOfSearchForUc, char firstCharOfSearchForLc) {
        return Character.toLowerCase(searchIn.charAt(pos)) != firstCharOfSearchForLc && Character.toUpperCase(searchIn.charAt(pos)) != firstCharOfSearchForUc;
    }

    private static boolean isCharEqualIgnoreCase(char charToCompare, char compareToCharUC, char compareToCharLC) {
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
     */
    public static List<String> split(String stringToSplit, String delimiter, boolean trim) {
        if (stringToSplit == null) {
            return new ArrayList<String>();
        }

        if (delimiter == null) {
            throw new IllegalArgumentException();
        }

        StringTokenizer tokenizer = new StringTokenizer(stringToSplit, delimiter, false);

        List<String> splitTokens = new ArrayList<String>(tokenizer.countTokens());

        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();

            if (trim) {
                token = token.trim();
            }

            splitTokens.add(token);
        }

        return splitTokens;
    }

    /**
     * Splits stringToSplit into a list, using the given delimiter and skipping all between the given markers.
     * 
     * @param stringToSplit
     *            the string to split
     * @param delimiter
     *            the string to split on
     * @param openingMarkers
     *            characters which delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters which delimit the end of a text block to skip
     * @param trim
     *            should the split strings be whitespace trimmed?
     * 
     * @return the list of strings, split by delimiter
     * 
     * @throws IllegalArgumentException
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
     *            characters which delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters which delimit the end of a text block to skip
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
     */
    public static List<String> split(String stringToSplit, String delimiter, String openingMarkers, String closingMarkers, String overridingMarkers,
            boolean trim) {
        if (stringToSplit == null) {
            return new ArrayList<String>();
        }

        if (delimiter == null) {
            throw new IllegalArgumentException();
        }

        int delimPos = 0;
        int currentPos = 0;

        List<String> splitTokens = new ArrayList<String>();

        while ((delimPos = indexOfIgnoreCase(currentPos, stringToSplit, delimiter, openingMarkers, closingMarkers, overridingMarkers,
                SEARCH_MODE__MRK_COM_WS)) != -1) {
            String token = stringToSplit.substring(currentPos, delimPos);

            if (trim) {
                token = token.trim();
            }

            splitTokens.add(token);
            currentPos = delimPos + 1;
        }

        if (currentPos < stringToSplit.length()) {
            String token = stringToSplit.substring(currentPos);

            if (trim) {
                token = token.trim();
            }

            splitTokens.add(token);
        }

        return splitTokens;
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
     * Determines whether or not the string 'searchIn' contains the string
     * 'searchFor', dis-regarding case starting at 'startAt' Shorthand for a
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
    public static boolean startsWithIgnoreCase(String searchIn, int startAt, String searchFor) {
        return searchIn.regionMatches(true, startAt, searchFor, 0, searchFor.length());
    }

    /**
     * Determines whether or not the string 'searchIn' contains the string
     * 'searchFor', dis-regarding case. Shorthand for a String.regionMatch(...)
     * 
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for
     * 
     * @return whether searchIn starts with searchFor, ignoring case
     */
    public static boolean startsWithIgnoreCase(String searchIn, String searchFor) {
        return startsWithIgnoreCase(searchIn, 0, searchFor);
    }

    /**
     * Determines whether or not the string 'searchIn' contains the string
     * 'searchFor', disregarding case,leading whitespace and non-alphanumeric
     * characters.
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

        return startsWithIgnoreCase(searchIn, beginPos, searchFor);
    }

    /**
     * Determines whether or not the string 'searchIn' contains the string
     * 'searchFor', disregarding case and leading whitespace
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
     * Determines whether or not the string 'searchIn' contains the string
     * 'searchFor', disregarding case and leading whitespace
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

        int inLength = searchIn.length();

        for (; beginPos < inLength; beginPos++) {
            if (!Character.isWhitespace(searchIn.charAt(beginPos))) {
                break;
            }
        }

        return startsWithIgnoreCase(searchIn, beginPos, searchFor);
    }

    /**
     * Determines whether or not the string 'searchIn' starts with one of the strings in 'searchFor', disregarding case
     * and leading whitespace
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
     * @param bytesToStrip
     * @param prefix
     * @param suffix
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
        char[] charArray = new char[length];
        int readpoint = startPos;

        for (int i = 0; i < length; i++) {
            charArray[i] = (char) buffer[readpoint];
            readpoint++;
        }

        return new String(charArray);
    }

    /**
     * Compares searchIn against searchForWildcard with wildcards, in a case insensitive manner.
     * 
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for, using the 'standard' SQL wildcard chars of '%' and '_'
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
        if ((searchIn == null) || (searchFor == null)) {
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
            while ((searchFor.charAt(searchForPos) != WILDCARD_MANY) && (searchFor.charAt(searchForPos) != WILDCARD_ONE)) {
                if ((searchFor.charAt(searchForPos) == WILDCARD_ESCAPE) && ((searchForPos + 1) != searchForEnd)) {
                    searchForPos++;
                }

                if ((searchInPos == searchInEnd)
                        || (Character.toUpperCase(searchFor.charAt(searchForPos++)) != Character.toUpperCase(searchIn.charAt(searchInPos++)))) {
                    return WILD_COMPARE_CONTINUE_WITH_WILD; /* No match */
                }

                if (searchForPos == searchForEnd) {
                    return ((searchInPos != searchInEnd) ? WILD_COMPARE_CONTINUE_WITH_WILD : WILD_COMPARE_MATCH); /* Match if both are at end */
                }

                result = WILD_COMPARE_CONTINUE_WITH_WILD; /* Found an anchor char */
            }

            if (searchFor.charAt(searchForPos) == WILDCARD_ONE) {
                do {
                    if (searchInPos == searchInEnd) { /* Skip one char if possible */
                        return result;
                    }
                    searchInPos++;
                } while ((++searchForPos < searchForEnd) && (searchFor.charAt(searchForPos) == WILDCARD_ONE));

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
                if (((cmp = searchFor.charAt(searchForPos)) == WILDCARD_ESCAPE) && ((searchForPos + 1) != searchForEnd)) {
                    cmp = searchFor.charAt(++searchForPos);
                }

                searchForPos++;

                do {
                    while ((searchInPos != searchInEnd) && (Character.toUpperCase(searchIn.charAt(searchInPos)) != Character.toUpperCase(cmp))) {
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

        return ((searchInPos != searchInEnd) ? WILD_COMPARE_CONTINUE_WITH_WILD : WILD_COMPARE_MATCH);
    }

    static byte[] s2b(String s, MySQLConnection conn) throws SQLException {
        if (s == null) {
            return null;
        }

        if ((conn != null) && conn.getUseUnicode()) {
            try {
                String encoding = conn.getEncoding();

                if (encoding == null) {
                    return s.getBytes();
                }

                SingleByteCharsetConverter converter = conn.getCharsetConverter(encoding);

                if (converter != null) {
                    return converter.toBytes(s);
                }

                return s.getBytes(encoding);
            } catch (java.io.UnsupportedEncodingException E) {
                return s.getBytes();
            }
        }

        return s.getBytes();
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

    public static boolean isNullOrEmpty(String toTest) {
        return (toTest == null || toTest.length() == 0);
    }

    /**
     * Returns the given string, with comments removed
     * 
     * @param src
     *            the source string
     * @param stringOpens
     *            characters which delimit the "open" of a string
     * @param stringCloses
     *            characters which delimit the "close" of a string, in
     *            counterpart order to <code>stringOpens</code>
     * @param slashStarComments
     *            strip slash-star type "C" style comments
     * @param slashSlashComments
     *            strip slash-slash C++ style comments to end-of-line
     * @param hashComments
     *            strip #-style comments to end-of-line
     * @param dashDashComments
     *            strip "--" style comments to end-of-line
     * @return the input string with all comment-delimited data removed
     */
    public static String stripComments(String src, String stringOpens, String stringCloses, boolean slashStarComments, boolean slashSlashComments,
            boolean hashComments, boolean dashDashComments) {
        if (src == null) {
            return null;
        }

        StringBuilder strBuilder = new StringBuilder(src.length());

        // It's just more natural to deal with this as a stream when parsing..This code is currently only called when parsing the kind of metadata that
        // developers are strongly recommended to cache anyways, so we're not worried about the _1_ extra object allocation if it cleans up the code

        StringReader sourceReader = new StringReader(src);

        int contextMarker = Character.MIN_VALUE;
        boolean escaped = false;
        int markerTypeFound = -1;

        int ind = 0;

        int currentChar = 0;

        try {
            while ((currentChar = sourceReader.read()) != -1) {

                if (markerTypeFound != -1 && currentChar == stringCloses.charAt(markerTypeFound) && !escaped) {
                    contextMarker = Character.MIN_VALUE;
                    markerTypeFound = -1;
                } else if ((ind = stringOpens.indexOf(currentChar)) != -1 && !escaped && contextMarker == Character.MIN_VALUE) {
                    markerTypeFound = ind;
                    contextMarker = currentChar;
                }

                if (contextMarker == Character.MIN_VALUE && currentChar == '/' && (slashSlashComments || slashStarComments)) {
                    currentChar = sourceReader.read();
                    if (currentChar == '*' && slashStarComments) {
                        int prevChar = 0;
                        while ((currentChar = sourceReader.read()) != '/' || prevChar != '*') {
                            if (currentChar == '\r') {

                                currentChar = sourceReader.read();
                                if (currentChar == '\n') {
                                    currentChar = sourceReader.read();
                                }
                            } else {
                                if (currentChar == '\n') {

                                    currentChar = sourceReader.read();
                                }
                            }
                            if (currentChar < 0) {
                                break;
                            }
                            prevChar = currentChar;
                        }
                        continue;
                    } else if (currentChar == '/' && slashSlashComments) {
                        while ((currentChar = sourceReader.read()) != '\n' && currentChar != '\r' && currentChar >= 0) {
                        }
                    }
                } else if (contextMarker == Character.MIN_VALUE && currentChar == '#' && hashComments) {
                    // Slurp up everything until the newline
                    while ((currentChar = sourceReader.read()) != '\n' && currentChar != '\r' && currentChar >= 0) {
                    }
                } else if (contextMarker == Character.MIN_VALUE && currentChar == '-' && dashDashComments) {
                    currentChar = sourceReader.read();

                    if (currentChar == -1 || currentChar != '-') {
                        strBuilder.append('-');

                        if (currentChar != -1) {
                            strBuilder.append((char) currentChar);
                        }

                        continue;
                    }

                    // Slurp up everything until the newline

                    while ((currentChar = sourceReader.read()) != '\n' && currentChar != '\r' && currentChar >= 0) {
                    }
                }

                if (currentChar != -1) {
                    strBuilder.append((char) currentChar);
                }
            }
        } catch (IOException ioEx) {
            // we'll never see this from a StringReader
        }

        return strBuilder.toString();
    }

    /**
     * Next two functions are to help DBMD check if
     * the given string is in form of database.name and return it
     * as "database";"name" with comments removed.
     * If string is NULL or wildcard (%), returns null and exits.
     * 
     * First, we sanitize...
     * 
     * @param src
     *            the source string
     * @return the input string with all comment-delimited data removed
     */
    public static String sanitizeProcOrFuncName(String src) {
        if ((src == null) || (src.equals("%"))) {
            return null;
        }

        return src;
    }

    /**
     * Splits an entity identifier into its parts (database and entity name) and returns a list containing the two elements. If the identifier doesn't contain
     * the database part then the argument <code>catalog</code> is used in its place and <code>source</code> corresponds to the full entity name.
     * If argument <code>source</code> is NULL or wildcard (%), returns an empty list.
     * 
     * @param source
     *            the source string
     * @param catalog
     *            Catalog, if available
     * @param quoteId
     *            quote character as defined on server
     * @param isNoBslashEscSet
     *            is our connection in no BackSlashEscape mode
     * @return the input string with all comment-delimited data removed
     */
    public static List<String> splitDBdotName(String source, String catalog, String quoteId, boolean isNoBslashEscSet) {
        if ((source == null) || (source.equals("%"))) {
            return Collections.emptyList();
        }

        int dotIndex = -1;
        if (" ".equals(quoteId)) {
            dotIndex = source.indexOf(".");
        } else {
            dotIndex = indexOfIgnoreCase(0, source, ".", quoteId, quoteId, isNoBslashEscSet ? SEARCH_MODE__MRK_WS : SEARCH_MODE__BSESC_MRK_WS);
        }

        String database = catalog;
        String entityName;
        if (dotIndex != -1) {
            database = unQuoteIdentifier(source.substring(0, dotIndex), quoteId);
            entityName = unQuoteIdentifier(source.substring(dotIndex + 1), quoteId);
        } else {
            entityName = unQuoteIdentifier(source, quoteId);
        }

        return Arrays.asList(database, entityName);
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

    public static String escapeQuote(String src, String quotChar) {
        if (src == null) {
            return null;
        }

        src = StringUtils.toString(stripEnclosure(src.getBytes(), quotChar, quotChar));

        int lastNdx = src.indexOf(quotChar);
        String tmpSrc;
        String tmpRest;

        tmpSrc = src.substring(0, lastNdx);
        tmpSrc = tmpSrc + quotChar + quotChar;

        tmpRest = src.substring(lastNdx + 1, src.length());

        lastNdx = tmpRest.indexOf(quotChar);
        while (lastNdx > -1) {

            tmpSrc = tmpSrc + tmpRest.substring(0, lastNdx);
            tmpSrc = tmpSrc + quotChar + quotChar;
            tmpRest = tmpRest.substring(lastNdx + 1, tmpRest.length());

            lastNdx = tmpRest.indexOf(quotChar);
        }

        tmpSrc = tmpSrc + tmpRest;
        src = tmpSrc;

        return src;
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
     *         <li>null -> null</li>
     *         <li>abc -> `abc`</li>
     *         <li>ab`c -> `ab``c`</li>
     *         <li>ab"c -> `ab"c`</li>
     *         <li>`ab``c` -> `ab``c` in non-pedantic mode or ```ab````c``` in pedantic mode</li>
     *         </ul>
     *         With quoteChar="\"":<br>
     *         <ul>
     *         <li>null -> null</li>
     *         <li>abc -> "abc"</li>
     *         <li>ab`c -> "ab`c"</li>
     *         <li>ab"c -> "ab""c"</li>
     *         <li>"ab""c" -> "ab""c" in non-pedantic mode or """ab""""c""" in pedantic mode</li>
     *         </ul>
     */
    public static String quoteIdentifier(String identifier, String quoteChar, boolean isPedantic) {
        if (identifier == null) {
            return null;
        }

        identifier = identifier.trim();

        int quoteCharLength = quoteChar.length();
        if (quoteCharLength == 0 || " ".equals(quoteChar)) {
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
     *            in pedantic mode (connection property pedantic=true) identifier is treated as unquoted
     *            (as it is stored in the database) even if it starts and ends with "`";
     *            in non-pedantic mode if identifier starts and ends with "`" method treats it as already quoted and doesn't modify.
     * @param isPedantic
     *            are we in pedantic mode
     * 
     * @return
     *         <ul>
     *         <li>null -> null</li>
     *         <li>abc -> `abc`</li>
     *         <li>ab`c -> `ab``c`</li>
     *         <li>ab"c -> `ab"c`</li>
     *         <li>`ab``c` -> `ab``c` in non-pedantic mode or ```ab````c``` in pedantic mode</li>
     *         </ul>
     */
    public static String quoteIdentifier(String identifier, boolean isPedantic) {
        return quoteIdentifier(identifier, "`", isPedantic);
    }

    /**
     * Trims identifier, removes quote chars from first and last positions
     * and replaces double occurrences of quote char from entire identifier,
     * i.e converts quoted identifier into form as it is stored in database.
     * 
     * @param identifier
     * @param quoteChar
     *            ` or "
     * @return
     *         <ul>
     *         <li>null -> null</li>
     *         <li>abc -> abc</li>
     *         <li>`abc` -> abc</li>
     *         <li>`ab``c` -> ab`c</li>
     *         <li>`"ab`c"` -> "ab`c"</li>
     *         <li>`ab"c` -> ab"c</li>
     *         <li>"abc" -> abc</li>
     *         <li>"`ab""c`" -> `ab"c`</li>
     *         <li>"ab`c" -> ab`c</li>
     *         </ul>
     */
    public static String unQuoteIdentifier(String identifier, String quoteChar) {
        if (identifier == null) {
            return null;
        }

        identifier = identifier.trim();

        int quoteCharLength = quoteChar.length();
        if (quoteCharLength == 0 || " ".equals(quoteChar)) {
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

            return identifier.substring(quoteCharLength, (identifier.length() - quoteCharLength)).replaceAll(quoteChar + quoteChar, quoteChar);
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

    // The following methods all exist because of the Java bug
    //
    // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6790402
    // 
    // which has been observed by users and reported as MySQL Bug#61105
    //
    // We can turn around and replace them with their java.lang.String equivalents if/when that bug is ever fixed.

    public static String toString(byte[] value, int offset, int length, String encoding) throws UnsupportedEncodingException {
        Charset cs = findCharset(encoding);

        return cs.decode(ByteBuffer.wrap(value, offset, length)).toString();
    }

    public static String toString(byte[] value, String encoding) throws UnsupportedEncodingException {
        Charset cs = findCharset(encoding);

        return cs.decode(ByteBuffer.wrap(value)).toString();
    }

    public static String toString(byte[] value, int offset, int length) {
        try {
            Charset cs = findCharset(platformEncoding);

            return cs.decode(ByteBuffer.wrap(value, offset, length)).toString();
        } catch (UnsupportedEncodingException e) {
            // can't happen, emulating new String(byte[])
        }

        return null;
    }

    public static String toString(byte[] value) {
        try {
            Charset cs = findCharset(platformEncoding);

            return cs.decode(ByteBuffer.wrap(value)).toString();
        } catch (UnsupportedEncodingException e) {
            // can't happen, emulating new String(byte[])
        }

        return null;
    }

    public static byte[] getBytes(char[] value) {
        try {
            return getBytes(value, 0, value.length, platformEncoding);
        } catch (UnsupportedEncodingException e) {
            // can't happen, emulating new String(byte[])
        }

        return null;
    }

    public static byte[] getBytes(char[] value, int offset, int length) {
        try {
            return getBytes(value, offset, length, platformEncoding);
        } catch (UnsupportedEncodingException e) {
            // can't happen, emulating new String(byte[])
        }

        return null;
    }

    public static byte[] getBytes(char[] value, String encoding) throws UnsupportedEncodingException {
        return getBytes(value, 0, value.length, encoding);
    }

    public static byte[] getBytes(char[] value, int offset, int length, String encoding) throws UnsupportedEncodingException {
        Charset cs = findCharset(encoding);

        ByteBuffer buf = cs.encode(CharBuffer.wrap(value, offset, length));

        // can't simply .array() this to get the bytes especially with variable-length charsets the buffer is sometimes larger than the actual encoded data
        int encodedLen = buf.limit();
        byte[] asBytes = new byte[encodedLen];
        buf.get(asBytes, 0, encodedLen);

        return asBytes;
    }

    public static byte[] getBytes(String value) {
        try {
            return getBytes(value, 0, value.length(), platformEncoding);
        } catch (UnsupportedEncodingException e) {
            // can't happen, emulating new String(byte[])
        }

        return null;
    }

    public static byte[] getBytes(String value, int offset, int length) {
        try {
            return getBytes(value, offset, length, platformEncoding);
        } catch (UnsupportedEncodingException e) {
            // can't happen, emulating new String(byte[])
        }

        return null;
    }

    public static byte[] getBytes(String value, String encoding) throws UnsupportedEncodingException {
        return getBytes(value, 0, value.length(), encoding);
    }

    public static byte[] getBytes(String value, int offset, int length, String encoding) throws UnsupportedEncodingException {
        // Some CharsetEncoders (e.g. CP942, CP943, CP948, CP950, CP1381, CP1383, x-COMPOUND_TEXT or ISO-2022-JP) can't
        // handle correctly when encoding directly from its methods while calling the encoder from String object works
        // just fine. Most of these problems occur only in Java 1.5.
        // CharsetEncoder#encode() may be used in Java 1.6+ but only the method that receives a char[] as argument as
        // the one that receives a String argument doesn't always behaves correctly.
        if (!Util.isJdbc4()) {
            if (offset != 0 || length != value.length()) {
                return value.substring(offset, offset + length).getBytes(encoding);
            }
            return value.getBytes(encoding);
        }

        Charset cs = findCharset(encoding);

        ByteBuffer buf = cs.encode(CharBuffer.wrap(value.toCharArray(), offset, length));

        // can't simply .array() this to get the bytes especially with variable-length charsets the buffer is sometimes larger than the actual encoded data
        int encodedLen = buf.limit();
        byte[] asBytes = new byte[encodedLen];
        buf.get(asBytes, 0, encodedLen);

        return asBytes;
    }

    public static final boolean isValidIdChar(char c) {
        return VALID_ID_CHARS.indexOf(c) != -1;
    }

    private static final char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    public static void appendAsHex(StringBuilder builder, byte[] bytes) {
        builder.append("0x");
        for (byte b : bytes) {
            builder.append(HEX_DIGITS[(b >>> 4) & 0xF]).append(HEX_DIGITS[b & 0xF]);
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
            nibble = (byte) ((value >>> shift) & 0xF);
            if (nonZeroFound) {
                builder.append(HEX_DIGITS[nibble]);
            } else if (nibble != 0) {
                builder.append(HEX_DIGITS[nibble]);
                nonZeroFound = true;
            }
        } while (shift != 0);
    }

    public static byte[] getBytesNullTerminated(String value, String encoding) throws UnsupportedEncodingException {
        Charset cs = findCharset(encoding);

        ByteBuffer buf = cs.encode(value);

        int encodedLen = buf.limit();
        byte[] asBytes = new byte[encodedLen + 1];
        buf.get(asBytes, 0, encodedLen);
        asBytes[encodedLen] = 0;

        return asBytes;
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
}
