/*
 * Copyright (c) 2020, 2024, Oracle and/or its affiliates.
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

import java.text.Normalizer;
import java.text.Normalizer.Form;

import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;

/**
 * Implementation for SASLprep: Stringprep Profile for User Names and Passwords, as specified in <a href="https://tools.ietf.org/html/rfc4013">RFC 4013</a>.
 *
 * @see <a href="https://tools.ietf.org/html/rfc3454">RFC 3454</a>
 */
public class SaslPrep {

    /**
     * The type of string usage regarding the support for unassigned code points as described in <a href="https://tools.ietf.org/html/rfc3454#section-7">RFC
     * 3454, Section 7</a>.
     */
    public enum StringType {
        /**
         * Stored strings using the profile MUST NOT contain any unassigned code points.
         */
        STORED,
        /**
         * Queries for matching strings MAY contain unassigned code points.
         */
        QUERY;
    }

    /**
     * Prepares the given string by applying the "SASLprep" profile of the "stringprep" algorithm.
     *
     * @param str
     *            the string to prepare.
     * @param sType
     *            the type of preparation with regard to the support for unassigned code points.
     *
     * @return
     *         the prepared version of the given string.
     * @see <a href="https://tools.ietf.org/html/rfc4013">RFC 4013</a>
     * @see <a href="https://tools.ietf.org/html/rfc3454">RFC 3454</a>
     */
    public static String prepare(String str, StringType sType) {
        if (str.length() == 0) {
            return str;
        }

        StringBuilder sb = new StringBuilder(str.length());

        // 2.1. Mapping.
        for (char chr : str.toCharArray()) {
            if (isNonAsciiSpaceChar(chr)) {
                sb.append(' ');
            } else if (!isMappeableToNothing(chr)) {
                sb.append(chr);
            }
        }

        // 2.2. Normalization.
        String preparedStr = normalizeKc(sb);

        // 2.3. Prohibited Output & 2.4. Bidirectional Characters & 2.5. Unassigned Code Points.
        boolean startsWithRAndAlCat = isBidiRAndAlCat(preparedStr.codePointAt(0));
        boolean endsWithRAndAlCat = isBidiRAndAlCat(
                preparedStr.codePointAt(preparedStr.length() - (Character.isLowSurrogate(preparedStr.charAt(preparedStr.length() - 1)) ? 2 : 1)));
        boolean containsRAndAlCat = startsWithRAndAlCat || endsWithRAndAlCat;
        boolean containsLCat = false;
        for (int i = 0, ni; i < preparedStr.length(); i = ni) {
            char chr = preparedStr.charAt(i);
            int cp = preparedStr.codePointAt(i);
            ni = i + Character.charCount(cp);

            // 2.3. Prohibited Output.
            if (isProhibited(chr, cp)) {
                throw ExceptionFactory.createException(WrongArgumentException.class, "Prohibited character at position " + i + ".");
            }

            // 2.4. Bidirectional Characters.
            // (Already covered: MUST be prohibited - change display properties or are deprecated.)
            // <a href="https://tools.ietf.org/html/rfc3454#section-5.8">RFC 3454, Section 5.8</a>.
            if (!containsRAndAlCat) {
                containsRAndAlCat = isBidiRAndAlCat(cp);
            }
            if (!containsLCat) {
                containsLCat = isBidiLCat(cp);
            }
            if (containsRAndAlCat && containsLCat) {
                throw ExceptionFactory.createException(WrongArgumentException.class, "Cannot contain both RandALCat characters and LCat characters.");
            }
            if (ni >= preparedStr.length() && containsRAndAlCat && (!startsWithRAndAlCat || !endsWithRAndAlCat)) {
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        "Cannot contain RandALCat characters and not start and end with RandALCat characters.");
            }

            // 2.5. Unassigned Code Points.
            if (sType == StringType.STORED && isUnassigned(cp)) {
                throw ExceptionFactory.createException(WrongArgumentException.class, "Unassigned character at position " + i + ".");
            }
        }

        return preparedStr;
    }

    /**
     * Mapping: non-ASCII space characters [<a href="https://tools.ietf.org/html/rfc3454#section-3.1">StringPrep, C.1.2</a>] that can be mapped to SPACE
     * (U+0020).
     *
     * @param chr
     *            the character to check.
     * @return
     *         <code>true</code> if the character is one of the non-ASCII space characters, <code>false</code> otherwise.
     */
    private static boolean isNonAsciiSpaceChar(char chr) {
        return chr == '\u00A0' || chr == '\u1680' || chr >= '\u2000' && chr <= '\u200B' || chr == '\u202F' || chr == '\u205F' || chr == '\u3000';
    }

    /**
     * Mapping: the "commonly mapped to nothing" characters [<a href="https://tools.ietf.org/html/rfc3454#appendix-B.1">StringPrep, B.1</a>] that can be mapped
     * to nothing.
     *
     * @param chr
     *            the character to check.
     * @return
     *         <code>true</code> if the character is one of the "commonly mapped to nothing" characters, <code>false</code> otherwise.
     */
    private static boolean isMappeableToNothing(char chr) {
        return chr == '\u00AD' || chr == '\u034F' || chr == '\u1806' || chr >= '\u180B' && chr <= '\u180D' || chr >= '\u200B' && chr <= '\u200D'
                || chr == '\u2060' || chr >= '\uFE00' && chr <= '\uFE0F' || chr == '\uFEFF';
    }

    /**
     * Normalization: Unicode normalization form KC.
     *
     * @param str
     *            the string to be normalized.
     * @return
     *         a normalized version of the given string by the rules of the Unicode normalization form KC.
     */
    private static String normalizeKc(CharSequence str) {
        return Normalizer.normalize(str, Form.NFKC);
    }

    /**
     * Checks if the given character is one of the prohibited characters under the "SASLprep Profile" rules.
     *
     * @param chr
     *            the character to check.
     * @param cp
     *            the code point of the character to check.
     * @return
     *         <code>true</code> if the character is prohibited according to the profile rules, <code>false</code> otherwise.
     * @see <a href="https://tools.ietf.org/html/rfc4013#section-2.3">RFC 4013, Section 2.3</a>
     */
    private static boolean isProhibited(char chr, int cp) {
        return /* already covered: isNonAsciiSpaceChar(chr) || */ isAsciiControlCharacter(chr) || isNonAsciiControlCharacter(cp) || isPrivateUseCharacter(cp)
                || isNonCharacterCodePoint(cp) || isSurrogateCode(chr) || isInappropriateForPlainTextCharacter(chr)
                || isInappropriateForCanonicalRepresentationCharacter(chr) || isChangeDisplayPropertiesOrDeprecatedCharacter(chr) || isTaggingCharacter(cp);
    }

    /**
     * Prohibited Output: ASCII control characters [<a href="https://tools.ietf.org/html/rfc3454#appendix-C.2.1">StringPrep, C.2.1</a>].
     *
     * @param chr
     *            the character to check.
     * @return
     *         <code>true</code> if the character is one of the ASCII control characters, <code>false</code> otherwise.
     */
    private static boolean isAsciiControlCharacter(char chr) {
        return chr <= '\u001F' || chr == '\u007F';
    }

    /**
     * Prohibited Output: non-ASCII control characters [<a href="https://tools.ietf.org/html/rfc3454#appendix-C.2.2">StringPrep, C.2.2</a>].
     *
     * @param cp
     *            the code point of the character to check.
     * @return
     *         <code>true</code> if the character is one of the non-ASCII control characters, <code>false</code> otherwise.
     */
    private static boolean isNonAsciiControlCharacter(int cp) {
        return cp >= 0x0080 && cp <= 0x009F || cp == 0x06DD || cp == 0x070F || cp == 0x180E || cp == 0x200C || cp == 0x200D || cp == 0x2028 || cp == 0x2029
                || cp >= 0x2060 && cp <= 0x2063 || cp >= 0x206A && cp <= 0x206F || cp == 0xFEFF || cp >= 0xFFF9 && cp <= 0xFFFC
                || cp >= 0x1D173 && cp <= 0x1D17A;
    }

    /**
     * Prohibited Output: private use characters [<a href="https://tools.ietf.org/html/rfc3454#appendix-C.3">StringPrep, C.3</a>].
     *
     * @param cp
     *            the code point of the character to check.
     * @return
     *         <code>true</code> if the character is one of the private use characters, <code>false</code> otherwise.
     */
    private static boolean isPrivateUseCharacter(int cp) {
        return cp >= 0xE000 && cp <= 0xF8FF || cp >= 0xF0000 && cp <= 0xFFFFD || cp >= 0x100000 && cp <= 0x10FFFD;
    }

    /**
     * Prohibited Output: non-character code points [<a href="https://tools.ietf.org/html/rfc3454#appendix-C.4">StringPrep, C.4</a>].
     *
     * @param cp
     *            the code point of the character to check.
     * @return
     *         <code>true</code> if the character is one of the non-character code points, <code>false</code> otherwise.
     */
    private static boolean isNonCharacterCodePoint(int cp) {
        return cp >= 0xFDD0 && cp <= 0xFDEF || cp >= 0xFFFE && cp <= 0xFFFF || cp >= 0x1FFFE && cp <= 0x1FFFF || cp >= 0x2FFFE && cp <= 0x2FFFF
                || cp >= 0x3FFFE && cp <= 0x3FFFF || cp >= 0x4FFFE && cp <= 0x4FFFF || cp >= 0x5FFFE && cp <= 0x5FFFF || cp >= 0x6FFFE && cp <= 0x6FFFF
                || cp >= 0x7FFFE && cp <= 0x7FFFF || cp >= 0x8FFFE && cp <= 0x8FFFF || cp >= 0x9FFFE && cp <= 0x9FFFF || cp >= 0xAFFFE && cp <= 0xAFFFF
                || cp >= 0xBFFFE && cp <= 0xBFFFF || cp >= 0xCFFFE && cp <= 0xCFFFF || cp >= 0xDFFFE && cp <= 0xDFFFF || cp >= 0xEFFFE && cp <= 0xEFFFF
                || cp >= 0xFFFFE && cp <= 0xFFFFF || cp >= 0x10FFFE && cp <= 0x10FFFF;
    }

    /**
     * Prohibited Output: surrogate code points [<a href="https://tools.ietf.org/html/rfc3454#appendix-C.5">StringPrep, C.5</a>].
     *
     * @param chr
     *            the character to check.
     * @return
     *         <code>true</code> if the character is one of the surrogate code points, <code>false</code> otherwise.
     */
    private static boolean isSurrogateCode(char chr) {
        return chr >= '\uD800' && chr <= '\uDFFF';
    }

    /**
     * Prohibited Output: inappropriate for plain text characters [<a href="https://tools.ietf.org/html/rfc3454#appendix-C.6">StringPrep, C.6</a>].
     *
     * @param chr
     *            the character to check.
     * @return
     *         <code>true</code> if the character is one of the inappropriate for plain text characters, <code>false</code> otherwise.
     */
    private static boolean isInappropriateForPlainTextCharacter(char chr) {
        return chr == '\uFFF9' || chr >= '\uFFFA' && chr <= '\uFFFD';
    }

    /**
     * Prohibited Output: inappropriate for canonical representation characters [<a href="https://tools.ietf.org/html/rfc3454#appendix-C.7">StringPrep,
     * C.7</a>].
     *
     * @param chr
     *            the character to check.
     * @return
     *         <code>true</code> if the character is one of the inappropriate for canonical representation characters, <code>false</code> otherwise.
     */
    private static boolean isInappropriateForCanonicalRepresentationCharacter(char chr) {
        return chr >= '\u2FF0' && chr <= '\u2FFB';
    }

    /**
     * Prohibited Output: change display properties or deprecated characters [<a href="https://tools.ietf.org/html/rfc3454#appendix-C.8">StringPrep, C.8</a>].
     *
     * @param chr
     *            the character to check.
     * @return
     *         <code>true</code> if the character is one of the change display properties or deprecated characters, <code>false</code> otherwise.
     */
    private static boolean isChangeDisplayPropertiesOrDeprecatedCharacter(char chr) {
        return chr == '\u0340' || chr == '\u0341' || chr == '\u200E' || chr == '\u200F' || chr >= '\u202A' && chr <= '\u202E'
                || chr >= '\u206A' && chr <= '\u206F';
    }

    /**
     * Prohibited Output: tagging characters [<a href="https://tools.ietf.org/html/rfc3454#appendix-C.9">StringPrep, C.9</a>].
     *
     * @param cp
     *            the code point of the character to check.
     * @return
     *         <code>true</code> if the character is one of the tagging characters, <code>false</code> otherwise.
     */
    private static boolean isTaggingCharacter(int cp) {
        return cp == 0xE0001 || cp >= 0xE0020 && cp <= 0xE007F;
    }

    /**
     * Bidirectional Characters: RandALCat characters.
     * See also <a href="https://tools.ietf.org/html/rfc3454#section-6">RFC 3454, Section 6</a>
     *
     * @param cp
     *            the code point of the character to check.
     * @return
     *         <code>true</code> if the character is one of the RandALCat characters, <code>false</code> otherwise.
     */
    private static boolean isBidiRAndAlCat(int cp) {
        byte dir = Character.getDirectionality(cp);
        return dir == Character.DIRECTIONALITY_RIGHT_TO_LEFT || dir == Character.DIRECTIONALITY_RIGHT_TO_LEFT_ARABIC;
    }

    /**
     * Bidirectional Characters: LCat characters.
     * See also <a href="https://tools.ietf.org/html/rfc3454#section-6">RFC 3454, Section 6</a>
     *
     * @param cp
     *            the code point of the character to check.
     * @return
     *         <code>true</code> if the character is one of the LCat characters, <code>false</code> otherwise.
     */
    private static boolean isBidiLCat(int cp) {
        byte dir = Character.getDirectionality(cp);
        return dir == Character.DIRECTIONALITY_LEFT_TO_RIGHT;
    }

    /**
     * Unassigned Code Points: list of unassigned code points.
     * See also <a href="https://tools.ietf.org/html/rfc3454#section-7">RFC 3454, Section 7</a>.
     *
     * <p>
     * Note that this implementation does not check exactly the unassigned code points as specified in the RFC since it is based on Java's Unicode support,
     * which is updated regularly while the specification is based on a static list of code points. This should have no major impact, though.
     *
     * @param cp
     *            the code point of the character to check.
     * @return
     *         <code>true</code> if the character is unassigned, <code>false</code> otherwise.
     */
    private static boolean isUnassigned(int cp) {
        return !Character.isDefined(cp);
    }

}
