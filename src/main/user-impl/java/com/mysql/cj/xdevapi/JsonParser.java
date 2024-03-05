/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.xdevapi;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.AssertionFailedException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;

public class JsonParser {

    enum Whitespace {

        TAB('\u0009'), LF('\n'), CR('\r'), SPACE('\u0020');

        public final char CHAR;

        private Whitespace(char character) {
            this.CHAR = character;
        }

    }

    enum StructuralToken {

        /**
         * [ U+005B left square bracket
         */
        LSQBRACKET('\u005B'),
        /**
         * ] U+005D right square bracket
         */
        RSQBRACKET('\u005D'),
        /**
         * { U+007B left curly bracket
         */
        LCRBRACKET('\u007B'),
        /**
         * } U+007D right curly bracket
         */
        RCRBRACKET('\u007D'),
        /**
         * : U+003A colon
         */
        COLON('\u003A'),
        /**
         * , U+002C comma
         */
        COMMA('\u002C');

        public final char CHAR;

        private StructuralToken(char character) {
            this.CHAR = character;
        }

    }

    enum EscapeChar {

        /**
         * \\" represents the quotation mark character (U+0022)
         */
        QUOTE('\u0022', "\\\"", true),
        /**
         * \\\\ represents the reverse solidus character (U+005C)
         */
        RSOLIDUS('\\', "\\\\", true),
        /**
         * \\/ represents the solidus character (U+002F)
         */
        SOLIDUS('\u002F', "\\\u002F", false),
        /**
         * \\b represents the backspace character (U+0008)
         */
        BACKSPACE('\u0008', "\\b", true),
        /**
         * \\f represents the form feed character (U+000C)
         */
        FF('\u000C', "\\f", true),
        /**
         * \\n represents the line feed character (U+000A)
         */
        LF('\n', "\\n", true),
        /**
         * \\r represents the carriage return character (U+000D)
         */
        CR('\r', "\\r", true),
        /**
         * \\t represents the character tabulation character (U+0009)
         */
        TAB('\t', "\\t", true);

        public final char CHAR;
        public final String ESCAPED;
        public final boolean NEEDS_ESCAPING;

        private EscapeChar(char character, String escaped, boolean needsEscaping) {
            this.CHAR = character;
            this.ESCAPED = escaped;
            this.NEEDS_ESCAPING = needsEscaping;
        }

    }

    static Set<Character> whitespaceChars = new HashSet<>();
    static HashMap<Character, Character> escapeChars = new HashMap<>();

    static {
        for (EscapeChar ec : EscapeChar.values()) {
            escapeChars.put(ec.ESCAPED.charAt(1), ec.CHAR);
        }
        for (Whitespace ws : Whitespace.values()) {
            whitespaceChars.add(ws.CHAR);
        }
    }

    private static boolean isValidEndOfValue(char ch) {
        return StructuralToken.COMMA.CHAR == ch || StructuralToken.RCRBRACKET.CHAR == ch || StructuralToken.RSQBRACKET.CHAR == ch;
    }

    /**
     * Create {@link DbDoc} object from JSON string.
     *
     * @param jsonString
     *            JSON string representing a document
     * @return New {@link DbDoc} object initialized by parsed JSON string.
     */
    public static DbDoc parseDoc(String jsonString) {
        try {
            return JsonParser.parseDoc(new StringReader(jsonString));
        } catch (IOException ex) {
            throw AssertionFailedException.shouldNotHappen(ex);
        }
    }

    /**
     * Create {@link DbDoc} object from JSON string provided by reader.
     *
     * @param reader
     *            JSON string reader.
     * @return
     *         New {@link DbDoc} object initialized by parsed JSON string.
     * @throws IOException
     *             if can't read
     */
    public static DbDoc parseDoc(StringReader reader) throws IOException {
        DbDoc doc = new DbDocImpl();
        JsonValue val;

        int leftBrackets = 0;
        int rightBrackets = 0;

        int intch;
        while ((intch = reader.read()) != -1) {
            String key = null;
            char ch = (char) intch;
            if (ch == StructuralToken.LCRBRACKET.CHAR || ch == StructuralToken.COMMA.CHAR) {
                if (ch == StructuralToken.LCRBRACKET.CHAR) {
                    leftBrackets++;
                }
                if ((key = nextKey(reader)) != null) {
                    try {
                        if ((val = nextValue(reader)) != null) {
                            doc.put(key, val);
                        } else {
                            reader.reset();
                        }
                    } catch (WrongArgumentException ex) {
                        throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.0", new String[] { key }), ex);
                    }
                } else {
                    reader.reset();
                }
            } else if (ch == StructuralToken.RCRBRACKET.CHAR) {
                rightBrackets++;
                break;
            } else if (!whitespaceChars.contains(ch)) {
                throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.1", new Character[] { ch }));
            }
        }

        if (leftBrackets == 0) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.2"));
        } else if (leftBrackets > rightBrackets) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    Messages.getString("JsonParser.3", new Character[] { StructuralToken.RCRBRACKET.CHAR }));
        }

        return doc;
    }

    /**
     * Create {@link JsonArray} object from JSON string provided by reader.
     *
     * @param reader
     *            JSON string reader.
     * @return
     *         New {@link JsonArray} object initialized by parsed JSON string.
     * @throws IOException
     *             if can't read
     */
    public static JsonArray parseArray(StringReader reader) throws IOException {
        JsonArray arr = new JsonArray();
        JsonValue val;
        int openings = 0;

        int intch;
        while ((intch = reader.read()) != -1) {
            char ch = (char) intch;
            if (ch == StructuralToken.LSQBRACKET.CHAR || ch == StructuralToken.COMMA.CHAR) {
                if (ch == StructuralToken.LSQBRACKET.CHAR) {
                    openings++;
                }
                if ((val = nextValue(reader)) != null) {
                    arr.add(val);
                } else {
                    reader.reset();
                }

            } else if (ch == StructuralToken.RSQBRACKET.CHAR) {
                openings--;
                break;

            } else if (!whitespaceChars.contains(ch)) {
                throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.1", new Character[] { ch }));

            }
        }

        if (openings > 0) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    Messages.getString("JsonParser.3", new Character[] { StructuralToken.RSQBRACKET.CHAR }));
        }

        return arr;
    }

    private static String nextKey(StringReader reader) throws IOException {
        reader.mark(1);

        JsonString val = parseString(reader);
        if (val == null) {
            reader.reset();
        }

        // find delimiter
        int intch;
        char ch = ' ';
        while ((intch = reader.read()) != -1) {
            ch = (char) intch;
            if (ch == StructuralToken.COLON.CHAR) {
                // key/value delimiter found
                break;
            } else if (ch == StructuralToken.RCRBRACKET.CHAR) {
                // end of document
                break;
            } else if (!whitespaceChars.contains(ch)) {
                throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.1", new Character[] { ch }));
            }
        }

        if (ch != StructuralToken.COLON.CHAR && val != null && val.getString().length() > 0) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.4", new String[] { val.getString() }));
        }
        return val != null ? val.getString() : null;
    }

    private static JsonValue nextValue(StringReader reader) throws IOException {
        reader.mark(1);
        int intch;
        while ((intch = reader.read()) != -1) {
            char ch = (char) intch;
            if (ch == EscapeChar.QUOTE.CHAR) {
                // String detected
                reader.reset();
                return parseString(reader);

            } else if (ch == StructuralToken.LSQBRACKET.CHAR) {
                // array detected
                reader.reset();
                return parseArray(reader);

            } else if (ch == StructuralToken.LCRBRACKET.CHAR) {
                // inner Object detected
                reader.reset();
                return parseDoc(reader);

            } else if (ch == '\u002D' || ch >= '\u0030' && ch <= '\u0039') { // {-,0-9}
                // Number detected
                reader.reset();
                return parseNumber(reader);

            } else if (ch == JsonLiteral.TRUE.value.charAt(0)) {
                // "true" literal detected
                reader.reset();
                return parseLiteral(reader);

            } else if (ch == JsonLiteral.FALSE.value.charAt(0)) {
                // "false" literal detected
                reader.reset();
                return parseLiteral(reader);

            } else if (ch == JsonLiteral.NULL.value.charAt(0)) {
                // "null" literal detected
                reader.reset();
                return parseLiteral(reader);

            } else if (ch == StructuralToken.RSQBRACKET.CHAR) {
                // empty array
                return null;

            } else if (!whitespaceChars.contains(ch)) {
                throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.1", new Character[] { ch }));
            }
            reader.mark(1);
        }

        throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.5"));
    }

    private static void appendChar(StringBuilder sb, char ch) {
        if (sb == null) {
            if (!whitespaceChars.contains(ch)) {
                throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.6", new Character[] { ch }));
            }
        } else {
            sb.append(ch);
        }
    }

    /**
     * Create {@link JsonString} object from JSON string provided by reader.
     *
     * @param reader
     *            JSON string reader.
     * @return
     *         New {@link JsonString} object initialized by parsed JSON string or <code>null</code> if no JSON string was found.
     * @throws IOException
     *             if can't read
     */
    static JsonString parseString(StringReader reader) throws IOException {
        int quotes = 0;
        boolean escapeNextChar = false;

        StringBuilder sb = null; // stays null until starting quotation mark is found

        int intch;
        while ((intch = reader.read()) != -1) {
            char ch = (char) intch;
            if (escapeNextChar) {
                if (escapeChars.containsKey(ch)) {
                    appendChar(sb, escapeChars.get(ch));
                } else if (ch == 'u') {
                    // \\u[4 hex digits] represents a unicode code point (ISO/IEC 10646)
                    char[] buf = new char[4];
                    int countRead = reader.read(buf);
                    String hexCodePoint = countRead == -1 ? "" : String.valueOf(buf, 0, countRead);
                    if (countRead != 4) {
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("JsonParser.13", new String[] { hexCodePoint }));
                    }
                    try {
                        appendChar(sb, (char) Integer.parseInt(hexCodePoint, 16));
                    } catch (NumberFormatException e) {
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("JsonParser.13", new String[] { hexCodePoint }));
                    }
                } else {
                    throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.7", new Character[] { ch }));
                }
                escapeNextChar = false;

            } else if (ch == EscapeChar.QUOTE.CHAR) {
                if (sb == null) {
                    // start of string detected
                    sb = new StringBuilder();
                    quotes++;
                } else {
                    // end of string detected
                    quotes--;
                    break;
                }

            } else if (quotes == 0 && ch == StructuralToken.RCRBRACKET.CHAR) {
                // end of document
                break;

            } else if (ch == EscapeChar.RSOLIDUS.CHAR) {
                escapeNextChar = true;

            } else {
                appendChar(sb, ch);
            }
        }

        if (quotes > 0) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.3", new Character[] { EscapeChar.QUOTE.CHAR }));
        }

        return sb == null ? null : new JsonString().setValue(sb.toString());
    }

    /**
     * Create {@link JsonNumber} object from JSON string provided by reader.
     *
     * @param reader
     *            JSON string reader.
     * @return
     *         New {@link JsonNumber} object initialized by parsed JSON string.
     * @throws IOException
     *             if can't read
     */
    static JsonNumber parseNumber(StringReader reader) throws IOException {
        StringBuilder sb = null;
        char lastChar = ' ';
        boolean hasFractionalPart = false;
        boolean hasExponent = false;

        int intch;
        while ((intch = reader.read()) != -1) {
            char ch = (char) intch;

            if (sb == null) {
                // number is still not found
                if (ch == '\u002D') {
                    // first char of number is minus
                    sb = new StringBuilder();
                    sb.append(ch);
                } else if (ch >= '\u0030' && ch <= '\u0039') {
                    // first char of number is digit
                    sb = new StringBuilder();
                    sb.append(ch);
                } else if (!whitespaceChars.contains(ch)) {
                    // only white spaces are allowed before value
                    throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.1", new Character[] { ch }));
                }
            } else if (ch == '\u002D') {
                // '-' is allowed only on first position and after exponent character
                if (lastChar == 'E' || lastChar == 'e') {
                    sb.append(ch);
                } else {
                    throw ExceptionFactory.createException(WrongArgumentException.class,
                            Messages.getString("JsonParser.8", new Object[] { ch, sb.toString() }));
                }

            } else if (ch >= '\u0030' && ch <= '\u0039') { // 0-9
                sb.append(ch);

            } else if (ch == 'E' || ch == 'e') {
                // exponent character is allowed only after a digit
                if (lastChar >= '\u0030' && lastChar <= '\u0039') {
                    hasExponent = true;
                    sb.append(ch);
                } else {
                    throw ExceptionFactory.createException(WrongArgumentException.class,
                            Messages.getString("JsonParser.8", new Object[] { ch, sb.toString() }));
                }

            } else if (ch == '\u002E') {
                // '.' is allowed only once, after a digit and not in exponent part
                if (hasFractionalPart) {
                    throw ExceptionFactory.createException(WrongArgumentException.class,
                            Messages.getString("JsonParser.10", new Object[] { ch, sb.toString() }));
                }
                if (hasExponent) {
                    throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.11"));
                }
                if (lastChar >= '\u0030' && lastChar <= '\u0039') {
                    hasFractionalPart = true;
                    sb.append(ch);
                } else {
                    throw ExceptionFactory.createException(WrongArgumentException.class,
                            Messages.getString("JsonParser.8", new Object[] { ch, sb.toString() }));
                }

            } else if (ch == '\u002B') {
                // '+' is allowed only once after exponent character
                if (lastChar == 'E' || lastChar == 'e') {
                    sb.append(ch);
                } else {
                    throw ExceptionFactory.createException(WrongArgumentException.class,
                            Messages.getString("JsonParser.8", new Object[] { ch, sb.toString() }));
                }

            } else if (whitespaceChars.contains(ch) || isValidEndOfValue(ch)) {
                // any whitespace, comma or right bracket character means the end of Number in case we already placed something to buffer
                reader.reset(); // set reader position to last number char
                break;

            } else {
                // no other characters are allowed after value
                throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.1", new Character[] { ch }));
            }
            lastChar = ch;
            // it's safe to mark() here because the "higher" level marks won't be reset() once we start reading a number
            reader.mark(1);
        }

        if (sb == null || sb.length() == 0) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.5"));
        }

        return new JsonNumber().setValue(sb.toString());
    }

    /**
     * Create {@link JsonLiteral} object from JSON string provided by reader.
     *
     * @param reader
     *            JSON string reader.
     * @return
     *         New {@link JsonLiteral} object initialized by parsed JSON string.
     * @throws IOException
     *             if can't read
     */
    static JsonLiteral parseLiteral(StringReader reader) throws IOException {
        StringBuilder sb = null;
        JsonLiteral res = null;
        int literalIndex = 0;

        int intch;
        while ((intch = reader.read()) != -1) {
            char ch = (char) intch;
            if (sb == null) {
                // literal is still not found
                if (ch == JsonLiteral.TRUE.value.charAt(0)) {
                    // first char of "true" literal is found
                    res = JsonLiteral.TRUE;
                    sb = new StringBuilder();
                    sb.append(ch);
                    literalIndex++;
                } else if (ch == JsonLiteral.FALSE.value.charAt(0)) {
                    // first char of "false" literal is found
                    res = JsonLiteral.FALSE;
                    sb = new StringBuilder();
                    sb.append(ch);
                    literalIndex++;
                } else if (ch == JsonLiteral.NULL.value.charAt(0)) {
                    // first char of "null" literal is found
                    res = JsonLiteral.NULL;
                    sb = new StringBuilder();
                    sb.append(ch);
                    literalIndex++;
                } else if (!whitespaceChars.contains(ch)) {
                    // only whitespace chars are allowed before value
                    throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.1", new Character[] { ch }));
                }
            } else if (literalIndex < res.value.length() && ch == res.value.charAt(literalIndex)) {
                sb.append(ch);
                literalIndex++;

            } else if (whitespaceChars.contains(ch) || isValidEndOfValue(ch)) {
                // any whitespace, colon or right bracket character means the end of literal in case we already placed something to buffer
                reader.reset(); // set reader position to last number char
                break;

            } else {
                // no other characters are allowed after value
                throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.1", new Character[] { ch }));
            }
            // it's safe to mark() here because the "higher" level marks won't be reset() once we start reading a number
            reader.mark(1);
        }

        if (sb == null) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.5"));
        }

        if (literalIndex == res.value.length()) {
            return res;
        }

        throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.12", new String[] { sb.toString() }));
    }

}
