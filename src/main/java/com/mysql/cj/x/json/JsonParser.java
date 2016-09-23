/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.x.json;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.mysql.cj.api.x.JsonValue;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.WrongArgumentException;

public class JsonParser {

    enum Whitespace {
        TAB('\u0009'), LF('\n'), CR('\r'), SPACE('\u0020');

        public final char CHAR;

        private Whitespace(char character) {
            this.CHAR = character;
        }
    };

    enum StructuralToken {
        /**
         * [ U+005B left square bracket
         */
        LSQBRACKET('\u005B'), /**
                               * ] U+005D right square bracket
                               */
        RSQBRACKET('\u005D'), /**
                               * { U+007B left curly bracket
                               */
        LCRBRACKET('\u007B'), /**
                               * } U+007D right curly bracket
                               */
        RCRBRACKET('\u007D'), /**
                               * : U+003A colon
                               */
        COLON('\u003A'), /**
                          * , U+002C comma
                          */
        COMMA('\u002C');

        public final char CHAR;

        private StructuralToken(char character) {
            this.CHAR = character;
        }

    };

    enum EscapeChar {
        /**
         * \" represents the quotation mark character (U+0022)
         */
        QUOTE('\u0022', "\\\""), /**
                                  * \\ represents the reverse solidus character (U+005C)
                                  */
        RSOLIDUS('\\', "\\\\"), /**
                                 * \/ represents the solidus character (U+002F)
                                 */
        SOLIDUS('\u002F', "\\\u002F"), /**
                                        * \b represents the backspace character (U+0008)
                                        */
        BACKSPACE('\u0008', "\\b"), /**
                                     * \f represents the form feed character (U+000C)
                                     */
        FF('\u000C', "\\f"), /**
                              * \n represents the line feed character (U+000A)
                              */
        LF('\n', "\\n"), /**
                          * \r represents the carriage return character (U+000D)
                          */
        CR('\r', "\\r"), /**
                          * \t represents the character tabulation character (U+0009)
                          */
        TAB('\t', "\\t");

        public final char CHAR;
        public final String ESCAPED;

        private EscapeChar(char character, String escaped) {
            this.CHAR = character;
            this.ESCAPED = escaped;
        }
    };

    static Set<Character> whitespaceChars = new HashSet<Character>();
    static HashMap<Character, Character> unescapeChars = new HashMap<Character, Character>();

    static {
        for (EscapeChar ec : EscapeChar.values()) {
            unescapeChars.put(ec.ESCAPED.charAt(1), ec.CHAR);
        }
        for (Whitespace ws : Whitespace.values()) {
            whitespaceChars.add(ws.CHAR);
        }
    }

    private static boolean isValidEndOfValue(char ch) {
        return StructuralToken.COMMA.CHAR == ch || StructuralToken.RCRBRACKET.CHAR == ch || StructuralToken.RSQBRACKET.CHAR == ch;
    }

    /**
     * 
     * @param reader
     *            JSON string reader.
     * @return
     *         New {@link DbDoc} object initialized by parsed JSON string.
     * @throws IOException
     */
    public static DbDoc parseDoc(StringReader reader) throws IOException {

        DbDoc doc = new DbDoc();

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
                if (!(key = nextKey(reader)).equals("")) {
                    try {
                        doc.put(key, nextValue(reader));
                    } catch (WrongArgumentException ex) {
                        throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.0", new String[] { key }), ex);
                    }
                } else {
                    reader.reset();
                }

            } else if (ch == StructuralToken.RCRBRACKET.CHAR) {
                rightBrackets++;
                break;
            } else {
                if (!whitespaceChars.contains(ch)) {
                    throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.1", new Character[] { ch }));
                }
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
     * 
     * @param reader
     *            JSON string reader.
     * @return
     *         New {@link JsonArray} object initialized by parsed JSON string.
     * @throws IOException
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

        if (ch != StructuralToken.COLON.CHAR && val.getString().length() > 0) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("JsonParser.4", new String[] { val.getString() }));
        }
        return val.getString();
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

            } else if (ch == '\u002D' || (ch >= '\u0030' && ch <= '\u0039')) { // {-,0-9}
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
     * 
     * @param reader
     *            JSON string reader.
     * @return
     *         New {@link JsonString} object initialized by parsed JSON string.
     * @throws IOException
     */
    static JsonString parseString(StringReader reader) throws IOException {
        int quotes = 0;
        boolean escapeNextChar = false;

        StringBuilder sb = null; // stays null until starting quotation mark is found

        int intch;
        while ((intch = reader.read()) != -1) {
            char ch = (char) intch;
            if (escapeNextChar) {
                if (unescapeChars.containsKey(ch)) {
                    appendChar(sb, unescapeChars.get(ch));
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

        if (sb == null) {
            return new JsonString(); // empty string
        }
        return new JsonString().setValue(sb.toString());
    }

    /**
     * 
     * @param reader
     *            JSON string reader.
     * @return
     *         New {@link JsonNumber} object initialized by parsed JSON string.
     * @throws IOException
     */
    static JsonNumber parseNumber(StringReader reader) throws IOException {

        StringBuilder sb = null;
        char lastChar = ' ';
        int baseLength = 0;
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
                    baseLength++;
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
                if (!hasFractionalPart && !hasExponent) {
                    if (baseLength < 10) {
                        baseLength++;
                    } else {
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("JsonParser.9", new Object[] { sb.toString() }));
                    }
                }

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
     * 
     * @param reader
     *            JSON string reader.
     * @return
     *         New {@link JsonLiteral} object initialized by parsed JSON string.
     * @throws IOException
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
