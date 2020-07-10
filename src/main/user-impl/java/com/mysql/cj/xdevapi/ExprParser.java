/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.xdevapi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.x.protobuf.MysqlxCrud.Column;
import com.mysql.cj.x.protobuf.MysqlxCrud.Order;
import com.mysql.cj.x.protobuf.MysqlxCrud.Projection;
import com.mysql.cj.x.protobuf.MysqlxExpr.Array;
import com.mysql.cj.x.protobuf.MysqlxExpr.ColumnIdentifier;
import com.mysql.cj.x.protobuf.MysqlxExpr.DocumentPathItem;
import com.mysql.cj.x.protobuf.MysqlxExpr.Expr;
import com.mysql.cj.x.protobuf.MysqlxExpr.FunctionCall;
import com.mysql.cj.x.protobuf.MysqlxExpr.Identifier;
import com.mysql.cj.x.protobuf.MysqlxExpr.Object;
import com.mysql.cj.x.protobuf.MysqlxExpr.Object.ObjectField;
import com.mysql.cj.x.protobuf.MysqlxExpr.Operator;

// Grammar includes precedence & associativity of binary operators:
// (^ refers to the preceding production)
// (c.f. https://dev.mysql.com/doc/refman/5.7/en/operator-precedence.html)
//
// AtomicExpr: [Unary]OpExpr | Identifier | FunctionCall | '(' Expr ')'
//
// AddSubIntervalExpr: ^ (ADD/SUB ^)* | (ADD/SUB 'INTERVAL' ^ UNIT)*
//
// MulDivExpr: ^ (STAR/SLASH/MOD ^)*
//
// ShiftExpr: ^ (LSHIFT/RSHIFT ^)*
//
// BitExpr: ^ (BITAND/BITOR/BITXOR ^)*
//
// CompExpr: ^ (GE/GT/LE/LT/EQ/NE ^)*
//
// IlriExpr(ilri=IS/LIKE/REGEXP/IN/BETWEEN): ^ (ilri ^)
//
// AndExpr: ^ (AND ^)*
//
// OrExpr: ^ (OR ^)*
//
// Expr: ^
//
/**
 * Expression parser for X protocol.
 */
public class ExprParser {
    /** String being parsed. */
    String string;
    /** Token stream produced by lexer. */
    List<Token> tokens = new ArrayList<>();
    /** Parser's position in token stream. */
    int tokenPos = 0;
    /**
     * Mapping of names to positions for named placeholders. Used for both string values ":arg" and numeric values ":2".
     */
    Map<String, Integer> placeholderNameToPosition = new HashMap<>();
    /** Number of positional placeholders. */
    int positionalPlaceholderCount = 0;

    /** Are relational columns identifiers allowed? */
    private boolean allowRelationalColumns;

    /**
     * Constructor.
     * 
     * @param s
     *            expression string to parse
     */
    public ExprParser(String s) {
        this(s, true);
    }

    /**
     * Constructor.
     * 
     * @param s
     *            expression string to parse
     * @param allowRelationalColumns
     *            are relational columns identifiers allowed?
     */
    public ExprParser(String s, boolean allowRelationalColumns) {
        this.string = s;
        lex();
        // java.util.stream.IntStream.range(0, this.tokens.size()).forEach(i -> System.err.println("[" + i + "] = " + this.tokens.get(i)));
        this.allowRelationalColumns = allowRelationalColumns;
    }

    /**
     * Token types used by the lexer.
     */
    private static enum TokenType {
        NOT, AND, ANDAND, OR, OROR, XOR, IS, LPAREN, RPAREN, LSQBRACKET, RSQBRACKET, BETWEEN, TRUE, NULL, FALSE, IN, LIKE, INTERVAL, REGEXP, ESCAPE, IDENT,
        LSTRING, LNUM_INT, LNUM_DOUBLE, DOT, DOLLAR, COMMA, EQ, NE, GT, GE, LT, LE, BITAND, BITOR, BITXOR, LSHIFT, RSHIFT, PLUS, MINUS, STAR, SLASH, HEX, BIN,
        NEG, BANG, EROTEME, MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR, SECOND_MICROSECOND, MINUTE_MICROSECOND, MINUTE_SECOND,
        HOUR_MICROSECOND, HOUR_SECOND, HOUR_MINUTE, DAY_MICROSECOND, DAY_SECOND, DAY_MINUTE, DAY_HOUR, YEAR_MONTH, DOUBLESTAR, MOD, COLON, ORDERBY_ASC,
        ORDERBY_DESC, AS, LCURLY, RCURLY, DOTSTAR, CAST, DECIMAL, UNSIGNED, SIGNED, INTEGER, DATE, TIME, DATETIME, CHAR, BINARY, JSON, COLDOCPATH, OVERLAPS
    }

    /**
     * Token. Includes type and string value of the token.
     */
    static class Token {
        TokenType type;
        String value;

        public Token(TokenType x, char c) {
            this.type = x;
            this.value = new String(new char[] { c });
        }

        public Token(TokenType t, String v) {
            this.type = t;
            this.value = v;
        }

        @Override
        public String toString() {
            if (this.type == TokenType.IDENT || this.type == TokenType.LNUM_INT || this.type == TokenType.LNUM_DOUBLE || this.type == TokenType.LSTRING) {
                return this.type.toString() + "(" + this.value + ")";
            }
            return this.type.toString();
        }
    }

    /** Mapping of reserved words to token types. */
    static Map<String, TokenType> reservedWords = new HashMap<>();

    static {
        reservedWords.put("and", TokenType.AND);
        reservedWords.put("or", TokenType.OR);
        reservedWords.put("xor", TokenType.XOR);
        reservedWords.put("is", TokenType.IS);
        reservedWords.put("not", TokenType.NOT);
        reservedWords.put("like", TokenType.LIKE);
        reservedWords.put("in", TokenType.IN);
        reservedWords.put("regexp", TokenType.REGEXP);
        reservedWords.put("between", TokenType.BETWEEN);
        reservedWords.put("interval", TokenType.INTERVAL);
        reservedWords.put("escape", TokenType.ESCAPE);
        reservedWords.put("div", TokenType.SLASH);
        reservedWords.put("hex", TokenType.HEX);
        reservedWords.put("bin", TokenType.BIN);
        reservedWords.put("true", TokenType.TRUE);
        reservedWords.put("false", TokenType.FALSE);
        reservedWords.put("null", TokenType.NULL);
        reservedWords.put("microsecond", TokenType.MICROSECOND);
        reservedWords.put("second", TokenType.SECOND);
        reservedWords.put("minute", TokenType.MINUTE);
        reservedWords.put("hour", TokenType.HOUR);
        reservedWords.put("day", TokenType.DAY);
        reservedWords.put("week", TokenType.WEEK);
        reservedWords.put("month", TokenType.MONTH);
        reservedWords.put("quarter", TokenType.QUARTER);
        reservedWords.put("year", TokenType.YEAR);
        reservedWords.put("second_microsecond", TokenType.SECOND_MICROSECOND);
        reservedWords.put("minute_microsecond", TokenType.MINUTE_MICROSECOND);
        reservedWords.put("minute_second", TokenType.MINUTE_SECOND);
        reservedWords.put("hour_microsecond", TokenType.HOUR_MICROSECOND);
        reservedWords.put("hour_second", TokenType.HOUR_SECOND);
        reservedWords.put("hour_minute", TokenType.HOUR_MINUTE);
        reservedWords.put("day_microsecond", TokenType.DAY_MICROSECOND);
        reservedWords.put("day_second", TokenType.DAY_SECOND);
        reservedWords.put("day_minute", TokenType.DAY_MINUTE);
        reservedWords.put("day_hour", TokenType.DAY_HOUR);
        reservedWords.put("year_month", TokenType.YEAR_MONTH);
        reservedWords.put("asc", TokenType.ORDERBY_ASC);
        reservedWords.put("desc", TokenType.ORDERBY_DESC);
        reservedWords.put("as", TokenType.AS);
        reservedWords.put("cast", TokenType.CAST);
        reservedWords.put("decimal", TokenType.DECIMAL);
        reservedWords.put("unsigned", TokenType.UNSIGNED);
        reservedWords.put("signed", TokenType.SIGNED);
        reservedWords.put("integer", TokenType.INTEGER);
        reservedWords.put("date", TokenType.DATE);
        reservedWords.put("time", TokenType.TIME);
        reservedWords.put("datetime", TokenType.DATETIME);
        reservedWords.put("char", TokenType.CHAR);
        reservedWords.put("binary", TokenType.BINARY);
        reservedWords.put("json", TokenType.BINARY);
        reservedWords.put("overlaps", TokenType.OVERLAPS);
    }

    /**
     * Does the next character equal the given character? (respects bounds)
     * 
     * @param i
     *            The current position in the string
     * @param c
     *            character to compare with
     * @return true if equals
     */
    boolean nextCharEquals(int i, char c) {
        return (i + 1 < this.string.length()) && this.string.charAt(i + 1) == c;
    }

    /**
     * Helper function to match integer or floating point numbers. This function should be called when the position is on the first character of the number (a
     * digit or '.').
     *
     * @param i
     *            The current position in the string
     * @return the next position in the string after the number.
     */
    private int lexNumber(int i) {
        boolean isInt = true;
        char c;
        int start = i;
        for (; i < this.string.length(); ++i) {
            c = this.string.charAt(i);
            if (c == '.') {
                isInt = false;
            } else if (c == 'e' || c == 'E') {
                isInt = false;
                if (nextCharEquals(i, '-') || nextCharEquals(i, '+')) {
                    i++;
                }
            } else if (!Character.isDigit(c)) {
                break;
            }
        }
        if (isInt) {
            this.tokens.add(new Token(TokenType.LNUM_INT, this.string.substring(start, i)));
        } else {
            this.tokens.add(new Token(TokenType.LNUM_DOUBLE, this.string.substring(start, i)));
        }
        --i;
        return i;
    }

    /**
     * Lexer for X DevAPI expression language.
     */
    void lex() {
        for (int i = 0; i < this.string.length(); ++i) {
            int start = i; // for routines that consume more than one char
            char c = this.string.charAt(i);
            if (Character.isWhitespace(c)) {
                // ignore
            } else if (Character.isDigit(c)) {
                i = lexNumber(i);
            } else if (!(c == '_' || Character.isUnicodeIdentifierStart(c))) {
                // non-identifier, e.g. operator or quoted literal
                switch (c) {
                    case ':':
                        this.tokens.add(new Token(TokenType.COLON, c));
                        break;
                    case '+':
                        this.tokens.add(new Token(TokenType.PLUS, c));
                        break;
                    case '-':
                        if (nextCharEquals(i, '>')) {
                            i++;
                            this.tokens.add(new Token(TokenType.COLDOCPATH, "->"));
                        } else {
                            this.tokens.add(new Token(TokenType.MINUS, c));
                        }
                        break;
                    case '*':
                        if (nextCharEquals(i, '*')) {
                            i++;
                            this.tokens.add(new Token(TokenType.DOUBLESTAR, "**"));
                        } else {
                            this.tokens.add(new Token(TokenType.STAR, c));
                        }
                        break;
                    case '/':
                        this.tokens.add(new Token(TokenType.SLASH, c));
                        break;
                    case '$':
                        this.tokens.add(new Token(TokenType.DOLLAR, c));
                        break;
                    case '%':
                        this.tokens.add(new Token(TokenType.MOD, c));
                        break;
                    case '=':
                        if (nextCharEquals(i, '=')) {
                            i++;
                        }
                        this.tokens.add(new Token(TokenType.EQ, "=="));
                        break;
                    case '&':
                        if (nextCharEquals(i, '&')) {
                            i++;
                            this.tokens.add(new Token(TokenType.ANDAND, "&&"));
                        } else {
                            this.tokens.add(new Token(TokenType.BITAND, c));
                        }
                        break;
                    case '|':
                        if (nextCharEquals(i, '|')) {
                            i++;
                            this.tokens.add(new Token(TokenType.OROR, "||"));
                        } else {
                            this.tokens.add(new Token(TokenType.BITOR, c));
                        }
                        break;
                    case '^':
                        this.tokens.add(new Token(TokenType.BITXOR, c));
                        break;
                    case '(':
                        this.tokens.add(new Token(TokenType.LPAREN, c));
                        break;
                    case ')':
                        this.tokens.add(new Token(TokenType.RPAREN, c));
                        break;
                    case '[':
                        this.tokens.add(new Token(TokenType.LSQBRACKET, c));
                        break;
                    case ']':
                        this.tokens.add(new Token(TokenType.RSQBRACKET, c));
                        break;
                    case '{':
                        this.tokens.add(new Token(TokenType.LCURLY, c));
                        break;
                    case '}':
                        this.tokens.add(new Token(TokenType.RCURLY, c));
                        break;
                    case '~':
                        this.tokens.add(new Token(TokenType.NEG, c));
                        break;
                    case ',':
                        this.tokens.add(new Token(TokenType.COMMA, c));
                        break;
                    case '!':
                        if (nextCharEquals(i, '=')) {
                            i++;
                            this.tokens.add(new Token(TokenType.NE, "!="));
                        } else {
                            this.tokens.add(new Token(TokenType.BANG, c));
                        }
                        break;
                    case '?':
                        this.tokens.add(new Token(TokenType.EROTEME, c));
                        break;
                    case '<':
                        if (nextCharEquals(i, '<')) {
                            i++;
                            this.tokens.add(new Token(TokenType.LSHIFT, "<<"));
                        } else if (nextCharEquals(i, '=')) {
                            i++;
                            this.tokens.add(new Token(TokenType.LE, "<="));
                        } else {
                            this.tokens.add(new Token(TokenType.LT, c));
                        }
                        break;
                    case '>':
                        if (nextCharEquals(i, '>')) {
                            i++;
                            this.tokens.add(new Token(TokenType.RSHIFT, ">>"));
                        } else if (nextCharEquals(i, '=')) {
                            i++;
                            this.tokens.add(new Token(TokenType.GE, ">="));
                        } else {
                            this.tokens.add(new Token(TokenType.GT, c));
                        }
                        break;
                    case '.':
                        if (nextCharEquals(i, '*')) {
                            i++;
                            this.tokens.add(new Token(TokenType.DOTSTAR, ".*"));
                        } else if (i + 1 < this.string.length() && Character.isDigit(this.string.charAt(i + 1))) {
                            i = lexNumber(i);
                        } else {
                            this.tokens.add(new Token(TokenType.DOT, c));
                        }
                        break;
                    case '"':
                    case '\'':
                    case '`':
                        char quoteChar = c;
                        StringBuilder val = new StringBuilder();
                        try {
                            for (c = this.string.charAt(++i); c != quoteChar
                                    || (i + 1 < this.string.length() && this.string.charAt(i + 1) == quoteChar); c = this.string.charAt(++i)) {
                                if (c == '\\' || c == quoteChar) {
                                    ++i;
                                }
                                val.append(this.string.charAt(i));
                            }
                        } catch (StringIndexOutOfBoundsException ex) {
                            throw new WrongArgumentException("Unterminated string starting at " + start);
                        }
                        this.tokens.add(new Token(quoteChar == '`' ? TokenType.IDENT : TokenType.LSTRING, val.toString()));
                        break;
                    default:
                        throw new WrongArgumentException("Can't parse at pos: " + i);
                }
            } else {
                // otherwise, it's an identifier
                for (; i < this.string.length() && Character.isUnicodeIdentifierPart(this.string.charAt(i)); ++i) {
                }
                String val = this.string.substring(start, i);
                String valLower = val.toLowerCase();
                if (i < this.string.length()) {
                    // last char, this logic is artifact of the preceding loop
                    --i;
                }
                if (reservedWords.containsKey(valLower)) {
                    // Map operator names to values the server understands
                    if ("and".equals(valLower)) {
                        this.tokens.add(new Token(reservedWords.get(valLower), "&&"));
                    } else if ("or".equals(valLower)) {
                        this.tokens.add(new Token(reservedWords.get(valLower), "||"));
                    } else {
                        // we case-normalize reserved words
                        this.tokens.add(new Token(reservedWords.get(valLower), valLower));
                    }
                } else {
                    this.tokens.add(new Token(TokenType.IDENT, val));
                }
            }
        }
    }

    /**
     * Assert that the token at <i>pos</i> is of type <i>type</i>.
     * 
     * @param pos
     *            The current position in the string
     * @param type
     *            {@link TokenType}
     */
    void assertTokenAt(int pos, TokenType type) {
        if (this.tokens.size() <= pos) {
            throw new WrongArgumentException("No more tokens when expecting " + type + " at token pos " + pos);
        }
        if (this.tokens.get(pos).type != type) {
            throw new WrongArgumentException("Expected token type " + type + " at token pos " + pos);
        }
    }

    /**
     * Does the current token have type `t'?
     * 
     * @param t
     *            {@link TokenType}
     * @return true if equals
     */
    boolean currentTokenTypeEquals(TokenType t) {
        return posTokenTypeEquals(this.tokenPos, t);
    }

    /**
     * Does the next token have type `t'?
     * 
     * @param t
     *            {@link TokenType}
     * @return true if equals
     */
    boolean nextTokenTypeEquals(TokenType t) {
        return posTokenTypeEquals(this.tokenPos + 1, t);
    }

    /**
     * Does the token at position `pos' have type `t'?
     * 
     * @param pos
     *            The current position in the string
     * @param t
     *            {@link TokenType}
     * @return true if equals
     */
    boolean posTokenTypeEquals(int pos, TokenType t) {
        return this.tokens.size() > pos && this.tokens.get(pos).type == t;
    }

    /**
     * Consume token.
     *
     * @param t
     *            {@link TokenType}
     * @return the string value of the consumed token
     */
    String consumeToken(TokenType t) {
        assertTokenAt(this.tokenPos, t);
        String value = this.tokens.get(this.tokenPos).value;
        this.tokenPos++;
        return value;
    }

    /**
     * Parse a paren-enclosed expression list. This is used for function params or IN params.
     *
     * @return a List of expressions
     */
    List<Expr> parenExprList() {
        List<Expr> exprs = new ArrayList<>();
        consumeToken(TokenType.LPAREN);
        if (!currentTokenTypeEquals(TokenType.RPAREN)) {
            exprs.add(expr());
            while (currentTokenTypeEquals(TokenType.COMMA)) {
                consumeToken(TokenType.COMMA);
                exprs.add(expr());
            }
        }
        consumeToken(TokenType.RPAREN);
        return exprs;
    }

    /**
     * Parse a function call of the form: IDENTIFIER PAREN_EXPR_LIST.
     *
     * @return an Expr representing the function call.
     */
    Expr functionCall() {
        Identifier id = identifier();
        FunctionCall.Builder b = FunctionCall.newBuilder();
        b.setName(id);
        b.addAllParam(parenExprList());
        return Expr.newBuilder().setType(Expr.Type.FUNC_CALL).setFunctionCall(b.build()).build();
    }

    Expr starOperator() {
        Operator op = Operator.newBuilder().setName("*").build();
        return Expr.newBuilder().setType(Expr.Type.OPERATOR).setOperator(op).build();
    }

    /**
     * Parse an identifier for a function call: [schema.]name
     * 
     * @return {@link Identifier}
     */
    Identifier identifier() {
        Identifier.Builder builder = Identifier.newBuilder();
        assertTokenAt(this.tokenPos, TokenType.IDENT);
        if (nextTokenTypeEquals(TokenType.DOT)) {
            builder.setSchemaName(this.tokens.get(this.tokenPos).value);
            consumeToken(TokenType.IDENT);
            consumeToken(TokenType.DOT);
            assertTokenAt(this.tokenPos, TokenType.IDENT);
        }
        builder.setName(this.tokens.get(this.tokenPos).value);
        consumeToken(TokenType.IDENT);
        return builder.build();
    }

    /**
     * Parse a document path member.
     * 
     * @return {@link DocumentPathItem}
     */
    DocumentPathItem docPathMember() {
        consumeToken(TokenType.DOT);
        Token t = this.tokens.get(this.tokenPos);
        String memberName;
        if (currentTokenTypeEquals(TokenType.IDENT)) {
            // this shouldn't be allowed to be quoted with backticks, but the lexer allows it
            if (!t.value.equals(ExprUnparser.quoteIdentifier(t.value))) {
                throw new WrongArgumentException("'" + t.value + "' is not a valid JSON/ECMAScript identifier");
            }
            consumeToken(TokenType.IDENT);
            memberName = t.value;
        } else if (currentTokenTypeEquals(TokenType.LSTRING)) {
            consumeToken(TokenType.LSTRING);
            memberName = t.value;
        } else {
            throw new WrongArgumentException("Expected token type IDENT or LSTRING in JSON path at token pos " + this.tokenPos);
        }
        DocumentPathItem.Builder item = DocumentPathItem.newBuilder();
        item.setType(DocumentPathItem.Type.MEMBER);
        item.setValue(memberName);
        return item.build();
    }

    /**
     * Parse a document path array index.
     * 
     * @return {@link DocumentPathItem}
     */
    DocumentPathItem docPathArrayLoc() {
        DocumentPathItem.Builder builder = DocumentPathItem.newBuilder();
        consumeToken(TokenType.LSQBRACKET);
        if (currentTokenTypeEquals(TokenType.STAR)) {
            consumeToken(TokenType.STAR);
            consumeToken(TokenType.RSQBRACKET);
            return builder.setType(DocumentPathItem.Type.ARRAY_INDEX_ASTERISK).build();
        } else if (currentTokenTypeEquals(TokenType.LNUM_INT)) {
            Integer v = Integer.valueOf(this.tokens.get(this.tokenPos).value);
            if (v < 0) {
                throw new WrongArgumentException("Array index cannot be negative at " + this.tokenPos);
            }
            consumeToken(TokenType.LNUM_INT);
            consumeToken(TokenType.RSQBRACKET);
            return builder.setType(DocumentPathItem.Type.ARRAY_INDEX).setIndex(v).build();
        } else {
            throw new WrongArgumentException("Expected token type STAR or LNUM_INT in JSON path array index at token pos " + this.tokenPos);
        }
    }

    /**
     * Parse a JSON-style document path, like WL#7909, but prefix by @. instead of $.
     * 
     * @return list of {@link DocumentPathItem} objects
     */
    public List<DocumentPathItem> documentPath() {
        List<DocumentPathItem> items = new ArrayList<>();
        while (true) {
            if (currentTokenTypeEquals(TokenType.DOT)) {
                items.add(docPathMember());
            } else if (currentTokenTypeEquals(TokenType.DOTSTAR)) {
                consumeToken(TokenType.DOTSTAR);
                items.add(DocumentPathItem.newBuilder().setType(DocumentPathItem.Type.MEMBER_ASTERISK).build());
            } else if (currentTokenTypeEquals(TokenType.LSQBRACKET)) {
                items.add(docPathArrayLoc());
            } else if (currentTokenTypeEquals(TokenType.DOUBLESTAR)) {
                consumeToken(TokenType.DOUBLESTAR);
                items.add(DocumentPathItem.newBuilder().setType(DocumentPathItem.Type.DOUBLE_ASTERISK).build());
            } else {
                break;
            }
        }
        if (items.size() > 0 && items.get(items.size() - 1).getType() == DocumentPathItem.Type.DOUBLE_ASTERISK) {
            throw new WrongArgumentException("JSON path may not end in '**' at " + this.tokenPos);
        }
        return items;
    }

    /**
     * Parse a document field.
     * 
     * @return {@link Expr}
     */
    public Expr documentField() {
        ColumnIdentifier.Builder builder = ColumnIdentifier.newBuilder();
        if (currentTokenTypeEquals(TokenType.IDENT)) {
            builder.addDocumentPath(DocumentPathItem.newBuilder().setType(DocumentPathItem.Type.MEMBER).setValue(consumeToken(TokenType.IDENT)).build());
        }
        builder.addAllDocumentPath(documentPath());
        return Expr.newBuilder().setType(Expr.Type.IDENT).setIdentifier(builder.build()).build();
    }

    /**
     * Parse a column identifier (which may optionally include a JSON document path).
     * 
     * @return {@link Expr}
     */
    Expr columnIdentifier() {
        List<String> parts = new LinkedList<>();
        parts.add(consumeToken(TokenType.IDENT));
        while (currentTokenTypeEquals(TokenType.DOT)) {
            consumeToken(TokenType.DOT);
            parts.add(consumeToken(TokenType.IDENT));
            // identifier can be at most three parts
            if (parts.size() == 3) {
                break;
            }
        }
        Collections.reverse(parts);
        ColumnIdentifier.Builder id = ColumnIdentifier.newBuilder();
        for (int i = 0; i < parts.size(); ++i) {
            switch (i) {
                case 0:
                    id.setName(parts.get(0));
                    break;
                case 1:
                    id.setTableName(parts.get(1));
                    break;
                case 2:
                    id.setSchemaName(parts.get(2));
                    break;
            }
        }
        if (currentTokenTypeEquals(TokenType.COLDOCPATH)) {
            consumeToken(TokenType.COLDOCPATH);
            if (currentTokenTypeEquals(TokenType.DOLLAR)) {
                consumeToken(TokenType.DOLLAR);
                id.addAllDocumentPath(documentPath());
            } else if (currentTokenTypeEquals(TokenType.LSTRING)) {
                String path = consumeToken(TokenType.LSTRING);
                if (path.charAt(0) != '$') {
                    throw new WrongArgumentException("Invalid document path at " + this.tokenPos);
                }
                id.addAllDocumentPath(new ExprParser(path.substring(1, path.length())).documentPath());
            }
            if (id.getDocumentPathCount() == 0) {
                throw new WrongArgumentException("Invalid document path at " + this.tokenPos);
            }
        }
        return Expr.newBuilder().setType(Expr.Type.IDENT).setIdentifier(id.build()).build();
    }

    /**
     * Build a unary operator expression.
     * 
     * @param name
     *            operator name
     * @param param
     *            operator parameter
     * @return {@link Expr}
     */
    Expr buildUnaryOp(String name, Expr param) {
        String opName = "-".equals(name) ? "sign_minus" : ("+".equals(name) ? "sign_plus" : name);
        Operator op = Operator.newBuilder().setName(opName).addParam(param).build();
        return Expr.newBuilder().setType(Expr.Type.OPERATOR).setOperator(op).build();
    }

    /**
     * Parse an atomic expression. (c.f. grammar at top)
     * 
     * @return {@link Expr}
     */
    Expr atomicExpr() { // constant, identifier, variable, function call, etc
        if (this.tokenPos >= this.tokens.size()) {
            throw new WrongArgumentException("No more tokens when expecting one at token pos " + this.tokenPos);
        }
        Token t = this.tokens.get(this.tokenPos);
        this.tokenPos++; // consume
        switch (t.type) {
            case EROTEME:
            case COLON: {
                String placeholderName;
                if (currentTokenTypeEquals(TokenType.LNUM_INT)) {
                    // int pos = Integer.valueOf(consumeToken(TokenType.LNUM_INT));
                    // return Expr.newBuilder().setType(Expr.Type.PLACEHOLDER).setPosition(pos).build();
                    placeholderName = consumeToken(TokenType.LNUM_INT);
                } else if (currentTokenTypeEquals(TokenType.IDENT)) {
                    placeholderName = consumeToken(TokenType.IDENT);
                } else if (t.type == TokenType.EROTEME) {
                    placeholderName = String.valueOf(this.positionalPlaceholderCount);
                } else {
                    throw new WrongArgumentException("Invalid placeholder name at token pos " + this.tokenPos);
                }
                Expr.Builder placeholder = Expr.newBuilder().setType(Expr.Type.PLACEHOLDER);
                if (this.placeholderNameToPosition.containsKey(placeholderName)) {
                    placeholder.setPosition(this.placeholderNameToPosition.get(placeholderName));
                } else {
                    placeholder.setPosition(this.positionalPlaceholderCount);
                    this.placeholderNameToPosition.put(placeholderName, this.positionalPlaceholderCount);
                    this.positionalPlaceholderCount++;
                }
                return placeholder.build();
            }
            case LPAREN: {
                Expr e = expr();
                consumeToken(TokenType.RPAREN);
                return e;
            }
            case LCURLY: { // JSON object
                Object.Builder builder = Object.newBuilder();
                if (currentTokenTypeEquals(TokenType.LSTRING)) {
                    parseCommaSeparatedList(() -> {
                        String key = consumeToken(TokenType.LSTRING);
                        consumeToken(TokenType.COLON);
                        Expr value = expr();
                        return Collections.singletonMap(key, value);
                    }).stream().map(pair -> pair.entrySet().iterator().next()).map(e -> ObjectField.newBuilder().setKey(e.getKey()).setValue(e.getValue()))
                            .forEach(builder::addFld);
                }
                consumeToken(TokenType.RCURLY);
                return Expr.newBuilder().setType(Expr.Type.OBJECT).setObject(builder.build()).build();
            }
            case LSQBRACKET: { // Array
                Array.Builder builder = Expr.newBuilder().setType(Expr.Type.ARRAY).getArrayBuilder();
                if (!currentTokenTypeEquals(TokenType.RSQBRACKET)) {
                    parseCommaSeparatedList(() -> {
                        return expr();
                    }).stream().forEach(builder::addValue);
                }
                consumeToken(TokenType.RSQBRACKET);
                return Expr.newBuilder().setType(Expr.Type.ARRAY).setArray(builder).build();
            }
            case CAST: {
                consumeToken(TokenType.LPAREN);
                Operator.Builder builder = Operator.newBuilder().setName(TokenType.CAST.toString().toLowerCase());
                builder.addParam(expr());
                consumeToken(TokenType.AS);
                StringBuilder typeStr = new StringBuilder(this.tokens.get(this.tokenPos).value.toUpperCase());
                // ensure next token is a valid type argument to CAST
                if (currentTokenTypeEquals(TokenType.DECIMAL)) {
                    this.tokenPos++;
                    if (currentTokenTypeEquals(TokenType.LPAREN)) {
                        typeStr.append(consumeToken(TokenType.LPAREN));
                        typeStr.append(consumeToken(TokenType.LNUM_INT));
                        if (currentTokenTypeEquals(TokenType.COMMA)) {
                            typeStr.append(consumeToken(TokenType.COMMA));
                            typeStr.append(consumeToken(TokenType.LNUM_INT));
                        }
                        typeStr.append(consumeToken(TokenType.RPAREN));
                    }
                } else if (currentTokenTypeEquals(TokenType.CHAR) || currentTokenTypeEquals(TokenType.BINARY)) {
                    this.tokenPos++;
                    if (currentTokenTypeEquals(TokenType.LPAREN)) {
                        typeStr.append(consumeToken(TokenType.LPAREN));
                        typeStr.append(consumeToken(TokenType.LNUM_INT));
                        typeStr.append(consumeToken(TokenType.RPAREN));
                    }
                } else if (currentTokenTypeEquals(TokenType.UNSIGNED) || currentTokenTypeEquals(TokenType.SIGNED)) {
                    this.tokenPos++;
                    if (currentTokenTypeEquals(TokenType.INTEGER)) {
                        // don't add optional INTEGER to type string argument
                        consumeToken(TokenType.INTEGER);
                    }
                } else if (currentTokenTypeEquals(TokenType.JSON) || currentTokenTypeEquals(TokenType.DATE) || currentTokenTypeEquals(TokenType.DATETIME)
                        || currentTokenTypeEquals(TokenType.TIME)) {
                    this.tokenPos++;
                } else {
                    throw new WrongArgumentException("Expected valid CAST type argument at " + this.tokenPos);
                }
                consumeToken(TokenType.RPAREN);
                // TODO charset?
                builder.addParam(ExprUtil.buildLiteralScalar(typeStr.toString().getBytes()));
                return Expr.newBuilder().setType(Expr.Type.OPERATOR).setOperator(builder.build()).build();
            }
            case PLUS:
            case MINUS:
                if (currentTokenTypeEquals(TokenType.LNUM_INT) || currentTokenTypeEquals(TokenType.LNUM_DOUBLE)) {
                    // unary operators are handled inline making positive or negative numeric literals
                    this.tokens.get(this.tokenPos).value = t.value + this.tokens.get(this.tokenPos).value;
                    return atomicExpr();
                }
                return buildUnaryOp(t.value, atomicExpr());

            case NOT:
            case NEG:
            case BANG:
                return buildUnaryOp(t.value, atomicExpr());
            case LSTRING:
                return ExprUtil.buildLiteralScalar(t.value);
            case NULL:
                return ExprUtil.buildLiteralNullScalar();
            case LNUM_INT:
                return ExprUtil.buildLiteralScalar(Long.valueOf(t.value));
            case LNUM_DOUBLE:
                return ExprUtil.buildLiteralScalar(Double.valueOf(t.value));
            case TRUE:
            case FALSE:
                return ExprUtil.buildLiteralScalar(t.type == TokenType.TRUE);
            case DOLLAR:
                return documentField();
            case STAR:
                // special "0-ary" consideration of "*" as an operator (for COUNT(*), etc)
                return starOperator();
            case IDENT:
                this.tokenPos--; // stay on the identifier
                // check for function call which may be: func(...) or schema.func(...)
                if (nextTokenTypeEquals(TokenType.LPAREN) || (posTokenTypeEquals(this.tokenPos + 1, TokenType.DOT)
                        && posTokenTypeEquals(this.tokenPos + 2, TokenType.IDENT) && posTokenTypeEquals(this.tokenPos + 3, TokenType.LPAREN))) {
                    return functionCall();
                }
                if (this.allowRelationalColumns) {
                    return columnIdentifier();
                }
                return documentField();
            default:
                break;
        }
        throw new WrongArgumentException("Cannot find atomic expression at token pos: " + (this.tokenPos - 1));
    }

    /**
     * An expression parser. (used in {@link #parseLeftAssocBinaryOpExpr(TokenType[], ParseExpr)})
     */
    @FunctionalInterface
    static interface ParseExpr {
        Expr parseExpr();
    }

    /**
     * Parse a left-associated binary operator.
     *
     * @param types
     *            The token types that denote this operator.
     * @param innerParser
     *            The inner parser that should be called to parse operands.
     * @return an expression tree of the binary operator or a single operand
     */
    Expr parseLeftAssocBinaryOpExpr(TokenType[] types, ParseExpr innerParser) {
        Expr lhs = innerParser.parseExpr();
        while (this.tokenPos < this.tokens.size() && Arrays.asList(types).contains(this.tokens.get(this.tokenPos).type)) {
            Operator.Builder builder = Operator.newBuilder().setName(this.tokens.get(this.tokenPos).value).addParam(lhs);
            this.tokenPos++;
            builder.addParam(innerParser.parseExpr());
            lhs = Expr.newBuilder().setType(Expr.Type.OPERATOR).setOperator(builder.build()).build();
        }
        return lhs;
    }

    Expr addSubIntervalExpr() {
        Expr lhs = atomicExpr();
        while ((currentTokenTypeEquals(TokenType.PLUS) || currentTokenTypeEquals(TokenType.MINUS)) && nextTokenTypeEquals(TokenType.INTERVAL)) {
            Token op = this.tokens.get(this.tokenPos);
            this.tokenPos++;
            Operator.Builder builder = Operator.newBuilder().addParam(lhs);

            // INTERVAL expression
            consumeToken(TokenType.INTERVAL);

            if (op.type == TokenType.PLUS) {
                builder.setName("date_add");
            } else {
                builder.setName("date_sub");
            }

            builder.addParam(bitExpr()); // amount

            // ensure next token is an interval unit
            if (currentTokenTypeEquals(TokenType.MICROSECOND) || currentTokenTypeEquals(TokenType.SECOND) || currentTokenTypeEquals(TokenType.MINUTE)
                    || currentTokenTypeEquals(TokenType.HOUR) || currentTokenTypeEquals(TokenType.DAY) || currentTokenTypeEquals(TokenType.WEEK)
                    || currentTokenTypeEquals(TokenType.MONTH) || currentTokenTypeEquals(TokenType.QUARTER) || currentTokenTypeEquals(TokenType.YEAR)
                    || currentTokenTypeEquals(TokenType.SECOND_MICROSECOND) || currentTokenTypeEquals(TokenType.MINUTE_MICROSECOND)
                    || currentTokenTypeEquals(TokenType.MINUTE_SECOND) || currentTokenTypeEquals(TokenType.HOUR_MICROSECOND)
                    || currentTokenTypeEquals(TokenType.HOUR_SECOND) || currentTokenTypeEquals(TokenType.HOUR_MINUTE)
                    || currentTokenTypeEquals(TokenType.DAY_MICROSECOND) || currentTokenTypeEquals(TokenType.DAY_SECOND)
                    || currentTokenTypeEquals(TokenType.DAY_MINUTE) || currentTokenTypeEquals(TokenType.DAY_HOUR)
                    || currentTokenTypeEquals(TokenType.YEAR_MONTH)) {
            } else {
                throw new WrongArgumentException("Expected interval units at " + this.tokenPos);
            }
            // xplugin demands that intervals be sent uppercase
            // TODO: we need to propagate the appropriate encoding here? it's ascii but it might not *always* be a superset encoding??
            builder.addParam(ExprUtil.buildLiteralScalar(this.tokens.get(this.tokenPos).value.toUpperCase().getBytes()));
            this.tokenPos++;

            lhs = Expr.newBuilder().setType(Expr.Type.OPERATOR).setOperator(builder.build()).build();
        }
        return lhs;
    }

    Expr mulDivExpr() {
        return parseLeftAssocBinaryOpExpr(new TokenType[] { TokenType.STAR, TokenType.SLASH, TokenType.MOD }, this::addSubIntervalExpr);
    }

    Expr addSubExpr() {
        return parseLeftAssocBinaryOpExpr(new TokenType[] { TokenType.PLUS, TokenType.MINUS }, this::mulDivExpr);
    }

    Expr shiftExpr() {
        return parseLeftAssocBinaryOpExpr(new TokenType[] { TokenType.LSHIFT, TokenType.RSHIFT }, this::addSubExpr);
    }

    Expr bitExpr() {
        return parseLeftAssocBinaryOpExpr(new TokenType[] { TokenType.BITAND, TokenType.BITOR, TokenType.BITXOR }, this::shiftExpr);
    }

    Expr compExpr() {
        return parseLeftAssocBinaryOpExpr(new TokenType[] { TokenType.GE, TokenType.GT, TokenType.LE, TokenType.LT, TokenType.EQ, TokenType.NE },
                this::bitExpr);
    }

    Expr ilriExpr() {
        Expr lhs = compExpr();
        List<TokenType> expected = Arrays
                .asList(new TokenType[] { TokenType.IS, TokenType.IN, TokenType.LIKE, TokenType.BETWEEN, TokenType.REGEXP, TokenType.NOT, TokenType.OVERLAPS });
        while (this.tokenPos < this.tokens.size() && expected.contains(this.tokens.get(this.tokenPos).type)) {
            boolean isNot = false;
            if (currentTokenTypeEquals(TokenType.NOT)) {
                consumeToken(TokenType.NOT);
                isNot = true;
            }
            if (this.tokenPos < this.tokens.size()) {
                List<Expr> params = new ArrayList<>();
                params.add(lhs);
                String opName = this.tokens.get(this.tokenPos).value.toLowerCase();
                switch (this.tokens.get(this.tokenPos).type) {
                    case IS: // for IS, NOT comes AFTER
                        consumeToken(TokenType.IS);
                        if (currentTokenTypeEquals(TokenType.NOT)) {
                            consumeToken(TokenType.NOT);
                            opName = "is_not";
                        }
                        params.add(compExpr());
                        break;
                    case IN:
                        consumeToken(TokenType.IN);
                        if (currentTokenTypeEquals(TokenType.LPAREN)) {
                            params.addAll(parenExprList());
                        } else {
                            opName = "cont_in";
                            params.add(compExpr());
                        }
                        break;
                    case LIKE:
                        consumeToken(TokenType.LIKE);
                        params.add(compExpr());
                        if (currentTokenTypeEquals(TokenType.ESCAPE)) {
                            consumeToken(TokenType.ESCAPE);
                            // add as a third (optional) param
                            params.add(compExpr());
                        }
                        break;
                    case BETWEEN:
                        consumeToken(TokenType.BETWEEN);
                        params.add(compExpr());
                        assertTokenAt(this.tokenPos, TokenType.AND);
                        consumeToken(TokenType.AND);
                        params.add(compExpr());
                        break;
                    case REGEXP:
                        consumeToken(TokenType.REGEXP);
                        params.add(compExpr());
                        break;
                    case OVERLAPS:
                        consumeToken(TokenType.OVERLAPS);
                        params.add(compExpr());
                        break;
                    default:
                        throw new WrongArgumentException("Unknown token after NOT at pos: " + this.tokenPos);
                }
                if (isNot) {
                    opName = "not_" + opName;
                }
                Operator.Builder builder = Operator.newBuilder().setName(opName).addAllParam(params);
                lhs = Expr.newBuilder().setType(Expr.Type.OPERATOR).setOperator(builder.build()).build();
            }
        }
        return lhs;
    }

    Expr andExpr() {
        return parseLeftAssocBinaryOpExpr(new TokenType[] { TokenType.AND, TokenType.ANDAND }, this::ilriExpr);
    }

    Expr orExpr() {
        return parseLeftAssocBinaryOpExpr(new TokenType[] { TokenType.OR, TokenType.OROR }, this::andExpr);
    }

    Expr expr() {
        Expr e = orExpr();
        return e;
    }

    /**
     * Parse the entire string as an expression.
     *
     * @return an X protocol expression tree
     */
    public Expr parse() {
        try {
            Expr e = expr();
            if (this.tokenPos != this.tokens.size()) {
                throw new WrongArgumentException("Only " + this.tokenPos + " tokens consumed, out of " + this.tokens.size());
            }
            return e;
        } catch (IllegalArgumentException ex) {
            throw new WrongArgumentException("Unable to parse query '" + this.string + "'", ex);
        }
    }

    /**
     * Utility method to wrap a parser of a list of elements separated by comma.
     *
     * @param <T>
     *            the type of element to be parsed
     * @param elementParser
     *            the single element parser
     * @return a list of elements parsed
     */
    private <T> List<T> parseCommaSeparatedList(Supplier<T> elementParser) {
        List<T> elements = new ArrayList<>();
        boolean first = true;
        while (first || currentTokenTypeEquals(TokenType.COMMA)) {
            if (!first) {
                consumeToken(TokenType.COMMA);
            } else {
                first = false;
            }
            elements.add(elementParser.get());
        }
        return elements;
    }

    /**
     * Parse an ORDER BY specification which is a comma-separated list of expressions, each may be optionally suffixed by ASC/DESC.
     * 
     * @return list of {@link Order} objects
     */
    public List<Order> parseOrderSpec() {
        return parseCommaSeparatedList(() -> {
            Order.Builder builder = Order.newBuilder();
            builder.setExpr(expr());
            if (currentTokenTypeEquals(TokenType.ORDERBY_ASC)) {
                consumeToken(TokenType.ORDERBY_ASC);
                builder.setDirection(Order.Direction.ASC);
            } else if (currentTokenTypeEquals(TokenType.ORDERBY_DESC)) {
                consumeToken(TokenType.ORDERBY_DESC);
                builder.setDirection(Order.Direction.DESC);
            }
            return builder.build();
        });
    }

    /**
     * Parse a SELECT projection which is a comma-separated list of expressions, each optionally suffixed with a target alias.
     * 
     * @return list of {@link Projection} objects
     */
    public List<Projection> parseTableSelectProjection() {
        return parseCommaSeparatedList(() -> {
            Projection.Builder builder = Projection.newBuilder();
            builder.setSource(expr());
            if (currentTokenTypeEquals(TokenType.AS)) {
                consumeToken(TokenType.AS);
                builder.setAlias(consumeToken(TokenType.IDENT));
            }
            return builder.build();
        });
    }

    /**
     * Parse an INSERT field name.
     * 
     * @return {@link Column}
     */
    // TODO unit test
    public Column parseTableInsertField() {
        return Column.newBuilder().setName(consumeToken(TokenType.IDENT)).build();
    }

    /**
     * Parse an UPDATE field which can include can document paths.
     * 
     * @return {@link ColumnIdentifier}
     */
    public ColumnIdentifier parseTableUpdateField() {
        return columnIdentifier().getIdentifier();
    }

    /**
     * Parse a document projection which is similar to SELECT but with document paths as the target alias.
     * 
     * @return list of {@link Projection} objects
     */
    public List<Projection> parseDocumentProjection() {
        this.allowRelationalColumns = false;
        return parseCommaSeparatedList(() -> {
            Projection.Builder builder = Projection.newBuilder();
            builder.setSource(expr());
            // alias is not optional for document projection
            consumeToken(TokenType.AS);
            builder.setAlias(consumeToken(TokenType.IDENT));
            return builder.build();
        });
    }

    /**
     * Parse a list of expressions used for GROUP BY.
     * 
     * @return list of {@link Expr} objects
     */
    public List<Expr> parseExprList() {
        return parseCommaSeparatedList(this::expr);
    }

    /**
     * Return the number of positional placeholders in the expression.
     * 
     * @return the number of positional placeholders in the expression
     */
    public int getPositionalPlaceholderCount() {
        return this.positionalPlaceholderCount;
    }

    /**
     * Get a mapping of parameter names to positions.
     * 
     * @return a mapping of parameter names to positions.
     */
    public Map<String, Integer> getPlaceholderNameToPositionMap() {
        return Collections.unmodifiableMap(this.placeholderNameToPosition);
    }
}
