/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

package com.mysql.cj.mysqlx;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Order;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.ColumnIdentifier;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.DocumentPathItem;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.Expr;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.FunctionCall;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.Identifier;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.Operator;

// Grammar includes precedence & associativity of binary operators:
// (^ refers to the preceding production)
// (c.f. https://dev.mysql.com/doc/refman/5.7/en/operator-precedence.html)
//
// AtomicExpr: [Unary]OpExpr | Identifier | FunctionCall | '(' Expr ')'
//
// MulDivExpr: ^ (STAR/SLASH/MOD ^)*
//
// AddSubOrIntervalExpr: ^ (ADD/SUB ^)* | (ADD/SUB 'INTERVAL' ^ UNIT)*
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
 * Expression parser for MySQL-X protocol.
 */
public class ExprParser {
    /** String being parsed. */
    String string;
    /** Token stream produced by lexer. */
    List<Token> tokens = new ArrayList<>();
    /** Parser's position in token stream. */
    int tokenPos = 0;

    public ExprParser(String s) {
        this.string = s;
        lex();
    }

    /**
     * Token types used by the lexer.
     */
    public static enum TokenType {
        NOT, AND, ANDAND, OR, OROR, XOR, IS, LPAREN, RPAREN, LSQBRACKET, RSQBRACKET, BETWEEN, TRUE, NULL, FALSE, IN, LIKE, INTERVAL, REGEXP, ESCAPE, IDENT,
        LSTRING, LNUM_INT, LNUM_DOUBLE, DOT, AT, COMMA, EQ, NE, GT, GE, LT, LE, BITAND, BITOR, BITXOR, LSHIFT, RSHIFT, PLUS, MINUS, STAR, SLASH, HEX,
        BIN, NEG, BANG, MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR, SECOND_MICROSECOND, MINUTE_MICROSECOND, MINUTE_SECOND,
        HOUR_MICROSECOND, HOUR_SECOND, HOUR_MINUTE, DAY_MICROSECOND, DAY_SECOND, DAY_MINUTE, DAY_HOUR, YEAR_MONTH, DOUBLESTAR, MOD, COLON, ORDERBY_ASC,
        ORDERBY_DESC
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
            } else {
                return this.type.toString();
            }
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
    }

    /**
     * Does the next character equal the given character? (respects bounds)
     */
    boolean nextCharEquals(int i, char c) {
        return (i + 1 < this.string.length()) && this.string.charAt(i + 1) == c;
    }

    /**
     * Helper function to match integer or floating point numbers. This function should be called when the position is on the first character of the number (a
     * digit or '.').
     *
     * @param i The current position in the string
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
     * Lexer for MySQL-X expression language.
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
                        this.tokens.add(new Token(TokenType.MINUS, c));
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
                    case '@':
                        this.tokens.add(new Token(TokenType.AT, c));
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
                        if (i + 1 < this.string.length() && Character.isDigit(this.string.charAt(i + 1))) {
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
                            for (c = this.string.charAt(++i); c != quoteChar || (i + 1 < this.string.length() && this.string.charAt(i + 1) == quoteChar);
                                 c = this.string.charAt(++i)) {
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
     */
    boolean currentTokenTypeEquals(TokenType t) {
        return posTokenTypeEquals(this.tokenPos, t);
    }

    /**
     * Does the next token have type `t'?
     */
    boolean nextTokenTypeEquals(TokenType t) {
        return posTokenTypeEquals(this.tokenPos + 1, t);
    }

    /**
     * Does the token at position `pos' have type `t'?
     */
    boolean posTokenTypeEquals(int pos, TokenType t) {
        return this.tokens.size() > pos && this.tokens.get(pos).type == t;
    }

    /**
     * Consume token.
     *
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

    /**
     * Parse an identifier for a function call: [schema.]name
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
        } else if (currentTokenTypeEquals(TokenType.STAR)) {
            consumeToken(TokenType.STAR);
            memberName = "*";
            return DocumentPathItem.newBuilder().setType(DocumentPathItem.Type.MEMBER_ASTERISK).build();
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
     */
    public List<DocumentPathItem> documentPath() {
        List<DocumentPathItem> items = new ArrayList<>();
        while (true) {
            if (currentTokenTypeEquals(TokenType.DOT)) {
                items.add(docPathMember());
            } else if (currentTokenTypeEquals(TokenType.LSQBRACKET)) {
                items.add(docPathArrayLoc());
            } else if (currentTokenTypeEquals(TokenType.DOUBLESTAR)) {
                consumeToken(TokenType.DOUBLESTAR);
                items.add(DocumentPathItem.newBuilder().setType(DocumentPathItem.Type.DOUBLE_ASTERISK).build());
            } else {
                break;
            }
        }
        if (items.size() == 0) {
            throw new WrongArgumentException("Invalid JSON path at " + this.tokenPos);
        }
        if (items.get(items.size() - 1).getType() == DocumentPathItem.Type.DOUBLE_ASTERISK) {
            throw new WrongArgumentException("JSON path may not end in '**' at " + this.tokenPos);
        }
        return items;
    }

    /**
     * Parse a column identifier (which may optionally include a JSON document path).
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
        if (currentTokenTypeEquals(TokenType.AT)) {
            consumeToken(TokenType.AT);
            id.addAllDocumentPath(documentPath());
        }
        return Expr.newBuilder().setType(Expr.Type.IDENT).setIdentifier(id.build()).build();
    }

    /**
     * Build a unary operator expression.
     */
    Expr buildUnaryOp(String name, Expr param) {
        Operator op = Operator.newBuilder().setName(name).addParam(param).build();
        return Expr.newBuilder().setType(Expr.Type.OPERATOR).setOperator(op).build();
    }

    /**
     * Parse an atomic expression. (c.f. grammar at top)
     */
    Expr atomicExpr() { // constant, identifier, variable, function call, etc
        if (this.tokenPos >= this.tokens.size()) {
            throw new WrongArgumentException("No more tokens when expecting one at token pos " + this.tokenPos);
        }
        Token t = this.tokens.get(this.tokenPos);
        this.tokenPos++; // consume
        switch (t.type) {
            case COLON:
                int pos = Integer.valueOf(consumeToken(TokenType.LNUM_INT));
                if (this.placeholderValues != null) {
                    return this.placeholderValues.get(pos);
                } else {
                    return Expr.newBuilder().setType(Expr.Type.PLACEHOLDER).setPosition(pos).build();
                }
            case AT: {
                ColumnIdentifier colId = ColumnIdentifier.newBuilder().addAllDocumentPath(documentPath()).build();
                return Expr.newBuilder().setType(Expr.Type.IDENT).setIdentifier(colId).build();
            }
            case LPAREN: {
                Expr e = expr();
                consumeToken(TokenType.RPAREN);
                return e;
            }
            case PLUS:
            case MINUS:
                if (currentTokenTypeEquals(TokenType.LNUM_INT) || currentTokenTypeEquals(TokenType.LNUM_DOUBLE)) {
                    // unary operators are handled inline making positive or negative numeric literals
                    this.tokens.get(this.tokenPos).value = t.value + this.tokens.get(this.tokenPos).value;
                    return atomicExpr();
                }
            case NOT:
            case NEG:
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
            case IDENT:
                this.tokenPos--; // stay on the identifier
                // check for function call which may be: func(...) or schema.func(...)
                if (nextTokenTypeEquals(TokenType.LPAREN)
                        || (posTokenTypeEquals(this.tokenPos + 1, TokenType.DOT) && posTokenTypeEquals(this.tokenPos + 2, TokenType.IDENT) && posTokenTypeEquals(
                                this.tokenPos + 3, TokenType.LPAREN))) {
                    return functionCall();
                } else {
                    return columnIdentifier();
                }
        }
        throw new WrongArgumentException("Cannot find atomic expression at token pos: " + (this.tokenPos - 1));
    }

    /**
     * Helper interface for the method references used in {@link parseLeftAssocBinaryOpExpr(TokenType[], ParseExpr)}
     */
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

    Expr mulDivExpr() {
        return parseLeftAssocBinaryOpExpr(new TokenType[] { TokenType.STAR, TokenType.SLASH, TokenType.MOD }, this::atomicExpr);
    }

    Expr addSubOrIntervalExpr() {
        Expr lhs = mulDivExpr();
        while (true) {
            if (!(currentTokenTypeEquals(TokenType.PLUS) || currentTokenTypeEquals(TokenType.MINUS))) {
                return lhs;
            }
            Token op = this.tokens.get(this.tokenPos);
            this.tokenPos++;
            Operator.Builder builder = Operator.newBuilder().addParam(lhs);
            if (currentTokenTypeEquals(TokenType.INTERVAL)) {
                // INTERVAL expression
                consumeToken(TokenType.INTERVAL);

                if (op.type == TokenType.PLUS) {
                    builder.setName("date_add");
                } else {
                    builder.setName("date_sub");
                }

                builder.addParam(mulDivExpr()); // amount

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
            } else {
                // add/sub expression
                builder.setName(op.value);
                builder.addParam(mulDivExpr());
            }
            lhs = Expr.newBuilder().setType(Expr.Type.OPERATOR).setOperator(builder.build()).build();
        }
    }

    Expr addSubExpr() {
        return parseLeftAssocBinaryOpExpr(new TokenType[] { TokenType.PLUS, TokenType.MINUS }, this::addSubOrIntervalExpr);
    }

    Expr shiftExpr() {
        return parseLeftAssocBinaryOpExpr(new TokenType[] { TokenType.LSHIFT, TokenType.RSHIFT }, this::addSubOrIntervalExpr);
    }

    Expr bitExpr() {
        return parseLeftAssocBinaryOpExpr(new TokenType[] { TokenType.BITAND, TokenType.BITOR, TokenType.BITXOR }, this::shiftExpr);
    }

    Expr compExpr() {
        return parseLeftAssocBinaryOpExpr(new TokenType[] { TokenType.GE, TokenType.GT, TokenType.LE, TokenType.LT, TokenType.EQ, TokenType.NE }, this::bitExpr);
    }

    Expr ilriExpr() {
        Expr lhs = compExpr();
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
                    params.addAll(parenExprList());
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
                    params.addAll(parenExprList());
                    break;
                default:
                    if (isNot) {
                        throw new WrongArgumentException("Unknown token after NOT at pos: " + this.tokenPos);
                    }
                    opName = null; // not an ILRI operator
            }
            if (opName != null) {
                if (isNot) {
                    opName = opName + "_not";
                }
                Operator.Builder builder = Operator.newBuilder().setName(opName).addAllParam(params);
                Expr opExpr = Expr.newBuilder().setType(Expr.Type.OPERATOR).setOperator(builder.build()).build();
                return opExpr;
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
     * @return an X-protocol expression tree
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
     * Parse an ORDER BY specification which is a comma-separated list of expressions, each may be optionally suffixed by ASC/DESC.
     */
    public List<Order> parseOrderSpec() {
        List<Order> orderSpec = new ArrayList<>();
        while (this.tokenPos < this.tokens.size()) {
            if (this.tokenPos > 0) {
                consumeToken(TokenType.COMMA);
            }
            Order.Builder builder = Order.newBuilder();
            builder.setField(expr());
            if (currentTokenTypeEquals(TokenType.ORDERBY_ASC)) {
                consumeToken(TokenType.ORDERBY_ASC);
                builder.setDirection(Order.Direction.ASC);
            } else if (currentTokenTypeEquals(TokenType.ORDERBY_DESC)) {
                consumeToken(TokenType.ORDERBY_DESC);
                builder.setDirection(Order.Direction.DESC);
            }
            orderSpec.add(builder.build());
        }
        return orderSpec;
    }

    private List<Expr> placeholderValues;
    /**
     * Parse the string and perform inline replacement of placeholders with the given Exprs.
     *
     * @param exprs an indexed list of expressions to insert in place of the placeholders
     * @return an X-protocol expression tree with no placeholders
     */
    public Expr parseReplacePlaceholders(List<Expr> exprs) {
        this.placeholderValues = exprs;
        return parse();
    }
}
