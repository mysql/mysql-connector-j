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

package com.mysql.cj.mysqlx;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Scalar;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.ColumnIdentifier;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.DocumentPathItem;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.Expr;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.FunctionCall;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.Identifier;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.Object;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.Operator;

/**
 * Serializer utility for dealing with X Protocol expression trees.
 */
public class ExprUnparser {
    /**
     * List of operators which will be serialized as infix operators.
     */
    static Set<String> infixOperators = new HashSet<>();

    static {
        infixOperators.add("and");
        infixOperators.add("or");
    }

    // /**
    //  * Convert an "Any" (scalar) to a string.
    //  */
    // static String anyToString(Any e) {
    //     switch (e.getType()) {
    //         case SCALAR:
    //             return scalarToString(e.getScalar());
    //         default:
    //             throw new IllegalArgumentException("Unknown type tag: " + e.getType());
    //     }
    // }

    /**
     * Scalar to string.
     */
    static String scalarToString(Scalar e) {
        switch (e.getType()) {
            case V_SINT:
                return "" + e.getVSignedInt();
            case V_OCTETS:
                return "\"" + escapeLiteral(e.getVOctets().getValue().toStringUtf8()) + "\"";
            case V_STRING:
                return "\"" + escapeLiteral(e.getVString().getValue().toStringUtf8()) + "\"";
            case V_DOUBLE:
                return "" + e.getVDouble();
            case V_BOOL:
                return e.getVBool() ? "TRUE" : "FALSE";
            case V_NULL:
                return "NULL";
            default:
                throw new IllegalArgumentException("Unknown type tag: " + e.getType());
        }
    }

    /**
     * JSON document path to string.
     */
    static String documentPathToString(List<DocumentPathItem> items) {
        StringBuilder docPathString = new StringBuilder();
        for (DocumentPathItem item : items) {
            switch (item.getType()) {
                case MEMBER:
                    docPathString.append(".").append(quoteDocumentPathMember(item.getValue()));
                    break;
                case MEMBER_ASTERISK:
                    docPathString.append(".*");
                    break;
                case ARRAY_INDEX:
                    docPathString.append("[").append("" + Integer.toUnsignedLong(item.getIndex())).append("]");
                    break;
                case ARRAY_INDEX_ASTERISK:
                    docPathString.append("[*]");
                    break;
                case DOUBLE_ASTERISK:
                    docPathString.append("**");
                    break;
            }
        }
        return docPathString.toString();
    }

    /**
     * Column identifier (or JSON path) to string.
     */
    static String columnIdentifierToString(ColumnIdentifier e) {
        if (e.hasName()) {
            String s = quoteIdentifier(e.getName());
            if (e.hasTableName()) {
                s = quoteIdentifier(e.getTableName()) + "." + s;
            }
            if (e.hasSchemaName()) {
                s = quoteIdentifier(e.getSchemaName()) + "." + s;
            }
            if (e.getDocumentPathCount() > 0) {
                s = s + "->$" + documentPathToString(e.getDocumentPathList());
            }
            return s;
        }
        return "$" + documentPathToString(e.getDocumentPathList());
    }

    /**
     * Function call to string.
     */
    static String functionCallToString(FunctionCall e) {
        Identifier i = e.getName();
        String s = quoteIdentifier(i.getName());
        if (i.hasSchemaName()) {
            s = quoteIdentifier(i.getSchemaName()) + "." + s;
        }
        s = s + "(";
        for (Expr p : e.getParamList()) {
            s += exprToString(p) + ", ";
        }
        s = s.replaceAll(", $", "");
        s += ")";
        return s;
    }

    /**
     * Create a string from a list of (already stringified) parameters. Surround by parens and separate by commas.
     */
    static String paramListToString(List<String> params) {
        String s = "(";
        boolean first = true;
        for (String param : params) {
            if (!first) {
                s += ", ";
            }
            first = false;
            s += param;
        }
        return s + ")";
    }

    /**
     * Convert an operator to a string. Includes special cases for chosen infix operators (AND, OR) and special forms such as LIKE and BETWEEN.
     */
    static String operatorToString(Operator e) {
        String name = e.getName();
        List<String> params = new ArrayList<>();
        for (Expr p : e.getParamList()) {
            params.add(exprToString(p));
        }
        if ("between".equals(name) || "not_between".equals(name)) {
            name = name.replaceAll("not_between", "not between");
            return String.format("(%s %s %s AND %s)", params.get(0), name, params.get(1), params.get(2));
        } else if ("in".equals(name) || "not_in".equals(name)) {
            name = name.replaceAll("not_in", "not in");
            return String.format("%s %s%s", params.get(0), name, paramListToString(params.subList(1, params.size())));
        } else if ("like".equals(name) || "not_like".equals(name)) {
            name = name.replaceAll("not_like", "not like");
            String s = String.format("%s %s %s", params.get(0), name, params.get(1));
            if (params.size() == 3) {
                s += " ESCAPE " + params.get(2);
            }
            return s;
        } else if ("regexp".equals(name) || "not_regexp".equals("name")) {
            name = name.replaceAll("not_regexp", "not regexp");
            return String.format("(%s %s %s)", params.get(0), name, params.get(1));
        } else if ("cast".equals(name)) {
            return String.format("cast(%s AS %s)", params.get(0), params.get(1).replaceAll("\"", ""));
        } else if ((name.length() < 3 || infixOperators.contains(name)) && params.size() == 2) {
            return String.format("(%s %s %s)", params.get(0), name, params.get(1));
        } else if (params.size() == 1) {
            return String.format("%s%s", name, params.get(0));
        } else if (params.size() == 0) {
            return name;
        } else {
            return name + paramListToString(params);
        }
    }

    static String objectToString(Object o) {
        String fields = o.getFldList().stream().map(
                f -> new StringBuilder().append("'").append(quoteJsonKey(f.getKey())).append("'").append(":").append(exprToString(f.getValue())).toString())
                .collect(Collectors.joining(", "));
        return new StringBuilder("{").append(fields).append("}").toString();
    }

    /**
     * Escape a string literal.
     */
    public static String escapeLiteral(String s) {
        return s.replaceAll("\"", "\"\"");
    }

    /**
     * Quote a named identifer.
     */
    public static String quoteIdentifier(String ident) {
        // TODO: make sure this is correct
        if (ident.contains("`") || ident.contains("\"") || ident.contains("'") || ident.contains("$") || ident.contains(".") || ident.contains("-")) {
            return "`" + ident.replaceAll("`", "``") + "`";
        }
        return ident;
    }

    public static String quoteJsonKey(String key) {
        return key.replaceAll("'", "\\\\'");
    }

    public static String quoteDocumentPathMember(String member) {
        if (!member.matches("[a-zA-Z0-9_]*")) {
            return "\"" + member.replaceAll("\"", "\\\\\"") + "\"";
        }
        return member;
    }

    /**
     * Serialize an expression to a string.
     */
    public static String exprToString(Expr e) {
        switch (e.getType()) {
            case LITERAL:
                return scalarToString(e.getLiteral());
            case IDENT:
                return columnIdentifierToString(e.getIdentifier());
            case FUNC_CALL:
                return functionCallToString(e.getFunctionCall());
            case OPERATOR:
                return operatorToString(e.getOperator());
            case PLACEHOLDER:
                return ":" + Integer.toUnsignedLong(e.getPosition());
            case OBJECT:
                return objectToString(e.getObject());
            default:
                throw new IllegalArgumentException("Unknown type tag: " + e.getType());
        }
    }
}
