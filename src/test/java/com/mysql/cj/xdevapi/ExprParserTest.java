/*
 * Copyright (c) 2015, 2023, Oracle and/or its affiliates.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.x.protobuf.MysqlxCrud.Column;
import com.mysql.cj.x.protobuf.MysqlxCrud.Order;
import com.mysql.cj.x.protobuf.MysqlxCrud.Projection;
import com.mysql.cj.x.protobuf.MysqlxDatatypes.Scalar;
import com.mysql.cj.x.protobuf.MysqlxExpr.ColumnIdentifier;
import com.mysql.cj.x.protobuf.MysqlxExpr.DocumentPathItem;
import com.mysql.cj.x.protobuf.MysqlxExpr.Expr;
import com.mysql.cj.x.protobuf.MysqlxExpr.Object;
import com.mysql.cj.x.protobuf.MysqlxExpr.Object.ObjectField;

/**
 * Expression parser tests.
 */
public class ExprParserTest {

    /**
     * Check that a string doesn't parse.
     *
     * @param s
     */
    private void checkBadParse(String s) {
        assertThrows(WrongArgumentException.class, () -> {
            Expr e = new ExprParser(s).parse();
            System.err.println("Parsed as: " + e);
        }, "Expected exception while parsing: '" + s + "'");
    }

    @Test
    public void testUnparseables() {
        checkBadParse("1ee1");
        checkBadParse("1 + ");
        checkBadParse("x 1,2,3)");
        checkBadParse("x(1,2,3");
        checkBadParse("x(1 2,3)");
        checkBadParse("x(1,, 2,3)");
        checkBadParse("x not y");
        checkBadParse("x like");
        checkBadParse("like");
        checkBadParse("like x");
        checkBadParse("x + interval 1 MACROsecond");
        checkBadParse("x + interval 1 + 1");
        checkBadParse("x * interval 1 hour");
        checkBadParse("1.1.1");
        checkBadParse("a->$**");
        checkBadParse("a.b.c.d > 1");
        checkBadParse("a->$[1.1]");
        checkBadParse("a->$[-1]");
        checkBadParse("a->$1");
        checkBadParse("a->$.1");
        checkBadParse("a->$a");
        checkBadParse("a->$.+");
        checkBadParse("a->$(x)");
        checkBadParse("\"xyz");
        checkBadParse("x between 1");
        checkBadParse("x.1 > 1");
        checkBadParse("x->$ > 1");
        checkBadParse(":>1");
        checkBadParse(":1.1");
        checkBadParse("cast(x as varchar)");
        checkBadParse("not");
        checkBadParse("->$.a[-1]");
        checkBadParse("x->'not a docpath'");
        // TODO: test bad JSON identifiers (quoting?)
    }

    /**
     * Check that a string parses and is reconstituted as a string that we expect. Further we parse the canonical version to make sure it doesn't change.
     *
     * @param input
     * @param expected
     */
    private void checkParseRoundTrip(String input, String expected) {
        if (expected == null) {
            expected = input;
        }
        Expr expr = new ExprParser(input).parse();
        String canonicalized = ExprUnparser.exprToString(expr);
        assertEquals(expected, canonicalized);

        // System.err.println("Canonicalized: " + canonicalized);
        Expr expr2 = new ExprParser(canonicalized).parse();
        String recanonicalized = ExprUnparser.exprToString(expr2);
        assertEquals(expected, recanonicalized);
    }

    /**
     * Test that expressions parsed and serialize back to the expected form.
     */
    @Test
    public void testRoundTrips() {
        checkParseRoundTrip("now () - interval '10:20' hour_MiNuTe", "date_sub(now(), \"10:20\", \"HOUR_MINUTE\")");
        checkParseRoundTrip("now () - interval 1 hour - interval 2 minute - interval 3 second",
                "date_sub(date_sub(date_sub(now(), 1, \"HOUR\"), 2, \"MINUTE\"), 3, \"SECOND\")");
        // this needs parens around 1+1 in interval expression
        checkParseRoundTrip("a + interval 1 hour + 1 + interval (1 + 1) second", "(date_add(a, 1, \"HOUR\") + date_add(1, (1 + 1), \"SECOND\"))");
        checkParseRoundTrip("a + interval 1 hour + 1 + interval 1 * 1 second", "(date_add(a, 1, \"HOUR\") + date_add(1, (1 * 1), \"SECOND\"))");
        checkParseRoundTrip("now () - interval -2 day", "date_sub(now(), -2, \"DAY\")"); // interval exprs compile to date_add/date_sub calls
        checkParseRoundTrip("1", "1");
        checkParseRoundTrip("1^0", "(1 ^ 0)");
        checkParseRoundTrip("1e1", "10.0");
        checkParseRoundTrip("-1e1", "-10.0");
        checkParseRoundTrip("!0", "!0");
        checkParseRoundTrip("1e4", "10000.0");
        checkParseRoundTrip("12e-4", "0.0012");
        checkParseRoundTrip("a + 314.1592e-2", "(a + 3.141592)");
        checkParseRoundTrip("a + 0.0271e+2", "(a + 2.71)");
        checkParseRoundTrip("a + 0.0271e2", "(a + 2.71)");
        checkParseRoundTrip("10+1", "(10 + 1)");
        checkParseRoundTrip("(abC == 1)", "(abC == 1)");
        checkParseRoundTrip("(abC = 1)", "(abC == 1)");
        checkParseRoundTrip("(Func(abc)==1)", "(Func(abc) == 1)");
        checkParseRoundTrip("(abc == \"jess\")", "(abc == \"jess\")");
        checkParseRoundTrip("(abc == \"with \\\"\")", "(abc == \"with \"\"\")"); // we escape with two internal quotes
        checkParseRoundTrip("(abc != .10)", "(abc != 0.1)");
        checkParseRoundTrip("(abc != \"xyz\")", "(abc != \"xyz\")");
        checkParseRoundTrip("a + b * c + d", "((a + (b * c)) + d)"); // shows precedence and associativity
        checkParseRoundTrip("a * b + c * d", "((a * b) + (c * d))");
        checkParseRoundTrip("(a + b) * c + d", "(((a + b) * c) + d)");
        checkParseRoundTrip("(field not in ('a',func('b', 2.0),'c'))", "field not in(\"a\", func(\"b\", 2.0), \"c\")");
        checkParseRoundTrip("jess.age beTwEEn 30 and death", "(jess.age between 30 AND death)");
        checkParseRoundTrip("jess.age not BeTweeN 30 and death", "(jess.age not between 30 AND death)");
        checkParseRoundTrip("a + b * c + d", "((a + (b * c)) + d)");
        checkParseRoundTrip("x > 10 and Y >= -20", "((x > 10) && (Y >= -20))");
        checkParseRoundTrip("a is true and b is null and C + 1 > 40 and (thetime == now() or hungry())",
                "((((a is TRUE) && (b is NULL)) && ((C + 1) > 40)) && ((thetime == now()) || hungry()))");
        checkParseRoundTrip("a + b + -c > 2", "(((a + b) + -c) > 2)");
        checkParseRoundTrip("a + b - +c > 2", "(((a + b) - +c) > 2)");
        checkParseRoundTrip("now () + b + c > 2", "(((now() + b) + c) > 2)");
        checkParseRoundTrip("now () + $.b + c > 2", "(((now() + $.b) + c) > 2)");
        checkParseRoundTrip("now () - interval +2 day > some_other_time() or something_else IS NOT NULL",
                "((date_sub(now(), 2, \"DAY\") > some_other_time()) || is_not(something_else, NULL))");
        checkParseRoundTrip("\"two quotes to one\"\"\"", null);
        checkParseRoundTrip("'two quotes to one'''", "\"two quotes to one'\"");
        checkParseRoundTrip("'different quote \"'", "\"different quote \"\"\"");
        checkParseRoundTrip("\"different quote '\"", "\"different quote '\"");
        checkParseRoundTrip("`ident`", "ident"); // doesn't need quoting
        checkParseRoundTrip("`ident```", "`ident```");
        checkParseRoundTrip("`ident\"'`", "`ident\"'`");
        checkParseRoundTrip(":0 > x and func(:3, :2, :1)", "((:0 > x) && func(:1, :2, :3))"); // serialized in order of position (needs mapped externally)
        checkParseRoundTrip("a > now() + interval (2 + x) MiNuTe", "(a > date_add(now(), (2 + x), \"MINUTE\"))");
        checkParseRoundTrip("a between 1 and 2", "(a between 1 AND 2)");
        checkParseRoundTrip("a not between 1 and 2", "(a not between 1 AND 2)");
        checkParseRoundTrip("a in (1,2,a.b(3),4,5,x)", "a in(1, 2, a.b(3), 4, 5, x)");
        checkParseRoundTrip("a not in (1,2,3,4,5,$.x)", "a not in(1, 2, 3, 4, 5, $.x)");
        checkParseRoundTrip("a like b escape c", "a like b ESCAPE c");
        checkParseRoundTrip("a not like b escape c", "a not like b ESCAPE c");
        checkParseRoundTrip("(1 + 3) in (3, 4, 5)", "(1 + 3) in(3, 4, 5)");
        checkParseRoundTrip("`a crazy \"function\"``'name'`(1 + 3) in (3, 4, 5)", "`a crazy \"function\"``'name'`((1 + 3)) in(3, 4, 5)");
        checkParseRoundTrip("a->$.b", "a->$.b");
        checkParseRoundTrip("a->'$.b'", "a->$.b");
        checkParseRoundTrip("a->$.\"bcd\"", "a->$.bcd");
        checkParseRoundTrip("a->$.*", "a->$.*");
        checkParseRoundTrip("a->$[0].*", "a->$[0].*");
        checkParseRoundTrip("a->$[*].*", "a->$[*].*");
        checkParseRoundTrip("a->$**[0].*", "a->$**[0].*");
        checkParseRoundTrip("$._id", "$._id");
        checkParseRoundTrip("$._id == :0", "($._id == :0)");
        checkParseRoundTrip("'Monty!' REGEXP '.*'", "(\"Monty!\" regexp \".*\")");
        checkParseRoundTrip("a regexp b regexp c", "((a regexp b) regexp c)");
        checkParseRoundTrip("a + b + c", "((a + b) + c)");
        checkParseRoundTrip("a + cast(b as json)", "(a + cast(b AS JSON))");
        checkParseRoundTrip("a + cast(b as decimal)", "(a + cast(b AS DECIMAL))");
        checkParseRoundTrip("a + cast(b as decimal(2))", "(a + cast(b AS DECIMAL(2)))");
        checkParseRoundTrip("a + cast(b as decimal(1, 2))", "(a + cast(b AS DECIMAL(1,2)))");
        checkParseRoundTrip("a + cast(b as binary)", "(a + cast(b AS BINARY))");
        checkParseRoundTrip("a + cast(b as DaTe)", "(a + cast(b AS DATE))");
        checkParseRoundTrip("a + cast(b as char)", "(a + cast(b AS CHAR))");
        checkParseRoundTrip("a + cast(b as DaTeTiMe)", "(a + cast(b AS DATETIME))");
        checkParseRoundTrip("a + cast(b as time)", "(a + cast(b AS TIME))");
        checkParseRoundTrip("a + cast(b as binary(3))", "(a + cast(b AS BINARY(3)))");
        checkParseRoundTrip("a + cast(b as unsigned)", "(a + cast(b AS UNSIGNED))");
        checkParseRoundTrip("a + cast(b as unsigned integer)", "(a + cast(b AS UNSIGNED))");
        checkParseRoundTrip("a is true or a is false", "((a is TRUE) || (a is FALSE))");
        checkParseRoundTrip("colId + .1e-3", "(colId + 1.0E-4)");
        // TODO: this isn't serialized correctly by the unparser
        //checkParseRoundTrip("a@.b[0][0].c**.d.\"a weird\\\"key name\"", "");
        // star function
        checkParseRoundTrip("*", "*");
        checkParseRoundTrip("count(*) + 1", "(count(*) + 1)");
        checkParseRoundTrip("foo\u003Dbar", "(foo == bar)");
        checkParseRoundTrip("\"foo\"", "\"foo\"");
        checkParseRoundTrip("\"foo\\\"bar\"", "\"foo\"\"bar\"");
        checkParseRoundTrip("\"foo\nbar\"", "\"foo\nbar\""); // TODO: Could it be that the unparsed \n should be escaped?
    }

    /**
     * Explicit test inspecting the expression tree.
     */
    @Test
    public void testExprTree() {
        Expr expr = new ExprParser("a like 'xyz' and $.count > 10 + 1").parse();
        assertEquals(Expr.Type.OPERATOR, expr.getType());
        assertEquals("&&", expr.getOperator().getName());
        assertEquals(2, expr.getOperator().getParamCount());

        // check left side of AND: (a like 'xyz')
        Expr andLeft = expr.getOperator().getParam(0);
        assertEquals(Expr.Type.OPERATOR, andLeft.getType());
        assertEquals("like", andLeft.getOperator().getName());
        assertEquals(2, andLeft.getOperator().getParamCount());
        Expr identA = andLeft.getOperator().getParam(0);
        assertEquals(Expr.Type.IDENT, identA.getType());
        assertEquals("a", identA.getIdentifier().getName());
        Expr literalXyz = andLeft.getOperator().getParam(1);
        assertEquals(Expr.Type.LITERAL, literalXyz.getType());
        Scalar scalarXyz = literalXyz.getLiteral();
        assertEquals(Scalar.Type.V_STRING, scalarXyz.getType());
        assertEquals("xyz", scalarXyz.getVString().getValue().toStringUtf8());

        // check right side of AND: (@.count > 10 + 1)
        Expr andRight = expr.getOperator().getParam(1);
        assertEquals(Expr.Type.OPERATOR, andRight.getType());
        assertEquals(">", andRight.getOperator().getName());
        assertEquals(2, andRight.getOperator().getParamCount());
        Expr countDocPath = andRight.getOperator().getParam(0);
        assertEquals(Expr.Type.IDENT, countDocPath.getType());
        ColumnIdentifier countId = countDocPath.getIdentifier();
        assertFalse(countId.hasName());
        assertFalse(countId.hasTableName());
        assertFalse(countId.hasSchemaName());
        assertEquals(1, countId.getDocumentPathCount());
        assertEquals(DocumentPathItem.Type.MEMBER, countId.getDocumentPath(0).getType());
        assertEquals("count", countId.getDocumentPath(0).getValue());
        Expr addition = andRight.getOperator().getParam(1);
        Scalar addLeftScalar = addition.getOperator().getParam(0).getLiteral();
        Scalar addRightScalar = addition.getOperator().getParam(1).getLiteral();
        assertEquals(Expr.Type.OPERATOR, addition.getType());
        assertEquals("+", addition.getOperator().getName());
        assertEquals(2, addition.getOperator().getParamCount());
        assertEquals(Expr.Type.LITERAL, addition.getOperator().getParam(0).getType());
        assertEquals(Expr.Type.LITERAL, addition.getOperator().getParam(1).getType());
        assertEquals(Scalar.Type.V_SINT, addLeftScalar.getType());
        assertEquals(Scalar.Type.V_SINT, addRightScalar.getType());
        assertEquals(10, addLeftScalar.getVSignedInt());
        assertEquals(1, addRightScalar.getVSignedInt());
    }

    @Test
    public void testOrderByParserBasic() {
        List<Order> orderSpec = new ExprParser("a, b desc").parseOrderSpec();
        assertEquals(2, orderSpec.size());
        Order o1 = orderSpec.get(0);
        assertFalse(o1.hasDirection());
        assertEquals("a", ExprUnparser.exprToString(o1.getExpr()));
        Order o2 = orderSpec.get(1);
        assertTrue(o2.hasDirection());
        assertEquals(Order.Direction.DESC, o2.getDirection());
        assertEquals("b", ExprUnparser.exprToString(o2.getExpr()));
    }

    @Test
    public void testOrderByParserComplexExpressions() {
        List<Order> orderSpec = new ExprParser("field not in ('a',func('b', 2.0),'c') desc, 1-a->$**[0].*, now () + $.b + c > 2 asc").parseOrderSpec();
        assertEquals(3, orderSpec.size());
        Order o1 = orderSpec.get(0);
        assertTrue(o1.hasDirection());
        assertEquals(Order.Direction.DESC, o1.getDirection());
        assertEquals("field not in(\"a\", func(\"b\", 2.0), \"c\")", ExprUnparser.exprToString(o1.getExpr()));
        Order o2 = orderSpec.get(1);
        assertFalse(o2.hasDirection());
        assertEquals("(1 - a->$**[0].*)", ExprUnparser.exprToString(o2.getExpr()));
        Order o3 = orderSpec.get(2);
        assertTrue(o3.hasDirection());
        assertEquals(Order.Direction.ASC, o3.getDirection());
        assertEquals("(((now() + $.b) + c) > 2)", ExprUnparser.exprToString(o3.getExpr()));
    }

    @Test
    public void testNamedPlaceholders() {
        ExprParser parser = new ExprParser("a = :a and b = :b and (c = 'x' or d = :b)");
        assertEquals("IDENT(a)", parser.tokens.get(0).toString());
        assertEquals("EQ", parser.tokens.get(1).toString());
        Expr e = parser.parse();
        assertEquals(new Integer(0), parser.placeholderNameToPosition.get("a"));
        assertEquals(new Integer(1), parser.placeholderNameToPosition.get("b"));
        assertEquals(2, parser.positionalPlaceholderCount);

        Expr aEqualsPlaceholder = e.getOperator().getParam(0).getOperator().getParam(0).getOperator().getParam(1);
        assertEquals(Expr.Type.PLACEHOLDER, aEqualsPlaceholder.getType());
        assertEquals(0, aEqualsPlaceholder.getPosition());
        Expr bEqualsPlaceholder = e.getOperator().getParam(0).getOperator().getParam(1).getOperator().getParam(1);
        assertEquals(Expr.Type.PLACEHOLDER, bEqualsPlaceholder.getType());
        assertEquals(1, bEqualsPlaceholder.getPosition());
        Expr dEqualsPlaceholder = e.getOperator().getParam(1).getOperator().getParam(1).getOperator().getParam(1);
        assertEquals(Expr.Type.PLACEHOLDER, dEqualsPlaceholder.getType());
        assertEquals(1, dEqualsPlaceholder.getPosition());
    }

    @Test
    public void testNumberedPlaceholders() {
        ExprParser parser = new ExprParser("a == :1 and b == :3 and (c == :2 or d == :2)");
        Expr e = parser.parse();
        assertEquals(new Integer(0), parser.placeholderNameToPosition.get("1"));
        assertEquals(new Integer(1), parser.placeholderNameToPosition.get("3"));
        assertEquals(new Integer(2), parser.placeholderNameToPosition.get("2"));
        assertEquals(3, parser.positionalPlaceholderCount);

        Expr aEqualsPlaceholder = e.getOperator().getParam(0).getOperator().getParam(0).getOperator().getParam(1);
        assertEquals(Expr.Type.PLACEHOLDER, aEqualsPlaceholder.getType());
        assertEquals(0, aEqualsPlaceholder.getPosition());
        Expr bEqualsPlaceholder = e.getOperator().getParam(0).getOperator().getParam(1).getOperator().getParam(1);
        assertEquals(Expr.Type.PLACEHOLDER, bEqualsPlaceholder.getType());
        assertEquals(1, bEqualsPlaceholder.getPosition());
        Expr cEqualsPlaceholder = e.getOperator().getParam(1).getOperator().getParam(0).getOperator().getParam(1);
        assertEquals(Expr.Type.PLACEHOLDER, cEqualsPlaceholder.getType());
        assertEquals(2, cEqualsPlaceholder.getPosition());
        Expr dEqualsPlaceholder = e.getOperator().getParam(1).getOperator().getParam(1).getOperator().getParam(1);
        assertEquals(Expr.Type.PLACEHOLDER, dEqualsPlaceholder.getType());
        assertEquals(2, dEqualsPlaceholder.getPosition());
    }

    @Test
    public void testUnnumberedPlaceholders() {
        ExprParser parser = new ExprParser("a = ? and b = ? and (c = 'x' or d = ?)");
        Expr e = parser.parse();
        assertEquals(new Integer(0), parser.placeholderNameToPosition.get("0"));
        assertEquals(new Integer(1), parser.placeholderNameToPosition.get("1"));
        assertEquals(new Integer(2), parser.placeholderNameToPosition.get("2"));
        assertEquals(3, parser.positionalPlaceholderCount);

        Expr aEqualsPlaceholder = e.getOperator().getParam(0).getOperator().getParam(0).getOperator().getParam(1);
        assertEquals(Expr.Type.PLACEHOLDER, aEqualsPlaceholder.getType());
        assertEquals(0, aEqualsPlaceholder.getPosition());
        Expr bEqualsPlaceholder = e.getOperator().getParam(0).getOperator().getParam(1).getOperator().getParam(1);
        assertEquals(Expr.Type.PLACEHOLDER, bEqualsPlaceholder.getType());
        assertEquals(1, bEqualsPlaceholder.getPosition());
        Expr dEqualsPlaceholder = e.getOperator().getParam(1).getOperator().getParam(1).getOperator().getParam(1);
        assertEquals(Expr.Type.PLACEHOLDER, dEqualsPlaceholder.getType());
        assertEquals(2, dEqualsPlaceholder.getPosition());
    }

    @Test
    public void testJsonLiteral() {
        Expr e = new ExprParser("{'a':1, 'b':\"a string\"}").parse();

        assertEquals("{'a':1, 'b':\"a string\"}", ExprUnparser.exprToString(e));

        assertEquals(Expr.Type.OBJECT, e.getType());
        Object o = e.getObject();
        assertEquals(2, o.getFldCount());
        ObjectField of;

        of = o.getFld(0);
        assertEquals("a", of.getKey());
        e = of.getValue();
        assertEquals(Expr.Type.LITERAL, e.getType());
        assertEquals(1, e.getLiteral().getVSignedInt());

        of = o.getFld(1);
        assertEquals("b", of.getKey());
        e = of.getValue();
        assertEquals(Expr.Type.LITERAL, e.getType());
        assertEquals("a string", e.getLiteral().getVString().getValue().toStringUtf8());
    }

    @Test
    public void testTrivialDocumentProjection() {
        List<Projection> proj;

        proj = new ExprParser("$.a as a").parseDocumentProjection();
        assertEquals(1, proj.size());
        assertTrue(proj.get(0).hasAlias());
        assertEquals("a", proj.get(0).getAlias());

        proj = new ExprParser("$.a as a, $.b as b, $.c as c").parseDocumentProjection();
    }

    @Test
    public void testExprAsPathDocumentProjection() {
        List<Projection> projList = new ExprParser("$.a as b, (1 + 1) * 100 as x, 2 as j42").parseDocumentProjection();

        assertEquals(3, projList.size());

        // check @.a as b
        Projection proj = projList.get(0);
        List<DocumentPathItem> paths = proj.getSource().getIdentifier().getDocumentPathList();
        assertEquals(1, paths.size());
        assertEquals(DocumentPathItem.Type.MEMBER, paths.get(0).getType());
        assertEquals("a", paths.get(0).getValue());

        assertEquals("b", proj.getAlias());

        // check (1 + 1) * 100 as x
        proj = projList.get(1);
        assertEquals("((1 + 1) * 100)", ExprUnparser.exprToString(proj.getSource()));
        assertEquals("x", proj.getAlias());

        // check 2 as j42
        proj = projList.get(2);
        assertEquals("2", ExprUnparser.exprToString(proj.getSource()));
        assertEquals("j42", proj.getAlias());
    }

    @Test
    public void testJsonConstructorAsDocumentProjection() {
        // same as we use in find().field("{...}")
        String projString = "{'a':'value for a', 'b':1+1, 'c'::bindvar, 'd':$.member[22], 'e':{'nested':'doc'}}";
        Projection proj = Projection.newBuilder().setSource(new ExprParser(projString, false).parse()).build();
        assertEquals(Expr.Type.OBJECT, proj.getSource().getType());

        Iterator<ObjectField> fields = proj.getSource().getObject().getFldList().iterator();

        Arrays.stream(new String[][] { new String[] { "a", "\"value for a\"" }, new String[] { "b", "(1 + 1)" }, new String[] { "c", ":0" },
                new String[] { "d", "$.member[22]" }, new String[] { "e", "{'nested':\"doc\"}" } }).forEach(pair -> {
                    ObjectField f = fields.next();
                    assertEquals(pair[0], f.getKey());
                    assertEquals(pair[1], ExprUnparser.exprToString(f.getValue()));
                });
        assertFalse(fields.hasNext());
    }

    @Test
    public void testJsonExprsInDocumentProjection() {
        // this is not a single doc as the project but multiple docs as embedded fields
        String projString = "{'a':1} as a, {'b':2} as b";
        List<Projection> projList = new ExprParser(projString).parseDocumentProjection();
        assertEquals(2, projList.size());
        // TODO: verification of remaining elements
    }

    @Test
    public void testTableInsertProjection() {
        Column col = new ExprParser("a").parseTableInsertField();
        assertEquals("a", col.getName());

        col = new ExprParser("`double weird `` string`").parseTableInsertField();
        assertEquals("double weird ` string", col.getName());
    }

    @Test
    public void testTableUpdateField() {
        ColumnIdentifier col;
        col = new ExprParser("a").parseTableUpdateField();
        assertEquals("a", col.getName());

        col = new ExprParser("b.c").parseTableUpdateField();
        assertEquals("b", col.getTableName());
        assertEquals("c", col.getName());

        col = new ExprParser("d.e->$.the_path[2]").parseTableUpdateField();
        assertEquals("d", col.getTableName());
        assertEquals("e", col.getName());
        assertEquals(2, col.getDocumentPathCount());
        assertEquals("the_path", col.getDocumentPath(0).getValue());
        assertEquals(2, col.getDocumentPath(1).getIndex());

        col = new ExprParser("`zzz\\``").parseTableUpdateField();
        assertEquals("zzz`", col.getName());
    }

    @Test
    public void testTrivialTableSelectProjection() {
        List<Projection> proj = new ExprParser("a, b as c").parseTableSelectProjection();
        assertEquals(2, proj.size());
        assertEquals("a", ExprUnparser.exprToString(proj.get(0).getSource()));
        assertFalse(proj.get(0).hasAlias());
        assertEquals("b", ExprUnparser.exprToString(proj.get(1).getSource()));
        assertTrue(proj.get(1).hasAlias());
        assertEquals("c", proj.get(1).getAlias());
    }

    @Test
    public void testStarTableSelectProjection() {
        List<Projection> proj = new ExprParser("*, b as c").parseTableSelectProjection();
        assertEquals(2, proj.size());
        assertEquals("*", ExprUnparser.exprToString(proj.get(0).getSource()));
        assertFalse(proj.get(0).hasAlias());
        assertEquals("b", ExprUnparser.exprToString(proj.get(1).getSource()));
        assertTrue(proj.get(1).hasAlias());
        assertEquals("c", proj.get(1).getAlias());
    }

    @Test
    public void testComplexTableSelectProjection() {
        String projectionString = "(1 + 1) * 100 as `one-o-two`, 'a is \\'a\\'' as `what is 'a'`";
        List<Projection> proj = new ExprParser(projectionString).parseTableSelectProjection();
        assertEquals(2, proj.size());

        assertEquals("((1 + 1) * 100)", ExprUnparser.exprToString(proj.get(0).getSource()));
        assertEquals("one-o-two", proj.get(0).getAlias());

        assertEquals("a is 'a'", proj.get(1).getSource().getLiteral().getVString().getValue().toStringUtf8());
        assertEquals("what is 'a'", proj.get(1).getAlias());
    }

    @Test
    public void testRandom() {
        // tests generated by the random expression generator
        checkParseRoundTrip("x - INTERVAL { } DAY_HOUR * { } + { }", "((date_sub(x, {}, \"DAY_HOUR\") * {}) + {})");
        checkParseRoundTrip(
                "NULL - INTERVAL $ ** [ 89 ] << { '' : { } - $ . V << { '' : { } + { } REGEXP ? << { } - { } < { } | { } << { '' : : 8 + : 26 ^ { } } + { } >> { } } || { } } & { } SECOND",
                "date_sub(NULL, (($**[89] << {'':((({} - $.V) << {'':(({} + {}) regexp ((:0 << ({} - {})) < ({} | (({} << ({'':((:1 + :2) ^ {})} + {})) >> {}))))}) || {})}) & {}), \"SECOND\")");
        // TODO: check the validity of this:
        // checkParseRoundTrip("_XJl . F ( `ho` $ [*] [*] - ~ ! { '' : { } LIKE { } && : rkc & 1 & y ->$ ** . d [*] [*] || { } ^ { } REGEXP { } } || { } - { } ^ { } < { } IN ( ) >= { } IN ( ) )", "");
    }

    @Test
    public void unqualifiedDocPaths() {
        Expr expr = new ExprParser("1 + b[0]", false).parse();
        assertEquals("(1 + $.b[0])", ExprUnparser.exprToString(expr));
        expr = new ExprParser("a.*", false).parse();
        assertEquals("$.a.*", ExprUnparser.exprToString(expr));
        expr = new ExprParser("bL . vT .*", false).parse();
        assertEquals("$.bL.vT.*", ExprUnparser.exprToString(expr));
        expr = new ExprParser("dd ** .X", false).parse();
        assertEquals("$.dd**.X", ExprUnparser.exprToString(expr));
    }

    /**
     * Fix for Bug#95503 (29821029), Operator IN not mapping consistently to the right X Plugin operation.
     */
    @Test
    public void testBug95503() {
        Expr expr;

        expr = new ExprParser("field IN (1, 2, 3)").parse();
        assertEquals(Expr.Type.OPERATOR, expr.getType());
        assertEquals("in", expr.getOperator().getName());

        expr = new ExprParser("field IN value").parse();
        assertEquals(Expr.Type.OPERATOR, expr.getType());
        assertEquals("cont_in", expr.getOperator().getName());

        expr = new ExprParser("field IN 123 + 456").parse();
        assertEquals(Expr.Type.OPERATOR, expr.getType());
        assertEquals("cont_in", expr.getOperator().getName());

        expr = new ExprParser("field IN [1, 2, 3]").parse();
        assertEquals(Expr.Type.OPERATOR, expr.getType());
        assertEquals("cont_in", expr.getOperator().getName());

        expr = new ExprParser("field IN {\"foo\": \"bar\"}").parse();
        assertEquals(Expr.Type.OPERATOR, expr.getType());
        assertEquals("cont_in", expr.getOperator().getName());
    }

    @Test
    public void testOverlaps() {
        Expr expr = new ExprParser("[1, 2, 3] OVERLAPS $.list", false).parse();
        assertEquals("[1, 2, 3] overlaps $.list", ExprUnparser.exprToString(expr));

        expr = new ExprParser("$.list OVERLAPS [4]", false).parse();
        assertEquals("$.list overlaps [4]", ExprUnparser.exprToString(expr));

        expr = new ExprParser("[1, 2, 3] NOT OVERLAPS $.list", false).parse();
        assertEquals("[1, 2, 3] not overlaps $.list", ExprUnparser.exprToString(expr));

        expr = new ExprParser("$.list NOT OVERLAPS [4]", false).parse();
        assertEquals("$.list not overlaps [4]", ExprUnparser.exprToString(expr));
    }

    @Test
    public void testOverlapsInProjection() {
        List<Projection> proj;

        proj = new ExprParser("$.`overlaps` as `overlaps`").parseDocumentProjection();
        assertEquals(1, proj.size());
        assertTrue(proj.get(0).hasAlias());
        assertEquals("overlaps", proj.get(0).getAlias());
    }

}
