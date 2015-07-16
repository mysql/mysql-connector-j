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
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Any;
import com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Scalar;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Order;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.ColumnIdentifier;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.DocumentPathItem;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.Expr;

/**
 * Expression parser tests.
 */
public class ExprParserTest {

    /**
     * Check that a string doesn't parse.
     */
    private void checkBadParse(String s) {
        try {
            Expr e = new ExprParser(s).parse();
            System.err.println("Parsed as: " + e);
            fail("Expected exception while parsing: '" + s + "'");
        } catch (WrongArgumentException ex) {
            // expected
        }
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
        checkBadParse("a@**");
        checkBadParse("a.b.c.d > 1");
        checkBadParse("a@[1.1]");
        checkBadParse("a@[-1]");
        checkBadParse("a@1");
        checkBadParse("a@.1");
        checkBadParse("a@a");
        checkBadParse("a@.+");
        checkBadParse("a@(x)");
        checkBadParse("\"xyz");
        checkBadParse("x between 1");
        checkBadParse("x.1 > 1");
        checkBadParse("x@ > 1");
        checkBadParse(":");
        checkBadParse(":x");
        checkBadParse(":1.1");
        // TODO: test bad JSON identifiers (quoting?)
    }

    /**
     * Check that a string parses and is reconstituted as a string that we expect. Futher we parse the canonical version to make sure it doesn't change.
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
        checkParseRoundTrip("now () - interval '10:20' hour_MiNuTe", "date_sub(now(), \"10:20\", \"hour_minute\")");
        checkParseRoundTrip("now () - interval 1 hour - interval 2 minute - interval 3 second",
                "date_sub(date_sub(date_sub(now(), 1, \"hour\"), 2, \"minute\"), 3, \"second\")");
        // this needs parens around 1+1 in interval expression
        checkParseRoundTrip("a + interval 1 hour + 1 + interval (1 + 1) second", "date_add((date_add(a, 1, \"hour\") + 1), (1 + 1), \"second\")");
        checkParseRoundTrip("a + interval 1 hour + 1 + interval 1 * 1 second", "date_add((date_add(a, 1, \"hour\") + 1), (1 * 1), \"second\")");
        checkParseRoundTrip("now () - interval -2 day", "date_sub(now(), -2, \"day\")"); // interval exprs compile to date_add/date_sub calls
        checkParseRoundTrip("1", "1");
        checkParseRoundTrip("1e1", "10.0");
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
        checkParseRoundTrip("(a + b) * c + d", "(((a + b) * c) + d)");
        checkParseRoundTrip("(field not in ('a',func('b', 2.0),'c'))", "field not in(\"a\", func(\"b\", 2.0), \"c\")");
        checkParseRoundTrip("jess.age beTwEEn 30 and death", "(jess.age between 30 AND death)");
        checkParseRoundTrip("jess.age not BeTweeN 30 and death", "(jess.age not between 30 AND death)");
        checkParseRoundTrip("a + b * c + d", "((a + (b * c)) + d)");
        checkParseRoundTrip("x > 10 and Y >= -20", "((x > 10) and (Y >= -20))");
        checkParseRoundTrip("a is true and b is null and C + 1 > 40 and (time == now() or hungry())",
                "((((a is TRUE) and (b is NULL)) and ((C + 1) > 40)) and ((time == now()) or hungry()))");
        checkParseRoundTrip("a + b + -c > 2", "(((a + b) + -c) > 2)");
        checkParseRoundTrip("now () + b + c > 2", "(((now() + b) + c) > 2)");
        checkParseRoundTrip("now () + @.b + c > 2", "(((now() + @.b) + c) > 2)");
        checkParseRoundTrip("now () - interval +2 day > some_other_time() or something_else IS NOT NULL",
                "((date_sub(now(), 2, \"day\") > some_other_time()) or is_not(something_else, NULL))");
        checkParseRoundTrip("\"two quotes to one\"\"\"", null);
        checkParseRoundTrip("'two quotes to one'''", "\"two quotes to one'\"");
        checkParseRoundTrip("'different quote \"'", "\"different quote \"\"\"");
        checkParseRoundTrip("\"different quote '\"", "\"different quote '\"");
        checkParseRoundTrip("`ident`", "ident"); // doesn't need quoting
        checkParseRoundTrip("`ident```", "`ident```");
        checkParseRoundTrip("`ident\"'`", "`ident\"'`");
        checkParseRoundTrip(":0 > x and func(:3, :2, :1)", "((:0 > x) and func(:3, :2, :1))");
        checkParseRoundTrip("a > now() + interval (2 + x) MiNuTe", "(a > date_add(now(), (2 + x), \"minute\"))");
        checkParseRoundTrip("a between 1 and 2", "(a between 1 AND 2)");
        checkParseRoundTrip("a not between 1 and 2", "(a not between 1 AND 2)");
        checkParseRoundTrip("a in (1,2,a.b(3),4,5,x)", "a in(1, 2, a.b(3), 4, 5, x)");
        checkParseRoundTrip("a not in (1,2,3,4,5,@.x)", "a not in(1, 2, 3, 4, 5, @.x)");
        checkParseRoundTrip("a like b escape c", "a like b ESCAPE c");
        checkParseRoundTrip("a not like b escape c", "a not like b ESCAPE c");
        checkParseRoundTrip("(1 + 3) in (3, 4, 5)", "(1 + 3) in(3, 4, 5)");
        checkParseRoundTrip("`a crazy \"function\"``'name'`(1 + 3) in (3, 4, 5)", "`a crazy \"function\"``'name'`((1 + 3)) in(3, 4, 5)");
        checkParseRoundTrip("a@.b", "a@.b");
        checkParseRoundTrip("a@.*", "a@.*");
        checkParseRoundTrip("a@[0].*", "a@[0].*");
        checkParseRoundTrip("a@**[0].*", "a@**[0].*");
        checkParseRoundTrip("@._id", "@._id");
        checkParseRoundTrip("@._id == :0", "(@._id == :0)");
        // TODO: this isn't serialized correctly by the unparser
        //checkParseRoundTrip("a@.b[0][0].c**.d.\"a weird\\\"key name\"", "");
    }

    /**
     * Test a basic example of placeholder replacement.
     */
    @Test
    public void testBasicPlaceholderReplacement() {
        String criteria = "name == :1 and age > :0";
        List<Expr> paramValues = new ArrayList<>();
        paramValues.add(ExprUtil.buildLiteralScalar(1));
        paramValues.add(ExprUtil.buildLiteralScalar("Niccolo"));
        Expr expr = new ExprParser(criteria).parseReplacePlaceholders(paramValues);
        String canonicalized = ExprUnparser.exprToString(expr);
        assertEquals("((name == \"Niccolo\") and (age > 1))", canonicalized);
    }

    /**
     * Explicit test inspecting the expression tree.
     */
    @Test
    public void testExprTree() {
        Expr expr = new ExprParser("a like 'xyz' and @.count > 10 + 1").parse();
        assertEquals(Expr.Type.OPERATOR, expr.getType());
        assertEquals("and", expr.getOperator().getName());
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
        assertEquals(Any.Type.SCALAR, literalXyz.getConstant().getType());
        Scalar scalarXyz = literalXyz.getConstant().getScalar();
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
        Scalar addLeftScalar = addition.getOperator().getParam(0).getConstant().getScalar();
        Scalar addRightScalar = addition.getOperator().getParam(1).getConstant().getScalar();
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
        assertEquals("a", ExprUnparser.exprToString(o1.getField()));
        Order o2 = orderSpec.get(1);
        assertTrue(o2.hasDirection());
        assertEquals(Order.Direction.DESC, o2.getDirection());
        assertEquals("b", ExprUnparser.exprToString(o2.getField()));
    }

    @Test
    public void testOrderByParserComplexExpressions() {
        List<Order> orderSpec = new ExprParser("field not in ('a',func('b', 2.0),'c') desc, 1-a@**[0].*, now () + @.b + c > 2 asc").parseOrderSpec();
        assertEquals(3, orderSpec.size());
        Order o1 = orderSpec.get(0);
        assertTrue(o1.hasDirection());
        assertEquals(Order.Direction.DESC, o1.getDirection());
        assertEquals("field not in(\"a\", func(\"b\", 2.0), \"c\")", ExprUnparser.exprToString(o1.getField()));
        Order o2 = orderSpec.get(1);
        assertFalse(o2.hasDirection());
        assertEquals("(1 - a@**[0].*)", ExprUnparser.exprToString(o2.getField()));
        Order o3 = orderSpec.get(2);
        assertTrue(o3.hasDirection());
        assertEquals(Order.Direction.ASC, o3.getDirection());
        assertEquals("(((now() + @.b) + c) > 2)", ExprUnparser.exprToString(o3.getField()));
    }
}
