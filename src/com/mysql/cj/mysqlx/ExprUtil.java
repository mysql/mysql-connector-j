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

import com.google.protobuf.ByteString;

import com.mysql.cj.api.x.Expression;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Collection;
import com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Any;
import com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Scalar;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.Expr;
import com.mysql.cj.x.json.JsonArray;
import com.mysql.cj.x.json.JsonDoc;

/**
 * Utilities to deal with Expr (and related) structures.
 *
 * @todo rename to ProtobufUtil(s)
 */
public class ExprUtil {
    /**
     * Proto-buf helper to build a LITERAL Expr with a Scalar NULL type.
     */
    public static Expr buildLiteralNullScalar() {
        return buildLiteralExpr(nullScalar());
    }

    /**
     * Proto-buf helper to build a LITERAL Expr with a Scalar DOUBLE type (wrapped in Any).
     */
    public static Expr buildLiteralScalar(double d) {
        return buildLiteralExpr(scalarOf(d));
    }

    /**
     * Proto-buf helper to build a LITERAL Expr with a Scalar SINT (signed int) type (wrapped in Any).
     */
    public static Expr buildLiteralScalar(long l) {
        return buildLiteralExpr(scalarOf(l));
    }

    /**
     * Proto-buf helper to build a LITERAL Expr with a Scalar STRING type (wrapped in Any).
     */
    public static Expr buildLiteralScalar(String str) {
        return buildLiteralExpr(scalarOf(str));
    }

    /**
     * Proto-buf helper to build a LITERAL Expr with a Scalar OCTETS type (wrapped in Any).
     */
    public static Expr buildLiteralScalar(byte[] bytes) {
        return buildLiteralExpr(scalarOf(bytes));
    }

    /**
     * Proto-buf helper to build a LITERAL Expr with a Scalar BOOL type (wrapped in Any).
     */
    public static Expr buildLiteralScalar(boolean b) {
        return buildLiteralExpr(scalarOf(b));
    }

    /**
     * Wrap an Any value in a LITERAL expression.
     */
    public static Expr buildLiteralExpr(Scalar scalar) {
        return Expr.newBuilder().setType(Expr.Type.LITERAL).setLiteral(scalar).build();
    }

    public static Scalar nullScalar() {
        return Scalar.newBuilder().setType(Scalar.Type.V_NULL).build();
    }

    public static Scalar scalarOf(double d) {
        return Scalar.newBuilder().setType(Scalar.Type.V_DOUBLE).setVDouble(d).build();
    }

    public static Scalar scalarOf(long l) {
        return Scalar.newBuilder().setType(Scalar.Type.V_SINT).setVSignedInt(l).build();
    }

    public static Scalar scalarOf(String str) {
        Scalar.String sstr = Scalar.String.newBuilder().setValue(ByteString.copyFromUtf8(str)).build();
        return Scalar.newBuilder().setType(Scalar.Type.V_STRING).setVString(sstr).build();
    }

    public static Scalar scalarOf(byte[] bytes) {
        return Scalar.newBuilder().setType(Scalar.Type.V_OCTETS).setVOpaque(ByteString.copyFrom(bytes)).build();
    }

    public static Scalar scalarOf(boolean b) {
        return Scalar.newBuilder().setType(Scalar.Type.V_BOOL).setVBool(b).build();
    }

    /**
     * Build an Any with a string value.
     */
    public static Any buildAny(String str) {
        // same as Expr
        Scalar.String sstr = Scalar.String.newBuilder().setValue(ByteString.copyFromUtf8(str)).build();
        Scalar s = Scalar.newBuilder().setType(Scalar.Type.V_STRING).setVString(sstr).build();
        Any a = Any.newBuilder().setType(Any.Type.SCALAR).setScalar(s).build();
        return a;
    }

    public static Collection buildCollection(String schemaName, String collectionName) {
        return Collection.newBuilder().setSchema(schemaName).setName(collectionName).build();
    }

    public static Any argObjectToAny(Object value) {
        Scalar s = argObjectToExpr(value).getLiteral();
        return Any.newBuilder().setType(Any.Type.SCALAR).setScalar(s).build();
    }

    public static Expr argObjectToExpr(Object value) {
        if (value == null) {
            return buildLiteralNullScalar();
        } else if (value.getClass() == Boolean.class) {
            return buildLiteralScalar((boolean) value);
        } else if (value.getClass() == Byte.class) {
            return buildLiteralScalar(((Byte) value).longValue());
        } else if (value.getClass() == Short.class) {
            return buildLiteralScalar(((Short) value).longValue());
        } else if (value.getClass() == Integer.class) {
            return buildLiteralScalar(((Integer) value).longValue());
        } else if (value.getClass() == Long.class) {
            return buildLiteralScalar((long) value);
        } else if (value.getClass() == Float.class) {
            return buildLiteralScalar(((Float) value).doubleValue());
        } else if (value.getClass() == Double.class) {
            return buildLiteralScalar((double) value);
        } else if (value.getClass() == String.class) {
            return buildLiteralScalar((String) value);
        } else if (value.getClass() == Expression.class) {
            return new ExprParser(((Expression) value).getExpressionString()).parse();
        } else if (value.getClass() == JsonDoc.class) {
            // TODO: check how xplugin handles this
        } else if (value.getClass() == JsonArray.class) {
            // TODO: check how xplugin handles this
        }
        throw new NullPointerException("TODO: other types? BigDecimal, Date, Timestamp, Time");
    }
}
