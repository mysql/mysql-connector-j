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

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.mysql.cj.api.x.Expression;
import com.mysql.cj.core.exceptions.FeatureNotAvailableException;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Collection;
import com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Any;
import com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Scalar;
import com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Scalar.Octets;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.Expr;
import com.mysql.cj.x.json.DbDoc;
import com.mysql.cj.x.json.JsonArray;
import com.mysql.cj.x.json.JsonNumber;
import com.mysql.cj.x.json.JsonString;

/**
 * Utilities to deal with Expr (and related) structures.
 *
 * @todo rename to ProtobufUtil(s)
 */
public class ExprUtil {
    // Date formats for sending dates and times to the server as strings.
    private static SimpleDateFormat javaSqlDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static SimpleDateFormat javaSqlTimestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S");
    private static SimpleDateFormat javaSqlTimeFormat = new SimpleDateFormat("HH:mm:ss.S");
    private static SimpleDateFormat javaUtilDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S");

    /**
     * Proto-buf helper to build a LITERAL Expr with a Scalar NULL type.
     */
    public static Expr buildLiteralNullScalar() {
        return buildLiteralExpr(nullScalar());
    }

    /**
     * Proto-buf helper to build a LITERAL Expr with a Scalar DOUBLE type.
     */
    public static Expr buildLiteralScalar(double d) {
        return buildLiteralExpr(scalarOf(d));
    }

    /**
     * Proto-buf helper to build a LITERAL Expr with a Scalar SINT (signed int) type.
     */
    public static Expr buildLiteralScalar(long l) {
        return buildLiteralExpr(scalarOf(l));
    }

    /**
     * Proto-buf helper to build a LITERAL Expr with a Scalar STRING type.
     */
    public static Expr buildLiteralScalar(String str) {
        return buildLiteralExpr(scalarOf(str));
    }

    /**
     * Proto-buf helper to build a LITERAL Expr with a Scalar OCTETS type.
     */
    public static Expr buildLiteralScalar(byte[] bytes) {
        return buildLiteralExpr(scalarOf(bytes));
    }

    /**
     * Proto-buf helper to build a LITERAL Expr with a Scalar BOOL type.
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
        Octets.Builder o = Octets.newBuilder().setValue(ByteString.copyFrom(bytes));
        return Scalar.newBuilder().setType(Scalar.Type.V_OCTETS).setVOctets(o).build();
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

    public static Any buildAny(boolean b) {
        return Any.newBuilder().setType(Any.Type.SCALAR).setScalar(scalarOf(b)).build();
    }

    public static Collection buildCollection(String schemaName, String collectionName) {
        return Collection.newBuilder().setSchema(schemaName).setName(collectionName).build();
    }

    public static Scalar argObjectToScalar(Object value) {
        Expr e = argObjectToExpr(value, false);
        if (!e.hasLiteral()) {
            throw new WrongArgumentException("No literal interpretation of argument: " + value);
        }
        return e.getLiteral();
    }

    public static Any argObjectToScalarAny(Object value) {
        Scalar s = argObjectToScalar(value);
        return Any.newBuilder().setType(Any.Type.SCALAR).setScalar(s).build();
    }

    public static Expr argObjectToExpr(Object value, boolean allowRelationalColumns) {
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
            return new ExprParser(((Expression) value).getExpressionString(), allowRelationalColumns).parse();
        } else if (value.getClass() == Date.class) {
            return buildLiteralScalar(javaSqlDateFormat.format((java.util.Date) value));
        } else if (value.getClass() == Time.class) {
            return buildLiteralScalar(javaSqlTimeFormat.format((java.util.Date) value));
        } else if (value.getClass() == Timestamp.class) {
            return buildLiteralScalar(javaSqlTimestampFormat.format((java.util.Date) value));
        } else if (value.getClass() == java.util.Date.class) {
            return buildLiteralScalar(javaUtilDateFormat.format((java.util.Date) value));
        } else if (value.getClass() == DbDoc.class) {
            return (new ExprParser(((DbDoc) value).toString())).parse();
        } else if (value.getClass() == JsonArray.class) {
            return Expr.newBuilder().setType(Expr.Type.ARRAY).setArray(Expr.newBuilder().setType(Expr.Type.ARRAY).getArrayBuilder()
                    .addAllValue(((JsonArray) value).stream().map(f -> ExprUtil.argObjectToExpr(f, true)).collect(Collectors.toList()))).build();
        } else if (value.getClass() == JsonString.class) {
            return buildLiteralScalar(((JsonString) value).getString());
        } else if (value.getClass() == JsonNumber.class) {
            return buildLiteralScalar(((JsonNumber) value).getInteger());
        }
        throw new FeatureNotAvailableException("TODO: other types: BigDecimal");
    }
}
