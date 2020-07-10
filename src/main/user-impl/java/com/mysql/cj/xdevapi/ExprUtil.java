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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.util.TimeUtil;
import com.mysql.cj.x.protobuf.MysqlxCrud.Collection;
import com.mysql.cj.x.protobuf.MysqlxDatatypes.Any;
import com.mysql.cj.x.protobuf.MysqlxDatatypes.Scalar;
import com.mysql.cj.x.protobuf.MysqlxDatatypes.Scalar.Octets;
import com.mysql.cj.x.protobuf.MysqlxExpr.Expr;

/**
 * Utilities to deal with Expr (and related) structures.
 */
public class ExprUtil {
    // Date formats for sending dates and times to the server as strings.
    private static SimpleDateFormat javaSqlDateFormat = TimeUtil.getSimpleDateFormat(null, "yyyy-MM-dd", null);
    private static SimpleDateFormat javaSqlTimestampFormat = TimeUtil.getSimpleDateFormat(null, "yyyy-MM-dd'T'HH:mm:ss.S", null);
    private static SimpleDateFormat javaSqlTimeFormat = TimeUtil.getSimpleDateFormat(null, "HH:mm:ss.S", null);
    private static SimpleDateFormat javaUtilDateFormat = TimeUtil.getSimpleDateFormat(null, "yyyy-MM-dd'T'HH:mm:ss.S", null);

    /**
     * Protocol buffers helper to build a LITERAL Expr with a Scalar NULL type.
     * 
     * @return {@link Expr}
     */
    public static Expr buildLiteralNullScalar() {
        return buildLiteralExpr(nullScalar());
    }

    /**
     * Protocol buffers helper to build a LITERAL Expr with a Scalar DOUBLE type.
     * 
     * @param d
     *            value
     * @return {@link Expr}
     */
    public static Expr buildLiteralScalar(double d) {
        return buildLiteralExpr(scalarOf(d));
    }

    /**
     * Protocol buffers helper to build a LITERAL Expr with a Scalar SINT (signed int) type.
     * 
     * @param l
     *            value
     * @return {@link Expr}
     */
    public static Expr buildLiteralScalar(long l) {
        return buildLiteralExpr(scalarOf(l));
    }

    /**
     * Protocol buffers helper to build a LITERAL Expr with a Scalar STRING type.
     * 
     * @param str
     *            value
     * @return {@link Expr}
     */
    public static Expr buildLiteralScalar(String str) {
        return buildLiteralExpr(scalarOf(str));
    }

    /**
     * Protocol buffers helper to build a LITERAL Expr with a Scalar OCTETS type.
     * 
     * @param bytes
     *            value
     * @return {@link Expr}
     */
    public static Expr buildLiteralScalar(byte[] bytes) {
        return buildLiteralExpr(scalarOf(bytes));
    }

    /**
     * Protocol buffers helper to build a LITERAL Expr with a Scalar BOOL type.
     * 
     * @param b
     *            value
     * @return {@link Expr}
     */
    public static Expr buildLiteralScalar(boolean b) {
        return buildLiteralExpr(scalarOf(b));
    }

    /**
     * Wrap an Any value in a LITERAL expression.
     * 
     * @param scalar
     *            {@link Scalar}
     * @return {@link Expr}
     */
    public static Expr buildLiteralExpr(Scalar scalar) {
        return Expr.newBuilder().setType(Expr.Type.LITERAL).setLiteral(scalar).build();
    }

    /**
     * Creates a placeholder expression for the given position in the <code>args</code> array
     * 
     * @param pos
     *            the position of the placeholder in the <code>args</code> array
     * @return {@link Expr}
     */
    public static Expr buildPlaceholderExpr(int pos) {
        return Expr.newBuilder().setType(Expr.Type.PLACEHOLDER).setPosition(pos).build();
    }

    /**
     * Protocol buffers helper to build a Scalar NULL type.
     * 
     * @return {@link Scalar}
     */
    public static Scalar nullScalar() {
        return Scalar.newBuilder().setType(Scalar.Type.V_NULL).build();
    }

    /**
     * Protocol buffers helper to build a Scalar DOUBLE type.
     * 
     * @param d
     *            value
     * @return {@link Scalar}
     */
    public static Scalar scalarOf(double d) {
        return Scalar.newBuilder().setType(Scalar.Type.V_DOUBLE).setVDouble(d).build();
    }

    /**
     * Protocol buffers helper to build a Scalar SINT (signed int) type.
     * 
     * @param l
     *            value
     * @return {@link Scalar}
     */
    public static Scalar scalarOf(long l) {
        return Scalar.newBuilder().setType(Scalar.Type.V_SINT).setVSignedInt(l).build();
    }

    /**
     * Protocol buffers helper to build a Scalar STRING type.
     * 
     * @param str
     *            value
     * @return {@link Scalar}
     */
    public static Scalar scalarOf(String str) {
        Scalar.String sstr = Scalar.String.newBuilder().setValue(ByteString.copyFromUtf8(str)).build();
        return Scalar.newBuilder().setType(Scalar.Type.V_STRING).setVString(sstr).build();
    }

    /**
     * Protocol buffers helper to build a Scalar OCTETS type.
     * 
     * @param bytes
     *            value
     * @return {@link Scalar}
     */
    public static Scalar scalarOf(byte[] bytes) {
        Octets.Builder o = Octets.newBuilder().setValue(ByteString.copyFrom(bytes));
        return Scalar.newBuilder().setType(Scalar.Type.V_OCTETS).setVOctets(o).build();
    }

    /**
     * Protocol buffers helper to build a Scalar BOOL type.
     * 
     * @param b
     *            value
     * @return {@link Scalar}
     */
    public static Scalar scalarOf(boolean b) {
        return Scalar.newBuilder().setType(Scalar.Type.V_BOOL).setVBool(b).build();
    }

    /**
     * Protocol buffers helper to build an Any Scalar type.
     * 
     * @param s
     *            value
     * @return {@link Any}
     */
    public static Any anyOf(Scalar s) {
        return Any.newBuilder().setType(Any.Type.SCALAR).setScalar(s).build();
    }

    /**
     * Build a Protocol buffers Any with a string value.
     * 
     * @param str
     *            value
     * @return {@link Any}
     */
    public static Any buildAny(String str) {
        // same as Expr
        Scalar.String sstr = Scalar.String.newBuilder().setValue(ByteString.copyFromUtf8(str)).build();
        Scalar s = Scalar.newBuilder().setType(Scalar.Type.V_STRING).setVString(sstr).build();
        return anyOf(s);
    }

    /**
     * Build a Protocol buffers Any with a boolean value.
     * 
     * @param b
     *            value
     * @return {@link Any}
     */
    public static Any buildAny(boolean b) {
        return Any.newBuilder().setType(Any.Type.SCALAR).setScalar(scalarOf(b)).build();
    }

    /**
     * Build a Protocol buffers Collection.
     * 
     * @param schemaName
     *            schema name
     * @param collectionName
     *            collection name
     * @return {@link Collection}
     */
    public static Collection buildCollection(String schemaName, String collectionName) {
        return Collection.newBuilder().setSchema(schemaName).setName(collectionName).build();
    }

    /**
     * Protocol buffers helper to build a Scalar type with any object.
     * 
     * @param value
     *            value
     * @return {@link Scalar}
     */
    public static Scalar argObjectToScalar(Object value) {
        Expr e = argObjectToExpr(value, false);
        if (!e.hasLiteral()) {
            throw new WrongArgumentException("No literal interpretation of argument: " + value);
        }
        return e.getLiteral();
    }

    /**
     * Protocol buffers helper to build an Any type with any object.
     * 
     * @param value
     *            value
     * @return {@link Any}
     */
    public static Any argObjectToScalarAny(Object value) {
        Scalar s = argObjectToScalar(value);
        return Any.newBuilder().setType(Any.Type.SCALAR).setScalar(s).build();
    }

    /**
     * Protocol buffers helper to build Expr with any object.
     * 
     * @param value
     *            value
     * @param allowRelationalColumns
     *            Are relational columns identifiers allowed?
     * @return {@link Expr}
     */
    public static Expr argObjectToExpr(Object value, boolean allowRelationalColumns) {
        if (value == null) {
            return buildLiteralNullScalar();
        }

        Class<? extends Object> cls = value.getClass();

        if (cls == Boolean.class) {
            return buildLiteralScalar((boolean) value);

        } else if (cls == Byte.class || cls == Short.class || cls == Integer.class || cls == Long.class || cls == BigInteger.class) {
            return buildLiteralScalar(((Number) value).longValue());

        } else if (cls == Float.class || cls == Double.class || cls == BigDecimal.class) {
            return buildLiteralScalar(((Number) value).doubleValue());

        } else if (cls == String.class) {
            return buildLiteralScalar((String) value);

        } else if (cls == Character.class) {
            return buildLiteralScalar(((Character) value).toString());

        } else if (cls == Expression.class) {
            return new ExprParser(((Expression) value).getExpressionString(), allowRelationalColumns).parse();

        } else if (cls == Date.class) {
            return buildLiteralScalar(javaSqlDateFormat.format((java.util.Date) value));

        } else if (cls == Time.class) {
            return buildLiteralScalar(javaSqlTimeFormat.format((java.util.Date) value));

        } else if (cls == Timestamp.class) {
            return buildLiteralScalar(javaSqlTimestampFormat.format((java.util.Date) value));

        } else if (cls == java.util.Date.class) {
            return buildLiteralScalar(javaUtilDateFormat.format((java.util.Date) value));

        } else if (DbDoc.class.isAssignableFrom(cls)) {
            return (new ExprParser(((DbDoc) value).toString())).parse();

        } else if (cls == JsonArray.class) {
            return Expr.newBuilder().setType(Expr.Type.ARRAY).setArray(Expr.newBuilder().setType(Expr.Type.ARRAY).getArrayBuilder()
                    .addAllValue(((JsonArray) value).stream().map(f -> ExprUtil.argObjectToExpr(f, true)).collect(Collectors.toList()))).build();

        } else if (cls == JsonString.class) {
            return buildLiteralScalar(((JsonString) value).getString());

        } else if (cls == JsonNumber.class) {
            return buildLiteralScalar(((JsonNumber) value).getInteger());
        }
        // TODO other types: LocalDate...

        throw new FeatureNotAvailableException("Can not create an expression from " + cls);
    }
}
