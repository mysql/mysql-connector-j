/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.core.util;

import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.mysql.api.ExceptionInterceptor;
import com.mysql.api.Extension;
import com.mysql.core.Messages;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.exceptions.SQLError;

/**
 * Various utility methods for the driver.
 */
public class Util {
    private static boolean isColdFusion = false;

    static {

        //
        // Detect the ColdFusion MX environment
        // 
        // Unfortunately, no easy-to-discern classes are available to our classloader to check...
        //

        String loadedFrom = stackTraceToString(new Throwable());

        if (loadedFrom != null) {
            isColdFusion = loadedFrom.indexOf("coldfusion") != -1;
        } else {
            isColdFusion = false;
        }
    }

    public static boolean isColdFusion() {
        return isColdFusion;
    }

    /**
     * Given a ResultSet and an index into the columns of that ResultSet, read
     * binary data from the column which represents a serialized object, and
     * re-create the object.
     * 
     * @param resultSet
     *            the ResultSet to use.
     * @param index
     *            an index into the ResultSet.
     * @return the object if it can be de-serialized
     * @throws Exception
     *             if an error occurs
     */
    public static Object readObject(java.sql.ResultSet resultSet, int index) throws Exception {
        ObjectInputStream objIn = new ObjectInputStream(resultSet.getBinaryStream(index));
        Object obj = objIn.readObject();
        objIn.close();

        return obj;
    }

    /**
     * Converts a nested exception into a nicer message
     * 
     * @param ex
     *            the exception to expand into a message.
     * 
     * @return a message containing the exception, the message (if any), and a
     *         stacktrace.
     */
    public static String stackTraceToString(Throwable ex) {
        StringBuilder traceBuf = new StringBuilder();
        traceBuf.append(Messages.getString("Util.1"));

        if (ex != null) {
            traceBuf.append(ex.getClass().getName());

            String message = ex.getMessage();

            if (message != null) {
                traceBuf.append(Messages.getString("Util.2"));
                traceBuf.append(message);
            }

            StringWriter out = new StringWriter();

            PrintWriter printOut = new PrintWriter(out);

            ex.printStackTrace(printOut);

            traceBuf.append(Messages.getString("Util.3"));
            traceBuf.append(out.toString());
        }

        traceBuf.append(Messages.getString("Util.4"));

        return traceBuf.toString();
    }

    public static Object getInstance(String className, Class<?>[] argTypes, Object[] args, ExceptionInterceptor exceptionInterceptor) throws SQLException {

        try {
            return handleNewInstance(Class.forName(className).getConstructor(argTypes), args, exceptionInterceptor);
        } catch (SecurityException e) {
            throw SQLError.createSQLException("Can't instantiate required class", SQLError.SQL_STATE_GENERAL_ERROR, e, exceptionInterceptor);
        } catch (NoSuchMethodException e) {
            throw SQLError.createSQLException("Can't instantiate required class", SQLError.SQL_STATE_GENERAL_ERROR, e, exceptionInterceptor);
        } catch (ClassNotFoundException e) {
            throw SQLError.createSQLException("Can't instantiate required class", SQLError.SQL_STATE_GENERAL_ERROR, e, exceptionInterceptor);
        }
    }

    /**
     * Handles constructing new instance with the given constructor and wrapping
     * (or not, as required) the exceptions that could possibly be generated
     */
    public static final Object handleNewInstance(Constructor<?> ctor, Object[] args, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        try {

            return ctor.newInstance(args);
        } catch (IllegalArgumentException e) {
            throw SQLError.createSQLException("Can't instantiate required class", SQLError.SQL_STATE_GENERAL_ERROR, e, exceptionInterceptor);
        } catch (InstantiationException e) {
            throw SQLError.createSQLException("Can't instantiate required class", SQLError.SQL_STATE_GENERAL_ERROR, e, exceptionInterceptor);
        } catch (IllegalAccessException e) {
            throw SQLError.createSQLException("Can't instantiate required class", SQLError.SQL_STATE_GENERAL_ERROR, e, exceptionInterceptor);
        } catch (InvocationTargetException e) {
            Throwable target = e.getTargetException();

            if (target instanceof SQLException) {
                throw (SQLException) target;
            }

            if (target instanceof ExceptionInInitializerError) {
                target = ((ExceptionInInitializerError) target).getException();
            }

            throw SQLError.createSQLException(target.toString(), SQLError.SQL_STATE_GENERAL_ERROR, target, exceptionInterceptor);
        }
    }

    /**
     * Does a network interface exist locally with the given hostname?
     * 
     * @param hostname
     *            the hostname (or IP address in string form) to check
     * @return true if it exists, false if no, or unable to determine due to VM
     *         version support of java.net.NetworkInterface
     */
    public static boolean interfaceExists(String hostname) {
        try {
            Class<?> networkInterfaceClass = Class.forName("java.net.NetworkInterface");
            return networkInterfaceClass.getMethod("getByName", (Class[]) null).invoke(networkInterfaceClass, new Object[] { hostname }) != null;
        } catch (Throwable t) {
            return false;
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void resultSetToMap(Map mappedValues, java.sql.ResultSet rs) throws SQLException {
        while (rs.next()) {
            mappedValues.put(rs.getObject(1), rs.getObject(2));
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void resultSetToMap(Map mappedValues, java.sql.ResultSet rs, int key, int value) throws SQLException {
        while (rs.next()) {
            mappedValues.put(rs.getObject(key), rs.getObject(value));
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void resultSetToMap(Map mappedValues, java.sql.ResultSet rs, String key, String value) throws SQLException {
        while (rs.next()) {
            mappedValues.put(rs.getObject(key), rs.getObject(value));
        }
    }

    public static Map<Object, Object> calculateDifferences(Map<?, ?> map1, Map<?, ?> map2) {
        Map<Object, Object> diffMap = new HashMap<Object, Object>();

        for (Map.Entry<?, ?> entry : map1.entrySet()) {
            Object key = entry.getKey();

            Number value1 = null;
            Number value2 = null;

            if (entry.getValue() instanceof Number) {

                value1 = (Number) entry.getValue();
                value2 = (Number) map2.get(key);
            } else {
                try {
                    value1 = new Double(entry.getValue().toString());
                    value2 = new Double(map2.get(key).toString());
                } catch (NumberFormatException nfe) {
                    continue;
                }
            }

            if (value1.equals(value2)) {
                continue;
            }

            if (value1 instanceof Byte) {
                diffMap.put(key, Byte.valueOf((byte) (((Byte) value2).byteValue() - ((Byte) value1).byteValue())));
            } else if (value1 instanceof Short) {
                diffMap.put(key, Short.valueOf((short) (((Short) value2).shortValue() - ((Short) value1).shortValue())));
            } else if (value1 instanceof Integer) {
                diffMap.put(key, Integer.valueOf((((Integer) value2).intValue() - ((Integer) value1).intValue())));
            } else if (value1 instanceof Long) {
                diffMap.put(key, Long.valueOf((((Long) value2).longValue() - ((Long) value1).longValue())));
            } else if (value1 instanceof Float) {
                diffMap.put(key, Float.valueOf(((Float) value2).floatValue() - ((Float) value1).floatValue()));
            } else if (value1 instanceof Double) {
                diffMap.put(key, Double.valueOf((((Double) value2).shortValue() - ((Double) value1).shortValue())));
            } else if (value1 instanceof BigDecimal) {
                diffMap.put(key, ((BigDecimal) value2).subtract((BigDecimal) value1));
            } else if (value1 instanceof BigInteger) {
                diffMap.put(key, ((BigInteger) value2).subtract((BigInteger) value1));
            }
        }

        return diffMap;
    }

    /**
     * Returns initialized instances of classes listed in extensionClassNames.
     * There is no need to call Extension.init() method after that if you don't change connection or properties.
     * 
     * @param conn
     * @param props
     * @param extensionClassNames
     * @param errorMessageKey
     * @param exceptionInterceptor
     * @throws SQLException
     */
    public static List<Extension> loadExtensions(Connection conn, Properties props, String extensionClassNames, String errorMessageKey,
            ExceptionInterceptor exceptionInterceptor) throws SQLException {
        List<Extension> extensionList = new LinkedList<Extension>();

        List<String> interceptorsToCreate = StringUtils.split(extensionClassNames, ",", true);

        String className = null;

        try {
            for (int i = 0, s = interceptorsToCreate.size(); i < s; i++) {
                className = interceptorsToCreate.get(i);
                Extension extensionInstance = (Extension) Class.forName(className).newInstance();
                extensionInstance.init(conn, props);

                extensionList.add(extensionInstance);
            }
        } catch (Throwable t) {
            SQLException sqlEx = SQLError.createSQLException(Messages.getString(errorMessageKey, new Object[] { className }), exceptionInterceptor);
            sqlEx.initCause(t);

            throw sqlEx;
        }

        return extensionList;
    }
}