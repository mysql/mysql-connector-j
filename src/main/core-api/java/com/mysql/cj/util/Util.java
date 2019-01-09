/*
 * Copyright (c) 2002, 2019, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mysql.cj.Constants;
import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.WrongArgumentException;

/**
 * Various utility methods for the driver.
 */
public class Util {
    private static int jvmVersion = 8; // use default base version supported

    private static int jvmUpdateNumber = -1;

    static {
        int startPos = Constants.JVM_VERSION.indexOf('.');
        int endPos = startPos + 1;
        if (startPos != -1) {
            while (Character.isDigit(Constants.JVM_VERSION.charAt(endPos)) && ++endPos < Constants.JVM_VERSION.length()) {
                // continue
            }
        }
        startPos++;
        if (endPos > startPos) {
            jvmVersion = Integer.parseInt(Constants.JVM_VERSION.substring(startPos, endPos));
        }
        startPos = Constants.JVM_VERSION.indexOf("_");
        endPos = startPos + 1;
        if (startPos != -1) {
            while (Character.isDigit(Constants.JVM_VERSION.charAt(endPos)) && ++endPos < Constants.JVM_VERSION.length()) {
                // continue
            }
        }
        startPos++;
        if (endPos > startPos) {
            jvmUpdateNumber = Integer.parseInt(Constants.JVM_VERSION.substring(startPos, endPos));
        }

    }

    public static int getJVMVersion() {
        return jvmVersion;
    }

    public static boolean jvmMeetsMinimum(int version, int updateNumber) {
        return getJVMVersion() > version || getJVMVersion() == version && getJVMUpdateNumber() >= updateNumber;
    }

    public static int getJVMUpdateNumber() {
        return jvmUpdateNumber;
    }

    /**
     * Checks whether the given server version string is a MySQL Community edition
     * 
     * @param serverVersion
     *            full server version string
     * @return true if version does not contain "enterprise", "commercial" or "advanced"
     */
    public static boolean isCommunityEdition(String serverVersion) {
        return !isEnterpriseEdition(serverVersion);
    }

    /**
     * Checks whether the given server version string is a MySQL Enterprise edition
     * 
     * @param serverVersion
     *            full server version string
     * @return true if version contains "enterprise", "commercial" or "advanced"
     */
    public static boolean isEnterpriseEdition(String serverVersion) {
        return serverVersion.contains("enterprise") || serverVersion.contains("commercial") || serverVersion.contains("advanced");
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

    public static Object getInstance(String className, Class<?>[] argTypes, Object[] args, ExceptionInterceptor exceptionInterceptor, String errorMessage) {

        try {
            return handleNewInstance(Class.forName(className).getConstructor(argTypes), args, exceptionInterceptor);
        } catch (SecurityException | NoSuchMethodException | ClassNotFoundException e) {
            throw ExceptionFactory.createException(WrongArgumentException.class, errorMessage, e, exceptionInterceptor);
        }
    }

    public static Object getInstance(String className, Class<?>[] argTypes, Object[] args, ExceptionInterceptor exceptionInterceptor) {
        return getInstance(className, argTypes, args, exceptionInterceptor, "Can't instantiate required class");
    }

    /**
     * Handles constructing new instance with the given constructor and wrapping
     * (or not, as required) the exceptions that could possibly be generated
     * 
     * @param ctor
     *            constructor
     * @param args
     *            arguments for constructor
     * @param exceptionInterceptor
     *            exception interceptor
     * @return object
     */
    public static Object handleNewInstance(Constructor<?> ctor, Object[] args, ExceptionInterceptor exceptionInterceptor) {
        try {

            return ctor.newInstance(args);
        } catch (IllegalArgumentException | InstantiationException | IllegalAccessException e) {
            throw ExceptionFactory.createException(WrongArgumentException.class, "Can't instantiate required class", e, exceptionInterceptor);
        } catch (InvocationTargetException e) {
            Throwable target = e.getTargetException();

            if (target instanceof ExceptionInInitializerError) {
                target = ((ExceptionInInitializerError) target).getException();
            } else if (target instanceof CJException) {
                throw (CJException) target;
            }

            throw ExceptionFactory.createException(WrongArgumentException.class, target.getMessage(), target, exceptionInterceptor);
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

    public static Map<Object, Object> calculateDifferences(Map<?, ?> map1, Map<?, ?> map2) {
        Map<Object, Object> diffMap = new HashMap<>();

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

    public static <T> List<T> loadClasses(String extensionClassNames, String errorMessageKey, ExceptionInterceptor exceptionInterceptor) {

        List<T> instances = new LinkedList<>();

        List<String> interceptorsToCreate = StringUtils.split(extensionClassNames, ",", true);

        String className = null;

        try {
            for (int i = 0, s = interceptorsToCreate.size(); i < s; i++) {
                className = interceptorsToCreate.get(i);
                @SuppressWarnings("unchecked")
                T instance = (T) Class.forName(className).newInstance();

                instances.add(instance);
            }

        } catch (Throwable t) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString(errorMessageKey, new Object[] { className }), t,
                    exceptionInterceptor);
        }

        return instances;
    }

    /** Cache for the JDBC interfaces already verified */
    private static final ConcurrentMap<Class<?>, Boolean> isJdbcInterfaceCache = new ConcurrentHashMap<>();

    /**
     * Recursively checks for interfaces on the given class to determine if it implements a java.sql, javax.sql or com.mysql.cj.jdbc interface.
     * 
     * @param clazz
     *            The class to investigate.
     * @return boolean
     */
    public static boolean isJdbcInterface(Class<?> clazz) {
        if (Util.isJdbcInterfaceCache.containsKey(clazz)) {
            return (Util.isJdbcInterfaceCache.get(clazz));
        }

        if (clazz.isInterface()) {
            try {
                if (isJdbcPackage(clazz.getPackage().getName())) {
                    Util.isJdbcInterfaceCache.putIfAbsent(clazz, true);
                    return true;
                }
            } catch (Exception ex) {
                /*
                 * We may experience a NPE from getPackage() returning null, or class-loading facilities.
                 * This happens when this class is instrumented to implement runtime-generated interfaces.
                 */
            }
        }

        for (Class<?> iface : clazz.getInterfaces()) {
            if (isJdbcInterface(iface)) {
                Util.isJdbcInterfaceCache.putIfAbsent(clazz, true);
                return true;
            }
        }

        if (clazz.getSuperclass() != null && isJdbcInterface(clazz.getSuperclass())) {
            Util.isJdbcInterfaceCache.putIfAbsent(clazz, true);
            return true;
        }

        Util.isJdbcInterfaceCache.putIfAbsent(clazz, false);
        return false;
    }

    /**
     * Check if the package name is a known JDBC package.
     * 
     * @param packageName
     *            The package name to check.
     * @return boolean
     */
    public static boolean isJdbcPackage(String packageName) {
        return packageName != null
                && (packageName.startsWith("java.sql") || packageName.startsWith("javax.sql") || packageName.startsWith("com.mysql.cj.jdbc"));
    }

    /** Cache for the implemented interfaces searched. */
    private static final ConcurrentMap<Class<?>, Class<?>[]> implementedInterfacesCache = new ConcurrentHashMap<>();

    /**
     * Retrieves a list with all interfaces implemented by the given class. If possible gets this information from a cache instead of navigating through the
     * object hierarchy. Results are stored in a cache for future reference.
     * 
     * @param clazz
     *            The class from which the interface list will be retrieved.
     * @return
     *         An array with all the interfaces for the given class.
     */
    public static Class<?>[] getImplementedInterfaces(Class<?> clazz) {
        Class<?>[] implementedInterfaces = Util.implementedInterfacesCache.get(clazz);
        if (implementedInterfaces != null) {
            return implementedInterfaces;
        }

        Set<Class<?>> interfaces = new LinkedHashSet<>();
        Class<?> superClass = clazz;
        do {
            Collections.addAll(interfaces, superClass.getInterfaces());
        } while ((superClass = superClass.getSuperclass()) != null);

        implementedInterfaces = interfaces.toArray(new Class<?>[interfaces.size()]);
        Class<?>[] oldValue = Util.implementedInterfacesCache.putIfAbsent(clazz, implementedInterfaces);
        if (oldValue != null) {
            implementedInterfaces = oldValue;
        }

        return implementedInterfaces;
    }

    /**
     * Computes the number of seconds elapsed since the given time in milliseconds.
     * 
     * @param timeInMillis
     *            The past instant in milliseconds.
     * @return
     *         The number of seconds, truncated, elapsed since timeInMillis.
     */
    public static long secondsSinceMillis(long timeInMillis) {
        return (System.currentTimeMillis() - timeInMillis) / 1000;
    }

    /**
     * Converts long to int, truncating to maximum/minimum value if needed.
     * 
     * @param longValue
     *            long value
     * @return int value
     */
    public static int truncateAndConvertToInt(long longValue) {
        return longValue > Integer.MAX_VALUE ? Integer.MAX_VALUE : longValue < Integer.MIN_VALUE ? Integer.MIN_VALUE : (int) longValue;
    }

    /**
     * Converts long[] to int[], truncating to maximum/minimum value if needed.
     * 
     * @param longArray
     *            log values
     * @return int values
     */
    public static int[] truncateAndConvertToInt(long[] longArray) {
        int[] intArray = new int[longArray.length];

        for (int i = 0; i < longArray.length; i++) {
            intArray[i] = longArray[i] > Integer.MAX_VALUE ? Integer.MAX_VALUE : longArray[i] < Integer.MIN_VALUE ? Integer.MIN_VALUE : (int) longArray[i];
        }
        return intArray;
    }

    /**
     * Returns the package name of the given class.
     * Using clazz.getPackage().getName() is not an alternative because under some class loaders the method getPackage() just returns null.
     * 
     * @param clazz
     *            the Class from which to get the package name
     * @return the package name
     */
    public static String getPackageName(Class<?> clazz) {
        String fqcn = clazz.getName();
        int classNameStartsAt = fqcn.lastIndexOf('.');
        if (classNameStartsAt > 0) {
            return fqcn.substring(0, classNameStartsAt);
        }
        return "";
    }

    /**
     * Checks if the JVM is running on Windows Operating System.
     * 
     * @return
     *         <code>true</code> if currently running on Windows, <code>false</code> otherwise.
     */
    public static boolean isRunningOnWindows() {
        return StringUtils.indexOfIgnoreCase(Constants.OS_NAME, "WINDOWS") != -1;
    }

    /**
     * Reads length bytes from reader into buf. Blocks until enough input is
     * available
     * 
     * @param reader
     *            {@link Reader}
     * @param buf
     *            char array to read into
     * @param length
     *            number of chars to read
     * 
     * @return the actual number of chars read
     * 
     * @throws IOException
     *             if an error occurs
     */
    public static int readFully(Reader reader, char[] buf, int length) throws IOException {
        int numCharsRead = 0;

        while (numCharsRead < length) {
            int count = reader.read(buf, numCharsRead, length - numCharsRead);

            if (count < 0) {
                break;
            }

            numCharsRead += count;
        }

        return numCharsRead;
    }

    public static final int readBlock(InputStream i, byte[] b, ExceptionInterceptor exceptionInterceptor) {
        try {
            return i.read(b);
        } catch (Throwable ex) {
            throw ExceptionFactory.createException(Messages.getString("Util.5") + ex.getClass().getName(), exceptionInterceptor);
        }
    }

    public static final int readBlock(InputStream i, byte[] b, int length, ExceptionInterceptor exceptionInterceptor) {
        try {
            int lengthToRead = length;

            if (lengthToRead > b.length) {
                lengthToRead = b.length;
            }

            return i.read(b, 0, lengthToRead);
        } catch (Throwable ex) {
            throw ExceptionFactory.createException(Messages.getString("Util.5") + ex.getClass().getName(), exceptionInterceptor);
        }
    }
}
