/*
 Copyright  2002-2007 MySQL AB, 2008 Sun Microsystems

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA



 */
package com.mysql.jdbc;

import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Various utility methods for the driver.
 * 
 * @author Mark Matthews
 */
public class Util {
	protected static Method systemNanoTimeMethod;

	static {
		try {
			systemNanoTimeMethod = System.class.getMethod("nanoTime", null);
		} catch (SecurityException e) {
			systemNanoTimeMethod = null;
		} catch (NoSuchMethodException e) {
			systemNanoTimeMethod = null;
		}
	}

	public static boolean nanoTimeAvailable() {
		return systemNanoTimeMethod != null;
	}

	private static Method CAST_METHOD;

	// cache this ourselves, as the method call is statically-synchronized in
	// all but JDK6!

	private static final TimeZone DEFAULT_TIMEZONE = TimeZone.getDefault();

	static final TimeZone getDefaultTimeZone() {
		return (TimeZone) DEFAULT_TIMEZONE.clone();
	}

	class RandStructcture {
		long maxValue;

		double maxValueDbl;

		long seed1;

		long seed2;
	}

	private static Util enclosingInstance = new Util();

	private static boolean isJdbc4 = false;
	
	private static boolean isColdFusion = false;

	static {
		try {
			CAST_METHOD = Class.class.getMethod("cast",
					new Class[] { Object.class });
		} catch (Throwable t) {
			// ignore - not available in this VM
		}

		try {
			Class.forName("java.sql.NClob");
			isJdbc4 = true;
		} catch (Throwable t) {
			isJdbc4 = false;
		}
		
		//
		// Detect the ColdFusion MX environment
		// 
		// Unfortunately, no easy-to-discern classes are available
		// to our classloader to check...
		//
		
		String loadedFrom = stackTraceToString(new Throwable());
		
		if (loadedFrom != null) {
			isColdFusion = loadedFrom.indexOf("coldfusion") != -1;
		} else {
			isColdFusion = false;
		}
	}

	// ~ Methods
	// ----------------------------------------------------------------

	public static boolean isJdbc4() {
		return isJdbc4;
	}
	
	public static boolean isColdFusion() {
		return isColdFusion;
	}

	// Right from Monty's code
	static String newCrypt(String password, String seed) {
		byte b;
		double d;

		if ((password == null) || (password.length() == 0)) {
			return password;
		}

		long[] pw = newHash(seed);
		long[] msg = newHash(password);
		long max = 0x3fffffffL;
		long seed1 = (pw[0] ^ msg[0]) % max;
		long seed2 = (pw[1] ^ msg[1]) % max;
		char[] chars = new char[seed.length()];

		for (int i = 0; i < seed.length(); i++) {
			seed1 = ((seed1 * 3) + seed2) % max;
			seed2 = (seed1 + seed2 + 33) % max;
			d = (double) seed1 / (double) max;
			b = (byte) java.lang.Math.floor((d * 31) + 64);
			chars[i] = (char) b;
		}

		seed1 = ((seed1 * 3) + seed2) % max;
		seed2 = (seed1 + seed2 + 33) % max;
		d = (double) seed1 / (double) max;
		b = (byte) java.lang.Math.floor(d * 31);

		for (int i = 0; i < seed.length(); i++) {
			chars[i] ^= (char) b;
		}

		return new String(chars);
	}

	static long[] newHash(String password) {
		long nr = 1345345333L;
		long add = 7;
		long nr2 = 0x12345671L;
		long tmp;

		for (int i = 0; i < password.length(); ++i) {
			if ((password.charAt(i) == ' ') || (password.charAt(i) == '\t')) {
				continue; // skip spaces
			}

			tmp = (0xff & password.charAt(i));
			nr ^= ((((nr & 63) + add) * tmp) + (nr << 8));
			nr2 += ((nr2 << 8) ^ nr);
			add += tmp;
		}

		long[] result = new long[2];
		result[0] = nr & 0x7fffffffL;
		result[1] = nr2 & 0x7fffffffL;

		return result;
	}

	static String oldCrypt(String password, String seed) {
		long hp;
		long hm;
		long s1;
		long s2;
		long max = 0x01FFFFFF;
		double d;
		byte b;

		if ((password == null) || (password.length() == 0)) {
			return password;
		}

		hp = oldHash(seed);
		hm = oldHash(password);

		long nr = hp ^ hm;
		nr %= max;
		s1 = nr;
		s2 = nr / 2;

		char[] chars = new char[seed.length()];

		for (int i = 0; i < seed.length(); i++) {
			s1 = ((s1 * 3) + s2) % max;
			s2 = (s1 + s2 + 33) % max;
			d = (double) s1 / max;
			b = (byte) java.lang.Math.floor((d * 31) + 64);
			chars[i] = (char) b;
		}

		return new String(chars);
	}

	static long oldHash(String password) {
		long nr = 1345345333;
		long nr2 = 7;
		long tmp;

		for (int i = 0; i < password.length(); i++) {
			if ((password.charAt(i) == ' ') || (password.charAt(i) == '\t')) {
				continue;
			}

			tmp = password.charAt(i);
			nr ^= ((((nr & 63) + nr2) * tmp) + (nr << 8));
			nr2 += tmp;
		}

		return nr & ((1L << 31) - 1L);
	}

	private static RandStructcture randomInit(long seed1, long seed2) {
		RandStructcture randStruct = enclosingInstance.new RandStructcture();

		randStruct.maxValue = 0x3FFFFFFFL;
		randStruct.maxValueDbl = randStruct.maxValue;
		randStruct.seed1 = seed1 % randStruct.maxValue;
		randStruct.seed2 = seed2 % randStruct.maxValue;

		return randStruct;
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
	public static Object readObject(java.sql.ResultSet resultSet, int index)
			throws Exception {
		ObjectInputStream objIn = new ObjectInputStream(resultSet
				.getBinaryStream(index));
		Object obj = objIn.readObject();
		objIn.close();

		return obj;
	}

	private static double rnd(RandStructcture randStruct) {
		randStruct.seed1 = ((randStruct.seed1 * 3) + randStruct.seed2)
				% randStruct.maxValue;
		randStruct.seed2 = (randStruct.seed1 + randStruct.seed2 + 33)
				% randStruct.maxValue;

		return ((randStruct.seed1) / randStruct.maxValueDbl);
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @param message
	 *            DOCUMENT ME!
	 * @param password
	 *            DOCUMENT ME!
	 * 
	 * @return DOCUMENT ME!
	 */
	public static String scramble(String message, String password) {
		long[] hashPass;
		long[] hashMessage;
		byte[] to = new byte[8];
		String val = ""; //$NON-NLS-1$

		message = message.substring(0, 8);

		if ((password != null) && (password.length() > 0)) {
			hashPass = newHash(password);
			hashMessage = newHash(message);

			RandStructcture randStruct = randomInit(hashPass[0]
					^ hashMessage[0], hashPass[1] ^ hashMessage[1]);

			int msgPos = 0;
			int msgLength = message.length();
			int toPos = 0;

			while (msgPos++ < msgLength) {
				to[toPos++] = (byte) (Math.floor(rnd(randStruct) * 31) + 64);
			}

			/* Make it harder to break */
			byte extra = (byte) (Math.floor(rnd(randStruct) * 31));

			for (int i = 0; i < to.length; i++) {
				to[i] ^= extra;
			}

			val = new String(to);
		}

		return val;
	}

	// ~ Inner Classes
	// ----------------------------------------------------------

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
		StringBuffer traceBuf = new StringBuffer();
		traceBuf.append(Messages.getString("Util.1")); //$NON-NLS-1$

		if (ex != null) {
			traceBuf.append(ex.getClass().getName());

			String message = ex.getMessage();

			if (message != null) {
				traceBuf.append(Messages.getString("Util.2")); //$NON-NLS-1$
				traceBuf.append(message);
			}

			StringWriter out = new StringWriter();

			PrintWriter printOut = new PrintWriter(out);

			ex.printStackTrace(printOut);

			traceBuf.append(Messages.getString("Util.3")); //$NON-NLS-1$
			traceBuf.append(out.toString());
		}

		traceBuf.append(Messages.getString("Util.4")); //$NON-NLS-1$

		return traceBuf.toString();
	}

	public static Object getInstance(String className, Class[] argTypes,
			Object[] args, ExceptionInterceptor exceptionInterceptor) throws SQLException {

		try {
			return handleNewInstance(Class.forName(className).getConstructor(
					argTypes), args, exceptionInterceptor);
		} catch (SecurityException e) {
			throw SQLError.createSQLException(
					"Can't instantiate required class",
					SQLError.SQL_STATE_GENERAL_ERROR, e, exceptionInterceptor);
		} catch (NoSuchMethodException e) {
			throw SQLError.createSQLException(
					"Can't instantiate required class",
					SQLError.SQL_STATE_GENERAL_ERROR, e, exceptionInterceptor);
		} catch (ClassNotFoundException e) {
			throw SQLError.createSQLException(
					"Can't instantiate required class",
					SQLError.SQL_STATE_GENERAL_ERROR, e, exceptionInterceptor);
		}
	}

	/**
	 * Handles constructing new instance with the given constructor and wrapping
	 * (or not, as required) the exceptions that could possibly be generated
	 */
	public static final Object handleNewInstance(Constructor ctor, Object[] args, ExceptionInterceptor exceptionInterceptor)
			throws SQLException {
		try {

			return ctor.newInstance(args);
		} catch (IllegalArgumentException e) {
			throw SQLError.createSQLException(
					"Can't instantiate required class",
					SQLError.SQL_STATE_GENERAL_ERROR, e, exceptionInterceptor);
		} catch (InstantiationException e) {
			throw SQLError.createSQLException(
					"Can't instantiate required class",
					SQLError.SQL_STATE_GENERAL_ERROR, e, exceptionInterceptor);
		} catch (IllegalAccessException e) {
			throw SQLError.createSQLException(
					"Can't instantiate required class",
					SQLError.SQL_STATE_GENERAL_ERROR, e, exceptionInterceptor);
		} catch (InvocationTargetException e) {
			Throwable target = e.getTargetException();

			if (target instanceof SQLException) {
				throw (SQLException) target;
			}

			if (target instanceof ExceptionInInitializerError) {
				target = ((ExceptionInInitializerError) target).getException();
			}

			throw SQLError.createSQLException(target.toString(),
					SQLError.SQL_STATE_GENERAL_ERROR, exceptionInterceptor);
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
			Class networkInterfaceClass = Class
					.forName("java.net.NetworkInterface");
			return networkInterfaceClass.getMethod("getByName", null).invoke(
					networkInterfaceClass, new Object[] { hostname }) != null;
		} catch (Throwable t) {
			return false;
		}
	}

	/**
	 * Reflexive access on JDK-1.5's Class.cast() method so we don't have to
	 * move that out into separate classes built for JDBC-4.0.
	 * 
	 * @param invokeOn
	 * @param toCast
	 * @return
	 */
	public static Object cast(Object invokeOn, Object toCast) {
		if (CAST_METHOD != null) {
			try {
				return CAST_METHOD.invoke(invokeOn, new Object[] { toCast });
			} catch (Throwable t) {
				return null;
			}
		}

		return null;
	}

	public static long getCurrentTimeNanosOrMillis() {
		if (systemNanoTimeMethod != null) {
			try {
				return ((Long) systemNanoTimeMethod.invoke(null, null))
						.longValue();
			} catch (IllegalArgumentException e) {
				// ignore - fall through to currentTimeMillis()
			} catch (IllegalAccessException e) {
				// ignore - fall through to currentTimeMillis()
			} catch (InvocationTargetException e) {
				// ignore - fall through to currentTimeMillis()
			}
		}

		return System.currentTimeMillis();
	}
	
	public static void resultSetToMap(Map mappedValues, java.sql.ResultSet rs)
			throws SQLException {
		while (rs.next()) {
			mappedValues.put(rs.getObject(1), rs.getObject(2));
		}
	}

	public static Map calculateDifferences(Map map1, Map map2) {
		Map diffMap = new HashMap();

		Iterator map1Entries = map1.entrySet().iterator();

		while (map1Entries.hasNext()) {
			Map.Entry entry = (Map.Entry) map1Entries.next();
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
				diffMap.put(key, new Byte(
						(byte) (((Byte) value2).byteValue() - ((Byte) value1)
								.byteValue())));
			} else if (value1 instanceof Short) {
				diffMap.put(key, new Short((short) (((Short) value2)
						.shortValue() - ((Short) value1).shortValue())));
			} else if (value1 instanceof Integer) {
				diffMap.put(key, new Integer(
						(((Integer) value2).intValue() - ((Integer) value1)
								.intValue())));
			} else if (value1 instanceof Long) {
				diffMap.put(key, new Long(
						(((Long) value2).longValue() - ((Long) value1)
								.longValue())));
			} else if (value1 instanceof Float) {
				diffMap.put(key, new Float(((Float) value2).floatValue()
						- ((Float) value1).floatValue()));
			} else if (value1 instanceof Double) {
				diffMap.put(key, new Double(
						(((Double) value2).shortValue() - ((Double) value1)
								.shortValue())));
			} else if (value1 instanceof BigDecimal) {
				diffMap.put(key, ((BigDecimal) value2)
						.subtract((BigDecimal) value1));
			} else if (value1 instanceof BigInteger) {
				diffMap.put(key, ((BigInteger) value2)
						.subtract((BigInteger) value1));
			}
		}

		return diffMap;
	}
	
	public static List loadExtensions(Connection conn,
			Properties props, String extensionClassNames,
			String errorMessageKey, ExceptionInterceptor exceptionInterceptor) throws SQLException {
		List extensionList = new LinkedList();

		List interceptorsToCreate = StringUtils.split(extensionClassNames, ",",
				true);

		Iterator iter = interceptorsToCreate.iterator();

		String className = null;

		try {
			while (iter.hasNext()) {
				className = iter.next().toString();
				Extension extensionInstance = (Extension) Class.forName(
						className).newInstance();
				extensionInstance.init(conn, props);

				extensionList.add(extensionInstance);
			}
		} catch (Throwable t) {
			SQLException sqlEx = SQLError.createSQLException(Messages
					.getString(errorMessageKey, new Object[] { className }), exceptionInterceptor);
			sqlEx.initCause(t);

			throw sqlEx;
		}

		return extensionList;
	}

}