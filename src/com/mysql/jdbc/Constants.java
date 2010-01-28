/*
 Copyright  2002-2004 MySQL AB, 2008 Sun Microsystems
 All rights reserved. Use is subject to license terms.

  The MySQL Connector/J is licensed under the terms of the GPL,
  like most MySQL Connectors. There are special exceptions to the
  terms and conditions of the GPL as it is applied to this software,
  see the FLOSS License Exception available on mysql.com.

  This program is free software; you can redistribute it and/or
  modify it under the terms of the GNU General Public License as
  published by the Free Software Foundation; version 2 of the
  License.

  This program is distributed in the hope that it will be useful,  
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  02110-1301 USA



 */
package com.mysql.jdbc;

/**
 * Represents various constants used in the driver.
 * 
 * @author Mark Matthews
 * 
 * @version $Id$
 */
public class Constants {
	/**
	 * Avoids allocation of empty byte[] when representing 0-length strings.
	 */
	public final static byte[] EMPTY_BYTE_ARRAY = new byte[0];

	/**
	 * I18N'd representation of the abbreviation for "ms"
	 */
	public final static String MILLIS_I18N = Messages.getString("Milliseconds");
	
	public final static byte[] SLASH_STAR_SPACE_AS_BYTES = new byte[] {
			(byte) '/', (byte) '*', (byte) ' ' };

	public final static byte[] SPACE_STAR_SLASH_SPACE_AS_BYTES = new byte[] {
			(byte) ' ', (byte) '*', (byte) '/', (byte) ' ' };

	/*
	 * We're still stuck on JDK-1.4.2, but want the Number.valueOf() methods
	 */
	private static final Character[] CHARACTER_CACHE = new Character[128];

	private static final int BYTE_CACHE_OFFSET = 128;

	private static final Byte[] BYTE_CACHE = new Byte[256];

	private static final int INTEGER_CACHE_OFFSET = 128;

	private static final Integer[] INTEGER_CACHE = new Integer[256];

	private static final int SHORT_CACHE_OFFSET = 128;

	private static final Short[] SHORT_CACHE = new Short[256];

	private static final Long[] LONG_CACHE = new Long[256];

	private static final int LONG_CACHE_OFFSET = 128;

	static {
		for (int i = 0; i < CHARACTER_CACHE.length; i++) {
			CHARACTER_CACHE[i] = new Character((char) i);
		}

		for (int i = 0; i < INTEGER_CACHE.length; i++) {
			INTEGER_CACHE[i] = new Integer(i - 128);
		}

		for (int i = 0; i < SHORT_CACHE.length; i++) {
			SHORT_CACHE[i] = new Short((short) (i - 128));
		}

		for (int i = 0; i < LONG_CACHE.length; i++) {
			LONG_CACHE[i] = new Long(i - 128);
		}

		for (int i = 0; i < BYTE_CACHE.length; i++)
			BYTE_CACHE[i] = new Byte((byte) (i - BYTE_CACHE_OFFSET));
	}

	/** Same behavior as JDK-1.5's Constants.characterValueOf(int) */
	
	public static Character characterValueOf(char c) {
		if (c <= 127) {
			return CHARACTER_CACHE[c];
		}

		return new Character(c);
	}

	/** Same behavior as JDK-1.5's Byte.valueOf(int) */

	public static final Byte byteValueOf(byte b) {
		return BYTE_CACHE[b + BYTE_CACHE_OFFSET];
	}

	/** Same behavior as JDK-1.5's Integer.valueOf(int) */

	public static final Integer integerValueOf(int i) {
		if (i >= -128 && i <= 127) {
			return INTEGER_CACHE[i + INTEGER_CACHE_OFFSET];
		}

		return new Integer(i);
	}

	/** Same behavior as JDK-1.5's Constants.shortValueOf(int) */
	
	public static Short shortValueOf(short s) {

		if (s >= -128 && s <= 127) {
			return SHORT_CACHE[s + SHORT_CACHE_OFFSET];
		}
		
		return new Short(s);
	}

	/** Same behavior as JDK-1.5's Long.valueOf(int) */
	
	public static final Long longValueOf(long l) {
		if (l >= -128 && l <= 127) {
			return LONG_CACHE[(int) l + LONG_CACHE_OFFSET];
		}

		return new Long(l);
	}

	/**
	 * Prevents instantiation
	 */
	private Constants() {
	}
}
