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

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Converter for char[]->byte[] and byte[]->char[] for single-byte character
 * sets.
 * 
 * Much faster (5-6x) than the built-in solution that ships with the JVM, even
 * with JDK-1.4.x and NewIo.
 * 
 * @author Mark Matthews
 */
public class SingleByteCharsetConverter {

	private static final int BYTE_RANGE = (1 + Byte.MAX_VALUE) - Byte.MIN_VALUE;
	private static byte[] allBytes = new byte[BYTE_RANGE];
	private static final Map CONVERTER_MAP = new HashMap();

	private final static byte[] EMPTY_BYTE_ARRAY = new byte[0];

	// The initial charToByteMap, with all char mappings mapped
	// to (byte) '?', so that unknown characters are mapped to '?'
	// instead of '\0' (which means end-of-string to MySQL).
	private static byte[] unknownCharsMap = new byte[65536];

	static {
		for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
			allBytes[i - Byte.MIN_VALUE] = (byte) i;
		}

		for (int i = 0; i < unknownCharsMap.length; i++) {
			unknownCharsMap[i] = (byte) '?'; // use something 'sane' for
												// unknown chars
		}
	}

	// ~ Instance fields
	// --------------------------------------------------------

	/**
	 * Get a converter for the given encoding name
	 * 
	 * @param encodingName
	 *            the Java character encoding name
	 * 
	 * @return a converter for the given encoding name
	 * @throws UnsupportedEncodingException
	 *             if the character encoding is not supported
	 */
	public static synchronized SingleByteCharsetConverter getInstance(
			String encodingName, Connection conn)
			throws UnsupportedEncodingException, SQLException {
		SingleByteCharsetConverter instance = (SingleByteCharsetConverter) CONVERTER_MAP
				.get(encodingName);

		if (instance == null) {
			instance = initCharset(encodingName);
		}

		return instance;
	}

	/**
	 * Initialize the shared instance of a converter for the given character
	 * encoding.
	 * 
	 * @param javaEncodingName
	 *            the Java name for the character set to initialize
	 * @return a converter for the given character set
	 * @throws UnsupportedEncodingException
	 *             if the character encoding is not supported
	 */
	public static SingleByteCharsetConverter initCharset(String javaEncodingName)
			throws UnsupportedEncodingException, SQLException {
		if (CharsetMapping.isMultibyteCharset(javaEncodingName)) {
			return null;
		}

		SingleByteCharsetConverter converter = new SingleByteCharsetConverter(
				javaEncodingName);

		CONVERTER_MAP.put(javaEncodingName, converter);

		return converter;
	}

	// ~ Constructors
	// -----------------------------------------------------------

	/**
	 * Convert the byte buffer from startPos to a length of length to a string
	 * using the default platform encoding.
	 * 
	 * @param buffer
	 *            the bytes to convert
	 * @param startPos
	 *            the index to start at
	 * @param length
	 *            the number of bytes to convert
	 * @return the String representation of the given bytes
	 */
	public static String toStringDefaultEncoding(byte[] buffer, int startPos,
			int length) {
		return new String(buffer, startPos, length);
	}

	// ~ Methods
	// ----------------------------------------------------------------

	private char[] byteToChars = new char[BYTE_RANGE];

	private byte[] charToByteMap = new byte[65536];

	/**
	 * Prevent instantiation, called out of static method initCharset().
	 * 
	 * @param encodingName
	 *            a JVM character encoding
	 * @throws UnsupportedEncodingException
	 *             if the JVM does not support the encoding
	 */
	private SingleByteCharsetConverter(String encodingName)
			throws UnsupportedEncodingException {
		String allBytesString = new String(allBytes, 0, BYTE_RANGE,
				encodingName);
		int allBytesLen = allBytesString.length();

		System.arraycopy(unknownCharsMap, 0, this.charToByteMap, 0,
				this.charToByteMap.length);

		for (int i = 0; i < BYTE_RANGE && i < allBytesLen; i++) {
			char c = allBytesString.charAt(i);
			this.byteToChars[i] = c;
			this.charToByteMap[c] = allBytes[i];
		}
	}

	public final byte[] toBytes(char[] c) {
		if (c == null) {
			return null;
		}

		int length = c.length;
		byte[] bytes = new byte[length];

		for (int i = 0; i < length; i++) {
			bytes[i] = this.charToByteMap[c[i]];
		}

		return bytes;
	}
	
	public final byte[] toBytesWrapped(char[] c, char beginWrap, char endWrap) {
		if (c == null) {
			return null;
		}

		int length = c.length + 2;
		int charLength = c.length;
		
		byte[] bytes = new byte[length];
		bytes[0] = this.charToByteMap[beginWrap];
		
		for (int i = 0; i < charLength; i++) {
			bytes[i + 1] = this.charToByteMap[c[i]];
		}
		
		bytes[length - 1] = this.charToByteMap[endWrap];

		return bytes;
	}

	public final byte[] toBytes(char[] chars, int offset, int length) {
		if (chars == null) {
			return null;
		}

		if (length == 0) {
			return EMPTY_BYTE_ARRAY;
		}

		byte[] bytes = new byte[length];

		for (int i = 0; (i < length); i++) {
			bytes[i] = this.charToByteMap[chars[i + offset]];
		}

		return bytes;
	}

	/**
	 * Convert the given string to an array of bytes.
	 * 
	 * @param s
	 *            the String to convert
	 * @return the bytes that make up the String
	 */
	public final byte[] toBytes(String s) {
		if (s == null) {
			return null;
		}

		int length = s.length();
		byte[] bytes = new byte[length];

		for (int i = 0; i < length; i++) {
			bytes[i] = this.charToByteMap[s.charAt(i)];
		}

		return bytes;
	}
	
	public final byte[] toBytesWrapped(String s, char beginWrap, char endWrap) {
		if (s == null) {
			return null;
		}

		int stringLength = s.length();
		
		int length = stringLength + 2;
		
		byte[] bytes = new byte[length];
		
		bytes[0] = this.charToByteMap[beginWrap];
		
		for (int i = 0; i < stringLength; i++) {
			bytes[i + 1] = this.charToByteMap[s.charAt(i)];
		}

		bytes[length - 1] = this.charToByteMap[endWrap];
		
		return bytes;
	}

	/**
	 * Convert the given string to an array of bytes.
	 * 
	 * @param s
	 *            the String to convert
	 * @param offset
	 *            the offset to start at
	 * @param length
	 *            length (max) to convert
	 * 
	 * @return the bytes that make up the String
	 */
	public final byte[] toBytes(String s, int offset, int length) {
		if (s == null) {
			return null;
		}

		if (length == 0) {
			return EMPTY_BYTE_ARRAY;
		}

		byte[] bytes = new byte[length];

		for (int i = 0; (i < length); i++) {
			char c = s.charAt(i + offset);
			bytes[i] = this.charToByteMap[c];
		}

		return bytes;
	}

	/**
	 * Convert the byte buffer to a string using this instance's character
	 * encoding.
	 * 
	 * @param buffer
	 *            the bytes to convert to a String
	 * @return the converted String
	 */
	public final String toString(byte[] buffer) {
		return toString(buffer, 0, buffer.length);
	}

	/**
	 * Convert the byte buffer from startPos to a length of length to a string
	 * using this instance's character encoding.
	 * 
	 * @param buffer
	 *            the bytes to convert
	 * @param startPos
	 *            the index to start at
	 * @param length
	 *            the number of bytes to convert
	 * @return the String representation of the given bytes
	 */
	public final String toString(byte[] buffer, int startPos, int length) {
		char[] charArray = new char[length];
		int readpoint = startPos;

		for (int i = 0; i < length; i++) {
			charArray[i] = this.byteToChars[buffer[readpoint] - Byte.MIN_VALUE];
			readpoint++;
		}

		return new String(charArray);
	}
}
