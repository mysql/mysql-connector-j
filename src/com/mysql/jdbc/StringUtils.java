/*
 Copyright (c) 2002, 2013, Oracle and/or its affiliates. All rights reserved.
 

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
package com.mysql.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Various utility methods for converting to/from byte arrays in the platform
 * encoding
 * 
 * @author Mark Matthews
 */
public class StringUtils {
	
	private static final int BYTE_RANGE = (1 + Byte.MAX_VALUE) - Byte.MIN_VALUE;

	private static byte[] allBytes = new byte[BYTE_RANGE];

	private static char[] byteToChars = new char[BYTE_RANGE];

	private static Method toPlainStringMethod;

	static final int WILD_COMPARE_MATCH_NO_WILD = 0;

	static final int WILD_COMPARE_MATCH_WITH_WILD = 1;

	static final int WILD_COMPARE_NO_MATCH = -1;

	private static final ConcurrentHashMap<String,Charset> charsetsByAlias = 
	     new ConcurrentHashMap<String,Charset>();

	private static final String platformEncoding = System.getProperty("file.encoding");
	
	private static final String VALID_ID_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIGKLMNOPQRSTUVWXYZ0123456789$_#@";
	
	static Charset findCharset(String alias) throws UnsupportedEncodingException {
		try {
			Charset cs = charsetsByAlias.get(alias);
			
			if (cs == null) {
				cs = Charset.forName(alias);
				charsetsByAlias.putIfAbsent(alias, cs);
			}
		
			return cs;
			
			// We re-throw these runtimes for compatibility with java.io
		} catch (UnsupportedCharsetException uce) {
			throw new UnsupportedEncodingException(alias);
		} catch (IllegalCharsetNameException icne) {
			throw new UnsupportedEncodingException(alias);
		} catch (IllegalArgumentException iae) {
			throw new UnsupportedEncodingException(alias);
		}
	}
	
	static {
		for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
			allBytes[i - Byte.MIN_VALUE] = (byte) i;
		}

		String allBytesString = new String(allBytes, 0, Byte.MAX_VALUE
				- Byte.MIN_VALUE);

		int allBytesStringLen = allBytesString.length();

		for (int i = 0; (i < (Byte.MAX_VALUE - Byte.MIN_VALUE))
				&& (i < allBytesStringLen); i++) {
			byteToChars[i] = allBytesString.charAt(i);
		}

		try {
			toPlainStringMethod = BigDecimal.class.getMethod("toPlainString",
					new Class[0]);
		} catch (NoSuchMethodException nsme) {
			// that's okay, we fallback to .toString()
		}
	}

	/**
	 * Takes care of the fact that Sun changed the output of
	 * BigDecimal.toString() between JDK-1.4 and JDK 5
	 * 
	 * @param decimal
	 *            the big decimal to stringify
	 * 
	 * @return a string representation of 'decimal'
	 */
	public static String consistentToString(BigDecimal decimal) {
		if (decimal == null) {
			return null;
		}

		if (toPlainStringMethod != null) {
			try {
				return (String) toPlainStringMethod.invoke(decimal, (Object[])null);
			} catch (InvocationTargetException invokeEx) {
				// that's okay, we fall-through to decimal.toString()
			} catch (IllegalAccessException accessEx) {
				// that's okay, we fall-through to decimal.toString()
			}
		}

		return decimal.toString();
	}

	/**
	 * Dumps the given bytes to STDOUT as a hex dump (up to length bytes).
	 * 
	 * @param byteBuffer
	 *            the data to print as hex
	 * @param length
	 *            the number of bytes to print
	 * 
	 * @return ...
	 */
	public static final String dumpAsHex(byte[] byteBuffer, int length) {
		StringBuffer outputBuf = new StringBuffer(length * 4);

		int p = 0;
		int rows = length / 8;

		for (int i = 0; (i < rows) && (p < length); i++) {
			int ptemp = p;

			for (int j = 0; j < 8; j++) {
				String hexVal = Integer.toHexString(byteBuffer[ptemp] & 0xff);

				if (hexVal.length() == 1) {
					hexVal = "0" + hexVal; //$NON-NLS-1$
				}

				outputBuf.append(hexVal + " "); //$NON-NLS-1$
				ptemp++;
			}

			outputBuf.append("    "); //$NON-NLS-1$

			for (int j = 0; j < 8; j++) {
				int b = 0xff & byteBuffer[p];
				
				if (b > 32 && b < 127) {
					outputBuf.append((char) b + " "); //$NON-NLS-1$
				} else {
					outputBuf.append(". "); //$NON-NLS-1$
				}

				p++;
			}

			outputBuf.append("\n"); //$NON-NLS-1$
		}

		int n = 0;

		for (int i = p; i < length; i++) {
			String hexVal = Integer.toHexString(byteBuffer[i] & 0xff);

			if (hexVal.length() == 1) {
				hexVal = "0" + hexVal; //$NON-NLS-1$
			}

			outputBuf.append(hexVal + " "); //$NON-NLS-1$
			n++;
		}

		for (int i = n; i < 8; i++) {
			outputBuf.append("   "); //$NON-NLS-1$
		}

		outputBuf.append("    "); //$NON-NLS-1$

		for (int i = p; i < length; i++) {
			int b = 0xff & byteBuffer[i];
			
			if (b > 32 && b < 127) {
				outputBuf.append((char) b + " "); //$NON-NLS-1$
			} else {
				outputBuf.append(". "); //$NON-NLS-1$
			}
		}

		outputBuf.append("\n"); //$NON-NLS-1$

		return outputBuf.toString();
	}

	private static boolean endsWith(byte[] dataFrom, String suffix) {
		for (int i = 1; i <= suffix.length(); i++) {
			int dfOffset = dataFrom.length - i;
			int suffixOffset = suffix.length() - i;
			if (dataFrom[dfOffset] != suffix.charAt(suffixOffset)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Unfortunately, SJIS has 0x5c as a high byte in some of its double-byte
	 * characters, so we need to escape it.
	 * 
	 * @param origBytes
	 *            the original bytes in SJIS format
	 * @param origString
	 *            the string that had .getBytes() called on it
	 * @param offset
	 *            where to start converting from
	 * @param length
	 *            how many characters to convert.
	 * 
	 * @return byte[] with 0x5c escaped
	 */
	public static byte[] escapeEasternUnicodeByteStream(byte[] origBytes,
			String origString, int offset, int length) {
		if ((origBytes == null) || (origBytes.length == 0)) {
			return origBytes;
		}

		int bytesLen = origBytes.length;
		int bufIndex = 0;
		int strIndex = 0;

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream(bytesLen);

		while (true) {
			if (origString.charAt(strIndex) == '\\') {
				// write it out as-is
				bytesOut.write(origBytes[bufIndex++]);

				// bytesOut.write(origBytes[bufIndex++]);
			} else {
				// Grab the first byte
				int loByte = origBytes[bufIndex];

				if (loByte < 0) {
					loByte += 256; // adjust for signedness/wrap-around
				}

				// We always write the first byte
				bytesOut.write(loByte);

				//
				// The codepage characters in question exist between
				// 0x81-0x9F and 0xE0-0xFC...
				//
				// See:
				//
				// http://www.microsoft.com/GLOBALDEV/Reference/dbcs/932.htm
				//
				// Problematic characters in GBK
				//
				// U+905C : CJK UNIFIED IDEOGRAPH
				//
				// Problematic characters in Big5
				//
				// B9F0 = U+5C62 : CJK UNIFIED IDEOGRAPH
				//
				if (loByte >= 0x80) {
					if (bufIndex < (bytesLen - 1)) {
						int hiByte = origBytes[bufIndex + 1];

						if (hiByte < 0) {
							hiByte += 256; // adjust for signedness/wrap-around
						}

						// write the high byte here, and increment the index
						// for the high byte
						bytesOut.write(hiByte);
						bufIndex++;

						// escape 0x5c if necessary
						if (hiByte == 0x5C) {
							bytesOut.write(hiByte);
						}
					}
				} else if (loByte == 0x5c) {
					if (bufIndex < (bytesLen - 1)) {
						int hiByte = origBytes[bufIndex + 1];

						if (hiByte < 0) {
							hiByte += 256; // adjust for signedness/wrap-around
						}

						if (hiByte == 0x62) {
							// we need to escape the 0x5c
							bytesOut.write(0x5c);
							bytesOut.write(0x62);
							bufIndex++;
						}
					}
				}

				bufIndex++;
			}

			if (bufIndex >= bytesLen) {
				// we're done
				break;
			}

			strIndex++;
		}

		return bytesOut.toByteArray();
	}

	/**
	 * Returns the first non whitespace char, converted to upper case
	 * 
	 * @param searchIn
	 *            the string to search in
	 * 
	 * @return the first non-whitespace character, upper cased.
	 */
	public static char firstNonWsCharUc(String searchIn) {
		return firstNonWsCharUc(searchIn, 0);
	}
	
	public static char firstNonWsCharUc(String searchIn, int startAt) {
		if (searchIn == null) {
			return 0;
		}

		int length = searchIn.length();

		for (int i = startAt; i < length; i++) {
			char c = searchIn.charAt(i);

			if (!Character.isWhitespace(c)) {
				return Character.toUpperCase(c);
			}
		}

		return 0;
	}

	public static char firstAlphaCharUc(String searchIn, int startAt) {
		if (searchIn == null) {
			return 0;
		}

		int length = searchIn.length();

		for (int i = startAt; i < length; i++) {
			char c = searchIn.charAt(i);

			if (Character.isLetter(c)) {
				return Character.toUpperCase(c);
			}
		}

		return 0;
	}
	
	/**
	 * Adds '+' to decimal numbers that are positive (MySQL doesn't understand
	 * them otherwise
	 * 
	 * @param dString
	 *            The value as a string
	 * 
	 * @return String the string with a '+' added (if needed)
	 */
	public static final String fixDecimalExponent(String dString) {
		int ePos = dString.indexOf("E"); //$NON-NLS-1$

		if (ePos == -1) {
			ePos = dString.indexOf("e"); //$NON-NLS-1$
		}

		if (ePos != -1) {
			if (dString.length() > (ePos + 1)) {
				char maybeMinusChar = dString.charAt(ePos + 1);

				if (maybeMinusChar != '-' && maybeMinusChar != '+') {
					StringBuffer buf = new StringBuffer(dString.length() + 1);
					buf.append(dString.substring(0, ePos + 1));
					buf.append('+');
					buf.append(dString.substring(ePos + 1, dString.length()));
					dString = buf.toString();
				}
			}
		}

		return dString;
	}

	public static final byte[] getBytes(char[] c,
			SingleByteCharsetConverter converter, String encoding,
			String serverEncoding, boolean parserKnowsUnicode, ExceptionInterceptor exceptionInterceptor)
			throws SQLException {
		try {
			byte[] b = null;

			if (converter != null) {
				b = converter.toBytes(c);
			} else if (encoding == null) {
				b = new String(c).getBytes();
			} else {
				String s = new String(c);

				b = s.getBytes(encoding);

				if (!parserKnowsUnicode && (encoding.equalsIgnoreCase("SJIS") //$NON-NLS-1$
						|| encoding.equalsIgnoreCase("BIG5") //$NON-NLS-1$
				|| encoding.equalsIgnoreCase("GBK"))) { //$NON-NLS-1$

					if (!encoding.equalsIgnoreCase(serverEncoding)) {
						b = escapeEasternUnicodeByteStream(b, s, 0, s.length());
					}
				}
			}

			return b;
		} catch (UnsupportedEncodingException uee) {
			throw SQLError.createSQLException(Messages.getString("StringUtils.5") //$NON-NLS-1$
					+ encoding + Messages.getString("StringUtils.6"),
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor); //$NON-NLS-1$
		}
	}

	public static final byte[] getBytes(char[] c,
			SingleByteCharsetConverter converter, String encoding,
			String serverEncoding, int offset, int length,
			boolean parserKnowsUnicode, ExceptionInterceptor exceptionInterceptor) throws SQLException {
		try {
			byte[] b = null;

			if (converter != null) {
				b = converter.toBytes(c, offset, length);
			} else if (encoding == null) {
				byte[] temp = new String(c, offset, length).getBytes();

				length = temp.length;
				
				b = new byte[length];
				System.arraycopy(temp, 0, b, 0, length);
			} else {
				String s = new String(c, offset, length);

				byte[] temp = s.getBytes(encoding);

				length = temp.length;
				
				b = new byte[length];
				System.arraycopy(temp, 0, b, 0, length);

				if (!parserKnowsUnicode && (encoding.equalsIgnoreCase("SJIS") //$NON-NLS-1$
						|| encoding.equalsIgnoreCase("BIG5") //$NON-NLS-1$
				|| encoding.equalsIgnoreCase("GBK"))) { //$NON-NLS-1$

					if (!encoding.equalsIgnoreCase(serverEncoding)) {
						b = escapeEasternUnicodeByteStream(b, s, offset, length);
					}
				}
			}

			return b;
		} catch (UnsupportedEncodingException uee) {
			throw SQLError.createSQLException(Messages.getString("StringUtils.10") //$NON-NLS-1$
					+ encoding + Messages.getString("StringUtils.11"),
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor); //$NON-NLS-1$
		}
	}

	public static final byte[] getBytes(char[] c, String encoding,
			String serverEncoding, boolean parserKnowsUnicode, 
			MySQLConnection conn, ExceptionInterceptor exceptionInterceptor)
			throws SQLException {
		try {
			
			SingleByteCharsetConverter converter = null;
			
			if (conn != null) {
				converter = conn.getCharsetConverter(encoding);
			} else {
				converter = SingleByteCharsetConverter.getInstance(encoding, null);
			}

			return getBytes(c, converter, encoding, serverEncoding,
					parserKnowsUnicode, exceptionInterceptor);
		} catch (UnsupportedEncodingException uee) {
			throw SQLError.createSQLException(Messages.getString("StringUtils.0") //$NON-NLS-1$
					+ encoding + Messages.getString("StringUtils.1"),
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor); //$NON-NLS-1$
		}
	}

	/**
	 * Returns the byte[] representation of the given string (re)using the given
	 * charset converter, and the given encoding.
	 * 
	 * @param s
	 *            the string to convert
	 * @param converter
	 *            the converter to reuse
	 * @param encoding
	 *            the character encoding to use
	 * @param serverEncoding
	 *            DOCUMENT ME!
	 * @param parserKnowsUnicode
	 *            DOCUMENT ME!
	 * 
	 * @return byte[] representation of the string
	 * 
	 * @throws SQLException
	 *             if an encoding unsupported by the JVM is supplied.
	 */
	public static final byte[] getBytes(String s,
			SingleByteCharsetConverter converter, String encoding,
			String serverEncoding, boolean parserKnowsUnicode, ExceptionInterceptor exceptionInterceptor)
			throws SQLException {
		try {
			byte[] b = null;

			if (converter != null) {
				b = converter.toBytes(s);
			} else if (encoding == null) {
				b = s.getBytes();
			} else {
				b = s.getBytes(encoding);

				if (!parserKnowsUnicode && (encoding.equalsIgnoreCase("SJIS") //$NON-NLS-1$
						|| encoding.equalsIgnoreCase("BIG5") //$NON-NLS-1$
				|| encoding.equalsIgnoreCase("GBK"))) { //$NON-NLS-1$

					if (!encoding.equalsIgnoreCase(serverEncoding)) {
						b = escapeEasternUnicodeByteStream(b, s, 0, s.length());
					}
				}
			}

			return b;
		} catch (UnsupportedEncodingException uee) {
			throw SQLError.createSQLException(Messages.getString("StringUtils.5") //$NON-NLS-1$
					+ encoding + Messages.getString("StringUtils.6"),
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor); //$NON-NLS-1$
		}
	}

	public static final byte[] getBytesWrapped(String s, char beginWrap, char endWrap,
			SingleByteCharsetConverter converter, String encoding,
			String serverEncoding, boolean parserKnowsUnicode, ExceptionInterceptor exceptionInterceptor)
			throws SQLException {
		try {
			byte[] b = null;

			if (converter != null) {
				b = converter.toBytesWrapped(s, beginWrap, endWrap);
			} else if (encoding == null) {
				StringBuffer buf = new StringBuffer(s.length() + 2);
				buf.append(beginWrap);
				buf.append(s);
				buf.append(endWrap);
				
				b = buf.toString().getBytes();
			} else {
				StringBuffer buf = new StringBuffer(s.length() + 2);
				buf.append(beginWrap);
				buf.append(s);
				buf.append(endWrap);
				
				s = buf.toString();
				b = s.getBytes(encoding);

				if (!parserKnowsUnicode && (encoding.equalsIgnoreCase("SJIS") //$NON-NLS-1$
						|| encoding.equalsIgnoreCase("BIG5") //$NON-NLS-1$
				|| encoding.equalsIgnoreCase("GBK"))) { //$NON-NLS-1$

					if (!encoding.equalsIgnoreCase(serverEncoding)) {
						b = escapeEasternUnicodeByteStream(b, s, 0, s.length());
					}
				}
			}

			return b;
		} catch (UnsupportedEncodingException uee) {
			throw SQLError.createSQLException(Messages.getString("StringUtils.5") //$NON-NLS-1$
					+ encoding + Messages.getString("StringUtils.6"),
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor); //$NON-NLS-1$
		}
	}
	
	/**
	 * DOCUMENT ME!
	 * 
	 * @param s
	 *            DOCUMENT ME!
	 * @param converter
	 *            DOCUMENT ME!
	 * @param encoding
	 *            DOCUMENT ME!
	 * @param serverEncoding
	 *            DOCUMENT ME!
	 * @param offset
	 *            DOCUMENT ME!
	 * @param length
	 *            DOCUMENT ME!
	 * @param parserKnowsUnicode
	 *            DOCUMENT ME!
	 * 
	 * @return DOCUMENT ME!
	 * 
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public static final byte[] getBytes(String s,
			SingleByteCharsetConverter converter, String encoding,
			String serverEncoding, int offset, int length,
			boolean parserKnowsUnicode, ExceptionInterceptor exceptionInterceptor) throws SQLException {
		try {
			byte[] b = null;

			if (converter != null) {
				b = converter.toBytes(s, offset, length);
			} else if (encoding == null) {
				byte[] temp = s.substring(offset, offset + length).getBytes();

				length = temp.length;
				
				b = new byte[length];
				System.arraycopy(temp, 0, b, 0, length);
			} else {

				byte[] temp = s.substring(offset, offset + length)
					.getBytes(encoding);

				length = temp.length;
				
				b = new byte[length];
				System.arraycopy(temp, 0, b, 0, length);

				if (!parserKnowsUnicode && (encoding.equalsIgnoreCase("SJIS") //$NON-NLS-1$
						|| encoding.equalsIgnoreCase("BIG5") //$NON-NLS-1$
				|| encoding.equalsIgnoreCase("GBK"))) { //$NON-NLS-1$

					if (!encoding.equalsIgnoreCase(serverEncoding)) {
						b = escapeEasternUnicodeByteStream(b, s, offset, length);
					}
				}
			}

			return b;
		} catch (UnsupportedEncodingException uee) {
			throw SQLError.createSQLException(Messages.getString("StringUtils.10") //$NON-NLS-1$
					+ encoding + Messages.getString("StringUtils.11"),
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor); //$NON-NLS-1$
		}
	}

	/**
	 * Returns the byte[] representation of the given string using given
	 * encoding.
	 * 
	 * @param s
	 *            the string to convert
	 * @param encoding
	 *            the character encoding to use
	 * @param parserKnowsUnicode
	 *            DOCUMENT ME!
	 * 
	 * @return byte[] representation of the string
	 * 
	 * @throws SQLException
	 *             if an encoding unsupported by the JVM is supplied.
	 */
	public static final byte[] getBytes(String s, String encoding,
			String serverEncoding, boolean parserKnowsUnicode, 
			MySQLConnection conn, ExceptionInterceptor exceptionInterceptor)
			throws SQLException {
		try {
			SingleByteCharsetConverter converter = null;
			
			if (conn != null) {
				converter = conn.getCharsetConverter(encoding);
			} else {
				converter = SingleByteCharsetConverter.getInstance(encoding, null);
			}

			return getBytes(s, converter, encoding, serverEncoding,
					parserKnowsUnicode, exceptionInterceptor);
		} catch (UnsupportedEncodingException uee) {
			throw SQLError.createSQLException(Messages.getString("StringUtils.0") //$NON-NLS-1$
					+ encoding + Messages.getString("StringUtils.1"),
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor); //$NON-NLS-1$
		}
	}

	public static int getInt(byte[] buf, int offset, int endPos) throws NumberFormatException {
		int base = 10;

		int s = offset;

		/* Skip white space. */
		while (Character.isWhitespace((char) buf[s]) && (s < endPos)) {
			++s;
		}

		if (s == endPos) {
			throw new NumberFormatException(StringUtils.toString(buf));
		}

		/* Check for a sign. */
		boolean negative = false;

		if ((char) buf[s] == '-') {
			negative = true;
			++s;
		} else if ((char) buf[s] == '+') {
			++s;
		}

		/* Save the pointer so we can check later if anything happened. */
		int save = s;

		int cutoff = Integer.MAX_VALUE / base;
		int cutlim = (Integer.MAX_VALUE % base);

		if (negative) {
			cutlim++;
		}

		boolean overflow = false;

		int i = 0;

		for (; s < endPos; s++) {
			char c = (char) buf[s];

			if (Character.isDigit(c)) {
				c -= '0';
			} else if (Character.isLetter(c)) {
				c = (char) (Character.toUpperCase(c) - 'A' + 10);
			} else {
				break;
			}

			if (c >= base) {
				break;
			}

			/* Check for overflow. */
			if ((i > cutoff) || ((i == cutoff) && (c > cutlim))) {
				overflow = true;
			} else {
				i *= base;
				i += c;
			}
		}

		if (s == save) {
			throw new NumberFormatException(StringUtils.toString(buf));
		}

		if (overflow) {
			throw new NumberFormatException(StringUtils.toString(buf));
		}

		/* Return the result of the appropriate sign. */
		return (negative ? (-i) : i);
	}
	
	public static int getInt(byte[] buf) throws NumberFormatException {
		return getInt(buf, 0, buf.length);
	}

	public static long getLong(byte[] buf) throws NumberFormatException {
		return getLong(buf, 0, buf.length);
	}
	
	public static long getLong(byte[] buf, int offset, int endpos) throws NumberFormatException {
		int base = 10;

		int s = offset;

		/* Skip white space. */
		while (Character.isWhitespace((char) buf[s]) && (s < endpos)) {
			++s;
		}

		if (s == endpos) {
			throw new NumberFormatException(StringUtils.toString(buf));
		}

		/* Check for a sign. */
		boolean negative = false;

		if ((char) buf[s] == '-') {
			negative = true;
			++s;
		} else if ((char) buf[s] == '+') {
			++s;
		}

		/* Save the pointer so we can check later if anything happened. */
		int save = s;

		long cutoff = Long.MAX_VALUE / base;
		long cutlim = (int) (Long.MAX_VALUE % base);

		if (negative) {
			cutlim++;
		}

		boolean overflow = false;
		long i = 0;

		for (; s < endpos; s++) {
			char c = (char) buf[s];

			if (Character.isDigit(c)) {
				c -= '0';
			} else if (Character.isLetter(c)) {
				c = (char) (Character.toUpperCase(c) - 'A' + 10);
			} else {
				break;
			}

			if (c >= base) {
				break;
			}

			/* Check for overflow. */
			if ((i > cutoff) || ((i == cutoff) && (c > cutlim))) {
				overflow = true;
			} else {
				i *= base;
				i += c;
			}
		}

		if (s == save) {
			throw new NumberFormatException(StringUtils.toString(buf));
		}

		if (overflow) {
			throw new NumberFormatException(StringUtils.toString(buf));
		}

		/* Return the result of the appropriate sign. */
		return (negative ? (-i) : i);
	}

	public static short getShort(byte[] buf) throws NumberFormatException {
		short base = 10;

		int s = 0;

		/* Skip white space. */
		while (Character.isWhitespace((char) buf[s]) && (s < buf.length)) {
			++s;
		}

		if (s == buf.length) {
			throw new NumberFormatException(StringUtils.toString(buf));
		}

		/* Check for a sign. */
		boolean negative = false;

		if ((char) buf[s] == '-') {
			negative = true;
			++s;
		} else if ((char) buf[s] == '+') {
			++s;
		}

		/* Save the pointer so we can check later if anything happened. */
		int save = s;

		short cutoff = (short) (Short.MAX_VALUE / base);
		short cutlim = (short) (Short.MAX_VALUE % base);

		if (negative) {
			cutlim++;
		}

		boolean overflow = false;
		short i = 0;

		for (; s < buf.length; s++) {
			char c = (char) buf[s];

			if (Character.isDigit(c)) {
				c -= '0';
			} else if (Character.isLetter(c)) {
				c = (char) (Character.toUpperCase(c) - 'A' + 10);
			} else {
				break;
			}

			if (c >= base) {
				break;
			}

			/* Check for overflow. */
			if ((i > cutoff) || ((i == cutoff) && (c > cutlim))) {
				overflow = true;
			} else {
				i *= base;
				i += c;
			}
		}

		if (s == save) {
			throw new NumberFormatException(StringUtils.toString(buf));
		}

		if (overflow) {
			throw new NumberFormatException(StringUtils.toString(buf));
		}

		/* Return the result of the appropriate sign. */
		return (negative ? (short) -i : (short) i);
	}

	public final static int indexOfIgnoreCase(int startingPosition,
			String searchIn, String searchFor) {
		if ((searchIn == null) || (searchFor == null)
				|| startingPosition > searchIn.length()) {
			return -1;
		}

		int patternLength = searchFor.length();
		int stringLength = searchIn.length();
		int stopSearchingAt = stringLength - patternLength;

		if (patternLength == 0) {
			return -1;
		}

		// Brute force string pattern matching
		// Some locales don't follow upper-case rule, so need to check both
		char firstCharOfPatternUc = Character.toUpperCase(searchFor.charAt(0));
		char firstCharOfPatternLc = Character.toLowerCase(searchFor.charAt(0));

		// note, this also catches the case where patternLength > stringLength
        for (int i = startingPosition; i <= stopSearchingAt; i++) {
            if (isNotEqualIgnoreCharCase(searchIn, firstCharOfPatternUc,
					firstCharOfPatternLc, i)) {
            	// find the first occurrence of the first character of searchFor in searchIn
                while (++i <= stopSearchingAt && (isNotEqualIgnoreCharCase(searchIn, firstCharOfPatternUc,
						firstCharOfPatternLc, i)));
            }

            if (i <= stopSearchingAt /* searchFor might be one character long! */) {
            	// walk searchIn and searchFor in lock-step starting just past the first match,bail out if not 
            	// a match, or we've hit the end of searchFor...
                int j = i + 1;
                int end = j + patternLength - 1;
                for (int k = 1; j < end && (Character.toLowerCase(searchIn.charAt(j)) == 
                	Character.toLowerCase(searchFor.charAt(k)) || Character.toUpperCase(searchIn.charAt(j)) == 
                    	Character.toUpperCase(searchFor.charAt(k))); j++, k++);

                if (j == end) {
                    return i;
                }
            }
        }
        
        return -1;
	}

	private final static boolean isNotEqualIgnoreCharCase(String searchIn,
			char firstCharOfPatternUc, char firstCharOfPatternLc, int i) {
		return Character.toLowerCase(searchIn.charAt(i)) != firstCharOfPatternLc && Character.toUpperCase(searchIn.charAt(i)) != firstCharOfPatternUc;
	}
	

	/**
	 * DOCUMENT ME!
	 * 
	 * @param searchIn
	 *            DOCUMENT ME!
	 * @param searchFor
	 *            DOCUMENT ME!
	 * 
	 * @return DOCUMENT ME!
	 */
	public final static int indexOfIgnoreCase(String searchIn, String searchFor) {
		return indexOfIgnoreCase(0, searchIn, searchFor);
	}

	public static int indexOfIgnoreCaseRespectMarker(int startAt, String src,
			String target, String marker, String markerCloses,
			boolean allowBackslashEscapes) {
		char contextMarker = Character.MIN_VALUE;
		boolean escaped = false;
		int markerTypeFound = 0;
		int srcLength = src.length();
		int ind = 0;

		for (int i = startAt; i < srcLength; i++) {
			char c = src.charAt(i);

			if (allowBackslashEscapes && c == '\\') {
				escaped = !escaped;
			} else if (contextMarker != Character.MIN_VALUE && c == markerCloses.charAt(markerTypeFound) && !escaped) {
				contextMarker = Character.MIN_VALUE;
			} else if ((ind = marker.indexOf(c)) != -1 && !escaped
					&& contextMarker == Character.MIN_VALUE) {
				markerTypeFound = ind;
				contextMarker = c;
			} else if ((Character.toUpperCase(c) == Character.toUpperCase(target.charAt(0)) ||
					Character.toLowerCase(c) == Character.toLowerCase(target.charAt(0))) && !escaped
					&& contextMarker == Character.MIN_VALUE) {
				if (startsWithIgnoreCase(src, i, target))
					return i;
			}
		}

		return -1;

	}

	public static int indexOfIgnoreCaseRespectQuotes(int startAt, String src,
			String target, char quoteChar, boolean allowBackslashEscapes) {
		char contextMarker = Character.MIN_VALUE;
		boolean escaped = false;

		int srcLength = src.length();

		for (int i = startAt; i < srcLength; i++) {
			char c = src.charAt(i);

			if (allowBackslashEscapes && c == '\\') {
				escaped = !escaped;
			} else if (c == contextMarker && !escaped) {
				contextMarker = Character.MIN_VALUE;
			} else if (c == quoteChar && !escaped
					&& contextMarker == Character.MIN_VALUE) {
				contextMarker = c;
			// This test looks complex, but remember that in certain locales, upper case
			// of two different codepoints coverts to same codepoint, and vice-versa.
			} else if ((Character.toUpperCase(c) == Character.toUpperCase(target.charAt(0)) ||
					Character.toLowerCase(c) == Character.toLowerCase(target.charAt(0))) && !escaped
					&& contextMarker == Character.MIN_VALUE) {
				if (startsWithIgnoreCase(src, i, target))
					return i;
			}
		}

		return -1;

	}

	/**
	 * Splits stringToSplit into a list, using the given delimitter
	 * 
	 * @param stringToSplit
	 *            the string to split
	 * @param delimitter
	 *            the string to split on
	 * @param trim
	 *            should the split strings be whitespace trimmed?
	 * 
	 * @return the list of strings, split by delimitter
	 * 
	 * @throws IllegalArgumentException
	 *             DOCUMENT ME!
	 */
	public static final List<String> split(String stringToSplit, String delimitter,
			boolean trim) {
		if (stringToSplit == null) {
			return new ArrayList<String>();
		}

		if (delimitter == null) {
			throw new IllegalArgumentException();
		}

		StringTokenizer tokenizer = new StringTokenizer(stringToSplit,
				delimitter, false);

		List<String> splitTokens = new ArrayList<String>(tokenizer.countTokens());

		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();

			if (trim) {
				token = token.trim();
			}

			splitTokens.add(token);
		}

		return splitTokens;
	}

	/**
	 * Splits stringToSplit into a list, using the given delimitter
	 * 
	 * @param stringToSplit
	 *            the string to split
	 * @param delimitter
	 *            the string to split on
	 * @param trim
	 *            should the split strings be whitespace trimmed?
	 * 
	 * @return the list of strings, split by delimiter
	 * 
	 * @throws IllegalArgumentException
	 *             DOCUMENT ME!
	 */
	public static final List<String> split(String stringToSplit, String delimiter,
			String markers, String markerCloses, boolean trim) {
		if (stringToSplit == null) {
			return new ArrayList<String>();
		}

		if (delimiter == null) {
			throw new IllegalArgumentException();
		}

		int delimPos = 0;
		int currentPos = 0;

		List<String> splitTokens = new ArrayList<String>();

		while ((delimPos = indexOfIgnoreCaseRespectMarker(currentPos,
				stringToSplit, delimiter, markers, markerCloses, false)) != -1) {
			String token = stringToSplit.substring(currentPos, delimPos);

			if (trim) {
				token = token.trim();
			}

			splitTokens.add(token);
			currentPos = delimPos + 1;
		}

		if (currentPos < stringToSplit.length()) {
			String token = stringToSplit.substring(currentPos);

			if (trim) {
				token = token.trim();
			}

			splitTokens.add(token);
		}

		return splitTokens;
	}

	private static boolean startsWith(byte[] dataFrom, String chars) {
		for (int i = 0; i < chars.length(); i++) {
			if (dataFrom[i] != chars.charAt(i)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Determines whether or not the string 'searchIn' contains the string
	 * 'searchFor', dis-regarding case starting at 'startAt' Shorthand for a
	 * String.regionMatch(...)
	 * 
	 * @param searchIn
	 *            the string to search in
	 * @param startAt
	 *            the position to start at
	 * @param searchFor
	 *            the string to search for
	 * 
	 * @return whether searchIn starts with searchFor, ignoring case
	 */
	public static boolean startsWithIgnoreCase(String searchIn, int startAt,
			String searchFor) {
		return searchIn.regionMatches(true, startAt, searchFor, 0, searchFor
				.length());
	}

	/**
	 * Determines whether or not the string 'searchIn' contains the string
	 * 'searchFor', dis-regarding case. Shorthand for a String.regionMatch(...)
	 * 
	 * @param searchIn
	 *            the string to search in
	 * @param searchFor
	 *            the string to search for
	 * 
	 * @return whether searchIn starts with searchFor, ignoring case
	 */
	public static boolean startsWithIgnoreCase(String searchIn, String searchFor) {
		return startsWithIgnoreCase(searchIn, 0, searchFor);
	}

	/**
	 * Determines whether or not the sting 'searchIn' contains the string
	 * 'searchFor', disregarding case,leading whitespace and non-alphanumeric
	 * characters.
	 * 
	 * @param searchIn
	 *            the string to search in
	 * @param searchFor
	 *            the string to search for
	 * 
	 * @return true if the string starts with 'searchFor' ignoring whitespace
	 */
	public static boolean startsWithIgnoreCaseAndNonAlphaNumeric(
			String searchIn, String searchFor) {
		if (searchIn == null) {
			return searchFor == null;
		}

		int beginPos = 0;

		int inLength = searchIn.length();

		for (beginPos = 0; beginPos < inLength; beginPos++) {
			char c = searchIn.charAt(beginPos);

			if (Character.isLetterOrDigit(c)) {
				break;
			}
		}

		return startsWithIgnoreCase(searchIn, beginPos, searchFor);
	}

	/**
	 * Determines whether or not the sting 'searchIn' contains the string
	 * 'searchFor', disregarding case and leading whitespace
	 * 
	 * @param searchIn
	 *            the string to search in
	 * @param searchFor
	 *            the string to search for
	 * 
	 * @return true if the string starts with 'searchFor' ignoring whitespace
	 */
	public static boolean startsWithIgnoreCaseAndWs(String searchIn,
			String searchFor) {
		return startsWithIgnoreCaseAndWs(searchIn, searchFor, 0);
	}
	
	/**
	 * Determines whether or not the sting 'searchIn' contains the string
	 * 'searchFor', disregarding case and leading whitespace
	 * 
	 * @param searchIn
	 *            the string to search in
	 * @param searchFor
	 *            the string to search for
	 * @param beginPos
	 *            where to start searching
	 * 
	 * @return true if the string starts with 'searchFor' ignoring whitespace
	 */
	
	public static boolean startsWithIgnoreCaseAndWs(String searchIn,
			String searchFor, int beginPos) {
		if (searchIn == null) {
			return searchFor == null;
		}

		int inLength = searchIn.length();

		for (; beginPos < inLength; beginPos++) {
			if (!Character.isWhitespace(searchIn.charAt(beginPos))) {
				break;
			}
		}

		return startsWithIgnoreCase(searchIn, beginPos, searchFor);
	}

	/**
	 * @param bytesToStrip
	 * @param prefix
	 * @param suffix
	 * @return
	 */
	public static byte[] stripEnclosure(byte[] source, String prefix,
			String suffix) {
		if (source.length >= prefix.length() + suffix.length()
				&& startsWith(source, prefix) && endsWith(source, suffix)) {

			int totalToStrip = prefix.length() + suffix.length();
			int enclosedLength = source.length - totalToStrip;
			byte[] enclosed = new byte[enclosedLength];

			int startPos = prefix.length();
			int numToCopy = enclosed.length;
			System.arraycopy(source, startPos, enclosed, 0, numToCopy);

			return enclosed;
		}
		return source;
	}

	/**
	 * Returns the bytes as an ASCII String.
	 * 
	 * @param buffer
	 *            the bytes representing the string
	 * 
	 * @return The ASCII String.
	 */
	public static final String toAsciiString(byte[] buffer) {
		return toAsciiString(buffer, 0, buffer.length);
	}

	/**
	 * Returns the bytes as an ASCII String.
	 * 
	 * @param buffer
	 *            the bytes to convert
	 * @param startPos
	 *            the position to start converting
	 * @param length
	 *            the length of the string to convert
	 * 
	 * @return the ASCII string
	 */
	public static final String toAsciiString(byte[] buffer, int startPos,
			int length) {
		char[] charArray = new char[length];
		int readpoint = startPos;

		for (int i = 0; i < length; i++) {
			charArray[i] = (char) buffer[readpoint];
			readpoint++;
		}

		return new String(charArray);
	}

	/**
	 * Compares searchIn against searchForWildcard with wildcards (heavily
	 * borrowed from strings/ctype-simple.c in the server sources)
	 * 
	 * @param searchIn
	 *            the string to search in
	 * @param searchForWildcard
	 *            the string to search for, using the 'standard' SQL wildcard
	 *            chars of '%' and '_'
	 * 
	 * @return WILD_COMPARE_MATCH_NO_WILD if matched, WILD_COMPARE_NO_MATCH if
	 *         not matched with wildcard, WILD_COMPARE_MATCH_WITH_WILD if
	 *         matched with wildcard
	 */
	public static int wildCompare(String searchIn, String searchForWildcard) {
		if ((searchIn == null) || (searchForWildcard == null)) {
			return WILD_COMPARE_NO_MATCH;
		}

		if (searchForWildcard.equals("%")) { //$NON-NLS-1$

			return WILD_COMPARE_MATCH_WITH_WILD;
		}

		int result = WILD_COMPARE_NO_MATCH; /* Not found, using wildcards */

		char wildcardMany = '%';
		char wildcardOne = '_';
		char wildcardEscape = '\\';

		int searchForPos = 0;
		int searchForEnd = searchForWildcard.length();

		int searchInPos = 0;
		int searchInEnd = searchIn.length();

		while (searchForPos != searchForEnd) {
			char wildstrChar = searchForWildcard.charAt(searchForPos);

			while ((searchForWildcard.charAt(searchForPos) != wildcardMany)
					&& (wildstrChar != wildcardOne)) {
				if ((searchForWildcard.charAt(searchForPos) == wildcardEscape)
						&& ((searchForPos + 1) != searchForEnd)) {
					searchForPos++;
				}

				if ((searchInPos == searchInEnd)
						|| (Character.toUpperCase(searchForWildcard
								.charAt(searchForPos++)) != Character
								.toUpperCase(searchIn.charAt(searchInPos++)))) {
					return WILD_COMPARE_MATCH_WITH_WILD; /* No match */
				}

				if (searchForPos == searchForEnd) {
					return ((searchInPos != searchInEnd) ? WILD_COMPARE_MATCH_WITH_WILD
							: WILD_COMPARE_MATCH_NO_WILD); /*
															 * Match if both are
															 * at end
															 */
				}

				result = WILD_COMPARE_MATCH_WITH_WILD; /* Found an anchor char */
			}

			if (searchForWildcard.charAt(searchForPos) == wildcardOne) {
				do {
					if (searchInPos == searchInEnd) { /*
														 * Skip one char if
														 * possible
														 */

						return (result);
					}

					searchInPos++;
				} while ((++searchForPos < searchForEnd)
						&& (searchForWildcard.charAt(searchForPos) == wildcardOne));

				if (searchForPos == searchForEnd) {
					break;
				}
			}

			if (searchForWildcard.charAt(searchForPos) == wildcardMany) { /*
																			 * Found
																			 * w_many
																			 */

				char cmp;

				searchForPos++;

				/* Remove any '%' and '_' from the wild search string */
				for (; searchForPos != searchForEnd; searchForPos++) {
					if (searchForWildcard.charAt(searchForPos) == wildcardMany) {
						continue;
					}

					if (searchForWildcard.charAt(searchForPos) == wildcardOne) {
						if (searchInPos == searchInEnd) {
							return (WILD_COMPARE_NO_MATCH);
						}

						searchInPos++;

						continue;
					}

					break; /* Not a wild character */
				}

				if (searchForPos == searchForEnd) {
					return WILD_COMPARE_MATCH_NO_WILD; /* Ok if w_many is last */
				}

				if (searchInPos == searchInEnd) {
					return WILD_COMPARE_NO_MATCH;
				}

				if (((cmp = searchForWildcard.charAt(searchForPos)) == wildcardEscape)
						&& ((searchForPos + 1) != searchForEnd)) {
					cmp = searchForWildcard.charAt(++searchForPos);
				}

				searchForPos++;

				do {
					while ((searchInPos != searchInEnd)
							&& (Character.toUpperCase(searchIn
									.charAt(searchInPos)) != Character
									.toUpperCase(cmp)))
						searchInPos++;

					if (searchInPos++ == searchInEnd) {
						return WILD_COMPARE_NO_MATCH;
					}

					{
						int tmp = wildCompare(searchIn, searchForWildcard);

						if (tmp <= 0) {
							return (tmp);
						}
					}
				} while ((searchInPos != searchInEnd)
						&& (searchForWildcard.charAt(0) != wildcardMany));

				return WILD_COMPARE_NO_MATCH;
			}
		}

		return ((searchInPos != searchInEnd) ? WILD_COMPARE_MATCH_WITH_WILD
				: WILD_COMPARE_MATCH_NO_WILD);
	}
	
	static byte[] s2b(String s, MySQLConnection conn) throws SQLException {
		if (s == null) {
			return null;
		}
		
		if ((conn != null) && conn.getUseUnicode()) {
			try {
				String encoding = conn.getEncoding();

				if (encoding == null) {
					return s.getBytes();
				}

				SingleByteCharsetConverter converter = conn
						.getCharsetConverter(encoding);

				if (converter != null) {
					return converter.toBytes(s);
				}

				return s.getBytes(encoding);
			} catch (java.io.UnsupportedEncodingException E) {
				return s.getBytes();
			}
		}

		return s.getBytes();
	}
	
	public static int lastIndexOf(byte[] s, char c) {
		if (s == null) {
			return -1;
		}
		
		for (int i = s.length - 1; i >= 0; i--) {
			if (s[i] == c) {
				return i;
			}
		}
		
		return -1;
	}

	public static int indexOf(byte[] s, char c) {
		if (s == null) {
			return -1;
		}
		
		int length = s.length;
		
		for (int i = 0; i < length; i++) {
			if (s[i] == c) {
				return i;
			}
		}
		
		return -1;
	}
	
	public static boolean isNullOrEmpty(String toTest) {
		return (toTest == null || toTest.length() == 0);
	}
	
	/**
	 * Returns the given string, with comments removed
	 * 
	 * @param src
	 *            the source string
	 * @param stringOpens
	 *            characters which delimit the "open" of a string
	 * @param stringCloses
	 *            characters which delimit the "close" of a string, in
	 *            counterpart order to <code>stringOpens</code>
	 * @param slashStarComments
	 *            strip slash-star type "C" style comments
	 * @param slashSlashComments
	 *            strip slash-slash C++ style comments to end-of-line
	 * @param hashComments
	 *            strip #-style comments to end-of-line
	 * @param dashDashComments
	 *            strip "--" style comments to end-of-line
	 * @return the input string with all comment-delimited data removed
	 */
	public static String stripComments(String src, String stringOpens,
			String stringCloses, boolean slashStarComments,
			boolean slashSlashComments, boolean hashComments,
			boolean dashDashComments) {
		if (src == null) {
			return null;
		}

		StringBuffer buf = new StringBuffer(src.length());

		// It's just more natural to deal with this as a stream
		// when parsing..This code is currently only called when
		// parsing the kind of metadata that developers are strongly
		// recommended to cache anyways, so we're not worried
		// about the _1_ extra object allocation if it cleans
		// up the code

		StringReader sourceReader = new StringReader(src);

		int contextMarker = Character.MIN_VALUE;
		boolean escaped = false;
		int markerTypeFound = -1;

		int ind = 0;

		int currentChar = 0;

		try {
			while ((currentChar = sourceReader.read()) != -1) {

				if (false && currentChar == '\\') {
					escaped = !escaped;
				} else if (markerTypeFound != -1 && currentChar == stringCloses.charAt(markerTypeFound)
						&& !escaped) {
					contextMarker = Character.MIN_VALUE;
					markerTypeFound = -1;
				} else if ((ind = stringOpens.indexOf(currentChar)) != -1
						&& !escaped && contextMarker == Character.MIN_VALUE) {
					markerTypeFound = ind;
					contextMarker = currentChar;
				}

				if (contextMarker == Character.MIN_VALUE && currentChar == '/'
						&& (slashSlashComments || slashStarComments)) {
					currentChar = sourceReader.read();
					if (currentChar == '*' && slashStarComments) {
						int prevChar = 0;
						while ((currentChar = sourceReader.read()) != '/'
								|| prevChar != '*') {
							if (currentChar == '\r') {

								currentChar = sourceReader.read();
								if (currentChar == '\n') {
									currentChar = sourceReader.read();
								}
							} else {
								if (currentChar == '\n') {

									currentChar = sourceReader.read();
								}
							}
							if (currentChar < 0)
								break;
							prevChar = currentChar;
						}
						continue;
					} else if (currentChar == '/' && slashSlashComments) {
						while ((currentChar = sourceReader.read()) != '\n'
								&& currentChar != '\r' && currentChar >= 0)
							;
					}
				} else if (contextMarker == Character.MIN_VALUE
						&& currentChar == '#' && hashComments) {
					// Slurp up everything until the newline
					while ((currentChar = sourceReader.read()) != '\n'
							&& currentChar != '\r' && currentChar >= 0)
						;
				} else if (contextMarker == Character.MIN_VALUE
						&& currentChar == '-' && dashDashComments) {
					currentChar = sourceReader.read();

					if (currentChar == -1 || currentChar != '-') {
						buf.append('-');

						if (currentChar != -1) {
							buf.append(currentChar);
						}

						continue;
					}

					// Slurp up everything until the newline

					while ((currentChar = sourceReader.read()) != '\n'
							&& currentChar != '\r' && currentChar >= 0)
						;
				}

				if (currentChar != -1) {
					buf.append((char) currentChar);
				}
			}
		} catch (IOException ioEx) {
			// we'll never see this from a StringReader
		}

		return buf.toString();
	}
	
	/**
	 * Next two functions are to help DBMD check if 
	 * the given string is in form of database.name and return it 
	 * as "database";"name" with comments removed.
	 * If string is NULL or wildcard (%), returns null and exits.
	 * 
	 * First, we sanitize...
	 * 
	 * @param src
	 *            the source string
	 * @return the input string with all comment-delimited data removed
	 */
	public static String sanitizeProcOrFuncName(String src) {
		if ((src == null) || (src.equals("%"))) {
			return null;
		}
			
		return src;
	}

	/**
	 * Next we check if there is anything to split. If so 
	 * we return result in form of "database";"name"
	 * If string is NULL or wildcard (%), returns null and exits.
	 * 
	 * @param src
	 *            the source string
	 * @param cat
	 *            Catalog, if available
	 * @param quotId
	 *            quoteId as defined on server
	 * @param isNoBslashEscSet
	 *            Is our connection in BackSlashEscape mode
	 * @return the input string with all comment-delimited data removed
	 */
	public static List<String> splitDBdotName(String src, String cat, String quotId,
			boolean isNoBslashEscSet) {
		if ((src == null) || (src.equals("%"))) {
			return new ArrayList<String>();
		}
		
		boolean isQuoted = StringUtils.indexOfIgnoreCase(0,src, quotId) > -1;
		
		
		String retval = src;
		String tmpCat = cat;
		//I.e., what if database is named `MyDatabase 1.0.0`... thus trueDotIndex
		int trueDotIndex = -1;
		if (!" ".equals(quotId)) {
			//Presumably, if there is a database name attached and it contains dots, then it should
			//be quoted so we first check for that
			if (isQuoted) {
				trueDotIndex = StringUtils.indexOfIgnoreCase(0,
					retval, quotId + "." + quotId);
			} else {
				//NOT quoted, fetch first DOT
				// ex: cStmt = this.conn.prepareCall("{call bug57022.procbug57022(?, ?)}");
				trueDotIndex = StringUtils.indexOfIgnoreCase(0,
						retval, ".");
			}
		} else {
			trueDotIndex = retval.indexOf(".");
		}

		List<String> retTokens = new ArrayList<String>(2);

		if (trueDotIndex != -1) {
			//There is a catalog attached
			if (isQuoted) {
				tmpCat = StringUtils.toString(StringUtils.stripEnclosure(retval.substring(0, trueDotIndex+1)
						.getBytes(), quotId, quotId));
				if (StringUtils.startsWithIgnoreCaseAndWs(tmpCat, quotId)) {
					tmpCat = tmpCat.substring(1, tmpCat.length() - 1);
				}

				retval = retval.substring(trueDotIndex + 2);
				retval = StringUtils.toString(StringUtils.stripEnclosure(retval
						.getBytes(), quotId, quotId));
			} else {
				//NOT quoted, adjust indexOf
				tmpCat = retval.substring(0, trueDotIndex);
				retval = retval.substring(trueDotIndex + 1);
			}
		} else {
			//No catalog attached, strip retval and return
			retval = StringUtils.toString(StringUtils.stripEnclosure(retval
				.getBytes(), quotId, quotId));
		}
		
		retTokens.add(tmpCat);
		retTokens.add(retval);
		return retTokens;
	}
	
	public static final boolean isEmptyOrWhitespaceOnly(String str) {
		if (str == null || str.length() == 0) {
			return true;
		}
		
		int length = str.length();
		
		for (int i = 0; i < length; i++) {
			if (!Character.isWhitespace(str.charAt(i))) {
				return false;
			}
		}
		
		return true;
	}
	
	public static String escapeQuote(String src, String quotChar) {
		if (src == null) {
			return null;
		}

		src = StringUtils.toString(stripEnclosure(src.getBytes(), quotChar, quotChar));

		int lastNdx = src.indexOf(quotChar);
		String tmpSrc;
		String tmpRest;

		tmpSrc = src.substring(0, lastNdx);
		tmpSrc = tmpSrc + quotChar + quotChar;
		
		tmpRest = src.substring(lastNdx+1, src.length());
		
		lastNdx = tmpRest.indexOf(quotChar);
		while (lastNdx > -1) {
			
			tmpSrc = tmpSrc + tmpRest.substring(0, lastNdx);
			tmpSrc = tmpSrc + quotChar + quotChar;
			tmpRest = tmpRest.substring(lastNdx+1, tmpRest.length());
			
			lastNdx = tmpRest.indexOf(quotChar);
		}
		
		tmpSrc = tmpSrc + tmpRest;
		src = tmpSrc;

		return src;
	}
	
	/**
	 * Surrounds identifier with quoteChar and duplicates these symbols inside the identifier.
     * 
	 * @param quoteChar ` or "
	 * @param identifier in pedantic mode (connection property pedantic=true) identifier is treated as unquoted
	 *        (as it is stored in the database) even if it starts and ends with quoteChar;
	 *        in non-pedantic mode if identifier starts and ends with quoteChar method treats it as already quoted and doesn't modify.
	 * @param isPedantic are we in pedantic mode
	 * 
	 * @return
	 * With quoteChar="`":<br>
	 * <li>null -> null</li>
	 * <li>abc -> `abc`</li>
	 * <li>ab`c -> `ab``c`</li>
	 * <li>ab"c -> `ab"c`</li>
	 * <li>`ab``c` -> `ab``c` in non-pedantic mode or ```ab````c``` in pedantic mode</li>
	 * With quoteChar="\"":<br>
	 * <li>null -> null</li>
	 * <li>abc -> "abc"</li>
	 * <li>ab`c -> "ab`c"</li>
	 * <li>ab"c -> "ab""c"</li>
	 * <li>"ab""c" -> "ab""c" in non-pedantic mode or """ab""""c""" in pedantic mode</li>
	 */
	public static String quoteIdentifier(String identifier, String quoteChar, boolean isPedantic) {
		if (identifier == null) {
			return null;
		}

		if (!isPedantic && identifier.startsWith(quoteChar) && identifier.endsWith(quoteChar)) {
			return identifier;
		}

		return quoteChar + identifier.replaceAll(quoteChar, quoteChar+quoteChar) + quoteChar;
	}

	/**
	 * Surrounds identifier with "`" and duplicates these symbols inside the identifier.
     * 
	 * @param identifier in pedantic mode (connection property pedantic=true) identifier is treated as unquoted
	 *        (as it is stored in the database) even if it starts and ends with "`";
	 *        in non-pedantic mode if identifier starts and ends with "`" method treats it as already quoted and doesn't modify.
	 * @param isPedantic are we in pedantic mode
	 * 
	 * @return
	 * <li>null -> null</li>
	 * <li>abc -> `abc`</li>
	 * <li>ab`c -> `ab``c`</li>
	 * <li>ab"c -> `ab"c`</li>
	 * <li>`ab``c` -> `ab``c` in non-pedantic mode or ```ab````c``` in pedantic mode</li>
	 */
	public static String quoteIdentifier(String identifier, boolean isPedantic) {
		return quoteIdentifier(identifier, "`", isPedantic);
	}

	/**
	 * Trims identifier, removes quote chars from first and last positions
	 * and replaces double occurrences of quote char from entire identifier,
	 * i.e converts quoted identifier into form as it is stored in database.
	 * 
	 * @param identifier
	 * @param useAnsiQuotedIdentifiers should we check for " quotes too.
	 * @return
	 * <li>null -> null</li>
	 * <li>abc -> abc</li>
	 * <li>`abc` -> abc</li>
	 * <li>`ab``c` -> ab`c</li>
	 * <li>`"ab`c"` -> "ab`c"</li>
	 * <li>`ab"c` -> ab"c</li>
	 * <li>"abc" -> abc</li>
	 * <li>"`ab""c`" -> `ab"c`</li>
	 * <li>"ab`c" -> ab`c</li>
	 */
	public static String unQuoteIdentifier(String identifier, boolean useAnsiQuotedIdentifiers) {
		if (identifier == null) {
			return null;
		}
		
		identifier = identifier.trim();
		
		String quoteChar = null;
		
		// Backquotes are always valid identifier quotes
		if (identifier.startsWith("`") && identifier.endsWith("`")) {
			quoteChar = "`";
		}
		
		if (quoteChar== null && useAnsiQuotedIdentifiers) {
			if (identifier.startsWith("\"") && identifier.endsWith("\"")) {
				quoteChar = "\"";
			}
		}
		
		if (quoteChar !=  null) {
			identifier = identifier.substring(1, (identifier.length() - 1));
			return identifier.replaceAll(quoteChar+quoteChar, quoteChar);
		}
		
		return identifier;
	}

	public static int indexOfQuoteDoubleAware(String line, String quoteChar, int startFrom) {
		int lastIndex = line.length() -1;
		
		int beginPos = startFrom;
		int pos = -1;

		boolean next = true;
		while (next) {
			pos = line.indexOf(quoteChar, beginPos);
			if (pos == -1 || pos == lastIndex || !line.substring(pos+1).startsWith(quoteChar)) {
				next = false;
			} else {
				beginPos = pos + 2;
			}
		}
		
		return pos;
	}

	// The following methods all exist because of the Java bug
	//
	// http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6790402
	// 
	// which has been observed by users and reported as MySQL Bug#61105
	//
	// We can turn around and replace them with their java.lang.String
	// equivalents if/when that bug is ever fixed.
	
	public static String toString(byte[] value, int offset, int length,
			String encoding) throws UnsupportedEncodingException {
		Charset cs = findCharset(encoding);

		return cs.decode(ByteBuffer.wrap(value, offset, length)).toString();
	}

	public static String toString(byte[] value, String encoding)
			throws UnsupportedEncodingException {
		Charset cs = findCharset(encoding);

		return cs.decode(ByteBuffer.wrap(value)).toString();
	}

	public static String toString(byte[] value, int offset, int length) {
		try {
			Charset cs = findCharset(platformEncoding);
			
			return cs.decode(ByteBuffer.wrap(value, offset, length)).toString();
		} catch (UnsupportedEncodingException e) {
			// can't happen, emulating new String(byte[])
		}
		
		return null;
	}

	public static String toString(byte[] value) {
		try {
			Charset cs = findCharset(platformEncoding);
			
			return cs.decode(ByteBuffer.wrap(value)).toString();
		} catch (UnsupportedEncodingException e) {
			// can't happen, emulating new String(byte[])
		}
		
		return null;
	}

	public static byte[] getBytes(String value, String encoding)
			throws UnsupportedEncodingException {
		Charset cs = findCharset(encoding);

		// can't simply .array() this to get the bytes
		// especially with variable-length charsets the 
		// buffer is sometimes larger than the actual encoded data
		ByteBuffer buf = cs.encode(value);
		
		int encodedLen = buf.limit();
		byte[] asBytes = new byte[encodedLen];
		buf.get(asBytes, 0, encodedLen);
		
		return asBytes;
	}
	
	public static byte[] getBytes(String value) {
		try {
			Charset cs = findCharset(platformEncoding);
			
			ByteBuffer buf = cs.encode(value);
			
			int encodedLen = buf.limit();
			byte[] asBytes = new byte[encodedLen];
			buf.get(asBytes, 0, encodedLen);
			
			return asBytes;
		} catch (UnsupportedEncodingException e) {
			// can't happen, emulating new String(byte[])
		}
		
		return null;
	}

	public static final boolean isValidIdChar(char c) {
		return VALID_ID_CHARS.indexOf(c) != -1;
	}
}
