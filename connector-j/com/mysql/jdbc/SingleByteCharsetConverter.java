/*
   Copyright (C) 2002 MySQL AB
     This program is free software; you can redistribute it and/or modify
     it under the terms of the GNU General Public License as published by
     the Free Software Foundation; either version 2 of the License, or
     (at your option) any later version.
     This program is distributed in the hope that it will be useful,
     but WITHOUT ANY WARRANTY; without even the implied warranty of
     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
     GNU General Public License for more details.
     You should have received a copy of the GNU General Public License
     along with this program; if not, write to the Free Software
     Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
     
 */
package com.mysql.jdbc;

import java.io.UnsupportedEncodingException;

import java.util.HashMap;


/**
 * Converter for char[]->byte[] and byte[]->char[]
 * for single-byte character sets.
 * 
 * Much faster than the built-in solution that ships
 * with the JVM.
 */
public class SingleByteCharsetConverter
{

    //~ Instance/static variables .............................................

    protected static byte[] charToByteMap = new byte[65535];
    private static final int BYTE_RANGE = (1 + Byte.MAX_VALUE) - 
                                          Byte.MIN_VALUE;
    private static final HashMap converterMap = new HashMap();
    public static byte[] allBytes = new byte[BYTE_RANGE];
    char[] byteToChars = new char[BYTE_RANGE];

    //~ Initializers ..........................................................

    static {

        for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
            allBytes[i - Byte.MIN_VALUE] = (byte)i;
        }
    }

    //~ Constructors ..........................................................

    /**
     * Prevent instantiation, called out of static method initCharset().
     * @param encodingName a JVM character encoding
     * @throws UnsupportedEncodingException if the JVM does not support 
     *                                       the encoding
     */
    private SingleByteCharsetConverter(String encodingName)
                                throws UnsupportedEncodingException
    {

        String allBytesString = new String(allBytes, 0, 
                                           Byte.MAX_VALUE - Byte.MIN_VALUE, 
                                           encodingName);
        int allBytesLen = allBytesString.length();

        for (int i = 0;
             (i < (Byte.MAX_VALUE - Byte.MIN_VALUE)) && (i < allBytesLen);
             i++) {

            char c = allBytesString.charAt(i);
            byteToChars[i] = c;
            charToByteMap[c] = allBytes[i];
        }
    }

    //~ Methods ...............................................................

    /**
     * Get a converter for the given encoding name
     */
    public static SingleByteCharsetConverter getInstance(String encodingName)
        throws UnsupportedEncodingException
    {

        SingleByteCharsetConverter instance = (SingleByteCharsetConverter)converterMap.get(
                                                      encodingName);

        if (instance == null) {
            throw new UnsupportedEncodingException("Character encoding '" + 
                                                   encodingName + 
                                                   "' is not supported.");
        }

        return instance;
    }

    /**
     * Initialize the shared instance of a converter for the given
     * character encoding.
     */
    public static void initCharset(String encodingName)
                            throws UnsupportedEncodingException
    {
        converterMap.put(encodingName, 
                         new SingleByteCharsetConverter(encodingName));
    }

    /**
     * Convert the byte buffer from startPos to a length of length
     * to a string with no encoding.
     */
    public static String toStringDefaultEncoding(byte[] buffer, int startPos, 
                                                 int length)
    {

        return new String(buffer, startPos, length);
    }

    /**
     * Convert the given string to an array of bytes.
     */
    public final byte[] toBytes(String s)
    {

        if (s == null) {

            return null;
        }

        int length = s.length();
        byte[] bytes = new byte[length];

        for (int i = 0; i < length; i++) {

            char c = s.charAt(i);
            bytes[i] = charToByteMap[c];
        }

        return bytes;
    }

    /**
     * Convert the byte buffer to a string using this instance's 
     * character encoding.
     */
    public final String toString(byte[] buffer)
    {

        return toString(buffer, 0, buffer.length);
    }

    /**
     * Convert the byte buffer from startPos to a length of length
     * to a string using this instance's character encoding.
     */
    public final String toString(byte[] buffer, int startPos, int length)
    {

        char[] charArray = new char[length];
        int readpoint = startPos;

        for (int i = 0; i < length; i++) {
            charArray[i] = byteToChars[(int)buffer[readpoint] - 
                           Byte.MIN_VALUE];
            readpoint++;
        }

        return new String(charArray);
    }
}