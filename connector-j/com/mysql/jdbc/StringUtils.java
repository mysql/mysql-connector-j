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


/**
 * Various utility methods for converting to/from byte
 * arrays in the platform encoding
 * 
 * @author Mark Matthews
 */
public class StringUtils {

    //~ Instance/static variables .............................................

    private static final int BYTE_RANGE = (1 + Byte.MAX_VALUE)
                                          - Byte.MIN_VALUE;
    private static byte[] allBytes = new byte[BYTE_RANGE];
    private static char[] byteToChars = new char[BYTE_RANGE];

    //~ Initializers ..........................................................

    static {

        for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
            allBytes[i - Byte.MIN_VALUE] = (byte) i;
        }

        String allBytesString = new String(allBytes, 0, 
                                           Byte.MAX_VALUE - Byte.MIN_VALUE);

        for (int i = 0; i < (Byte.MAX_VALUE - Byte.MIN_VALUE); i++) {
            byteToChars[i] = allBytesString.charAt(i);
        }
    }

    //~ Methods ...............................................................

    /**
     * DOCUMENT ME!
     * 
     * @param s DOCUMENT ME!
     * @param encoding DOCUMENT ME!
     * @return DOCUMENT ME! 
     * @throws UnsupportedEncodingException DOCUMENT ME!
     */
    public static final byte[] getBytes(String s, String encoding)
                                 throws UnsupportedEncodingException {

        byte[] b = null;
        SingleByteCharsetConverter converter = SingleByteCharsetConverter.getInstance(
                                                       encoding);

        if (converter != null) {
            b = converter.toBytes(s);
        } else {
            b = s.getBytes(encoding);
        }

        return b;
    }

    /**
     * DOCUMENT ME!
     * 
     * @param buffer DOCUMENT ME!
     * @param startPos DOCUMENT ME!
     * @param length DOCUMENT ME!
     * @return DOCUMENT ME! 
     */
    public static final String toAsciiString3(byte[] buffer, int startPos, 
                                              int length) {

        char[] charArray = new char[length];
        int readpoint = startPos;

        for (int i = 0; i < length; i++) {
            charArray[i] = byteToChars[(int) buffer[readpoint]
                           - Byte.MIN_VALUE];
            readpoint++;
        }

        return new String(charArray);
    }

    /**
     * DOCUMENT ME!
     * 
     * @param buffer DOCUMENT ME!
     * @return DOCUMENT ME! 
     */
    public static final String toAsciiString(byte[] buffer) {

        return toAsciiString3(buffer, 0, buffer.length);
    }

    /**
     * DOCUMENT ME!
     * 
     * @param buffer DOCUMENT ME!
     * @param startPos DOCUMENT ME!
     * @param length DOCUMENT ME!
     * @return DOCUMENT ME! 
     */
    public static final String toAsciiString2(byte[] buffer, int startPos, 
                                              int length) {

        return new String(buffer, startPos, length);
    }

    /**
     * DOCUMENT ME!
     * 
     * @param buffer DOCUMENT ME!
     * @param startPos DOCUMENT ME!
     * @param length DOCUMENT ME!
     * @return DOCUMENT ME! 
     */
    public static final String toAsciiString(byte[] buffer, int startPos, 
                                             int length) {

        StringBuffer result = new StringBuffer();
        int endPoint = startPos + length;

        for (int i = startPos; i < endPoint; i++) {
            result.append(byteToChars[(int) buffer[i] - Byte.MIN_VALUE]);
        }

        return result.toString();
    }
}