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

import java.io.ByteArrayOutputStream;
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

        int allBytesStringLen = allBytesString.length();
        
        for (int i = 0; 
             i < (Byte.MAX_VALUE - Byte.MIN_VALUE) && i < allBytesStringLen; 
             i++) {
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

            if (encoding.equalsIgnoreCase("SJIS")) {
                b = escapeSJISByteStream(b);
            }
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

    /**
     * Unfortunately, SJIS has 0x5c as a high byte in some
     * of its double-byte characters, so we need to escape
     * it.
     */
    static byte[] escapeSJISByteStream(byte[] origBytes) {
        if (origBytes == null || origBytes.length == 0) {
            return origBytes;
        }
        
        int stringLen = origBytes.length;
        int bufIndex = 0;
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream(stringLen);

        for (; ;) {

            // Grab the first byte
            int loByte = (int) origBytes[bufIndex];

            if (loByte < 0) {
                loByte += 256; // adjust for signedness/wrap-around
            }

            // We always write the first byte
            bytesOut.write(loByte);

            //
            // The codepage characters in question exist between
            // 0x80-0x9F and 0xE0-0xFC...
            //
            // See:
            //
            // http://www.microsoft.com/GLOBALDEV/Reference/dbcs/932.htm
            //
            if ((loByte >= 0x80 && loByte <= 0x9F)
                || (loByte >= 0xE0 && loByte <= 0xFC)) {

                if (bufIndex < (stringLen - 1)) {

                    int hiByte = (int) origBytes[bufIndex + 1];

                    if (hiByte < 0) {
                        hiByte += 256; // adjust for signedness/wrap-around
                    }

                    //
                    // Here's the problematic critter...
                    //
                    // we write it out, and it gets written
                    // again at the top of the loop, thus
                    // escaping it.
                    
                    if (hiByte == 0x5C) {
                        bytesOut.write(hiByte);
                    }
                }
            }

            bufIndex++;

            if (bufIndex >= stringLen) {

                // we're done
                break;
            }
        }

        return bytesOut.toByteArray();
    }
    
    /** 
     * Determines whether or not the string 'searchIn' contains
     * the string 'searchFor', dis-regarding case.
     * 
     * Shorthand for a String.regionMatch(...)
     * 
     * @param searchIn the string to search in
     * @param searchFor the string to search for
     * 
     * @return whether searchIn starts with searchFor, ignoring case
     */
    
    public static boolean startsWithIgnoreCase(String searchIn, String searchFor) {
        return searchIn.regionMatches(true, 0, searchFor, 0, searchFor.length());   
    }
}