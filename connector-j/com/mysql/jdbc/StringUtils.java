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

public class StringUtils 
{
  private static final int BYTE_RANGE = 1 + Byte.MAX_VALUE - Byte.MIN_VALUE;
  public static byte[] allBytes = new byte[BYTE_RANGE];
  
  private static char[] byteToChars = new char[BYTE_RANGE];
  
  static 
  {
    for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
      allBytes[i - Byte.MIN_VALUE] = (byte) i;
    }
    
    String allBytesString = new String(allBytes, 0, Byte.MAX_VALUE - Byte.MIN_VALUE);
    for (int i = 0; i < Byte.MAX_VALUE - Byte.MIN_VALUE; i++) {
      byteToChars[i] = allBytesString.charAt(i);
    }
  }
  

  public final static String toAsciiString(byte[] buffer,
					   int startPos,
					   int length) 
  {
    StringBuffer result = new StringBuffer();
    int endPoint = startPos + length;
    for (int i = startPos; i < endPoint; i++) {
      result.append(byteToChars[((int) buffer[i]) - Byte.MIN_VALUE]);
    }
    return result.toString();
  }

  public final static String toAsciiString2(byte[] buffer,
					    int startPos,
					    int length) 
  {
    return new String(buffer, startPos, length);
  }

  public final static String toAsciiString(byte[] buffer) 
  {
    return toAsciiString3(buffer,
			  0,
			  buffer.length);
  }

  public final static String toAsciiString3(byte[] buffer,
					    int startPos,
					    int length) 
  {
    char[] charArray = new char[length];
    int readpoint = startPos;
    for (int i = 0; i < length; i++) {
      charArray[i] = byteToChars[((int) buffer[readpoint]) - Byte.MIN_VALUE];
      readpoint++;
    }
    return new String(charArray);
  }
}
