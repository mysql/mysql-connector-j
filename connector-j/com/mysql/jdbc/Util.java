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

import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Various utility methods for the driver.
 * 
 * @author Mark Matthews
 */
public class Util {

    //~ Methods ...............................................................

    /**
     * Given a ResultSet and an index into the columns of that ResultSet,
     * read binary data from the column which represents a serialized object,
     * and re-create the object.
     *
     * @param resultSet the ResultSet to use.
     * @param index an index into the ResultSet.
     * @return the object if it can be de-serialized
     * @throws Exception if an error occurs
     */
    public static Object readObject(java.sql.ResultSet resultSet, int index)
                             throws Exception {

        ObjectInputStream objIn = new ObjectInputStream(resultSet.getBinaryStream(
                                                                index));
        Object obj = objIn.readObject();
        objIn.close();

        return obj;
    }

    // Right from Monty's code
    static String newCrypt(String password, String seed) {

        byte b;
        double d;

        if (password == null || password.length() == 0) {

            return password;
        }

        long[] pw = newHash(seed);
        long[] msg = newHash(password);
        long max = 0x3fffffffL;
        long seed1 = (pw[0] ^ msg[0]) % max;
        long seed2 = (pw[1] ^ msg[1]) % max;
        char[] chars = new char[seed.length()];

        for (int i = 0; i < seed.length(); i++) {
            seed1 = (seed1 * 3 + seed2) % max;
            seed2 = (seed1 + seed2 + 33) % max;
            d = (double) seed1 / (double) max;
            b = (byte) java.lang.Math.floor((d * 31) + 64);
            chars[i] = (char) b;
        }

        seed1 = (seed1 * 3 + seed2) % max;
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

            if (password.charAt(i) == ' ' || password.charAt(i) == '\t') {

                continue; // skip spaces
            }

            tmp = (long) (0xff & password.charAt(i));
            nr ^= (((nr & 63) + add) * tmp) + (nr << 8);
            nr2 += (nr2 << 8) ^ nr;
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

        if (password == null || password.length() == 0) {

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
            s1 = (s1 * 3 + s2) % max;
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

            tmp = (long) password.charAt(i);
            nr ^= (((nr & 63) + nr2) * tmp) + (nr << 8);
            nr2 += tmp;
        }

        return nr & (((long) 1L << 31) - 1L);
    }
    
    /** 
     * Converts a nested exception into a nicer message
     * 
     * @param ex the exception to expand into a message.
     * 
     * @return a message containing the exception, the
     * message (if any), and a stacktrace.
     */
    
    public static String stackTraceToString(Exception ex) {
        StringBuffer traceBuf = new StringBuffer();
        traceBuf.append("\n\n** BEGIN NESTED EXCEPTION ** \n\n");
        if (ex != null) {
            traceBuf.append(ex.getClass().getName());
            
            String message = ex.getMessage();
            
            if (message != null) {
                traceBuf.append("\nMESSAGE: ");
                traceBuf.append(message);
            }
            
            StringWriter out = new StringWriter();
            
            PrintWriter printOut = new PrintWriter(out);
            
            ex.printStackTrace(printOut);
            
            traceBuf.append("\n\nSTACKTRACE:\n\n");
            traceBuf.append(out.toString());
        }
        
        traceBuf.append("\n\n** END NESTED EXCEPTION **\n\n");
        
        return traceBuf.toString();
    }
                    
            
}