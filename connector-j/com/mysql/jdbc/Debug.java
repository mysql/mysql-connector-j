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

import java.sql.DriverManager;

import java.util.Hashtable;
import java.util.StringTokenizer;


/**
 * The Debug class allows debug messages on a per-class basis.
 * 
 * <p>
 * The user issues a trace() call, listing the classes they wish to debug.
 * </p>
 * 
 * @author Mark Matthews
 */
public class Debug {

    //~ Instance/static variables .............................................

    private static final Hashtable CLASSES = new Hashtable();
    private static final Object MUTEX = new Object();
    private static boolean watchAll = false;

    //~ Methods ...............................................................

    /**
     * Trace a method call.
     * 
     * <p>
     * If the user has registered in interest in the Class of Source, then  the
     * Source class can trace method calls through this method.
     * </p>
     * 
     * @param source the Object issuing the methodCall() method
     * @param method the name of the Method
     * @param args a list of arguments
     */
    public static void methodCall(Object source, String method, Object[] args) {

        synchronized (MUTEX) {

            if (watchAll || CLASSES.contains(source.getClass().getName())) {

                // Print the message
                StringBuffer mesg = new StringBuffer("\nTRACE: ");
                mesg.append(source.toString());
                mesg.append(".");
                mesg.append(method);
                mesg.append("( ");

                // Print the argument list
                for (int i = 0; i < (args.length - 1); i++) {

                    if (args[i] == null) {
                        mesg.append("null");
                    } else {

                        if (args[i] instanceof String) {
                            mesg.append("\"");
                        }

                        mesg.append(args[i].toString());

                        if (args[i] instanceof String) {
                            mesg.append("\"");
                        }
                    }

                    mesg.append(", ");
                }

                if (args.length > 0) {

                    if (args[args.length - 1] instanceof String) {
                        mesg.append("\"");
                    }

                    mesg.append(args[args.length - 1]);

                    if (args[args.length - 1] instanceof String) {
                        mesg.append("\"");
                    }
                }

                mesg.append(" )\n");

                if (DriverManager.getLogStream() == null) {
                    System.out.println(mesg.toString());
                } else {
                    DriverManager.println(mesg.toString());
                }
            }
        }
    }

    /**
     * Log a message.
     * 
     * <p>
     * If the user has registered in interest in the Class of Source, then  the
     * Source class can trace return calls through this method.
     * </p>
     * 
     * @param source the Object issuing the msg() method
     * @param message the name of the method
     */
    public static void msg(Object source, String message) {

        synchronized (MUTEX) {

            if (watchAll || CLASSES.contains(source.getClass().getName())) {

                // Print the message
                StringBuffer mesg = new StringBuffer("\nTRACE: ");
                mesg.append(source.toString());
                mesg.append(": ");
                mesg.append(message);
                mesg.append("\n");

                if (DriverManager.getLogStream() == null) {
                    System.out.println(mesg.toString());
                } else {
                    DriverManager.println(mesg.toString());
                }
            }
        }
    }

    /**
     * Trace a method call.
     * 
     * <p>
     * If the user has registered in interest in the Class of Source, then  the
     * Source class can trace return calls through this method.
     * </p>
     * 
     * @param source the Object issuing the returnValue() method
     * @param method the name of the method
     * @param value the return value
     */
    public static void returnValue(Object source, String method, Object value) {

        synchronized (MUTEX) {

            if (watchAll || CLASSES.contains(source.getClass().getName())) {

                // Print the message
                StringBuffer mesg = new StringBuffer("\nTRACE: ");
                mesg.append(source.toString());
                mesg.append(".");
                mesg.append(method);
                mesg.append(": Returning -> ");

                if (value == null) {
                    mesg.append("null");
                } else {
                    mesg.append(value.toString());
                }

                mesg.append("\n");

                if (DriverManager.getLogStream() == null) {
                    System.out.println(mesg.toString());
                } else {
                    DriverManager.println(mesg.toString());
                }
            }
        }
    }

    /**
     * Set the classes to trace.
     * 
     * @param classList the list of classes to trace, separated by colons or
     *        the keyword &quot;ALL&quot; to trace all classes that use the
     *        Debug class.
     */
    public static void trace(String classList) {

        StringTokenizer tokenizer = new StringTokenizer(classList, ":");

        synchronized (MUTEX) {
            watchAll = false;

            if (classList.equals("ALL")) {
                watchAll = true;
            } else {
                while (tokenizer.hasMoreTokens()) {

                    String className = tokenizer.nextToken().trim();

                    if (!CLASSES.contains(className)) {
                        CLASSES.put(className, className);
                    }
                }
            }
        }
    }
}