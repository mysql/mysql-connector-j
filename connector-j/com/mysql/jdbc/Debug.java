package com.mysql.jdbc;

import java.io.PrintStream;

import java.sql.DriverManager;

import java.util.Hashtable;
import java.util.StringTokenizer;

/**
 * The Debug class allows debug messages on a per-class
 * basis.
 * 
 * <p>
 * The user issues a trace() call, listing the classes
 * they wish to debug.
 */

public class Debug {
	private static Hashtable _Classes = new Hashtable();
	private static Object _Mutex = new Object();
	private static boolean _watch_all = false;

	/**
	 * Set the classes to trace.
	 *
	 * @param ClassList the list of classes to trace, separated by colons
	 *                  or the keyword &quot;ALL&quot; to trace
	 *                  all classes that use the Debug class.
	 */

	public static void trace(String ClassList) {
		StringTokenizer ST = new StringTokenizer(ClassList, ":");
		synchronized (_Mutex) {
			_watch_all = false;

			if (ClassList.equals("ALL")) {
				_watch_all = true;
			}
			else {
				_Classes = new Hashtable();

				while (ST.hasMoreTokens()) {
					String ClassName = ST.nextToken();

					if (!_Classes.contains(ClassName)) {
						_Classes.put(ClassName, ClassName);
					}
				}
			}
		}
	}

	/**
	 * Trace a method call.
	 *
	 * <p>
	 * If the user has registered in interest in the Class of Source, then 
	 * the Source class can trace method calls through this method.
	 *
	 * @param Source the Object issuing the methodCall() method
	 * @param Method the name of the Method
	 * @param Args a list of arguments
	 */

	public static void methodCall(Object Source, String Method, Object[] Args) {
		synchronized (_Mutex) {
			if (_watch_all || _Classes.contains(Source.getClass().getName())) {
				// Print the message
				StringBuffer Mesg = new StringBuffer("\nTRACE: ");
				Mesg.append(Source.toString());
				Mesg.append(".");
				Mesg.append(Method);
				Mesg.append("( ");

				// Print the argument list
				for (int i = 0; i < Args.length - 1; i++) {
					if (Args[i] == null) {
						Mesg.append("null");
					}
					else {
						if (Args[i] instanceof String) {
							Mesg.append("\"");
						}
						Mesg.append(Args[i].toString());
						if (Args[i] instanceof String) {
							Mesg.append("\"");
						}
					}
					Mesg.append(", ");
				}
				if (Args.length > 0) {
					if (Args[Args.length - 1] instanceof String) {
						Mesg.append("\"");
					}
					Mesg.append(Args[Args.length - 1]);
					if (Args[Args.length - 1] instanceof String) {
						Mesg.append("\"");
					}
				}
				Mesg.append(" )\n");

				if (DriverManager.getLogStream() == null) {
					System.out.println(Mesg.toString());
				}
				else {
					DriverManager.println(Mesg.toString());
				}
			}
		}
	}

	/**
	 * Trace a method call.
	 *
	 * <p>
	 * If the user has registered in interest in the Class of Source, then 
	 * the Source class can trace return calls through this method.
	 *
	 * @param Source the Object issuing the returnValue() method
	 * @param Method the name of the method
	 * @param Value the return value
	 */

	public static void returnValue(Object Source, String Method, Object Value) {
		synchronized (_Mutex) {
			if (_watch_all || _Classes.contains(Source.getClass().getName())) {
				// Print the message
				StringBuffer Mesg = new StringBuffer("\nTRACE: ");
				Mesg.append(Source.toString());
				Mesg.append(".");
				Mesg.append(Method);
				Mesg.append(": Returning -> ");

				if (Value == null) {
					Mesg.append("null");
				}
				else {
					Mesg.append(Value.toString());
				}
				Mesg.append("\n");

				if (DriverManager.getLogStream() == null) {
					System.out.println(Mesg.toString());
				}
				else {
					DriverManager.println(Mesg.toString());
				}
			}
		}
	}

	/**
	* Log a message.
	*
	* <p>
	* If the user has registered in interest in the Class of Source, then 
	* the Source class can trace return calls through this method.
	*
	* @param Source the Object issuing the msg() method
	* @param Method the name of the method
	* @param Value the return value
	*/

	public static void msg(Object Source, String Message) {
		synchronized (_Mutex) {
			if (_watch_all || _Classes.contains(Source.getClass().getName())) {
				// Print the message
				StringBuffer Mesg = new StringBuffer("\nTRACE: ");
				Mesg.append(Source.toString());
				Mesg.append(": ");
				Mesg.append(Message);
				Mesg.append("\n");

				if (DriverManager.getLogStream() == null) {
					System.out.println(Mesg.toString());
				}
				else {
					DriverManager.println(Mesg.toString());
				}
			}
		}
	}
}
