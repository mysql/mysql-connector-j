/*
 Copyright  2002-2007 MySQL AB, 2008 Sun Microsystems

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA



 */
package com.mysql.jdbc.log;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;

import com.mysql.jdbc.ExceptionInterceptor;
import com.mysql.jdbc.SQLError;

/**
 * Creates instances of loggers for the driver to use.
 * 
 * @author Mark Matthews
 * 
 * @version $Id$
 */
public class LogFactory {

	/**
	 * Returns a logger instance of the given class, with the given instance
	 * name.
	 * 
	 * @param className
	 *            the class to instantiate
	 * @param instanceName
	 *            the instance name
	 * @return a logger instance
	 * @throws SQLException
	 *             if unable to create a logger instance
	 */
	public static Log getLogger(String className, String instanceName, ExceptionInterceptor exceptionInterceptor)
			throws SQLException {

		if (className == null) {
			throw SQLError.createSQLException("Logger class can not be NULL",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
		}

		if (instanceName == null) {
			throw SQLError.createSQLException(
					"Logger instance name can not be NULL",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
		}

		try {
			Class loggerClass = null;

			try {
				loggerClass = Class.forName(className);
			} catch (ClassNotFoundException nfe) {
				loggerClass = Class.forName(Log.class.getPackage().getName()
						+ "." + className);
			}

			Constructor constructor = loggerClass
					.getConstructor(new Class[] { String.class });

			return (Log) constructor.newInstance(new Object[] { instanceName });
		} catch (ClassNotFoundException cnfe) {
			SQLException sqlEx = SQLError.createSQLException(
					"Unable to load class for logger '" + className + "'",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
			sqlEx.initCause(cnfe);

			throw sqlEx;
		} catch (NoSuchMethodException nsme) {
			SQLException sqlEx = SQLError
					.createSQLException(
							"Logger class does not have a single-arg constructor that takes an instance name",
							SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
			sqlEx.initCause(nsme);

			throw sqlEx;
		} catch (InstantiationException inse) {
			SQLException sqlEx = SQLError.createSQLException(
					"Unable to instantiate logger class '" + className
							+ "', exception in constructor?",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
			sqlEx.initCause(inse);

			throw sqlEx;
		} catch (InvocationTargetException ite) {
			SQLException sqlEx = SQLError.createSQLException(
					"Unable to instantiate logger class '" + className
							+ "', exception in constructor?",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
			sqlEx.initCause(ite);

			throw sqlEx;
		} catch (IllegalAccessException iae) {
			SQLException sqlEx = SQLError.createSQLException(
					"Unable to instantiate logger class '" + className
							+ "', constructor not public",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
			sqlEx.initCause(iae);

			throw sqlEx;
		} catch (ClassCastException cce) {
			SQLException sqlEx = SQLError.createSQLException("Logger class '"
					+ className + "' does not implement the '"
					+ Log.class.getName() + "' interface",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
			sqlEx.initCause(cce);

			throw sqlEx;
		}
	}
}
