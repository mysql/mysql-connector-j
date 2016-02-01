/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
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

package com.mysql.cj.core.log;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.WrongArgumentException;

/**
 * Creates instances of loggers for the driver to use.
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
     */
    public static Log getLogger(String className, String instanceName, ExceptionInterceptor exceptionInterceptor) {

        if (className == null) {
            throw ExceptionFactory.createException(WrongArgumentException.class, "Logger class can not be NULL", exceptionInterceptor);
        }

        if (instanceName == null) {
            throw ExceptionFactory.createException(WrongArgumentException.class, "Logger instance name can not be NULL", exceptionInterceptor);
        }

        try {
            Class<?> loggerClass = null;

            try {
                loggerClass = Class.forName(className);
            } catch (ClassNotFoundException nfe) {
                loggerClass = Class.forName(LogFactory.class.getPackage().getName() + "." + className);
            }

            Constructor<?> constructor = loggerClass.getConstructor(new Class<?>[] { String.class });

            return (Log) constructor.newInstance(new Object[] { instanceName });
        } catch (ClassNotFoundException cnfe) {
            throw ExceptionFactory.createException(WrongArgumentException.class, "Unable to load class for logger '" + className + "'", cnfe,
                    exceptionInterceptor);
        } catch (NoSuchMethodException nsme) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    "Logger class does not have a single-arg constructor that takes an instance name", nsme, exceptionInterceptor);
        } catch (InstantiationException inse) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    "Unable to instantiate logger class '" + className + "', exception in constructor?", inse, exceptionInterceptor);
        } catch (InvocationTargetException ite) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    "Unable to instantiate logger class '" + className + "', exception in constructor?", ite, exceptionInterceptor);
        } catch (IllegalAccessException iae) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    "Unable to instantiate logger class '" + className + "', constructor not public", iae, exceptionInterceptor);
        } catch (ClassCastException cce) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    "Logger class '" + className + "' does not implement the '" + Log.class.getName() + "' interface", cce, exceptionInterceptor);
        }
    }
}
