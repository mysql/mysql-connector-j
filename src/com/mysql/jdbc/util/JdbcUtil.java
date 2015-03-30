/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;

import com.mysql.cj.api.ExceptionInterceptor;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.util.Util;
import com.mysql.jdbc.exceptions.SQLError;

public class JdbcUtil extends Util {

    /**
     * Handles constructing new instance with the given constructor and wrapping
     * (or not, as required) the exceptions that could possibly be generated
     */
    public static final Object handleNewInstance(Constructor<?> ctor, Object[] args, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        try {

            return ctor.newInstance(args);
        } catch (IllegalArgumentException | InstantiationException | IllegalAccessException e) {
            throw SQLError.createSQLException(Messages.getString("JdbcUtil.0"), SQLError.SQL_STATE_GENERAL_ERROR, e, exceptionInterceptor);
        } catch (InvocationTargetException e) {
            Throwable target = e.getTargetException();

            if (target instanceof SQLException) {
                throw (SQLException) target;
            }

            if (target instanceof ExceptionInInitializerError) {
                target = ((ExceptionInInitializerError) target).getException();
            }

            throw SQLError.createSQLException(target.toString(), SQLError.SQL_STATE_GENERAL_ERROR, target, exceptionInterceptor);
        }
    }

}
