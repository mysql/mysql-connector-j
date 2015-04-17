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

package com.mysql.cj.core.exception;

import com.mysql.cj.api.exception.ExceptionInterceptor;

public class ExceptionFactory {

    // (message)
    public static CJException createException(String message) {
        return createException(CJException.class, message);
    }

    @SuppressWarnings("unchecked")
    public static <T extends CJException> T createException(Class<T> clazz, String message) {

        T sqlEx;
        try {
            sqlEx = clazz.getConstructor(String.class).newInstance(message);
        } catch (Throwable e) {
            sqlEx = (T) new CJException(message);
        }
        return sqlEx;
    }

    public static CJException createException(String message, ExceptionInterceptor interceptor) {
        return createException(CJException.class, message, interceptor);
    }

    public static <T extends CJException> T createException(Class<T> clazz, String message, ExceptionInterceptor interceptor) {
        T sqlEx = createException(clazz, message);

        // TODO: Decide whether we need to intercept exceptions at this level
        //if (interceptor != null) {
        //    @SuppressWarnings("unchecked")
        //    T interceptedEx = (T) interceptor.interceptException(sqlEx, null);
        //    if (interceptedEx != null) {
        //        return interceptedEx;
        //    }
        //}

        return sqlEx;
    }

    // (message, cause)
    public static CJException createException(String message, Throwable cause) {
        return createException(CJException.class, message, cause);
    }

    public static <T extends CJException> T createException(Class<T> clazz, String message, Throwable cause) {

        T sqlEx = createException(clazz, message);

        if (cause != null) {
            try {
                sqlEx.initCause(cause);
            } catch (Throwable t) {
                // we're not going to muck with that here, since it's an error condition anyway!
            }
        }
        return sqlEx;
    }

    public static CJException createException(String message, Throwable cause, ExceptionInterceptor interceptor) {
        return createException(CJException.class, message, cause, interceptor);
    }

    public static <T extends CJException> T createException(Class<T> clazz, String message, Throwable cause, ExceptionInterceptor interceptor) {
        T sqlEx = createException(clazz, message, cause);

        // TODO: Decide whether we need to intercept exceptions at this level
        //if (interceptor != null) {
        //    @SuppressWarnings("unchecked")
        //    T interceptedEx = (T) interceptor.interceptException(sqlEx, null);
        //    if (interceptedEx != null) {
        //        return interceptedEx;
        //    }
        //}

        return sqlEx;
    }

}
