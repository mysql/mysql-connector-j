/*
  Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.exceptions;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.core.util.Util;

public class ExceptionInterceptorChain implements ExceptionInterceptor {
    List<ExceptionInterceptor> interceptors;

    public ExceptionInterceptorChain(String interceptorClasses, Properties props, Log log) {
        this.interceptors = Util.<ExceptionInterceptor> loadClasses(interceptorClasses, "Connection.BadExceptionInterceptor", this).stream()
                .map(o -> o.init(props, log)).collect(Collectors.toList());
    }

    public void addRingZero(ExceptionInterceptor interceptor) {
        this.interceptors.add(0, interceptor);
    }

    public Exception interceptException(Exception sqlEx) {
        if (this.interceptors != null) {
            Iterator<ExceptionInterceptor> iter = this.interceptors.iterator();

            while (iter.hasNext()) {
                sqlEx = iter.next().interceptException(sqlEx);
            }
        }

        return sqlEx;
    }

    public void destroy() {
        if (this.interceptors != null) {
            Iterator<ExceptionInterceptor> iter = this.interceptors.iterator();

            while (iter.hasNext()) {
                iter.next().destroy();
            }
        }

    }

    public ExceptionInterceptor init(Properties properties, Log log) {
        if (this.interceptors != null) {
            Iterator<ExceptionInterceptor> iter = this.interceptors.iterator();

            while (iter.hasNext()) {
                iter.next().init(properties, log);
            }
        }
        return this;
    }

    public List<ExceptionInterceptor> getInterceptors() {
        return this.interceptors;
    }

}
