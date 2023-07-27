/*
 * Copyright (c) 2017, 2023, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.exceptions;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import com.mysql.cj.log.Log;
import com.mysql.cj.util.Util;

public class ExceptionInterceptorChain implements ExceptionInterceptor {

    private List<ExceptionInterceptor> interceptors;

    public ExceptionInterceptorChain(String interceptorClasses, Properties props, Log log) {
        this.interceptors = Util.loadClasses(ExceptionInterceptor.class, interceptorClasses, "Connection.BadExceptionInterceptor", null).stream()
                .map(i -> i.init(props, log)).collect(Collectors.toCollection(LinkedList::new));
    }

    public void addRingZero(ExceptionInterceptor interceptor) {
        this.interceptors.add(0, interceptor);
    }

    @Override
    public Exception interceptException(Exception sqlEx) {
        for (ExceptionInterceptor ie : this.interceptors) {
            sqlEx = ie.interceptException(sqlEx);
        }
        return sqlEx;
    }

    @Override
    public void destroy() {
        this.interceptors.forEach(ExceptionInterceptor::destroy);
    }

    @Override
    public ExceptionInterceptor init(Properties properties, Log log) {
        this.interceptors = this.interceptors.stream().map(i -> i.init(properties, log)).collect(Collectors.toCollection(LinkedList::new));
        return this;
    }

    public List<ExceptionInterceptor> getInterceptors() {
        return this.interceptors;
    }

}
