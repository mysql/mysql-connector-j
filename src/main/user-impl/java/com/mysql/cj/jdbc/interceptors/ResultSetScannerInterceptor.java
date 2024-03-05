/*
 * Copyright (c) 2007, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.jdbc.interceptors;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mysql.cj.Messages;
import com.mysql.cj.MysqlConnection;
import com.mysql.cj.Query;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.jdbc.result.ResultSetInternalMethods;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.ServerSession;

public class ResultSetScannerInterceptor implements QueryInterceptor {

    public static final String PNAME_resultSetScannerRegex = "resultSetScannerRegex";

    protected Pattern regexP;

    @Override
    public QueryInterceptor init(MysqlConnection conn, Properties props, Log log) {
        String regexFromUser = props.getProperty(PNAME_resultSetScannerRegex);

        if (regexFromUser == null || regexFromUser.length() == 0) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ResultSetScannerInterceptor.0"));
        }

        try {
            this.regexP = Pattern.compile(regexFromUser);
        } catch (Throwable t) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ResultSetScannerInterceptor.1"), t);
        }
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Resultset> T postProcess(Supplier<String> sql, Query interceptedQuery, T originalResultSet, ServerSession serverSession) {
        // requirement of anonymous class
        final T finalResultSet = originalResultSet;

        return (T) Proxy.newProxyInstance(originalResultSet.getClass().getClassLoader(), new Class<?>[] { Resultset.class, ResultSetInternalMethods.class },
                new InvocationHandler() {

                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        if ("equals".equals(method.getName())) {
                            // Let args[0] "unwrap" to its InvocationHandler if it is a proxy.
                            return args[0].equals(this);
                        }

                        Object invocationResult = method.invoke(finalResultSet, args);

                        String methodName = method.getName();

                        if (invocationResult != null && invocationResult instanceof String || "getString".equals(methodName) || "getObject".equals(methodName)
                                || "getObjectStoredProc".equals(methodName)) {
                            Matcher matcher = ResultSetScannerInterceptor.this.regexP.matcher(invocationResult.toString());

                            if (matcher.matches()) {
                                throw new SQLException(Messages.getString("ResultSetScannerInterceptor.2"));
                            }
                        }

                        return invocationResult;
                    }

                });
    }

    @Override
    public <T extends Resultset> T preProcess(Supplier<String> sql, Query interceptedQuery) {
        // we don't care about this event

        return null;
    }

    // we don't issue queries, so it should be safe to intercept at any point
    @Override
    public boolean executeTopLevelOnly() {
        return false;
    }

    @Override
    public void destroy() {
    }

}
