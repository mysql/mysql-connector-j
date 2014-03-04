/*
  Copyright (c) 2007, 2014, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc.interceptors;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.ResultSetInternalMethods;
import com.mysql.jdbc.Statement;
import com.mysql.jdbc.StatementInterceptor;

public class ResultSetScannerInterceptor implements StatementInterceptor {
	
	protected Pattern regexP;
	
	public void init(Connection conn, Properties props) throws SQLException {
		String regexFromUser = props.getProperty("resultSetScannerRegex");
		
		if (regexFromUser == null || regexFromUser.length() == 0) {
			throw new SQLException("resultSetScannerRegex must be configured, and must be > 0 characters");
		}
		
		try {
			this.regexP = Pattern.compile(regexFromUser);
		} catch (Throwable t) {
			SQLException sqlEx = new SQLException("Can't use configured regex due to underlying exception.");
			sqlEx.initCause(t);
			
			throw sqlEx;
		}
		
	}
	
	public ResultSetInternalMethods postProcess(String sql, Statement interceptedStatement,
			ResultSetInternalMethods originalResultSet, Connection connection)
			throws SQLException {
		
		// requirement of anonymous class
		final ResultSetInternalMethods finalResultSet = originalResultSet;
		
		return (ResultSetInternalMethods)Proxy.newProxyInstance(originalResultSet.getClass().getClassLoader(),
				new Class[] {ResultSetInternalMethods.class},
				new InvocationHandler() {

					public Object invoke(Object proxy, Method method,
							Object[] args) throws Throwable {
						
						Object invocationResult = method.invoke(finalResultSet, args);
						
						String methodName = method.getName();
						
						if (invocationResult != null && invocationResult instanceof String 
								|| "getString".equals(methodName) 
								|| "getObject".equals(methodName)
								|| "getObjectStoredProc".equals(methodName)) {
							Matcher matcher = regexP.matcher(invocationResult.toString());
							
							if (matcher.matches()) {
								throw new SQLException("value disallowed by filter");
							}
						}
						
						return invocationResult;
					}});
	
	}

	public ResultSetInternalMethods preProcess(String sql, Statement interceptedStatement,
			Connection connection) throws SQLException {
		// we don't care about this event
		
		return null;
	}

	// we don't issue queries, so it should be safe to intercept
	// at any point
	public boolean executeTopLevelOnly() {
		return false;
	}

	public void destroy() {

		
	}
}