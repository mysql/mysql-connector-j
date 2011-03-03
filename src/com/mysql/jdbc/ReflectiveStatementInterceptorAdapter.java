/*
 Copyright (c) 2009, 2011, Oracle and/or its affiliates. All rights reserved., Inc. All rights reserved.
 U.S. Government Rights - Commercial software. Government users are subject
 to the Sun Microsystems, Inc. standard license agreement and applicable
 provisions of the FAR and its supplements. Use is subject to license terms.
 This distribution may include materials developed by third parties.Sun,
 Sun Microsystems, the Sun logo and MySQL Enterprise Monitor are
 trademarks or registered trademarks of Sun Microsystems, Inc. in the U.S.
 and other countries.

 Copyright (c) 2009, 2010, Oracle and/or its affiliates. All rights reserved., Inc. Tous droits réservés.
 L'utilisation est soumise aux termes du contrat de licence.Cette
 distribution peut comprendre des composants développés par des tierces
 parties.Sun, Sun Microsystems,  le logo Sun et  MySQL Enterprise Monitor sont
 des marques de fabrique ou des marques déposées de Sun Microsystems, Inc.
 aux Etats-Unis et dans du'autres pays.

 */

package com.mysql.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Properties;

public class ReflectiveStatementInterceptorAdapter implements
		StatementInterceptorV2 {

	private final StatementInterceptor toProxy;
	
	final Method v2PostProcessMethod;

	public ReflectiveStatementInterceptorAdapter(StatementInterceptor toProxy) {
		this.toProxy = toProxy;
		this.v2PostProcessMethod = getV2PostProcessMethod(toProxy.getClass());
	}
	
	public void destroy() {
		toProxy.destroy();
	}

	public boolean executeTopLevelOnly() {
		return toProxy.executeTopLevelOnly();
	}

	public void init(Connection conn, Properties props) throws SQLException {
		toProxy.init(conn, props);
	}

	public ResultSetInternalMethods postProcess(String sql,
			Statement interceptedStatement,
			ResultSetInternalMethods originalResultSet, Connection connection,
			int warningCount, boolean noIndexUsed, boolean noGoodIndexUsed, 
			SQLException statementException) throws SQLException {
		// TODO Auto-generated method stub
		try {
			return (ResultSetInternalMethods) v2PostProcessMethod.invoke(toProxy, new Object[] {sql,
				interceptedStatement,
				originalResultSet, connection,
				Integer.valueOf(warningCount), noIndexUsed ? Boolean.TRUE : Boolean.FALSE, noGoodIndexUsed ? Boolean.TRUE: Boolean.FALSE,
						statementException});
		} catch (IllegalArgumentException e) {
			SQLException sqlEx = new SQLException("Unable to reflectively invoke interceptor");
			sqlEx.initCause(e);
			
			throw sqlEx;
		} catch (IllegalAccessException e) {
			SQLException sqlEx = new SQLException("Unable to reflectively invoke interceptor");
			sqlEx.initCause(e);
			
			throw sqlEx;
		} catch (InvocationTargetException e) {
			SQLException sqlEx = new SQLException("Unable to reflectively invoke interceptor");
			sqlEx.initCause(e);
			
			throw sqlEx;
		}
	}

	public ResultSetInternalMethods preProcess(String sql,
			Statement interceptedStatement, Connection connection)
			throws SQLException {
		return toProxy.preProcess(sql, interceptedStatement, connection);
	}

	public static final Method getV2PostProcessMethod(Class toProxyClass) {
		try {
			Method postProcessMethod = toProxyClass.getMethod("postProcess", new Class[] {
					String.class, Statement.class, ResultSetInternalMethods.class, Connection.class, Integer.TYPE, 
					Boolean.TYPE, Boolean.TYPE, SQLException.class});
			
			return postProcessMethod;
		} catch (SecurityException e) {
			return null;
		} catch (NoSuchMethodException e) {
			return null;
		}
	}
}
