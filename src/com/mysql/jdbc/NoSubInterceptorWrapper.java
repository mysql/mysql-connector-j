/*
 Copyright (c) 2009, 2010, Oracle and/or its affiliates. All rights reserved., Inc. All rights reserved.
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

import java.sql.SQLException;
import java.util.Properties;

/**
 * Wraps statement interceptors during driver startup so that they don't produce
 * different result sets than we expect.
 */
public class NoSubInterceptorWrapper implements StatementInterceptorV2 {

	private final StatementInterceptorV2 underlyingInterceptor;
	
	public NoSubInterceptorWrapper(StatementInterceptorV2 underlyingInterceptor) {
		if (underlyingInterceptor == null) {
			throw new RuntimeException("Interceptor to be wrapped can not be NULL");
		}
		
		this.underlyingInterceptor = underlyingInterceptor;
	}
	
	public void destroy() {
		underlyingInterceptor.destroy();
	}

	public boolean executeTopLevelOnly() {
		return underlyingInterceptor.executeTopLevelOnly();
	}

	public void init(Connection conn, Properties props) throws SQLException {
		underlyingInterceptor.init(conn, props);
	}

	public ResultSetInternalMethods postProcess(String sql,
			Statement interceptedStatement,
			ResultSetInternalMethods originalResultSet, Connection connection,
			int warningCount, boolean noIndexUsed, boolean noGoodIndexUsed,
			SQLException statementException) throws SQLException {
		underlyingInterceptor.postProcess(sql, interceptedStatement, originalResultSet, 
				connection, warningCount, noIndexUsed, noGoodIndexUsed, statementException);
		
		return null; // don't allow result set substitution
	}

	public ResultSetInternalMethods preProcess(String sql,
			Statement interceptedStatement, Connection connection)
			throws SQLException {
		underlyingInterceptor.preProcess(sql, interceptedStatement, connection);
		
		return null; // don't allow result set substitution
	}
	
	public StatementInterceptorV2 getUnderlyingInterceptor() {
		return underlyingInterceptor;
	}
}
