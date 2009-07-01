package com.mysql.jdbc.interceptors;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.ResultSetInternalMethods;
import com.mysql.jdbc.Statement;
import com.mysql.jdbc.StatementInterceptor;

public class SessionAssociationInterceptor implements StatementInterceptor {

	protected String currentSessionKey;
	protected static ThreadLocal sessionLocal = new ThreadLocal();
	
	public static final void setSessionKey(String key) {
		sessionLocal.set(key);
	}
	
	public static final void resetSessionKey() {
		sessionLocal.set(null);
	}
	
	public static final String getSessionKey() {
		return (String)sessionLocal.get();
	}
	
	public boolean executeTopLevelOnly() {
		return true;
	}

	public void init(Connection conn, Properties props) throws SQLException {

	}

	public ResultSetInternalMethods postProcess(String sql,
			Statement interceptedStatement,
			ResultSetInternalMethods originalResultSet, Connection connection)
			throws SQLException {
		return null;
	}

	public ResultSetInternalMethods preProcess(String sql,
			Statement interceptedStatement, Connection connection)
			throws SQLException {
		String key = getSessionKey();
		
		if (key != null && !key.equals(this.currentSessionKey)) {
			PreparedStatement pstmt = connection.clientPrepareStatement("SET @mysql_proxy_session=?");
			
			try {
				pstmt.setString(1, key);
				pstmt.execute();
			} finally {
				pstmt.close();
			}
			
			this.currentSessionKey = key;
		}
		
		return null;
	}

	public void destroy() {
		
	}
}