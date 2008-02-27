package com.mysql.jdbc;

import java.io.Reader;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;

import com.mysql.jdbc.PreparedStatement;


public class JDBC4PreparedStatementHelper {
	private JDBC4PreparedStatementHelper() {
		
	}
	
	static void setRowId(PreparedStatement pstmt, int parameterIndex, RowId x) throws SQLException {
		throw SQLError.notImplemented();
	}


	/**
	 * JDBC 4.0 Set a NCLOB parameter.
	 * 
	 * @param i
	 *            the first parameter is 1, the second is 2, ...
	 * @param x
	 *            an object representing a NCLOB
	 * 
	 * @throws SQLException
	 *             if a database error occurs
	 */
	static void setNClob(PreparedStatement pstmt, int parameterIndex, NClob value) throws SQLException {
		if (value == null) {
			pstmt.setNull(parameterIndex, java.sql.Types.NCLOB);
	    } else {
	    	pstmt.setNCharacterStream(parameterIndex, value.getCharacterStream(), value.length());
	    }
	}

	static void setNClob(PreparedStatement pstmt, int parameterIndex, Reader reader) throws SQLException {
		pstmt.setNCharacterStream(parameterIndex, reader);
	}

	/**
	 * JDBC 4.0 Set a NCLOB parameter.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1, the second is 2, ...
	 * @param reader
	 *            the java reader which contains the UNICODE data
	 * @param length
	 *            the number of characters in the stream
	 * 
	 * @throws SQLException
	 *             if a database error occurs
	 */
	static void setNClob(PreparedStatement pstmt, int parameterIndex, Reader reader, long length)
			throws SQLException {
	    if (reader == null) {
	    	pstmt.setNull(parameterIndex, java.sql.Types.NCLOB);
	    } else {
	    	pstmt.setNCharacterStream(parameterIndex, reader, length);
	    }
	}

	static void setSQLXML(PreparedStatement pstmt, int parameterIndex, SQLXML xmlObject)
			throws SQLException {
		if (xmlObject == null) {
			pstmt.setNull(parameterIndex, Types.SQLXML);
		} else {
			// FIXME: Won't work for Non-MYSQL SQLXMLs
			pstmt.setCharacterStream(parameterIndex, ((JDBC4MysqlSQLXML)xmlObject).serializeAsCharacterStream());	
		}
	}
}
