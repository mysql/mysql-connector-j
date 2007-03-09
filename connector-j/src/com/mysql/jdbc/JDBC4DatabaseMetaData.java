/*
 Copyright (C) 2002-2007 MySQL AB

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

 */

package com.mysql.jdbc;

import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.DatabaseMetaData;
import com.mysql.jdbc.Field;
import com.mysql.jdbc.exceptions.NotYetImplementedException;

public class JDBC4DatabaseMetaData extends DatabaseMetaData {
	public JDBC4DatabaseMetaData(Connection connToSet, String databaseToSet) {
		super(connToSet, databaseToSet);
		// TODO Auto-generated constructor stub
	}

	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		return false;
	}

	public ResultSet getClientInfoProperties() throws SQLException {
		throw new NotYetImplementedException();
	}

	public ResultSet getFunctionParameters(String catalog, String schemaPattern, String functionNamePattern, String parameterNamePattern) throws SQLException {
		throw new NotYetImplementedException();
	}

    /**
     * Retrieves a description of the  system and user functions available 
     * in the given catalog.
     * <P>
     * Only system and user function descriptions matching the schema and
     * function name criteria are returned.  They are ordered by
     * <code>FUNCTION_CAT</code>, <code>FUNCTION_SCHEM</code>,
     * <code>FUNCTION_NAME</code> and 
     * <code>SPECIFIC_ NAME</code>.
     *
     * <P>Each function description has the the following columns:
     *  <OL>
     *	<LI><B>FUNCTION_CAT</B> String => function catalog (may be <code>null</code>)
     *	<LI><B>FUNCTION_SCHEM</B> String => function schema (may be <code>null</code>)
     *	<LI><B>FUNCTION_NAME</B> String => function name.  This is the name 
     * used to invoke the function
     *	<LI><B>REMARKS</B> String => explanatory comment on the function
     * <LI><B>FUNCTION_TYPE</B> short => kind of function:
     *      <UL>
     *      <LI>functionResultUnknown - Cannot determine if a return value
     *       or table will be returned
     *      <LI> functionNoTable- Does not return a table
     *      <LI> functionReturnsTable - Returns a table
     *      </UL>
     *	<LI><B>SPECIFIC_NAME</B> String  => the name which uniquely identifies 
     *  this function within its schema.  This is a user specified, or DBMS
     * generated, name that may be different then the <code>FUNCTION_NAME</code> 
     * for example with overload functions
     *  </OL>
     * <p>
     * A user may not have permission to execute any of the functions that are
     * returned by <code>getFunctions</code>
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param functionNamePattern a function name pattern; must match the
     *        function name as it is stored in the database 
     * @return <code>ResultSet</code> - each row is a function description 
     * @exception SQLException if a database access error occurs
     * @see #getSearchStringEscape 
     * @since 1.6
     */
	public ResultSet getFunctions(String catalog, String schemaPattern,
			String functionNamePattern) throws SQLException {

		Field[] fields = new Field[6];
		fields[0] = new Field("", "FUNCTION_CAT", Types.CHAR,
				MAX_IDENTIFIER_LENGTH);
		fields[1] = new Field("", "FUNCTION_SCHEM", Types.CHAR,
				MAX_IDENTIFIER_LENGTH);
		fields[2] = new Field("", "FUNCTION_NAME", Types.CHAR,
				MAX_IDENTIFIER_LENGTH);
		fields[3] = new Field("", "REMARKS", Types.CHAR, MAX_IDENTIFIER_LENGTH);
		fields[4] = new Field("", "FUNCTION_TYPE", Types.SMALLINT, 0);
		fields[5] = new Field("", "SPECIFIC_NAME", Types.CHAR,
				MAX_IDENTIFIER_LENGTH);

		return getProceduresAndOrFunctions(fields, catalog, schemaPattern,
				functionNamePattern, false, true);

	}

	public RowIdLifetime getRowIdLifetime() throws SQLException {
		throw new NotYetImplementedException();
	}

	public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
		throw new NotYetImplementedException();
	}

	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		return true;
	}

	public boolean isWrapperFor(Class arg0) throws SQLException {
		throw new NotYetImplementedException();
	}

	public Object unwrap(Class arg0) throws SQLException {
		throw new NotYetImplementedException();
	}

	public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
		throw new NotYetImplementedException();
	}

	public boolean providesQueryObjectGenerator() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	protected int getJDBC4FunctionNoTableConstant() {
		return functionNoTable;
	}
}
